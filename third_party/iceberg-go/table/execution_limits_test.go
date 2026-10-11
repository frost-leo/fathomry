// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package table

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/config"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/metrics"
	"github.com/stretchr/testify/require"
)

func TestWorkerLimitContextIsolationAndValidation(t *testing.T) {
	before := config.EnvConfig.MaxWorkers
	first, err := WithWorkerLimit(t.Context(), 2)
	require.NoError(t, err)
	second, err := WithWorkerLimit(t.Context(), 17)
	require.NoError(t, err)
	require.Equal(t, 2, internal.ConfiguredWorkerCount(first, 0))
	require.Equal(t, 17, internal.ConfiguredWorkerCount(second, 0))
	require.Equal(t, 2, internal.WorkerCount(first, 100))
	require.Equal(t, 1, internal.WorkerCount(first, 1))
	inherited, err := WithWorkerLimit(first, 100)
	require.NoError(t, err)
	require.Equal(t, 2, internal.WorkerCount(inherited, 0))
	require.Equal(t, before, config.EnvConfig.MaxWorkers)
	require.Equal(t, before, internal.ConfiguredWorkerCount(t.Context(), 0))
	for _, invalid := range []int{0, -1} {
		_, err = WithWorkerLimit(t.Context(), invalid)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	}
	_, err = WithWorkerLimit(nil, 1)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestWorkerLimitBoundsActualMaintenancePool(t *testing.T) {
	bounded, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx, err := WithWorkerLimit(bounded, 2)
	require.NoError(t, err)
	var active, peak atomic.Int32
	entered := make(chan struct{}, 16)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := deleteFilesParallel(ctx, []string{"a", "b", "c", "d", "e", "f"}, 64, func(string) error {
			current := active.Add(1)
			defer active.Add(-1)
			for previous := peak.Load(); current > previous && !peak.CompareAndSwap(previous, current); previous = peak.Load() {
			}
			entered <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, nil)
		done <- err
	}()
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("workers did not start")
		}
	}
	close(release)
	require.NoError(t, <-done)
	require.Equal(t, int32(2), peak.Load())
	require.Zero(t, active.Load())
}

type loggerCanary struct{ calls *atomic.Int32 }

func (c loggerCanary) String() string {
	c.calls.Add(1)
	return "private-payload-canary"
}

type scopedPanicReporter struct{ canary loggerCanary }

func (r scopedPanicReporter) Report(context.Context, metrics.MetricsReport) { panic(r.canary) }
func (scopedPanicReporter) Close() error                                    { return nil }

func TestOperationLoggerOwnsReporterPanicPresentation(t *testing.T) {
	previous := slog.Default()
	var formatted atomic.Int32
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.Level(100)}))
	ctx, err := WithOperationLogger(t.Context(), logger)
	require.NoError(t, err)
	safeReport(ctx, scopedPanicReporter{canary: loggerCanary{calls: &formatted}}, nil)
	require.Zero(t, formatted.Load(), "disabled source logger must not format private native payloads")
	require.Same(t, previous, slog.Default())
	_, err = WithOperationLogger(t.Context(), nil)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

type manifestScopeGate struct {
	started   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	active    atomic.Int32
}

type scopedManifestIO struct {
	iceio.IO
	ctx  context.Context
	gate *manifestScopeGate
}

func (f *scopedManifestIO) Open(name string) (iceio.File, error) {
	if strings.HasSuffix(name, "snap-01.avro") {
		return f.IO.Open(name)
	}
	f.gate.active.Add(1)
	defer f.gate.active.Add(-1)
	f.gate.startOnce.Do(func() { close(f.gate.started) })
	<-f.ctx.Done()
	f.gate.stopOnce.Do(func() { close(f.gate.cancelled) })
	<-f.gate.release
	return nil, f.ctx.Err()
}

func TestAllManifestsIsLazyAndEarlyBreakJoinsWorkers(t *testing.T) {
	tbl, _ := tableWithManifestLists(t, 2)
	gate := &manifestScopeGate{started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(gate.release) })
	original := tbl.fsF
	var acquisitions atomic.Int32
	tbl.fsF = func(ctx context.Context) (iceio.IO, error) {
		acquisitions.Add(1)
		filesystem, err := original(ctx)
		return &scopedManifestIO{IO: filesystem, ctx: ctx, gate: gate}, err
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	seq := tbl.AllManifests(ctx)
	require.Zero(t, acquisitions.Load())
	done := make(chan error, 1)
	go func() {
		for _, err := range seq {
			if err != nil {
				done <- err
				return
			}
			select {
			case <-gate.started:
			case <-ctx.Done():
				done <- ctx.Err()
				return
			}
			break
		}
		done <- nil
	}()
	select {
	case <-gate.cancelled:
	case <-ctx.Done():
		t.Fatal("early break did not cancel active manifest work")
	}
	select {
	case <-done:
		t.Fatal("iterator returned before its producer was joined")
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(func() { close(gate.release) })
	require.NoError(t, <-done)
	require.Zero(t, gate.active.Load())
}

func TestCommitRetryLimitCapsAttemptsWithoutMetadataChanges(t *testing.T) {
	for _, limit := range []int{0, 2} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			cat := &flakyCatalog{failUntilAttempt: 100, failWith: ErrCommitFailed}
			tbl := newRetryTestTable(t, cat, iceberg.Properties{
				CommitNumRetriesKey: "4", CommitMinRetryWaitMsKey: "1", CommitMaxRetryWaitMsKey: "1",
			})
			cat.metadata = tbl.Metadata()
			ctx, err := WithCommitRetryLimit(t.Context(), limit)
			require.NoError(t, err)
			_, err = tbl.doCommit(ctx, nil, nil)
			require.ErrorIs(t, err, ErrCommitFailed)
			require.Equal(t, int32(limit+1), cat.attempts.Load())
			require.Equal(t, "4", tbl.Properties()[CommitNumRetriesKey])
		})
	}
}

func TestCommitRetryLimitValidationAndInheritance(t *testing.T) {
	ctx, err := WithCommitRetryLimit(t.Context(), 0)
	require.NoError(t, err)
	ctx, err = WithCommitRetryLimit(ctx, 10)
	require.NoError(t, err)
	require.Zero(t, limitCommitRetries(ctx, retryConfig{numRetries: 4}).numRetries)
	require.Equal(t, uint(4), limitCommitRetries(context.Background(), retryConfig{numRetries: 4}).numRetries)
	for _, invalid := range []int{-1, 1001} {
		_, err = WithCommitRetryLimit(t.Context(), invalid)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	}
	_, err = WithCommitRetryLimit(nil, 1)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}
