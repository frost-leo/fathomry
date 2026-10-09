/**
 * fathomry
 * Copyright (C) 2026  Frost Leo
 * SPDX-License-Identifier: GPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package otel

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/conformance"
)

func exportTestEndpoint(t *testing.T, capacity int) (*adapters.Runtime, *adapters.Inbox[Result], adapters.Endpoint[Result]) {
	t.Helper()
	runtime, err := adapters.New(context.Background(), adapters.Options{MaxActive: 2})
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](adapters.EvidenceOptions{Capacity: capacity})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := adapters.Bind(runtime, adapters.Declaration[Result]{Evidence: inbox, Copy: func(value Result) Result { return value }})
	if err != nil {
		t.Fatal(err)
	}
	return runtime, inbox, endpoint
}

func exportTestLoop(options ExportOptions, flush func(context.Context) (*adapters.Receipt[Result], error)) *ExportLoop {
	ctx, cancel := context.WithCancel(context.Background())
	state := &exportState{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	go state.run(options, flush)
	return &ExportLoop{state: state}
}

func ackExportEvidence(t *testing.T, inbox *adapters.Inbox[Result]) {
	t.Helper()
	delivery, err := inbox.NextReleased(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestExportLoopStopJoinsActualAcceptedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runtime, inbox, endpoint := exportTestEndpoint(t, 4)
		entered := make(chan struct{})
		var retained adapters.Guard
		var attempts atomic.Uint64
		loop := exportTestLoop(ExportOptions{Interval: time.Second, Timeout: time.Second},
			func(ctx context.Context) (*adapters.Receipt[Result], error) {
				attempts.Add(1)
				return endpoint.Run(ctx, adapters.Request{Operation: "test.export", WorkBytes: 1, EvidenceBytes: 1},
					func(call *adapters.Call[Result]) {
						var err error
						retained, err = call.Hold()
						if err != nil {
							t.Error(err)
						}
						_ = call.Resolve(adapters.Outcome[Result]{})
						close(entered)
					})
			})
		<-entered
		defer func() {
			_ = retained.Release()
			_ = loop.Stop(context.Background())
			_ = runtime.Close(context.Background())
		}()
		wait, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := loop.Stop(wait); !errors.Is(err, adapters.ErrWait) {
			t.Fatal("stop confirmed unjoined native ownership", err)
		}
		status, err := loop.Status()
		if err != nil || !status.Running || status.Stopped || attempts.Load() != 1 {
			t.Fatal("canceled export overlapped or was falsely joined", status, err)
		}
		usage, err := runtime.Inspect()
		if err != nil || usage.Active != 1 {
			t.Fatal("cancellation returned active ownership", usage, err)
		}
		if err := retained.Release(); err != nil {
			t.Fatal(err)
		}
		if err := loop.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		status, err = loop.Status()
		if err != nil || !status.Stopped || status.Running || status.Attempts != 1 || status.Failures != 1 ||
			status.Skipped != 2 || !errors.Is(status.LastError, context.Canceled) {
			t.Fatal("join or skipped/failure facts lost", status, err)
		}
		ackExportEvidence(t, inbox)
	})
}

func TestExportLoopCoalescesTicksAndRetainsFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runtime, inbox, endpoint := exportTestEndpoint(t, 4)
		defer func() { _ = runtime.Close(context.Background()) }()
		failure := fail(ErrExport, "test-export", errors.New("private-export-canary"))
		second := make(chan struct{})
		attempts := 0
		loop := exportTestLoop(ExportOptions{Interval: time.Second, Timeout: time.Minute},
			func(ctx context.Context) (*adapters.Receipt[Result], error) {
				attempts++
				if attempts == 1 {
					time.Sleep(2500 * time.Millisecond)
					return nil, failure
				}
				receipt, err := endpoint.Run(ctx, adapters.Request{Operation: "test.export", WorkBytes: 1, EvidenceBytes: 1},
					func(call *adapters.Call[Result]) { _ = call.Resolve(adapters.Outcome[Result]{}) })
				if attempts == 2 {
					close(second)
				}
				return receipt, err
			})
		defer func() { _ = loop.Stop(context.Background()) }()
		<-second
		synctest.Wait()
		status, err := loop.Status()
		if err != nil || status.Attempts != 2 || status.Succeeded != 1 || status.Failures != 1 ||
			status.Skipped != 2 || !errors.Is(status.LastError, failure) {
			t.Fatal("slow export was replayed or failure history erased", status, err)
		}
		conformance.Private(t, status, "private-export-canary")
		conformance.Private(t, status.LastError, "private-export-canary")
		ackExportEvidence(t, inbox)
	})
}

func TestExportLoopEvidenceRefusalIsObservableAndRecoverable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runtime, inbox, endpoint := exportTestEndpoint(t, 1)
		defer func() { _ = runtime.Close(context.Background()) }()
		loop := exportTestLoop(ExportOptions{Interval: time.Second, Timeout: time.Second},
			func(ctx context.Context) (*adapters.Receipt[Result], error) {
				return endpoint.Run(ctx, adapters.Request{Operation: "test.export", WorkBytes: 1, EvidenceBytes: 1},
					func(call *adapters.Call[Result]) { _ = call.Resolve(adapters.Outcome[Result]{}) })
			})
		defer func() { _ = loop.Stop(context.Background()) }()
		time.Sleep(time.Second)
		synctest.Wait()
		status, _ := loop.Status()
		if status.Attempts != 1 || status.Succeeded != 1 {
			t.Fatal("first export failed", status)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		status, _ = loop.Status()
		if status.Attempts != 2 || status.Failures != 1 || !errors.Is(status.LastError, adapters.ErrEvidence) {
			t.Fatal("full evidence inbox refusal was hidden", status)
		}
		ackExportEvidence(t, inbox)
		time.Sleep(time.Second)
		synctest.Wait()
		status, _ = loop.Status()
		if status.Attempts != 3 || status.Succeeded != 2 || status.Failures != 1 || !errors.Is(status.LastError, adapters.ErrEvidence) {
			t.Fatal("later independent export did not progress or erased refusal", status)
		}
		ackExportEvidence(t, inbox)
	})
}

func TestExportLoopBoundsAndInvalidHandles(t *testing.T) {
	if exportCount(math.MaxUint64-1, 2) != math.MaxUint64 || exportCount(0, 2) != 2 {
		t.Fatal("counter wrapped")
	}
	var loop *ExportLoop
	if _, err := loop.Status(); !errors.Is(err, ErrState) {
		t.Fatal("nil loop status admitted", err)
	}
	if err := loop.Stop(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal("nil loop stop admitted", err)
	}
	var owner *Owner
	if _, err := owner.StartExport(context.Background(), ExportOptions{Interval: time.Second, Timeout: time.Second}); !errors.Is(err, ErrInput) {
		t.Fatal("nil source admitted", err)
	}
}
