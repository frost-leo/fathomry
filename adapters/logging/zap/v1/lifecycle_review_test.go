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

package zap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
)

type reviewSink struct{}

func (reviewSink) Write(context.Context, Record) error { return nil }
func (reviewSink) Sync(context.Context) error          { return nil }

func reviewOwner(t *testing.T) (*Owner, *adapters.Runtime) {
	t.Helper()
	config := Settings{Name: "lifecycle-review", Version: 1, Structured: true}
	policy, err := Recommend(config)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := Open(context.Background(), config, Dependencies{Runtime: runtime, Evidence: inbox, Structured: reviewSink{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := owner.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := inbox.Seal(); err != nil {
			t.Error(err)
		}
		for {
			delivery, err := inbox.NextReleased(ctx)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
				break
			}
		}
	})
	return owner, runtime
}

type reviewOccurrence struct {
	core    *failure.Error
	once    sync.Once
	entered chan struct{}
	release <-chan struct{}
}

func (*reviewOccurrence) Error() string { panic("unsafe presentation invoked") }
func (value *reviewOccurrence) Failure() *failure.Error {
	value.once.Do(func() { close(value.entered) })
	<-value.release
	return value.core
}

func TestRetainedGatewayCancellationKeepsPreparationReservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		owner, runtime := reviewOwner(t)
		lifetime, cancel := context.WithCancel(context.Background())
		view, err := owner.Client().Retain(lifetime)
		if err != nil {
			t.Fatal(err)
		}
		handler, err := view.Slog()
		if err != nil {
			t.Fatal(err)
		}
		core, err := failure.New(logging.Definitions()[0], failure.Location{Operation: "review"})
		if err != nil {
			t.Fatal(err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		t.Cleanup(func() { once.Do(func() { close(release) }); cancel(); _ = handler.Close(context.Background()) })
		value := &reviewOccurrence{core: core, entered: entered, release: release}
		finished := make(chan struct{})
		go func() { _ = handler.WithAttrs([]slog.Attr{slog.Any("public_error", value)}); close(finished) }()
		<-entered
		before, err := runtime.Inspect()
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		synctest.Wait()
		after, err := runtime.Inspect()
		if err != nil {
			t.Fatal(err)
		}
		if view.family.complete.Load() || handler.ShutdownComplete() || after.Active != before.Active || after.WorkBytes != before.WorkBytes {
			t.Fatal("retained family reclaimed work/source ownership while gateway preparation was active")
		}
		wait, stop := context.WithTimeout(context.Background(), time.Second)
		if _, err := view.family.call.Receipt().WaitReleased(wait); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("active preparation had releasable family evidence", err)
		}
		stop()
		once.Do(func() { close(release) })
		<-finished
		synctest.Wait()
		if !handler.ShutdownComplete() || !view.family.complete.Load() {
			t.Fatal("automatic close did not join actual preparation")
		}
		after, err = runtime.Inspect()
		if err != nil || after.Active != before.Active-1 || after.WorkBytes >= before.WorkBytes {
			t.Fatal("joined family did not return its reservation", err)
		}
	})
}

func TestGatewayInstallationAndRetainedCloseAreAtomic(t *testing.T) {
	owner, _ := reviewOwner(t)
	for range 32 {
		view, err := owner.Client().Retain(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		installed := make(chan error, 1)
		closed := make(chan error, 1)
		go func() {
			<-start
			handler, err := view.Slog()
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				err = handler.Close(ctx)
			}
			installed <- err
		}()
		go func() {
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			closed <- view.Close(ctx)
		}()
		close(start)
		if err := <-installed; err != nil && !errors.Is(err, ErrState) && !errors.Is(err, logging.ErrState) {
			t.Fatal("gateway construction failed unexpectedly", err)
		}
		if err := <-closed; err != nil {
			t.Fatal("retained close failed", err)
		}
		if !view.family.complete.Load() {
			t.Fatal("retained close did not complete")
		}
		view.family.mu.Lock()
		handler := view.family.handler
		view.family.mu.Unlock()
		if handler != nil && !handler.ShutdownComplete() {
			t.Fatal("gateway was installed after bare family release")
		}
	}
}
