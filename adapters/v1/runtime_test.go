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

package adapters

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func testRuntime(t testing.TB, options Options) *Runtime {
	t.Helper()
	runtime, err := New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error("runtime cleanup", err)
		}
	})
	return runtime
}
func testEndpoint[T any](t testing.TB, runtime *Runtime, copy func(T) T, capacity int) (Endpoint[T], *Inbox[T]) {
	t.Helper()
	inbox, err := NewInbox[T](EvidenceOptions{Capacity: capacity, MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := Bind(runtime, Declaration[T]{Copy: copy, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	return endpoint, inbox
}
func request(name string) Request { return Request{Operation: name, WorkBytes: 1, EvidenceBytes: 1} }
func waitReleased[T any](t testing.TB, receipt *Receipt[T]) Snapshot[T] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func receive[T any](t testing.TB, input <-chan T) T {
	t.Helper()
	select {
	case value := <-input:
		return value
	case <-time.After(time.Second):
		t.Fatal("bounded test wait expired")
	}
	var zero T
	return zero
}
func TestRuntime(t *testing.T) {
	t.Run("granted_waiter_cancel_returns_capacity", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			owner := testRuntime(t, Options{MaxActive: 1, MaxQueued: 1, MaxWorkBytes: 1})
			if err := owner.state.acquire(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancelCause(context.Background())
			done := make(chan error, 1)
			go func() { done <- owner.state.acquire(ctx, 1) }()
			synctest.Wait()
			owner.state.mu.Lock()
			owner.state.releasePermitLocked(1)
			cause := errors.New("grant canceled")
			cancel(cause)
			owner.state.mu.Unlock()
			if err := receive(t, done); !errors.Is(err, cause) {
				t.Fatal("granted wait cancellation lost")
			}
			if status, _ := owner.Inspect(); status.Active != 0 || status.Queued != 0 || status.WorkBytes != 0 {
				t.Fatal("grant/cancel leaked permit")
			}
		})
	})
	t.Run("shutdown_returns_waiter_reservations", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			owner := testRuntime(t, Options{MaxActive: 1, MaxQueued: 1})
			endpoint, inbox := testEndpoint(t, owner, func(value int) int { return value }, 2)
			var guard Guard
			_, _ = endpoint.Run(context.Background(), request("live"), func(call *Call[int]) { guard, _ = call.Hold() })
			done := make(chan error, 1)
			go func() {
				_, err := endpoint.Run(context.Background(), request("queued"), func(*Call[int]) { t.Error("closed waiter dispatched") })
				done <- err
			}()
			synctest.Wait()
			wait, cancel := context.WithCancel(context.Background())
			cancel()
			if !errors.Is(owner.Close(wait), ErrWait) {
				t.Fatal("close released live work")
			}
			if err := receive(t, done); !errors.Is(err, ErrClosed) {
				t.Fatal("waiter not rejected on shutdown")
			}
			if status, _ := inbox.Inspect(); status.Reserved != 0 || status.Outstanding != 1 {
				t.Fatal("shutdown leaked admission reservation")
			}
			_ = guard.Release()
		})
	})
	t.Run("fifo_bytes_queue_cancellation_and_sealing", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			runtime := testRuntime(t, Options{MaxActive: 2, MaxQueued: 2, MaxWorkBytes: 2, MaxQueuedBytes: 4})
			endpoint, inbox := testEndpoint(t, runtime, func(value int) int { return value }, 8)
			var guard Guard
			first, err := endpoint.Run(context.Background(), request("first"), func(call *Call[int]) {
				guard, _ = call.Hold()
				_ = call.Resolve(Outcome[int]{Value: 1, Present: true})
			})
			if err != nil {
				t.Fatal(err)
			}
			defer guard.Release()
			secondContext, cancel := context.WithCancelCause(context.Background())
			secondDone := make(chan error, 1)
			large := request("large")
			large.WorkBytes = 2
			go func() {
				_, err := endpoint.Run(secondContext, large, func(*Call[int]) { t.Error("canceled queued producer dispatched") })
				secondDone <- err
			}()
			synctest.Wait()
			thirdDone := make(chan *Receipt[int], 1)
			go func() {
				value, err := endpoint.Run(context.Background(), request("third"), func(call *Call[int]) { _ = call.Resolve(Outcome[int]{Value: 3, Present: true}) })
				if err != nil {
					t.Error(err)
				}
				thirdDone <- value
			}()
			synctest.Wait()
			stats, _ := runtime.Inspect()
			if stats.Active != 1 || stats.Queued != 2 || stats.QueuedBytes != 3 {
				t.Fatal("FIFO or byte admission changed")
			}
			cause := errors.New("queue cancellation")
			cancel(cause)
			if !errors.Is(receive(t, secondDone), cause) {
				t.Fatal("queued cancellation cause lost")
			}
			third := receive(t, thirdDone)
			if value, _ := waitReleased(t, third).ValueCopy(); value != 3 {
				t.Fatal("head cancellation stranded capacity")
			}
			evidence, _ := inbox.Inspect()
			if evidence.Outstanding != 2 || evidence.Reserved != 0 {
				t.Fatal("rejected queue reservation leaked")
			}
			_ = guard.Release()
			_ = waitReleased(t, first)
			if runtime.Close(context.Background()) != nil {
				t.Fatal("close failed")
			}
			if _, err := endpoint.Run(context.Background(), request("closed"), func(*Call[int]) {}); !errors.Is(err, ErrClosed) {
				t.Fatal("closed runtime admitted")
			}
		})
	})
	t.Run("close_cancels_and_joins_without_discarding_evidence", func(t *testing.T) {
		runtime := testRuntime(t, Options{})
		endpoint, inbox := testEndpoint(t, runtime, func(value int) int { return value }, 2)
		entered, leave := make(chan context.Context, 1), make(chan struct{})
		receipt, err := endpoint.Start(context.Background(), request("blocked"), func(call *Call[int]) {
			entered <- call.Context()
			<-leave
			_ = call.Resolve(Outcome[int]{Value: 5, Present: true})
		})
		if err != nil {
			t.Fatal(err)
		}
		work := receive(t, entered)
		wait, cancel := context.WithCancel(context.Background())
		cancel()
		if !errors.Is(runtime.Close(wait), ErrWait) || work.Err() == nil {
			t.Fatal("close detached work or did not request stop")
		}
		close(leave)
		if runtime.Close(context.Background()) != nil {
			t.Fatal("owner could not join")
		}
		if value, _ := waitReleased(t, receipt).ValueCopy(); value != 5 {
			t.Fatal("actual late outcome erased")
		}
		status, _ := inbox.Inspect()
		if status.Outstanding != 1 {
			t.Fatal("close discarded required evidence")
		}
	})
	t.Run("parent_cancellation_fences_dispatch_before_callback", func(t *testing.T) {
		parent, cancel := context.WithCancelCause(context.Background())
		runtime, err := New(parent, Options{})
		if err != nil {
			t.Fatal(err)
		}
		endpoint, _ := testEndpoint(t, runtime, func(value int) int { return value }, 2)
		call, err := endpoint.begin(context.Background(), request("pending"), Scope{})
		if err != nil {
			t.Fatal(err)
		}
		runtime.state.mu.Lock()
		cancel(errors.New("parent stopped"))
		runtime.state.mu.Unlock()
		called := false
		call.dispatch(func(*Call[int]) { called = true })
		if called || !errors.Is(waitReleased(t, call.Receipt()).Err(), ErrClosed) {
			t.Fatal("known-canceled producer dispatched")
		}
		if runtime.Close(context.Background()) != nil {
			t.Fatal("parent cancellation lost owner")
		}
	})
}
