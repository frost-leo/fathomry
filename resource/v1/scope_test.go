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

package resource

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func FuzzLifecycleFailures(f *testing.F) {
	f.Add([]byte{1, 6, 12, 3, 18, 1, 4, 2})
	f.Add([]byte{0, 1, 1, 4, 5, 2, 2})
	f.Add([]byte{1, 12, 18, 0, 3, 3, 4})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 || len(data) > 32 {
			return
		}
		synctest.Test(t, func(t *testing.T) {
			type object struct {
				closed   atomic.Bool
				attempts atomic.Int32
			}
			scope, err := New(context.Background(), Options{})
			if err != nil {
				t.Fatal(err)
			}
			var acquired, completed atomic.Int32
			var leases []Lease[*object]
			defer func() {
				for _, lease := range leases {
					_ = lease.Release()
				}
				for range 8 {
					wait, cancel := context.WithTimeout(context.Background(), time.Second)
					err := scope.Close(wait)
					cancel()
					synctest.Wait()
					if err == nil {
						if acquired.Load() != completed.Load() {
							t.Error("native acquisitions were discarded")
						}
						return
					}
					if !errors.Is(err, ErrCleanup) {
						t.Fatal("shutdown could not join actual work", err)
					}
				}
				t.Fatal("cleanup continuation did not converge")
			}()
			base := testBinding("faults", Policy(data[0]%2))
			ref, err := Bind(scope, Binding[int, *object]{
				Name: base.Name, Policy: base.Policy, Select: base.Select, Clone: base.Clone, Equal: base.Equal,
				MaxGenerations: 3, MaxBorrowers: 4,
				Build: func(_ context.Context, config int) (*Instance[*object], error) {
					refusal := errors.New("native construction refusal")
					if config == 1 {
						return nil, refusal
					}
					value := &object{}
					acquired.Add(1)
					instance := &Instance[*object]{Value: value, Release: func(context.Context) ReleaseResult {
						if value.attempts.Add(1) == 1 {
							return ReleaseResult{}
						}
						if value.closed.Swap(true) {
							t.Error("completed native cleanup repeated")
						}
						completed.Add(1)
						return ReleaseResult{Complete: true}
					}}
					if config == 2 {
						return instance, refusal
					}
					return instance, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			latest := applyTest(t, scope, 0)
			waitTest(t, latest)
			closing := false
			for _, action := range data[1:] {
				switch action % 6 {
				case 0:
					update, err := scope.Apply(context.Background(), testView(int(action/6)%4))
					if closing {
						if !errors.Is(err, ErrClosed) {
							t.Fatal("closed scope admitted input")
						}
					} else if err != nil {
						t.Fatal(err)
					} else {
						latest = update
					}
				case 1:
					lease, err := ref.Acquire(context.Background())
					if closing {
						if !errors.Is(err, ErrClosed) {
							t.Fatal("closed scope admitted a borrow")
						}
					} else if errors.Is(err, ErrLimit) {
						if len(leases) != 4 {
							t.Fatal("false borrower saturation")
						}
					} else if err != nil {
						t.Fatal(err)
					} else {
						leases = append(leases, lease)
					}
				case 2:
					if len(leases) > 0 {
						if err := leases[0].Release(); err != nil {
							t.Fatal(err)
						}
						leases = leases[1:]
					}
				case 3:
					update, err := ref.Retry(context.Background())
					if closing {
						if !errors.Is(err, ErrClosed) {
							t.Fatal("closed scope admitted a retry")
						}
					} else if err != nil {
						t.Fatal(err)
					} else {
						latest = update
					}
				case 4:
					closing = true
					wait, cancel := context.WithCancel(context.Background())
					cancel()
					err := scope.Close(wait)
					if err != nil && !errors.Is(err, ErrWait) && !errors.Is(err, ErrCleanup) {
						t.Fatal(err)
					}
				case 5:
					wait, cancel := context.WithCancel(context.Background())
					cancel()
					err := latest.Wait(wait)
					if err != nil && !errors.Is(err, ErrWait) && !errors.Is(err, ErrBuild) &&
						!errors.Is(err, ErrClosed) && !errors.Is(err, ErrSuperseded) {
						t.Fatal(err)
					}
				}
				synctest.Wait()
				for _, lease := range leases {
					value, err := lease.Value()
					if err != nil || value.closed.Load() {
						t.Fatal("live borrow was cleaned")
					}
				}
				status, _ := ref.Inspect()
				owned := status.Retiring
				if status.Active {
					owned++
				}
				if status.Preparing {
					t.Fatal("instant constructor remained detached")
				}
				if owned > 3 || int(acquired.Load()-completed.Load()) != owned {
					t.Fatal("native ownership count differs from retained generations")
				}
			}
		})
	})
}

func TestScope(t *testing.T) {
	t.Run("parallel_apply_retry_borrow_and_shutdown", func(t *testing.T) {
		type object struct {
			closed   atomic.Bool
			attempts atomic.Int32
		}
		scope, err := New(context.Background(), Options{})
		if err != nil {
			t.Fatal(err)
		}
		var acquired, completed atomic.Int32
		base := testBinding("parallel", Follow)
		ref, err := Bind(scope, Binding[int, *object]{
			Name: base.Name, Policy: Follow, Select: base.Select, Clone: base.Clone, Equal: base.Equal,
			MaxGenerations: 3,
			Build: func(context.Context, int) (*Instance[*object], error) {
				value := &object{}
				acquired.Add(1)
				return &Instance[*object]{Value: value, Release: func(context.Context) ReleaseResult {
					if value.attempts.Add(1) == 1 {
						return ReleaseResult{}
					}
					if value.closed.Swap(true) {
						t.Error("native cleanup completed twice")
					}
					completed.Add(1)
					return ReleaseResult{Complete: true}
				}}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		waitTest(t, applyTest(t, scope, 1))
		pin, _ := ref.Acquire(context.Background())
		t.Cleanup(func() {
			_ = pin.Release()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			for range 8 {
				if err := scope.Close(ctx); err == nil {
					if acquired.Load() != completed.Load() {
						t.Error("native ownership lost during contention")
					}
					return
				} else if !errors.Is(err, ErrCleanup) {
					t.Error("parallel shutdown did not join", err)
					return
				}
			}
			t.Error("parallel cleanup continuation did not converge")
		})
		start, overlapped := make(chan struct{}), make(chan struct{})
		var accepted atomic.Int32
		var workers sync.WaitGroup
		for actor := range 10 {
			workers.Go(func() {
				<-start
				for step := range 128 {
					var err error
					switch {
					case actor < 4:
						_, err = scope.Apply(context.Background(), testView(actor*128+step))
						if err == nil && accepted.Add(1) == 32 {
							close(overlapped)
						}
					case actor < 6:
						_, err = ref.Retry(context.Background())
					default:
						var lease Lease[*object]
						lease, err = ref.Acquire(context.Background())
						if err == nil {
							value, readErr := lease.Value()
							if readErr != nil || value.closed.Load() {
								t.Error("borrowed object was already cleaned")
							}
							if releaseErr := lease.Release(); releaseErr != nil {
								t.Error(releaseErr)
							}
						}
					}
					if errors.Is(err, ErrClosed) {
						return
					}
					if err != nil {
						t.Error("unexpected concurrent admission failure", err)
						return
					}
				}
			})
		}
		close(start)
		receiveTest(t, overlapped)
		wait, cancel := context.WithCancel(context.Background())
		cancel()
		err = scope.Close(wait)
		if !errors.Is(err, ErrWait) && !errors.Is(err, ErrCleanup) {
			t.Error("shutdown abandoned a pinned generation")
		}
		workers.Wait()
		value, err := pin.Value()
		if err != nil || value.closed.Load() {
			t.Fatal("shutdown released the remaining borrow")
		}
	})

	t.Run("invalid_handles_contexts_and_counter_boundaries", func(t *testing.T) {
		var missing *Scope
		if _, err := missing.Apply(context.Background(), testView(1)); !errors.Is(err, ErrScope) {
			t.Fatal("nil scope admitted")
		}
		if _, err := missing.Inspect(); !errors.Is(err, ErrScope) {
			t.Fatal("nil scope inspected")
		}
		if !errors.Is(missing.Close(context.Background()), ErrScope) {
			t.Fatal("nil scope closed")
		}
		var ref Ref[int]
		if _, err := ref.Acquire(context.Background()); !errors.Is(err, ErrScope) {
			t.Fatal("zero reference admitted")
		}
		if _, err := ref.Inspect(); !errors.Is(err, ErrScope) {
			t.Fatal("zero reference inspected")
		}
		if _, err := ref.Retry(context.Background()); !errors.Is(err, ErrScope) {
			t.Fatal("zero reference retried")
		}
		var lease Lease[int]
		if _, err := lease.Value(); !errors.Is(err, ErrLease) {
			t.Fatal("zero lease admitted")
		}
		if !errors.Is(lease.Release(), ErrLease) || lease.Generation() != 0 {
			t.Fatal("zero lease released")
		}
		if !errors.Is(new(Update).Wait(context.Background()), ErrScope) {
			t.Fatal("zero receipt admitted")
		}
		var watcher *Watch
		if _, err := watcher.Next(context.Background()); !errors.Is(err, ErrScope) {
			t.Fatal("zero watcher admitted")
		}
		if !errors.Is(watcher.Close(context.Background()), ErrScope) {
			t.Fatal("zero watcher closed")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := New(ctx, Options{}); !errors.Is(err, ErrClosed) {
			t.Fatal("canceled lifetime admitted")
		}
		scope := testScope(t, Options{})
		ref, err := Bind(scope, testBinding("one", Follow))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ref.Retry(context.Background()); !errors.Is(err, ErrUnavailable) {
			t.Fatal("unconfigured retry admitted")
		}
		if _, err := ref.Acquire(nil); !errors.Is(err, ErrOptions) {
			t.Fatal("nil context admitted")
		}
		if _, err := ref.Acquire(ctx); !errors.Is(err, context.Canceled) {
			t.Fatal("canceled acquire admitted")
		}
		if _, err := scope.Apply(nil, testView(1)); !errors.Is(err, ErrOptions) {
			t.Fatal("nil apply context admitted")
		}
		if !errors.Is(scope.Close(nil), ErrOptions) {
			t.Fatal("nil close context admitted")
		}
		waitTest(t, applyTest(t, scope, 1))
		entry := ref.state.(*binding[int, int])
		entry.mu.Lock()
		entry.generation = math.MaxUint64
		entry.mu.Unlock()
		if !errors.Is(applyTest(t, scope, 2).Wait(context.Background()), ErrLimit) {
			t.Fatal("construction generation wrapped")
		}
		entry.mu.Lock()
		entry.version = math.MaxUint64
		entry.mu.Unlock()
		if _, err := scope.Apply(context.Background(), testView(3)); !errors.Is(err, ErrLimit) {
			t.Fatal("target counter wrapped")
		}
		if _, err := ref.Retry(context.Background()); !errors.Is(err, ErrLimit) {
			t.Fatal("retry counter wrapped")
		}
		scope.state.mu.Lock()
		scope.state.sequence = math.MaxUint64
		scope.state.mu.Unlock()
		if _, err := scope.Apply(context.Background(), testView(2)); !errors.Is(err, ErrLimit) {
			t.Fatal("scope sequence wrapped")
		}
		if readTest(t, ref) != 1 {
			t.Fatal("overflow changed active resource")
		}
	})
	t.Run("admission_and_sealing", func(t *testing.T) {
		for _, options := range []Options{{MaxBindings: -1}, {MaxPreparing: 65}, {CleanupTimeout: -1}, {Name: "private/path"}} {
			if scope, err := New(context.Background(), options); scope != nil || !errors.Is(err, ErrOptions) {
				t.Fatal("invalid scope options admitted")
			}
		}
		if _, err := New(nil, Options{}); !errors.Is(err, ErrOptions) {
			t.Fatal("nil lifetime admitted")
		}
		scope := testScope(t, Options{MaxBindings: 1})
		ref, err := Bind(scope, testBinding("one", Fixed))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Bind(scope, testBinding("one", Fixed)); !errors.Is(err, ErrOptions) {
			t.Fatal("duplicate identity admitted")
		}
		if _, err := Bind(scope, testBinding("two", Fixed)); !errors.Is(err, ErrLimit) {
			t.Fatal("binding bound ignored")
		}
		if _, err := scope.Apply(context.Background(), settings.View{}); !errors.Is(err, ErrSelection) {
			t.Fatal("zero view admitted")
		}
		waitTest(t, applyTest(t, scope, 0))
		if readTest(t, ref) != 0 {
			t.Fatal("legitimate zero instance rejected")
		}
		if _, err := Bind(scope, testBinding("two", Fixed)); !errors.Is(err, ErrSealed) {
			t.Fatal("declaration after Apply admitted")
		}
	})

	t.Run("canceled_selection_does_not_commit_and_close_joins_it", func(t *testing.T) {
		scope, _ := New(context.Background(), Options{})
		entered, release := make(chan struct{}), make(chan struct{})
		var built atomic.Int32
		options := testBinding("one", Follow)
		options.Select = func(settings.View) (int, error) { close(entered); <-release; return 7, nil }
		options.Build = func(context.Context, int) (*Instance[int], error) { built.Add(1); return &Instance[int]{}, nil }
		_, _ = Bind(scope, options)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { _, err := scope.Apply(ctx, testView(7)); result <- err }()
		receiveTest(t, entered)
		cancel()
		wait, end := context.WithTimeout(context.Background(), 15*time.Millisecond)
		defer end()
		if !errors.Is(scope.Close(wait), ErrWait) {
			t.Fatal("unfinished selector was abandoned")
		}
		close(release)
		if err := receiveTest(t, result); !errors.Is(err, ErrWait) && !errors.Is(err, ErrClosed) {
			t.Fatal("cancellation lost", err)
		}
		if err := scope.Close(context.Background()); err != nil || built.Load() != 0 {
			t.Fatal("canceled selection constructed", err)
		}
	})

	t.Run("partial_build_and_cleanup_continuation", func(t *testing.T) {
		scope, _ := New(context.Background(), Options{})
		refusal, pending := errors.New("construction evidence"), errors.New("cleanup evidence")
		var attempts atomic.Int32
		cleanup := make(chan struct{}, 2)
		options := testBinding("partial", Follow)
		options.Build = func(context.Context, int) (*Instance[int], error) {
			return &Instance[int]{Value: 7, Release: func(context.Context) ReleaseResult {
				defer func() { cleanup <- struct{}{} }()
				if attempts.Add(1) == 1 {
					return ReleaseResult{Err: pending}
				}
				return ReleaseResult{Complete: true}
			}}, refusal
		}
		ref, _ := Bind(scope, options)
		if err := applyTest(t, scope, 1).Wait(context.Background()); !errors.Is(err, refusal) {
			t.Fatal("partial acquisition failure lost")
		}
		receiveTest(t, cleanup)
		statusTest(t, scope, ref, func(status Status) bool { return status.PendingCleanup == 1 })
		// Close itself is an explicit cleanup continuation.
		if err := scope.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if attempts.Load() != 2 {
			t.Fatal("cleanup continuation absent")
		}
		if _, err := ref.Acquire(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatal("partially built value escaped")
		}
	})

	t.Run("close_reports_incomplete_without_discarding_owner", func(t *testing.T) {
		scope, _ := New(context.Background(), Options{})
		var finish atomic.Bool
		native := errors.New("native cleanup pending")
		options := testBinding("one", Fixed)
		options.Build = func(_ context.Context, value int) (*Instance[int], error) {
			return &Instance[int]{Value: value, Release: func(context.Context) ReleaseResult {
				return ReleaseResult{Complete: finish.Load(), Err: func() error {
					if !finish.Load() {
						return native
					}
					return nil
				}()}
			}}, nil
		}
		ref, _ := Bind(scope, options)
		waitTest(t, applyTest(t, scope, 1))
		err := scope.Close(context.Background())
		if !errors.Is(err, ErrCleanup) || !errors.Is(err, native) {
			t.Fatal("pending close not observable", err)
		}
		var detailed *failure.Detailed[Details]
		if !errors.As(err, &detailed) {
			t.Fatal("missing owner details")
		}
		details, _ := detailed.Details()
		if !details.Pending {
			t.Fatal("cleanup claimed completion")
		}
		finish.Store(true)
		if err := scope.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		status, _ := ref.Inspect()
		if !status.Closed || status.PendingCleanup != 0 {
			t.Fatal("owner could not join")
		}
	})

	t.Run("late_construction_after_shutdown_is_owned", func(t *testing.T) {
		scope, _ := New(context.Background(), Options{})
		started, finish, cleaned := make(chan struct{}), make(chan struct{}), make(chan struct{})
		options := testBinding("one", Follow)
		options.Build = func(context.Context, int) (*Instance[int], error) {
			close(started)
			<-finish
			return &Instance[int]{Value: 9, Release: func(context.Context) ReleaseResult { close(cleaned); return ReleaseResult{Complete: true} }}, nil
		}
		ref, _ := Bind(scope, options)
		update := applyTest(t, scope, 1)
		receiveTest(t, started)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
		defer cancel()
		if !errors.Is(scope.Close(ctx), ErrWait) {
			t.Fatal("constructor detached from owner")
		}
		close(finish)
		if err := scope.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		receiveTest(t, cleaned)
		if !errors.Is(update.Wait(context.Background()), ErrClosed) {
			t.Fatal("late candidate published")
		}
		if _, err := ref.Acquire(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatal("late value available")
		}
	})

	t.Run("cancellation_during_build_copy_prevents_dispatch", func(t *testing.T) {
		scope, _ := New(context.Background(), Options{})
		entered, resume := make(chan struct{}), make(chan struct{})
		var copied, built atomic.Int32
		options := testBinding("one", Fixed)
		options.Clone = func(value int) int {
			if copied.Add(1) == 2 {
				close(entered)
				<-resume
			}
			return value
		}
		options.Build = func(context.Context, int) (*Instance[int], error) { built.Add(1); return &Instance[int]{}, nil }
		_, _ = Bind(scope, options)
		applyTest(t, scope, 1)
		receiveTest(t, entered)
		wait, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		if !errors.Is(scope.Close(wait), ErrWait) {
			t.Fatal("copy work was abandoned")
		}
		cancel()
		close(resume)
		if err := scope.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if built.Load() != 0 {
			t.Fatal("constructor dispatched after known cancellation during copying")
		}
	})

	t.Run("parent_cancellation_prevents_dispatch_before_shutdown_callback", func(t *testing.T) {
		parent, cancel := context.WithCancelCause(context.Background())
		scope, err := New(parent, Options{})
		if err != nil {
			t.Fatal(err)
		}
		entered, resume := make(chan struct{}), make(chan struct{})
		var copied, built atomic.Int32
		options := testBinding("one", Fixed)
		options.Clone = func(value int) int {
			if copied.Add(1) == 2 {
				close(entered)
				<-resume
			}
			return value
		}
		options.Build = func(context.Context, int) (*Instance[int], error) {
			built.Add(1)
			return &Instance[int]{}, nil
		}
		if _, err := Bind(scope, options); err != nil {
			t.Fatal(err)
		}
		update := applyTest(t, scope, 1)
		receiveTest(t, entered)
		native := errors.New("parent stopped before dispatch")
		// Hold shutdown's state lock to make AfterFunc scheduling lag deterministic.
		scope.state.mu.Lock()
		cancel(native)
		close(resume)
		wait, end := context.WithTimeout(context.Background(), time.Second)
		err = update.Wait(wait)
		end()
		scope.state.mu.Unlock()
		if closeErr := scope.Close(context.Background()); closeErr != nil {
			t.Fatal(closeErr)
		}
		if !errors.Is(err, ErrClosed) || !errors.Is(err, native) {
			t.Fatal("parent cancellation evidence lost", err)
		}
		if built.Load() != 0 {
			t.Fatal("constructor dispatched after parent cancellation while shutdown callback lagged")
		}
	})

	t.Run("timed_out_cleanup_callback_remains_owned_and_serialized", func(t *testing.T) {
		scope, _ := New(context.Background(), Options{CleanupTimeout: time.Millisecond})
		entered, finish := make(chan context.Context, 1), make(chan struct{})
		var calls atomic.Int32
		options := testBinding("one", Fixed)
		options.Build = func(context.Context, int) (*Instance[int], error) {
			return &Instance[int]{Release: func(ctx context.Context) ReleaseResult {
				calls.Add(1)
				entered <- ctx
				<-finish
				return ReleaseResult{Complete: true}
			}}, nil
		}
		_, _ = Bind(scope, options)
		waitTest(t, applyTest(t, scope, 1))
		wait, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		if err := scope.Close(wait); !errors.Is(err, ErrWait) {
			t.Fatal("unfinished native cleanup was detached", err)
		}
		cancel()
		cleanupContext := receiveTest(t, entered)
		if cleanupContext.Err() == nil {
			t.Fatal("cleanup phase budget was absent")
		}
		wait, cancel = context.WithTimeout(context.Background(), time.Millisecond)
		if err := scope.Close(wait); !errors.Is(err, ErrWait) || calls.Load() != 1 {
			t.Fatal("parallel cleanup started", err)
		}
		cancel()
		close(finish)
		if err := scope.Close(context.Background()); err != nil || calls.Load() != 1 {
			t.Fatal("cleanup join did not finish", err)
		}
	})

	t.Run("completed_cleanup_error_is_retained_without_retry", func(t *testing.T) {
		scope, _ := New(context.Background(), Options{})
		native := errors.New("completed close error")
		var calls atomic.Int32
		options := testBinding("one", Fixed)
		options.Build = func(context.Context, int) (*Instance[int], error) {
			return &Instance[int]{Release: func(context.Context) ReleaseResult {
				calls.Add(1)
				return ReleaseResult{Complete: true, Err: native}
			}}, nil
		}
		ref, _ := Bind(scope, options)
		waitTest(t, applyTest(t, scope, 1))
		for range 2 {
			err := scope.Close(context.Background())
			if !errors.Is(err, ErrCleanup) || !errors.Is(err, native) {
				t.Fatal("completed cleanup evidence lost", err)
			}
		}
		status, _ := ref.Inspect()
		if !status.Closed || status.PendingCleanup != 0 || calls.Load() != 1 {
			t.Fatal("completed release was retried")
		}
	})
}
