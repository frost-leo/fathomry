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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLease(t *testing.T) {
	t.Run("copied_release_and_shutdown_share_one_authority", func(t *testing.T) {
		scope := testScope(t, Options{})
		var cleaned, successful atomic.Int32
		options := testBinding("shared", Fixed)
		options.Build = func(context.Context, int) (*Instance[int], error) {
			return &Instance[int]{Value: 7, Release: func(context.Context) ReleaseResult {
				cleaned.Add(1)
				return ReleaseResult{Complete: true}
			}}, nil
		}
		ref, err := Bind(scope, options)
		if err != nil {
			t.Fatal(err)
		}
		waitTest(t, applyTest(t, scope, 1))
		shared, _ := ref.Acquire(context.Background())
		pinned, _ := ref.Acquire(context.Background())
		defer pinned.Release()
		start := make(chan struct{})
		var workers sync.WaitGroup
		for range 32 {
			workers.Go(func() {
				<-start
				if err := shared.Release(); err == nil {
					successful.Add(1)
				} else if !errors.Is(err, ErrLease) {
					t.Error("unexpected concurrent release failure")
				}
			})
		}
		close(start)
		wait, cancel := context.WithCancel(context.Background())
		cancel()
		if !errors.Is(scope.Close(wait), ErrWait) {
			t.Fatal("shutdown abandoned the separate pinned lease")
		}
		workers.Wait()
		status, _ := ref.Inspect()
		value, err := pinned.Value()
		if successful.Load() != 1 || status.Borrowers != 1 || cleaned.Load() != 0 || err != nil || value != 7 {
			t.Fatal("copied lease release decremented a different borrow")
		}
		if err := pinned.Release(); err != nil {
			t.Fatal(err)
		}
		if err := scope.Close(context.Background()); err != nil || cleaned.Load() != 1 {
			t.Fatal("final native cleanup did not occur exactly once", err)
		}
	})

	t.Run("old_borrow_pins_generation_and_bounds_replacement", func(t *testing.T) {
		scope := testScope(t, Options{})
		cleaned := make(chan int, 4)
		options := testBinding("instance", Follow)
		options.Build = func(_ context.Context, value int) (*Instance[int], error) {
			return &Instance[int]{Value: value, Release: func(context.Context) ReleaseResult {
				cleaned <- value
				return ReleaseResult{Complete: true}
			}}, nil
		}
		ref, _ := Bind(scope, options)
		waitTest(t, applyTest(t, scope, 1))
		old, _ := ref.Acquire(context.Background())
		waitTest(t, applyTest(t, scope, 2))
		third := applyTest(t, scope, 3)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
		defer cancel()
		if !errors.Is(third.Wait(ctx), context.DeadlineExceeded) {
			t.Fatal("retirement did not backpressure third generation")
		}
		if value, err := old.Value(); err != nil || value != 1 || readTest(t, ref) != 2 {
			t.Fatal("borrow moved across generations")
		}
		select {
		case <-cleaned:
			t.Fatal("borrowed resource cleaned early")
		default:
		}
		copy := old
		if err := old.Release(); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(copy.Release(), ErrLease) {
			t.Fatal("copied lease released twice")
		}
		if copy.state.value != 0 || copy.state.release != nil {
			t.Fatal("released lease retained its copied instance or owner callback")
		}
		if _, err := old.Value(); !errors.Is(err, ErrLease) {
			t.Fatal("released value remained accessible")
		}
		if receiveTest(t, cleaned) != 1 {
			t.Fatal("wrong retired generation")
		}
		waitTest(t, third)
		if readTest(t, ref) != 3 {
			t.Fatal("latest desired value not adopted")
		}
	})

	t.Run("parent_and_wait_cancellation_do_not_close_borrowed_lifetime", func(t *testing.T) {
		parent, stop := context.WithCancel(context.Background())
		scope, err := New(parent, Options{})
		if err != nil {
			t.Fatal(err)
		}
		contexts := make(chan context.Context, 1)
		options := testBinding("instance", Fixed)
		options.Build = func(ctx context.Context, value int) (*Instance[int], error) {
			contexts <- ctx
			return &Instance[int]{Value: value}, nil
		}
		ref, _ := Bind(scope, options)
		request, cancel := context.WithCancel(context.Background())
		update, err := scope.Apply(request, testView(1))
		if err != nil {
			t.Fatal(err)
		}
		waitTest(t, update)
		lifetime := receiveTest(t, contexts)
		cancel()
		if lifetime.Err() != nil {
			t.Fatal("Apply context became instance lifetime")
		}
		lease, _ := ref.Acquire(context.Background())
		stop()
		wait, end := context.WithTimeout(context.Background(), 15*time.Millisecond)
		defer end()
		if !errors.Is(scope.Close(wait), ErrWait) || lifetime.Err() != nil {
			t.Fatal("borrow was forced closed")
		}
		if _, err := ref.Acquire(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatal("new borrow after close")
		}
		if err := lease.Release(); err != nil {
			t.Fatal(err)
		}
		if err := scope.Close(context.Background()); err != nil || lifetime.Err() == nil {
			t.Fatal("retired lifetime not canceled", err)
		}
	})

	t.Run("concurrent_use_and_replacement", func(t *testing.T) {
		type value struct {
			number int
			closed atomic.Bool
		}
		scope := testScope(t, Options{})
		base := testBinding("instance", Follow)
		ref, err := Bind(scope, Binding[int, *value]{
			Name: base.Name, Policy: Follow, Select: base.Select, Clone: base.Clone, Equal: base.Equal,
			Build: func(_ context.Context, number int) (*Instance[*value], error) {
				object := &value{number: number}
				return &Instance[*value]{Value: object, Release: func(context.Context) ReleaseResult {
					if object.closed.Swap(true) {
						t.Error("double native cleanup")
					}
					return ReleaseResult{Complete: true}
				}}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		waitTest(t, applyTest(t, scope, 1))
		var workers sync.WaitGroup
		for range 8 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for range 100 {
					lease, err := ref.Acquire(context.Background())
					if err != nil {
						t.Error(err)
						return
					}
					object, err := lease.Value()
					if err != nil || object.closed.Load() || object.number < 1 {
						t.Error("invalid live borrow")
					}
					if object.closed.Load() {
						t.Error("native cleanup raced live borrower")
					}
					if err := lease.Release(); err != nil {
						t.Error(err)
					}
				}
			}()
		}
		for number := 2; number <= 20; number++ {
			waitTest(t, applyTest(t, scope, number))
		}
		workers.Wait()
	})
}

func FuzzLifecycle(f *testing.F) {
	f.Add([]byte{1, 0, 1, 0, 2, 3, 0, 2})
	f.Add([]byte{0, 1, 1, 0, 0, 2, 2})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 || len(data) > 32 {
			return
		}
		type object struct{ closed atomic.Bool }
		scope, err := New(context.Background(), Options{})
		if err != nil {
			t.Fatal(err)
		}
		var leases []Lease[*object]
		defer func() {
			for _, lease := range leases {
				_ = lease.Release()
			}
			if err := scope.Close(context.Background()); err != nil {
				t.Error(err)
			}
		}()
		base := testBinding("fuzz", Policy(data[0]%2))
		ref, err := Bind(scope, Binding[int, *object]{Name: base.Name, Policy: base.Policy, Select: base.Select, Clone: base.Clone, Equal: base.Equal,
			Build: func(context.Context, int) (*Instance[*object], error) {
				value := &object{}
				return &Instance[*object]{Value: value, Release: func(context.Context) ReleaseResult {
					if value.closed.Swap(true) {
						t.Error("double cleanup")
					}
					return ReleaseResult{Complete: true}
				}}, nil
			}})
		if err != nil {
			t.Fatal(err)
		}
		latest := applyTest(t, scope, 0)
		waitTest(t, latest)
		for _, action := range data[1:] {
			switch action % 4 {
			case 0:
				latest = applyTest(t, scope, int(action/4)%4)
			case 1:
				if len(leases) < 4 {
					lease, err := ref.Acquire(context.Background())
					if err != nil {
						t.Fatal(err)
					}
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
				latest, err = ref.Retry(context.Background())
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, lease := range leases {
				value, err := lease.Value()
				if err != nil || value.closed.Load() {
					t.Fatal("use-after-cleanup")
				}
			}
			status, _ := ref.Inspect()
			owned := status.Retiring
			if status.Active {
				owned++
			}
			if status.Preparing {
				owned++
			}
			if owned > 2 {
				t.Fatal("generation bound exceeded")
			}
		}
		for _, lease := range leases {
			if err := lease.Release(); err != nil {
				t.Fatal(err)
			}
		}
		leases = nil
		waitTest(t, latest)
	})
}

func BenchmarkBorrow(b *testing.B) {
	for _, policy := range []Policy{Fixed, Follow} {
		name := "fixed"
		if policy == Follow {
			name = "follow"
		}
		b.Run(name, func(b *testing.B) {
			scope := testScope(b, Options{})
			ref, err := Bind(scope, testBinding("instance", policy))
			if err != nil {
				b.Fatal(err)
			}
			waitTest(b, applyTest(b, scope, 1))
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				lease, err := ref.Acquire(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := lease.Value(); err != nil {
					b.Fatal(err)
				}
				if err := lease.Release(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
