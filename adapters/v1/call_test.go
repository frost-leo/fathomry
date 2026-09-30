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
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCall(t *testing.T) {
	t.Run("async_child_retains_parent_and_independent_outcome", func(t *testing.T) {
		owner := testRuntime(t, Options{MaxActive: 1})
		endpoint, _ := testEndpoint(t, owner, func(value int) int { return value }, 2)
		entered := make(chan struct{})
		var root *Call[int]
		var child *Receipt[int]
		parent, err := endpoint.Run(context.Background(), request("parent"), func(call *Call[int]) {
			root = call
			input := request("async")
			input.WorkBytes = 0
			var err error
			child, err = endpoint.StartChild(context.Background(), call.Scope(), input, func(call *Call[int]) {
				close(entered)
				<-call.Context().Done()
				_ = call.Resolve(Outcome[int]{Primary: context.Cause(call.Context())})
			})
			if err != nil {
				t.Error(err)
			}
			_ = call.Resolve(Outcome[int]{Value: 1, Present: true})
		})
		if err != nil {
			t.Fatal(err)
		}
		receive(t, entered)
		if result, _ := parent.Snapshot(); result.Info().Released {
			t.Fatal("asynchronous child lost parent")
		}
		cause := errors.New("parent request stopped")
		_ = root.Cancel(cause)
		if result := waitReleased(t, child); !errors.Is(result.Primary(), cause) {
			t.Fatal("child lost parent cancellation")
		}
		if result := waitReleased(t, parent); result.Err() != nil {
			t.Fatal("child failure invented a parent disposition")
		}
	})
	t.Run("ancestor_cancel_fences_before_async_propagation", func(t *testing.T) {
		owner := testRuntime(t, Options{})
		endpoint, _ := testEndpoint(t, owner, func(value int) int { return value }, 8)
		var root, child *Call[int]
		var rootHold, childHold Guard
		_, _ = endpoint.Run(context.Background(), request("root"), func(call *Call[int]) { root = call; rootHold, _ = call.Hold() })
		defer rootHold.Release()
		input := request("child")
		input.WorkBytes = 0
		_, err := endpoint.Child(context.Background(), root.Scope(), input, func(call *Call[int]) { child = call; childHold, _ = call.Hold() })
		if err != nil {
			t.Fatal(err)
		}
		defer childHold.Release()
		pending, err := endpoint.begin(context.Background(), input, child.Scope())
		if err != nil {
			t.Fatal(err)
		}
		// Keep the immediate parent live to model delayed AfterFunc propagation.
		_ = child.state.node.releaseSource()
		_ = root.Cancel(errors.New("ancestor stopped"))
		called := false
		pending.dispatch(func(*Call[int]) { called = true })
		if called || !errors.Is(waitReleased(t, pending.Receipt()).Err(), ErrClosed) {
			t.Error("known-canceled ancestor reached descendant producer")
		}
		if _, err := endpoint.Child(context.Background(), child.Scope(), input, func(*Call[int]) { t.Error("canceled family admitted child") }); !errors.Is(err, ErrClosed) {
			t.Error("child admission ignored canceled ancestor")
		}
	})
	t.Run("copy_outside_lock_still_owns_actual_work", func(t *testing.T) {
		owner := testRuntime(t, Options{})
		copying, leave := make(chan struct{}), make(chan struct{})
		endpoint, _ := testEndpoint(t, owner, func(value int) int {
			_, _ = owner.Inspect()
			close(copying)
			<-leave
			return value
		}, 1)
		var call *Call[int]
		var guard Guard
		receipt, _ := endpoint.Run(context.Background(), request("copying"), func(value *Call[int]) { call = value; guard, _ = call.Hold() })
		done := make(chan error, 1)
		go func() { done <- call.Resolve(Outcome[int]{Value: 1, Present: true}) }()
		receive(t, copying)
		_ = guard.Release()
		if !errors.Is(call.Resolve(Outcome[int]{}), ErrSettled) {
			t.Error("concurrent publication admitted")
		}
		wait, cancel := context.WithCancel(context.Background())
		cancel()
		if !errors.Is(owner.Close(wait), ErrWait) {
			t.Error("copying work detached during shutdown")
		}
		close(leave)
		if receive(t, done) != nil || owner.Close(context.Background()) != nil {
			t.Fatal("copying did not release ownership")
		}
		if !waitReleased(t, receipt).Info().Released {
			t.Fatal("copy publication not released")
		}
	})
	t.Run("lease_cleanup_is_late_separate_evidence", func(t *testing.T) {
		owner := testRuntime(t, Options{})
		endpoint, _ := testEndpoint(t, owner, func(value int) int { return value }, 1)
		call, err := endpoint.begin(context.Background(), request("cleanup"), Scope{})
		if err != nil {
			t.Fatal(err)
		}
		cause := errors.New("lease release")
		call.state.node.releaseSource = func() error { return cause }
		_ = call.Resolve(Outcome[int]{})
		before, _ := call.Receipt().Snapshot()
		call.state.node.returned(false)
		after := waitReleased(t, call.Receipt())
		if before.Err() != nil || before.Info().Released || !errors.Is(after.Cleanup(), cause) || !errors.Is(after.Err(), cause) {
			t.Fatal("late cleanup erased outcome or changed history")
		}
	})
	t.Run("inline_outcome_does_not_release_submitting_stack", func(t *testing.T) {
		runtime := testRuntime(t, Options{MaxActive: 1})
		endpoint, inbox := testEndpoint(t, runtime, func(value int) int { return value }, 3)
		published, leave := make(chan struct{}), make(chan struct{})
		receipt, err := endpoint.Start(context.Background(), request("inline"), func(call *Call[int]) {
			guard, _ := call.Hold()
			_ = call.Resolve(Outcome[int]{Value: 7, Present: true})
			_ = guard.Release()
			close(published)
			<-leave
		})
		if err != nil {
			t.Fatal(err)
		}
		receive(t, published)
		value, err := receipt.Wait(context.Background())
		if err != nil || value.Info().Released {
			t.Fatal("callback freed submit stack")
		}
		delivery, _ := inbox.Next(context.Background())
		if !errors.Is(delivery.Ack(), ErrPending) {
			t.Fatal("live stack evidence acknowledged")
		}
		if _, err := endpoint.Run(context.Background(), request("excess"), func(*Call[int]) {}); !errors.Is(err, ErrLimit) {
			t.Fatal("active limit released early")
		}
		close(leave)
		_ = waitReleased(t, receipt)
		if delivery.Ack() != nil {
			t.Fatal("finished stack could not be acknowledged")
		}
	})
	t.Run("late_callback_wait_cancellation_and_immutable_history", func(t *testing.T) {
		runtime := testRuntime(t, Options{})
		endpoint, _ := testEndpoint(t, runtime, func(value []int) []int { return append([]int(nil), value...) }, 2)
		var call *Call[[]int]
		var guard Guard
		receipt, err := endpoint.Run(context.Background(), request("late"), func(value *Call[[]int]) { call = value; guard, _ = call.Hold() })
		if err != nil {
			t.Fatal(err)
		}
		wait, cancel := context.WithCancelCause(context.Background())
		cause := errors.New("wait only")
		cancel(cause)
		if _, err := receipt.Wait(wait); !errors.Is(err, cause) || call.Context().Err() != nil {
			t.Fatal("wait altered operation lifetime")
		}
		data := []int{3}
		if call.Resolve(Outcome[[]int]{Value: data, Present: true}) != nil {
			t.Fatal("publication failed")
		}
		data[0] = 8
		before, _ := receipt.Snapshot()
		value, _ := before.ValueCopy()
		value[0] = 9
		if value, _ := before.ValueCopy(); value[0] != 3 {
			t.Fatal("snapshot alias escaped")
		}
		if !errors.Is(call.Resolve(Outcome[[]int]{}), ErrSettled) {
			t.Fatal("outcome overwritten")
		}
		if guard.Release() != nil || !errors.Is(guard.Release(), ErrReleased) {
			t.Fatal("guard authority duplicated")
		}
		after := waitReleased(t, receipt)
		if before.Info().Released || !after.Info().Released {
			t.Fatal("historical observation mutated")
		}
	})
	t.Run("missing_outcome_and_guards_are_bounded", func(t *testing.T) {
		runtime := testRuntime(t, Options{MaxHolds: 1})
		endpoint, _ := testEndpoint(t, runtime, func(value int) int { return value }, 3)
		var guard Guard
		var call *Call[int]
		receipt, err := endpoint.Run(context.Background(), request("missing"), func(value *Call[int]) {
			call = value
			guard, _ = call.Hold()
			if _, err := call.Hold(); !errors.Is(err, ErrLimit) {
				t.Error("guard bound ignored")
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, ready := receipt.Snapshot(); ready {
			t.Fatal("unfinished work fabricated result")
		}
		_ = guard.Release()
		if !errors.Is(waitReleased(t, receipt).Err(), ErrOutcome) {
			t.Fatal("missing outcome became success")
		}
		if _, err := call.Hold(); !errors.Is(err, ErrReleased) {
			t.Fatal("released work reopened")
		}
	})
	t.Run("children_hold_parent_and_finalize_at_saturation", func(t *testing.T) {
		runtime := testRuntime(t, Options{MaxActive: 1, MaxTasks: 2, MaxDepth: 2})
		endpoint, inbox := testEndpoint(t, runtime, func(value int) int { return value }, 2)
		var child *Call[int]
		var hold Guard
		parent, err := endpoint.Run(context.Background(), request("parent"), func(call *Call[int]) {
			childRequest := request("child")
			childRequest.WorkBytes = 0
			_, err := endpoint.Child(call.Context(), call.Scope(), childRequest, func(value *Call[int]) {
				child = value
				hold, _ = value.Hold()
				_ = value.Resolve(Outcome[int]{Value: 2, Present: true})
			})
			if err != nil {
				t.Error(err)
			}
			if _, err := endpoint.Child(call.Context(), call.Scope(), childRequest, func(*Call[int]) {}); !errors.Is(err, ErrEvidence) {
				t.Error("full evidence admitted child")
			}
			_ = call.Resolve(Outcome[int]{Value: 1, Present: true})
		})
		if err != nil {
			t.Fatal(err)
		}
		wait, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if _, err := parent.WaitReleased(wait); !errors.Is(err, ErrWait) {
			t.Fatal("parent abandoned live child")
		}
		if child.Receipt() == nil {
			t.Fatal("child handle absent")
		}
		if hold.Release() != nil {
			t.Fatal("child release failed")
		}
		value := waitReleased(t, parent)
		if value.Info().Parent != 0 || value.Info().Depth != 1 {
			t.Fatal("parent metadata changed")
		}
		childInfo := waitReleased(t, child.Receipt()).Info()
		if childInfo.Parent != value.Info().Sequence || childInfo.Depth != 2 {
			t.Fatal("child attribution changed")
		}
		if inbox.DeliverOne(context.Background(), func(context.Context, Snapshot[int]) error { return nil }) != nil ||
			inbox.DeliverOne(context.Background(), func(context.Context, Snapshot[int]) error { return nil }) != nil {
			t.Fatal("saturated finalization requested capacity")
		}
	})
	t.Run("concurrent_guard_copies_release_once", func(t *testing.T) {
		runtime := testRuntime(t, Options{})
		endpoint, _ := testEndpoint(t, runtime, func(value int) int { return value }, 1)
		var guard Guard
		receipt, _ := endpoint.Run(context.Background(), request("shared"), func(call *Call[int]) {
			guard, _ = call.Hold()
			_ = call.Resolve(Outcome[int]{})
		})
		var workers sync.WaitGroup
		var successful atomic.Int32
		for range 32 {
			workers.Go(func() {
				if guard.Release() == nil {
					successful.Add(1)
				}
			})
		}
		workers.Wait()
		if successful.Load() != 1 {
			t.Fatal("work authority released more than once")
		}
		_ = waitReleased(t, receipt)
	})
}

type modeledCall struct {
	producer *Call[[]byte]
	receipt  *Receipt[[]byte]
	guard    Guard
	held     bool
	resolved bool
	parent   int
	depth    int
	datum    byte
}

func FuzzLifecycle(f *testing.F) {
	f.Add([]byte{0, 0, 0, 4, 1, 0, 3, 0, 2, 1, 2, 0, 3, 0})
	f.Add([]byte{0, 0, 4, 0, 4, 1, 6, 0, 5, 0, 2, 2, 2, 1, 2, 0})
	f.Add([]byte{0, 0, 3, 0, 7, 0, 1, 0, 2, 0, 3, 0})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 256 {
			input = input[:256]
		}
		owner := testRuntime(t, Options{MaxActive: 2, MaxWorkBytes: 2, MaxTasks: 3, MaxDepth: 3, MaxHolds: 1})
		endpoint, inbox := testEndpoint(t, owner, func(value []byte) []byte { return append([]byte(nil), value...) }, 8)
		var records []*modeledCall
		acknowledged := 0
		stopped, sealed := false, false
		var released func(int) bool
		released = func(index int) bool {
			if records[index].held {
				return false
			}
			for child, value := range records {
				if value.parent == index && !released(child) {
					return false
				}
			}
			return true
		}
		check := func() {
			t.Helper()
			active := 0
			for index, value := range records {
				result, ready := value.receipt.Snapshot()
				ended := released(index)
				if result.Info().Released != ended || ready != (value.resolved || ended) {
					t.Fatal("model disagrees with ownership/publication")
				}
				if value.parent == -1 && !ended {
					active++
				}
				if result.Info().Depth != value.depth || result.Info().Sequence != uint64(index+1) {
					t.Fatal("model attribution differs")
				}
				if value.parent >= 0 && result.Info().Parent != uint64(value.parent+1) {
					t.Fatal("parent attribution differs")
				}
				if value.resolved {
					data, present := result.ValueCopy()
					if !present || len(data) != 1 || data[0] != value.datum {
						t.Fatal("published facts mutated")
					}
					data[0]++
				} else if ended && !errors.Is(result.Err(), ErrOutcome) {
					t.Fatal("missing result became success")
				}
			}
			stats, err := owner.Inspect()
			if err != nil || stats.Active != active || stats.WorkBytes != int64(active) || stats.Queued != 0 || stats.Accepted != uint64(len(records)) {
				t.Fatal("model admission counters differ")
			}
			custody, err := inbox.Inspect()
			if err != nil || custody.Outstanding != len(records)-acknowledged || custody.Bytes != int64(custody.Outstanding) ||
				custody.Reserved != 0 || custody.Claimed != 0 || custody.Queued != custody.Outstanding {
				t.Fatal("model custody counters differ")
			}
		}
		for offset := 0; offset+1 < len(input); offset += 2 {
			action, selector := input[offset]%9, input[offset+1]
			var current *modeledCall
			if len(records) > 0 {
				current = records[int(selector)%len(records)]
			}
			switch action {
			case 0, 4:
				parent := -1
				if action == 4 {
					if current == nil {
						continue
					}
					parent = int(selector) % len(records)
				}
				value := &modeledCall{held: true, parent: parent, depth: 1, datum: selector}
				request := request("model")
				produce := func(call *Call[[]byte]) {
					value.producer = call
					var err error
					value.guard, err = call.Hold()
					if err != nil {
						t.Fatal("admitted model guard rejected")
					}
				}
				var receipt *Receipt[[]byte]
				var err error
				if parent == -1 {
					receipt, err = endpoint.Run(context.Background(), request, produce)
				} else {
					request.WorkBytes = 0
					value.depth = current.depth + 1
					receipt, err = endpoint.Child(context.Background(), current.producer.Scope(), request, produce)
				}
				if err == nil {
					if stopped || sealed || value.producer == nil {
						t.Fatal("sealed work dispatched")
					}
					value.receipt = receipt
					records = append(records, value)
				} else if !errors.Is(err, ErrClosed) && !errors.Is(err, ErrLimit) && !errors.Is(err, ErrEvidence) {
					t.Fatal("unexpected model admission failure", err)
				}
			case 1:
				if current == nil {
					continue
				}
				err := current.producer.Resolve(Outcome[[]byte]{Value: []byte{current.datum}, Present: true})
				if err == nil {
					current.resolved = true
				} else if !errors.Is(err, ErrSettled) {
					t.Fatal(err)
				}
			case 2:
				if current == nil {
					continue
				}
				err := current.guard.Release()
				if current.held {
					if err != nil {
						t.Fatal(err)
					}
					current.held = false
				} else if !errors.Is(err, ErrReleased) {
					t.Fatal("copied guard regained authority")
				}
			case 3:
				status, _ := inbox.Inspect()
				if status.Queued == 0 {
					continue
				}
				delivery, err := inbox.Next(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				receipt, _ := delivery.Receipt()
				result, _ := receipt.Snapshot()
				if selector%2 == 0 && result.Info().Released {
					if delivery.Ack() != nil {
						t.Fatal("released evidence not acknowledged")
					}
					acknowledged++
				} else {
					if !result.Info().Released && !errors.Is(delivery.Ack(), ErrPending) {
						t.Fatal("live evidence acknowledged")
					}
					if delivery.Retry() != nil {
						t.Fatal("custody retry failed")
					}
				}
				if !errors.Is(delivery.Ack(), ErrReleased) {
					t.Fatal("stale claim reused")
				}
			case 5:
				wait, cancel := context.WithCancel(context.Background())
				cancel()
				err := owner.Close(wait)
				if err != nil && !errors.Is(err, ErrWait) {
					t.Fatal(err)
				}
				stopped = true
			case 6:
				if current != nil {
					_ = current.producer.Cancel(errors.New("model cancellation"))
				}
			case 7:
				if inbox.Seal() != nil {
					t.Fatal("inbox seal failed")
				}
				sealed = true
			case 8:
				if current != nil {
					wait, cancel := context.WithCancel(context.Background())
					cancel()
					_, err := current.receipt.WaitReleased(wait)
					if err != nil && !errors.Is(err, ErrWait) {
						t.Fatal(err)
					}
				}
			}
			check()
		}
		for _, value := range records {
			if value.held {
				if value.guard.Release() != nil {
					t.Fatal("model finalization failed")
				}
				value.held = false
			}
		}
		check()
		if err := owner.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		_ = inbox.Seal()
		for {
			delivery, err := inbox.Next(context.Background())
			if err != nil {
				if !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
				break
			}
			if delivery.Ack() != nil {
				t.Fatal("model drain failed")
			}
			acknowledged++
		}
		check()
	})
}
