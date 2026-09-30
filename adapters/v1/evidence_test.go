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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestEvidence(t *testing.T) {
	t.Run("sealed_inbox_honors_queued_admission_reservations", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			owner := testRuntime(t, Options{MaxActive: 1, MaxQueued: 1})
			endpoint, inbox := testEndpoint(t, owner, func(value int) int { return value }, 2)
			var guard Guard
			_, err := endpoint.Run(context.Background(), request("first"), func(call *Call[int]) { guard, _ = call.Hold(); _ = call.Resolve(Outcome[int]{}) })
			if err != nil {
				t.Fatal(err)
			}
			first, _ := inbox.Next(context.Background())
			done := make(chan error, 1)
			go func() {
				_, err := endpoint.Run(context.Background(), request("queued"), func(call *Call[int]) { _ = call.Resolve(Outcome[int]{}) })
				done <- err
			}()
			synctest.Wait()
			if status, _ := inbox.Inspect(); status.Reserved != 1 {
				t.Fatal("queue reservation absent")
			}
			_ = inbox.Seal()
			next := make(chan Delivery[int], 1)
			go func() {
				value, err := inbox.Next(context.Background())
				if err != nil {
					t.Error(err)
				}
				next <- value
			}()
			synctest.Wait()
			select {
			case <-next:
				t.Fatal("seal revoked pending reservation")
			default:
			}
			_ = guard.Release()
			if err := receive(t, done); err != nil {
				t.Fatal(err)
			}
			second := receive(t, next)
			if first.Ack() != nil || second.Ack() != nil {
				t.Fatal("accepted reservation disappeared")
			}
			if _, err := inbox.Next(context.Background()); !errors.Is(err, io.EOF) {
				t.Fatal("drained inbox not sealed")
			}
		})
	})
	t.Run("interrupted_release_wait_returns_claim", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			owner := testRuntime(t, Options{})
			endpoint, inbox := testEndpoint(t, owner, func(value int) int { return value }, 1)
			var guard Guard
			_, _ = endpoint.Run(context.Background(), request("live"), func(call *Call[int]) { guard, _ = call.Hold() })
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- inbox.DeliverOne(ctx, func(context.Context, Snapshot[int]) error { t.Error("live sink dispatched"); return nil })
			}()
			synctest.Wait()
			cancel()
			if err := receive(t, done); !errors.Is(err, context.Canceled) {
				t.Fatal("release wait did not stop")
			}
			if status, _ := inbox.Inspect(); status.Queued != 1 || status.Claimed != 0 {
				t.Fatal("interrupted wait stranded claim")
			}
			_ = guard.Release()
			if err := inbox.DeliverOne(context.Background(), func(context.Context, Snapshot[int]) error { return nil }); err != nil {
				t.Fatal(err)
			}
		})
	})
	t.Run("copied_claims_have_one_concurrent_disposition", func(t *testing.T) {
		owner := testRuntime(t, Options{})
		endpoint, inbox := testEndpoint(t, owner, func(value int) int { return value }, 1)
		_, _ = endpoint.Run(context.Background(), request("claim"), func(call *Call[int]) { _ = call.Resolve(Outcome[int]{}) })
		delivery, _ := inbox.Next(context.Background())
		var workers sync.WaitGroup
		var successes atomic.Int32
		for index := range 32 {
			workers.Go(func() {
				var err error
				if index%2 == 0 {
					err = delivery.Ack()
				} else {
					err = delivery.Retry()
				}
				if err == nil {
					successes.Add(1)
				} else if !errors.Is(err, ErrReleased) {
					t.Error(err)
				}
			})
		}
		workers.Wait()
		if successes.Load() != 1 {
			t.Fatal("claim disposition duplicated")
		}
		if status, _ := inbox.Inspect(); status.Queued == 1 {
			fresh, _ := inbox.Next(context.Background())
			if !errors.Is(delivery.Ack(), ErrReleased) || fresh.Ack() != nil {
				t.Fatal("old claim affected new claim")
			}
		}
		if status, _ := inbox.Inspect(); status.Outstanding != 0 || status.Bytes != 0 {
			t.Fatal("disposition leaked capacity")
		}
	})
	t.Run("consumer_handle_overwrite_does_not_corrupt_custody", func(t *testing.T) {
		owner := testRuntime(t, Options{})
		endpoint, inbox := testEndpoint(t, owner, func(value int) int { return value }, 1)
		receipt, err := endpoint.Run(context.Background(), request("immutable"), func(call *Call[int]) { _ = call.Resolve(Outcome[int]{Value: 8, Present: true}) })
		if err != nil {
			t.Fatal(err)
		}
		original := receipt.state
		*receipt = Receipt[int]{}
		delivery, err := inbox.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		other, err := delivery.Receipt()
		if err != nil || other.state != original {
			t.Fatal("direct consumer overwrote independent evidence")
		}
		*other = Receipt[int]{}
		fresh, err := delivery.Receipt()
		if err != nil || fresh.state != original || delivery.Ack() != nil {
			t.Fatal("receiver handle overwrote private custody")
		}
	})
	t.Run("canceled_receiver_does_not_start_sink_after_completed_wait", func(t *testing.T) {
		owner := testRuntime(t, Options{})
		endpoint, inbox := testEndpoint(t, owner, func(value int) int { return value }, 1)
		receipt, err := endpoint.Run(context.Background(), request("complete"), func(call *Call[int]) { _ = call.Resolve(Outcome[int]{}) })
		if err != nil {
			t.Fatal(err)
		}
		_ = waitReleased(t, receipt)
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		cause := errors.New("receiver stopped")
		done := make(chan error, 1)
		owner.state.mu.Lock()
		go func() {
			done <- inbox.DeliverOne(ctx, func(context.Context, Snapshot[int]) error {
				t.Error("known-canceled receiver dispatched sink")
				return nil
			})
		}()
		deadline := time.Now().Add(time.Second)
		for {
			status, _ := inbox.Inspect()
			if status.Claimed == 1 {
				break
			}
			if time.Now().After(deadline) {
				owner.state.mu.Unlock()
				t.Fatal("receiver did not claim evidence")
			}
			runtime.Gosched()
		}
		cancel(cause)
		owner.state.mu.Unlock()
		if err := receive(t, done); !errors.Is(err, cause) || !errors.Is(err, ErrWait) {
			t.Fatal("receiver cancellation lost")
		}
		status, _ := inbox.Inspect()
		if status.Queued != 1 || status.Claimed != 0 || status.Outstanding != 1 {
			t.Fatal("canceled delivery discarded evidence")
		}
	})
	t.Run("required_capacity_precedes_dispatch", func(t *testing.T) {
		runtime := testRuntime(t, Options{})
		endpoint, inbox := testEndpoint(t, runtime, func(value int) int { return value }, 1)
		calls := 0
		first, err := endpoint.Run(context.Background(), request("first"), func(call *Call[int]) { calls++; _ = call.Resolve(Outcome[int]{}) })
		if err != nil {
			t.Fatal(err)
		}
		_ = waitReleased(t, first)
		if _, err := endpoint.Run(context.Background(), request("second"), func(*Call[int]) { calls++ }); !errors.Is(err, ErrEvidence) || calls != 1 {
			t.Fatal("full evidence reached producer")
		}
		record, _ := inbox.Next(context.Background())
		copy := record
		if record.Retry() != nil || !errors.Is(copy.Ack(), ErrReleased) {
			t.Fatal("old claim retained authority")
		}
		next, _ := inbox.Next(context.Background())
		received, err := next.Receipt()
		if err != nil || received.state != first.state {
			t.Fatal("redelivery replaced original facts")
		}
		if next.Ack() != nil {
			t.Fatal("record could not be acknowledged")
		}
		status, _ := inbox.Inspect()
		if status.Outstanding != 0 || status.Bytes != 0 {
			t.Fatal("custody reservation leaked")
		}
	})
	t.Run("failed_sink_retains_same_facts_without_sdk_retry", func(t *testing.T) {
		runtime := testRuntime(t, Options{})
		endpoint, inbox := testEndpoint(t, runtime, func(value int) int { return value }, 1)
		calls := 0
		receipt, err := endpoint.Run(context.Background(), request("mutation"), func(call *Call[int]) { calls++; _ = call.Resolve(Outcome[int]{Value: 5, Present: true}) })
		if err != nil {
			t.Fatal(err)
		}
		_ = waitReleased(t, receipt)
		cause := errors.New("sink unavailable")
		if err := inbox.DeliverOne(context.Background(), func(context.Context, Snapshot[int]) error { return cause }); !errors.Is(err, cause) {
			t.Fatal("sink failure lost")
		}
		if err := inbox.DeliverOne(context.Background(), func(_ context.Context, value Snapshot[int]) error {
			got, present := value.ValueCopy()
			if !present || got != 5 {
				t.Error("facts lost")
			}
			return nil
		}); err != nil || calls != 1 {
			t.Fatal("sink retry repeated SDK operation")
		}
		_ = inbox.Seal()
		if _, err := inbox.Next(context.Background()); !errors.Is(err, io.EOF) {
			t.Fatal("sealed drained receiver did not report EOF")
		}
		if _, err := endpoint.Run(context.Background(), request("sealed"), func(*Call[int]) {}); !errors.Is(err, ErrClosed) {
			t.Fatal("sealed receiver admitted")
		}
	})
	t.Run("wait_failure_requeues_and_live_ack_refuses", func(t *testing.T) {
		runtime := testRuntime(t, Options{})
		endpoint, inbox := testEndpoint(t, runtime, func(value int) int { return value }, 1)
		var guard Guard
		receipt, _ := endpoint.Run(context.Background(), request("retained"), func(call *Call[int]) { guard, _ = call.Hold(); _ = call.Resolve(Outcome[int]{}) })
		delivery, _ := inbox.Next(context.Background())
		if !errors.Is(delivery.Ack(), ErrPending) {
			t.Fatal("live work acknowledged")
		}
		if delivery.Retry() != nil {
			t.Fatal("claim not returned")
		}
		_ = guard.Release()
		_ = waitReleased(t, receipt)
		if inbox.DeliverOne(context.Background(), func(context.Context, Snapshot[int]) error { return nil }) != nil {
			t.Fatal("returned evidence unavailable")
		}
	})
}

func TestReleasedEvidenceSelection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		owner := testRuntime(t, Options{})
		endpoint, inbox := testEndpoint(t, owner, func(value int) int { return value }, 4)
		var guard Guard
		_, err := endpoint.Run(context.Background(), request("live"), func(call *Call[int]) {
			guard, _ = call.Hold()
			_ = call.Resolve(Outcome[int]{Value: 1, Present: true})
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = endpoint.Run(context.Background(), request("finite"), func(call *Call[int]) { _ = call.Resolve(Outcome[int]{Value: 2, Present: true}) })
		if err != nil {
			t.Fatal(err)
		}
		if err := inbox.Seal(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		invoked := false
		if err := inbox.DeliverOne(ctx, func(context.Context, Snapshot[int]) error { invoked = true; return nil }); !errors.Is(err, context.DeadlineExceeded) || invoked {
			t.Fatal("missing live-head control", err)
		}
		finite, err := inbox.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := finite.Retry(); err != nil {
			t.Fatal(err)
		}
		ready, err := inbox.NextReleased(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := ready.Receipt()
		if err != nil {
			t.Fatal(err)
		}
		value, _ := receipt.Snapshot()
		if facts, present := value.ValueCopy(); !present || facts != 2 || !value.Info().Released {
			t.Fatal("live record blocked completed fact")
		}
		if err := ready.Ack(); err != nil {
			t.Fatal(err)
		}
		received := make(chan Delivery[int], 1)
		go func() {
			value, err := inbox.NextReleased(context.Background())
			if err != nil {
				t.Error(err)
			}
			received <- value
		}()
		synctest.Wait()
		select {
		case <-received:
			t.Fatal("Seal discarded pending work")
		default:
		}
		if err := guard.Release(); err != nil {
			t.Fatal(err)
		}
		last := receive(t, received)
		if err := last.Ack(); err != nil {
			t.Fatal(err)
		}
		if _, err := inbox.NextReleased(context.Background()); !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
	})
}
