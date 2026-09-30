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

package framework

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestReceiver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runtime, err := New(context.Background(), Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer runtime.Close(context.Background())
		inbox, _ := adapters.NewInbox[int](adapters.EvidenceOptions{})
		endpoint, err := adapters.Bind(runtime.Operations(), adapters.Declaration[int]{Evidence: inbox, Copy: func(value int) int { return value }})
		if err != nil {
			t.Fatal(err)
		}
		var guard adapters.Guard
		_, err = endpoint.Run(context.Background(), adapters.Request{Operation: "fixture.live", WorkBytes: 64, EvidenceBytes: 64}, func(call *adapters.Call[int]) {
			guard, _ = call.Hold()
			_ = call.Resolve(adapters.Outcome[int]{Value: 1, Present: true})
		})
		if err != nil {
			t.Fatal(err)
		}
		var nativeCalls atomic.Int32
		_, err = endpoint.Run(context.Background(), adapters.Request{Operation: "fixture.finite", WorkBytes: 64, EvidenceBytes: 64}, func(call *adapters.Call[int]) {
			nativeCalls.Add(1)
			_ = call.Resolve(adapters.Outcome[int]{Value: 2, Present: true})
		})
		if err != nil {
			t.Fatal(err)
		}
		delivered := make(chan int, 2)
		marker := errors.New("sink-private-canary")
		attempts := 0
		receiver, err := StartReceiver(context.Background(), inbox, ReceiverOptions{RetryDelay: time.Millisecond}, func(_ context.Context, value adapters.Snapshot[int]) error {
			attempts++
			if attempts == 1 {
				return marker
			}
			facts, _ := value.ValueCopy()
			delivered <- facts
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if value := <-delivered; value != 2 {
			t.Fatal("live owner blocked finite receipt")
		}
		status, err := receiver.Status()
		if err != nil || status.Failures != 1 || !errors.Is(status.LastError, marker) {
			t.Fatal("reception failure missing", err)
		}
		if nativeCalls.Load() != 1 {
			t.Fatal("receipt retried operation")
		}
		if err := guard.Release(); err != nil {
			t.Fatal(err)
		}
		if value := <-delivered; value != 1 {
			t.Fatal("live receipt lost")
		}
		if err := receiver.Finish(context.Background()); err != nil {
			t.Fatal(err)
		}
		status, _ = receiver.Status()
		if status.Delivered != 2 || !status.Stopped {
			t.Fatal(status)
		}
	})
}
func TestReceiverRetainsBlockedSink(t *testing.T) {
	runtime, err := New(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, _ := adapters.NewInbox[int](adapters.EvidenceOptions{})
	endpoint, _ := adapters.Bind(runtime.Operations(), adapters.Declaration[int]{Evidence: inbox, Copy: func(value int) int { return value }})
	_, err = endpoint.Run(context.Background(), adapters.Request{Operation: "fixture.read", WorkBytes: 64, EvidenceBytes: 64}, func(call *adapters.Call[int]) { _ = call.Resolve(adapters.Outcome[int]{Value: 1, Present: true}) })
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	receiver, err := StartReceiver(context.Background(), inbox, ReceiverOptions{}, func(context.Context, adapters.Snapshot[int]) error { close(entered); <-release; return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := receiver.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	status, _ := inbox.Inspect()
	if status.Outstanding != 1 || status.Claimed != 1 {
		t.Fatal("abandoned sink custody")
	}
	close(release)
	if err := receiver.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestReceiverOtherCustody(t *testing.T) {
	runtime, err := New(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, _ := adapters.NewInbox[int](adapters.EvidenceOptions{})
	endpoint, _ := adapters.Bind(runtime.Operations(), adapters.Declaration[int]{Evidence: inbox, Copy: func(value int) int { return value }})
	_, err = endpoint.Run(context.Background(), adapters.Request{Operation: "fixture.read", WorkBytes: 64, EvidenceBytes: 64}, func(call *adapters.Call[int]) { _ = call.Resolve(adapters.Outcome[int]{}) })
	if err != nil {
		t.Fatal(err)
	}
	claim, err := inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := StartReceiver(context.Background(), inbox, ReceiverOptions{}, func(context.Context, adapters.Snapshot[int]) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := receiver.Finish(context.Background()); !errors.Is(err, ErrPending) {
		t.Fatal("foreign claim certified received", err)
	}
	if err := claim.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, options := range []ReceiverOptions{{RetryDelay: time.Nanosecond}, {RetryDelay: time.Hour}} {
		if _, err := StartReceiver(context.Background(), inbox, options, func(context.Context, adapters.Snapshot[int]) error { return nil }); !errors.Is(err, ErrOptions) {
			t.Fatal(err)
		}
	}
	if _, err := StartReceiver[int](nil, inbox, ReceiverOptions{}, nil); !errors.Is(err, ErrOptions) {
		t.Fatal(err)
	}
	if err := new(Receiver[int]).Close(context.Background()); !errors.Is(err, ErrHandle) {
		t.Fatal(err)
	}
}
