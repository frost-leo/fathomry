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
	"math"
	"testing"
)

func TestOptions(t *testing.T) {
	t.Run("binding_captures_receiver_handles", func(t *testing.T) {
		runtime := testRuntime(t, Options{})
		inbox, _ := NewInbox[int](EvidenceOptions{})
		observer, _ := NewObserver(3)
		receiver, events := *inbox, *observer
		endpoint, err := Bind(runtime, Declaration[int]{Copy: func(value int) int { return value }, Evidence: inbox, Observer: observer})
		if err != nil {
			t.Fatal(err)
		}
		*inbox, *observer = Inbox[int]{}, Observer{}
		if _, err := endpoint.Run(context.Background(), request("bound"), func(call *Call[int]) { _ = call.Resolve(Outcome[int]{}) }); err != nil {
			t.Fatal("binding retained caller-owned handle storage", err)
		}
		if status, _ := receiver.Inspect(); status.Outstanding != 1 {
			t.Fatal("binding changed required receiver")
		}
		if status, _ := events.Inspect(); status.Queued != 3 {
			t.Fatal("binding changed optional observer")
		}
	})
	t.Run("declarations_and_bounds", func(t *testing.T) {
		for _, options := range []Options{{Name: "private/path"}, {MaxActive: -1}, {MaxQueued: -1}, {MaxWorkBytes: -1}, {MaxQueuedBytes: 1}, {MaxDepth: 33}, {MaxHolds: -1}, {MaxTasks: -1}} {
			if _, err := New(context.Background(), options); !errors.Is(err, ErrOptions) {
				t.Fatal("invalid options admitted")
			}
		}
		if _, err := New(nil, Options{}); !errors.Is(err, ErrOptions) {
			t.Fatal("nil context admitted")
		}
		if _, err := Bind[int](nil, Declaration[int]{}); !errors.Is(err, ErrHandle) {
			t.Fatal("nil runtime admitted")
		}
		runtime := testRuntime(t, Options{})
		inbox, _ := NewInbox[int](EvidenceOptions{})
		for _, decl := range []Declaration[int]{{}, {Evidence: inbox}, {Copy: func(v int) int { return v }}, {Copy: func(v int) int { return v }, Evidence: inbox, Observer: new(Observer)}} {
			if _, err := Bind(runtime, decl); !errors.Is(err, ErrOptions) {
				t.Fatal("invalid declaration admitted")
			}
		}
		for _, options := range []EvidenceOptions{{Capacity: -1}, {Capacity: 65537}, {MaxBytes: -1}} {
			if _, err := NewInbox[int](options); !errors.Is(err, ErrOptions) {
				t.Fatal("invalid evidence bounds admitted")
			}
		}
		for _, capacity := range []int{-1, 0, 65537} {
			if _, err := NewObserver(capacity); !errors.Is(err, ErrOptions) {
				t.Fatal("invalid observer capacity admitted")
			}
		}
		endpoint, _ := testEndpoint(t, runtime, func(value int) int { return value }, 4)
		for _, input := range []Request{{}, {Operation: "Upper", WorkBytes: 1, EvidenceBytes: 1}, {Operation: "valid", WorkBytes: -1, EvidenceBytes: 1}, {Operation: "valid", WorkBytes: 1, EvidenceBytes: 0}, {Operation: "valid", ID: "private\x00", WorkBytes: 1, EvidenceBytes: 1}} {
			if _, err := endpoint.Run(context.Background(), input, func(*Call[int]) { t.Error("invalid producer dispatched") }); !errors.Is(err, ErrRequest) {
				t.Fatal("invalid request admitted")
			}
		}
		if _, err := endpoint.Run(nil, request("nil"), func(*Call[int]) {}); !errors.Is(err, ErrRequest) {
			t.Fatal("nil caller admitted")
		}
		if _, err := endpoint.Run(context.Background(), request("nil"), nil); !errors.Is(err, ErrOptions) {
			t.Fatal("nil producer admitted")
		}
	})
	t.Run("counter_and_tree_limits_do_not_leak_reservations", func(t *testing.T) {
		runtime := testRuntime(t, Options{MaxTasks: 1, MaxDepth: 1})
		endpoint, inbox := testEndpoint(t, runtime, func(value int) int { return value }, 4)
		receipt, err := endpoint.Run(context.Background(), request("root"), func(call *Call[int]) {
			child := request("child")
			child.WorkBytes = 0
			if _, err := endpoint.Child(call.Context(), call.Scope(), child, func(*Call[int]) {}); !errors.Is(err, ErrLimit) {
				t.Error("tree bound ignored")
			}
			_ = call.Resolve(Outcome[int]{})
		})
		if err != nil {
			t.Fatal(err)
		}
		_ = waitReleased(t, receipt)
		runtime.state.mu.Lock()
		runtime.state.sequence = math.MaxUint64
		runtime.state.mu.Unlock()
		if _, err := endpoint.Run(context.Background(), request("overflow"), func(*Call[int]) {}); !errors.Is(err, ErrLimit) {
			t.Fatal("sequence wrapped")
		}
		stats, _ := runtime.Inspect()
		usage, _ := inbox.Inspect()
		if stats.Active != 0 || stats.WorkBytes != 0 || usage.Outstanding != 1 || usage.Reserved != 0 {
			t.Fatal("rejected operation leaked capacity")
		}
	})
	t.Run("zero_handles_refuse_without_panics", func(t *testing.T) {
		var runtime *Runtime
		var inbox *Inbox[int]
		var receipt *Receipt[int]
		var call *Call[int]
		if !errors.Is(runtime.Close(context.Background()), ErrHandle) {
			t.Fatal("nil runtime closed")
		}
		if _, err := runtime.Inspect(); !errors.Is(err, ErrHandle) {
			t.Fatal("nil runtime inspected")
		}
		if _, err := inbox.Next(context.Background()); !errors.Is(err, ErrHandle) {
			t.Fatal("nil inbox read")
		}
		if !errors.Is(inbox.Seal(), ErrHandle) {
			t.Fatal("nil inbox sealed")
		}
		if _, err := inbox.Inspect(); !errors.Is(err, ErrHandle) {
			t.Fatal("nil inbox inspected")
		}
		if _, err := receipt.Wait(context.Background()); !errors.Is(err, ErrHandle) {
			t.Fatal("nil receipt waited")
		}
		if !errors.Is(call.Resolve(Outcome[int]{}), ErrHandle) {
			t.Fatal("nil call resolved")
		}
		if call.Context() != nil || call.Receipt() != nil || call.Scope().node != nil {
			t.Fatal("zero authority fabricated")
		}
		if _, err := call.Hold(); !errors.Is(err, ErrHandle) {
			t.Fatal("nil hold admitted")
		}
		if !errors.Is(call.Cancel(nil), ErrHandle) {
			t.Fatal("nil call canceled")
		}
		if !errors.Is((Guard{}).Release(), ErrHandle) {
			t.Fatal("zero guard released")
		}
		if !errors.Is((Delivery[int]{}).Ack(), ErrHandle) {
			t.Fatal("zero delivery acknowledged")
		}
		if _, err := (Delivery[int]{}).Receipt(); !errors.Is(err, ErrHandle) {
			t.Fatal("zero delivery read")
		}
		if !errors.Is((Snapshot[int]{}).Err(), ErrHandle) {
			t.Fatal("zero result became success")
		}
	})
}
