/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package consumer

import (
	"context"
	"errors"
	"github.com/frost-leo/fathomry/adapters/v1"
	"testing"
	"time"
)

type listenResult struct {
	result Result
	err    error
}

func publicEvents(t testing.TB, capacity int) *adapters.Inbox[Incoming] {
	t.Helper()
	result, err := adapters.NewInbox[Incoming](adapters.EvidenceOptions{Capacity: capacity, MaxBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func nextIncoming(t testing.TB, inbox *adapters.Inbox[Incoming]) (Incoming, adapters.Delivery[Incoming]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	delivery, err := inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil || snapshot.Err() != nil {
		t.Fatal("incoming evidence", err, snapshot.Err())
	}
	value, ok := snapshot.ValueCopy()
	if !ok || value.Receipt() == nil {
		t.Fatal("ACK authority not pre-reserved")
	}
	return value, delivery
}
func startListen(client *Client, events *adapters.Inbox[Incoming]) (context.CancelFunc, <-chan listenResult) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan listenResult, 1)
	go func() { result, err := client.Listen(ctx, events); done <- listenResult{result, err} }()
	return cancel, done
}
func finishListen(t testing.TB, cancel context.CancelFunc, done <-chan listenResult) listenResult {
	t.Helper()
	cancel()
	select {
	case result := <-done:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("listener failed to join")
		return listenResult{}
	}
}
func TestWebSocketPublicEvidenceACKAndFiniteFairness(t *testing.T) {
	peer := newWSPeer(t)
	owner, _, inbox := testOwner(t, peer.settings(), NativeOptions{})
	events := publicEvents(t, 8)
	cancel, done := startListen(owner.Client(), events)
	defer cancel()
	conn := nextConnection(t, peer)
	sendEvent(t, conn, "first")
	event, delivery := nextIncoming(t, events)
	if event.ConnectionGeneration() != 1 || event.Attribution().Source.Generation != 0 {
		t.Fatal("source/connection generations conflated")
	}
	if err := delivery.Retry(); err != nil {
		t.Fatal(err)
	}
	copyEvent, delivery := nextIncoming(t, events)
	if copyEvent.FrameID() != event.FrameID() {
		t.Fatal("redelivery changed event")
	}
	_ = delivery.Ack()
	noPositiveACK(t, peer)
	receipt, err := event.Acknowledge(context.Background(), fixtureJSON(t, `{"toast":{"type":"info","content":"synthetic"},"n":9007199254740993}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := copyEvent.Reject(context.Background()); err == nil {
		t.Fatal("copied ACK decided twice")
	}
	frame, code := peerACK(t, peer)
	if code != 200 || frame.headers["message_id"] != "first" {
		t.Fatal("wrong wire ACK")
	}
	snapshot, err := receipt.WaitReleased(context.Background())
	if err != nil || snapshot.Err() != nil {
		t.Fatal("ACK evidence", err, snapshot.Err())
	}
	result, _ := snapshot.ValueCopy()
	ack, ok := result.Acknowledgement()
	if !ok || ack.Effect() != WebSocketWritten || result.Attempts().Observed != 1 {
		t.Fatal("ACK effect/attempt lost")
	}
	if _, err := owner.Client().Send(context.Background(), Recipient{Type: "open_id", ID: "ou_fixture"}, "", fixtureText(t)); err != nil {
		t.Fatal("finite request blocked by listener", err)
	}
	found := false
	for index := 0; index < 2; index++ {
		record := consume(t, inbox)
		receipt, _ := record.Receipt()
		snapshot, _ := receipt.Snapshot()
		found = found || snapshot.Info().Operation == "notification.lark.send"
		_ = record.Ack()
	}
	if !found {
		t.Fatal("released finite evidence hidden by listener")
	}
	if result := finishListen(t, cancel, done); !errors.Is(result.err, context.Canceled) {
		t.Fatal("listen cancellation lost", result.err)
	}
}
func TestWebSocketPublicEventAndACKEvidenceSaturation(t *testing.T) {
	for _, mode := range []string{"event", "ack"} {
		t.Run(mode, func(t *testing.T) {
			peer := newWSPeer(t)
			owner, _, inbox := testOwner(t, peer.settings(), NativeOptions{})
			events := publicEvents(t, 1)
			if mode == "ack" {
				options, _ := inbox.Options()
				for index := 0; index < options.Capacity-2; index++ {
					if _, err := owner.Client().Send(context.Background(), Recipient{Type: "open_id", ID: "ou_fixture"}, "", fixtureText(t)); err != nil {
						t.Fatal(err)
					}
				}
			}
			cancel, done := startListen(owner.Client(), events)
			defer cancel()
			conn := nextConnection(t, peer)
			sendEvent(t, conn, "first")
			var old Incoming
			if mode == "event" {
				event, delivery := nextIncoming(t, events)
				old = event
				// Keep local evidence outstanding while returning it to the queue.
				_ = delivery.Retry()
				sendEvent(t, conn, "second")
			}
			var result listenResult
			select {
			case result = <-done:
			case <-time.After(2 * time.Second):
				cancel()
				t.Fatal("saturated bridge deadlocked")
			}
			if result.err == nil {
				t.Fatal("saturation was invisible")
			}
			noPositiveACK(t, peer)
			if mode == "ack" {
				status, _ := events.Inspect()
				if status.Outstanding != 0 || len(result.result.UnforwardedAcknowledgements()) == 0 {
					t.Fatal("unreserved ACK capability escaped or evidence lost")
				}
			} else {
				if len(result.result.UnforwardedEvents()) == 0 {
					t.Fatal("unforwarded event lost")
				}
				if _, err := old.Acknowledge(context.Background(), JSON{}); err == nil {
					t.Fatal("ended listener ACK succeeded")
				}
				_, delivery := nextIncoming(t, events)
				_ = delivery.Ack()
			}
		})
	}
}
func TestWebSocketUnclaimedExpiryAndReconnectBinding(t *testing.T) {
	peer := newWSPeer(t)
	owner, _, inbox := testOwner(t, peer.settings(), NativeOptions{})
	events := publicEvents(t, 8)
	cancel, done := startListen(owner.Client(), events)
	defer cancel()
	first := nextConnection(t, peer)
	sendEvent(t, first, "expired")
	_, code := peerACK(t, peer)
	if code != 500 {
		t.Fatal("unclaimed event got success")
	}
	old, delivery := nextIncoming(t, events)
	_ = delivery.Ack()
	if _, err := old.Acknowledge(context.Background(), JSON{}); err == nil {
		t.Fatal("expired ACK accepted")
	}
	snapshot, _ := old.Receipt().WaitReleased(context.Background())
	if snapshot.Err() == nil {
		t.Fatal("expiry missing evidence")
	}
	_ = consume(t, inbox).Ack()
	sendEvent(t, first, "disconnected")
	old, delivery = nextIncoming(t, events)
	_ = delivery.Ack()
	_ = first.conn.Close()
	second := nextConnection(t, peer)
	sendEvent(t, second, "current")
	current, delivery := nextIncoming(t, events)
	_ = delivery.Ack()
	if current.ConnectionGeneration() != 2 {
		t.Fatal("reconnect generation missing")
	}
	if _, err := old.Acknowledge(context.Background(), JSON{}); err == nil {
		t.Fatal("old ACK retargeted new connection")
	}
	if _, err := current.Acknowledge(context.Background(), JSON{}); err != nil {
		t.Fatal(err)
	}
	frame, code := peerACK(t, peer)
	if code != 200 || frame.headers["message_id"] != "current" {
		t.Fatal("ACK crossed connection")
	}
	finishListen(t, cancel, done)
}
