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

package lark

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

func TestWebSocketReceiveExplicitAckAndIndependentEvidence(t *testing.T) {
	peer := newSocketPeer(t, false, nil)
	fixture := bindReceiverTest(t, socketTestOptions(peer), 16, 4)
	cancel, done := startReceiver(t, fixture)
	socket := nextSocket(t, peer)
	if err := socket.send(eventFrame("frame_one", notificationEvent())); err != nil {
		t.Fatal(err)
	}
	event := nextEvent(t, fixture)
	if event.Event().ID() != "event_fixture" || event.FrameID() != "frame_one" || event.Generation() != 1 {
		t.Fatal("event identity lost")
	}
	select {
	case <-peer.acks:
		t.Fatal("event handoff implicitly acknowledged")
	case <-time.After(20 * time.Millisecond):
	}
	reply := jsonValue(t, `{"toast":{"type":"info","content":"Synthetic"}}`)
	receipt, err := event.Acknowledge(context.Background(), reply)
	if err != nil {
		t.Fatal(err)
	}
	result := awaitSocketResult(t, receipt)
	ack, present := result.Outcome.Value.Acknowledgement()
	if result.Err() != nil || !present || ack.Effect() != WebSocketWritten || ack.Code() != 200 || !result.Nested || result.Context.Correlation.Parent != "listener" {
		t.Fatal("ACK evidence lost")
	}
	frame, response := socketACK(t, peer)
	if frame.SeqID != 7 || frame.LogID != 8 || response.StatusCode != 200 || string(response.Data) != reply.value {
		t.Fatal("native ACK routing or response encoding changed")
	}
	if _, err = event.Acknowledge(context.Background(), JSON{}); !errors.Is(err, ErrAcknowledgement) {
		t.Fatal("duplicate ACK accepted")
	}
	summary := stopReceiver(t, cancel, done)
	if !errors.Is(summary.Err(), context.Canceled) || summary.Outcome.Value.Stats().Delivered != 1 || summary.Outcome.Value.Stats().AckWritten != 1 || fixture.receiver.Status().Connected {
		t.Fatal("session evidence/lifetime differs")
	}
	drainSocketResults(t, fixture)
}
func TestWebSocketExpiryNegativeAckAndStaleGeneration(t *testing.T) {
	peer := newSocketPeer(t, false, nil)
	fixture := bindReceiverTest(t, socketTestOptions(peer), 16, 4)
	cancel, done := startReceiver(t, fixture)
	socket := nextSocket(t, peer)
	_ = socket.send(eventFrame("expired", notificationEvent()))
	event := nextEvent(t, fixture)
	_, response := socketACK(t, peer)
	expired := awaitSocketResult(t, event.Receipt())
	ack, _ := expired.Outcome.Value.Acknowledgement()
	if response.StatusCode != 500 || ack.Code() != 500 || !errors.Is(expired.Err(), ErrAcknowledgement) {
		t.Fatal("silence became successful acknowledgement")
	}
	_ = socket.send(eventFrame("disconnected", notificationEvent()))
	old := nextEvent(t, fixture)
	_ = socket.conn.Close()
	newer := nextSocket(t, peer)
	if result := awaitSocketResult(t, old.Receipt()); result.Outcome.Value.Stats().Connected {
		t.Fatal("abandoned ACK reported connection")
	}
	if _, err := old.Acknowledge(context.Background(), JSON{}); !errors.Is(err, ErrAcknowledgement) {
		t.Fatal("old generation ACK accepted")
	}
	_ = newer.send(eventFrame("redelivered", notificationEvent()))
	current := nextEvent(t, fixture)
	if current.Generation() != 2 {
		t.Fatal("reconnection generation lost")
	}
	if _, err := current.Acknowledge(context.Background(), JSON{}); err != nil {
		t.Fatal(err)
	}
	frame, response := socketACK(t, peer)
	if larkws.Headers(frame.Headers).GetString("message_id") != "redelivered" || response.StatusCode != 200 {
		t.Fatal("old ACK crossed onto new socket")
	}
	stopReceiver(t, cancel, done)
	drainSocketResults(t, fixture)
}
func TestWebSocketEvidenceSaturationNeverAcknowledgesSuccess(t *testing.T) {
	for _, capacity := range []int{1, 4} {
		peer := newSocketPeer(t, false, nil)
		fixture := bindReceiverTest(t, socketTestOptions(peer), capacity, 1)
		cancel, done := startReceiver(t, fixture)
		socket := nextSocket(t, peer)
		_ = socket.send(eventFrame("first", notificationEvent()))
		if capacity > 1 {
			_ = socket.send(eventFrame("second", notificationEvent()))
		}
		select {
		case result := <-done:
			got := awaitSocketResult(t, result.receipt)
			if !errors.Is(got.Err(), invocation.ErrEvidence) {
				t.Fatal("saturation did not stop before ACK")
			}
		case <-time.After(2 * time.Second):
			cancel(context.Canceled)
			t.Fatal("saturated receiver kept running")
		}
		select {
		case frame := <-peer.acks:
			t.Fatalf("saturation emitted an ACK method %d", frame.Method)
		default:
		}
		drainSocketResults(t, fixture)
		if fixture.events.Usage().Outstanding > 0 {
			_ = nextEvent(t, fixture)
		}
	}
}
func TestWebSocketMissingPongBoundedReconnectAndBorrowedOwnership(t *testing.T) {
	peer := newSocketPeer(t, true, nil)
	options := socketTestOptions(peer)
	fixture := bindReceiverTest(t, options, 8, 2)
	borrowed := resource.Borrow("borrowed", fixture.assembly, fixture.selected)
	child, err := resource.Assemble(context.Background(), context.Background(), "borrow", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	inbox, _ := invocation.NewInbox[WebSocketResult](2, 2*WebSocketEvidenceBytes())
	alias, err := BindReceiver(child, borrowed, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan listened, 1)
	go func() {
		receipt, err := alias.Listen(ctx, fault.Correlation{Call: "borrowed-listen"}, fixture.events)
		done <- listened{receipt, err}
	}()
	_ = nextSocket(t, peer)
	duplicateCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err = fixture.receiver.Listen(duplicateCtx, fault.Correlation{Call: "duplicate"}, fixture.events); err == nil {
		t.Fatal("borrow created another root quota")
	}
	select {
	case result := <-done:
		got := awaitSocketResult(t, result.receipt)
		if !errors.Is(got.Err(), ErrLimit) || got.Outcome.Value.Stats().ConnectAttempts != 2 {
			t.Fatal("reconnect budget lost")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("missing pong did not terminate")
	}
	if err = child.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
