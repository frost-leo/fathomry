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
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWebSocketBootstrapCancellationAndErrorRefusal(t *testing.T) {
	for _, mode := range []string{"canceled", "malformed", "oversized", "auth"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{})
			peer := newSocketPeer(t, false, func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "canceled":
					_, _ = io.Copy(io.Discard, r.Body)
					_ = r.Body.Close()
					close(entered)
					<-r.Context().Done()
				case "malformed":
					_, _ = io.WriteString(w, `{"data":{}}`)
				case "oversized":
					_, _ = io.WriteString(w, strings.Repeat("x", 65<<10))
				case "auth":
					_, _ = io.WriteString(w, `{"code":403,"msg":"private-token"}`)
				}
			})
			fixture := bindReceiverTest(t, socketTestOptions(peer), 8, 2)
			cancel, done := startReceiver(t, fixture)
			if mode == "canceled" {
				<-entered
				cancel(context.Canceled)
			}
			select {
			case result := <-done:
				got := awaitSocketResult(t, result.receipt)
				if got.Err() == nil || got.Outcome.Value.Stats().Connections != 0 || peer.bootstraps.Load() != 1 {
					t.Fatal("bootstrap refusal leaked into retries or connection")
				}
			case <-time.After(2 * time.Second):
				cancel(context.Canceled)
				t.Fatal("bootstrap did not stop")
			}
			drainSocketResults(t, fixture)
		})
	}
}
func TestWebSocketConcurrentDecisionIsOneShot(t *testing.T) {
	peer := newSocketPeer(t, false, nil)
	fixture := bindReceiverTest(t, socketTestOptions(peer), 16, 4)
	cancel, done := startReceiver(t, fixture)
	socket := nextSocket(t, peer)
	_ = socket.send(eventFrame("concurrent", notificationEvent()))
	event := nextEvent(t, fixture)
	var accepted atomic.Int32
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			if _, err := event.Acknowledge(context.Background(), JSON{}); err == nil {
				accepted.Add(1)
			}
		})
	}
	group.Wait()
	if accepted.Load() != 1 {
		t.Fatal("one-shot acknowledgement claim violated")
	}
	_, ack := socketACK(t, peer)
	if ack.StatusCode != 200 {
		t.Fatal("claimed ACK lost")
	}
	stopReceiver(t, cancel, done)
	drainSocketResults(t, fixture)
}
func TestWebSocketFrameReadLimit(t *testing.T) {
	peer := newSocketPeer(t, false, nil)
	options := socketTestOptions(peer)
	options.WebSocket.MaxFrameBytes = 1024
	fixture := bindReceiverTest(t, options, 8, 2)
	cancel, done := startReceiver(t, fixture)
	socket := nextSocket(t, peer)
	socket.mu.Lock()
	_ = socket.conn.WriteMessage(websocket.BinaryMessage, []byte(strings.Repeat("x", 2048)))
	socket.mu.Unlock()
	select {
	case result := <-done:
		got := awaitSocketResult(t, result.receipt)
		if got.Err() == nil || got.Outcome.Value.Stats().Delivered != 0 || peer.bootstraps.Load() != 1 {
			t.Fatal("oversized message not contained")
		}
	case <-time.After(time.Second):
		cancel(context.Canceled)
		t.Fatal("read limit did not stop")
	}
	drainSocketResults(t, fixture)
}
func TestWebSocketSnapshotDoesNotAlias(t *testing.T) {
	peer := newSocketPeer(t, false, nil)
	fixture := bindReceiverTest(t, socketTestOptions(peer), 8, 2)
	cancel, done := startReceiver(t, fixture)
	socket := nextSocket(t, peer)
	_ = socket.send(eventFrame("snapshot", notificationEvent()))
	event := nextEvent(t, fixture)
	data := event.Event().JSONData()
	data[0] = 'X'
	if !json.Valid(event.Event().JSONData()) {
		t.Fatal("event bytes alias runtime")
	}
	status := fixture.receiver.Status()
	status.Generation = 999
	if fixture.receiver.Status().Generation != 1 {
		t.Fatal("status aliases runtime")
	}
	if _, err := event.Reject(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, ack := socketACK(t, peer)
	if ack.StatusCode != 500 {
		t.Fatal("explicit rejection not transmitted")
	}
	stopReceiver(t, cancel, done)
	drainSocketResults(t, fixture)
}
