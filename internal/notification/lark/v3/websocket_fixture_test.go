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
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/gorilla/websocket"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

type socketPeer struct {
	server            *httptest.Server
	connected         chan *peerSocket
	acks              chan larkws.Frame
	bootstraps, pings atomic.Int32
	mu                sync.Mutex
	sockets           []*peerSocket
	workers           sync.WaitGroup
	noPong            bool
	bootstrap         http.HandlerFunc
}
type peerSocket struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (peer *peerSocket) send(frame larkws.Frame) error {
	data, err := frame.Marshal()
	if err != nil {
		return err
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	_ = peer.conn.SetWriteDeadline(time.Now().Add(time.Second))
	return peer.conn.WriteMessage(websocket.BinaryMessage, data)
}
func newSocketPeer(t testing.TB, noPong bool, bootstrap http.HandlerFunc) *socketPeer {
	t.Helper()
	peer := &socketPeer{connected: make(chan *peerSocket, 16), acks: make(chan larkws.Frame, 64), noPong: noPong, bootstrap: bootstrap}
	peer.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == larkws.GenEndpointUri {
			peer.bootstraps.Add(1)
			w.Header().Set("Content-Type", "application/json")
			if peer.bootstrap != nil {
				peer.bootstrap(w, r)
				return
			}
			var input larkws.BootstrapRequest
			if json.NewDecoder(r.Body).Decode(&input) != nil || input.AppID != "app_fixture" || input.AppSecret != "secret-app_fixture" {
				w.WriteHeader(403)
				_, _ = io.WriteString(w, `{"code":403}`)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": strings.Replace(peer.server.URL, "https:", "wss:", 1) + "/ws?service_id=42&ticket=private-ticket", "ClientConfig": map[string]int{"ReconnectCount": -1, "ReconnectInterval": 0}}})
			return
		}
		if r.URL.Path != "/ws" {
			w.WriteHeader(404)
			return
		}
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		socket := &peerSocket{conn: conn}
		peer.mu.Lock()
		peer.sockets = append(peer.sockets, socket)
		peer.workers.Add(1)
		peer.mu.Unlock()
		defer peer.workers.Done()
		defer conn.Close()
		peer.connected <- socket
		for {
			kind, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if kind != websocket.BinaryMessage {
				continue
			}
			var frame larkws.Frame
			if frame.Unmarshal(data) != nil {
				return
			}
			if frame.Method == int32(larkws.FrameTypeControl) {
				peer.pings.Add(1)
				if !peer.noPong {
					if err := socket.send(larkws.Frame{Service: 42, Method: int32(larkws.FrameTypeControl), Headers: []larkws.Header{{Key: "type", Value: "pong"}}}); err != nil {
						return
					}
				}
			} else {
				select {
				case peer.acks <- frame:
				default:
					t.Error("fixture ACK capacity exceeded")
					return
				}
			}
		}
	}))
	t.Cleanup(func() {
		peer.server.CloseClientConnections()
		peer.mu.Lock()
		for _, socket := range peer.sockets {
			_ = socket.conn.Close()
		}
		peer.mu.Unlock()
		peer.server.Close()
		peer.workers.Wait()
	})
	return peer
}
func socketTestOptions(peer *socketPeer) OptionsV1 {
	return OptionsV1{Name: "feishu", Profile: "application", AppID: "app_fixture", AppSecret: "secret-app_fixture", TenantKey: "tenant_fixture",
		BaseURL: peer.server.URL, RootCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: peer.server.Certificate().Raw})), MaxActive: 1,
		MaxRequestBytes: 64 << 10, MaxAssetBytes: 64 << 10, MaxResponseBytes: 64 << 10, Timeout: time.Second,
		WebSocket: &WebSocketOptions{AllowedHosts: []string{strings.TrimPrefix(peer.server.URL, "https://")}, MaxPending: 4, MaxAssemblies: 2, MaxFragments: 4,
			MaxFrameBytes: 64 << 10, MaxMessageBytes: 64 << 10, MaxFragmentBytes: 128 << 10, FragmentTimeout: 100 * time.Millisecond, AckTimeout: 250 * time.Millisecond,
			WriteTimeout: 100 * time.Millisecond, PingInterval: 50 * time.Millisecond, PongTimeout: 150 * time.Millisecond, MaxConnectAttempts: 2,
			ReconnectMin: 5 * time.Millisecond, ReconnectMax: 10 * time.Millisecond}}
}

type receiverFixture struct {
	receiver *Receiver
	selected resource.Selection[Source]
	assembly *resource.Assembly
	results  *invocation.Inbox[WebSocketResult]
	events   *invocation.Inbox[WebSocketEvent]
}

func bindReceiverTest(t testing.TB, options OptionsV1, resultCapacity, eventCapacity int) *receiverFixture {
	t.Helper()
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "websocket-test", selected)
	if err != nil {
		t.Fatal(err)
	}
	results, err := invocation.NewInbox[WebSocketResult](resultCapacity, int64(resultCapacity)*WebSocketEvidenceBytes())
	if err != nil {
		t.Fatal(err)
	}
	events, err := invocation.NewInbox[WebSocketEvent](eventCapacity, int64(eventCapacity)*WebSocketEventBytesV1(options))
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := BindReceiver(assembly, selected, results, nil)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &receiverFixture{receiver, selected, assembly, results, events}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := assembly.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return fixture
}

type listened struct {
	receipt *invocation.Receipt[WebSocketResult]
	err     error
}

func startReceiver(t testing.TB, fixture *receiverFixture) (context.CancelCauseFunc, <-chan listened) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan listened, 1)
	go func() {
		receipt, err := fixture.receiver.Listen(ctx, fault.Correlation{Call: "listener", Owner: "test-run"}, fixture.events)
		done <- listened{receipt, err}
	}()
	t.Cleanup(func() { cancel(context.Canceled) })
	return cancel, done
}
func nextSocket(t testing.TB, peer *socketPeer) *peerSocket {
	t.Helper()
	select {
	case socket := <-peer.connected:
		return socket
	case <-time.After(3 * time.Second):
		t.Fatal("socket did not connect")
		return nil
	}
}
func nextEvent(t testing.TB, fixture *receiverFixture) WebSocketEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	record, err := fixture.events.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := record.Receipt().WaitReleased(ctx)
	if err != nil || result.Err() != nil {
		t.Fatal("event did not complete")
	}
	if result.Context.Correlation.Parent != "listener" || !result.Nested {
		t.Fatal("event lost its parent association")
	}
	value := result.Outcome.Value
	if err = record.Release(); err != nil {
		t.Fatal(err)
	}
	return value
}
func awaitSocketResult(t testing.TB, receipt *invocation.Receipt[WebSocketResult]) invocation.Result[WebSocketResult] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func stopReceiver(t testing.TB, cancel context.CancelCauseFunc, done <-chan listened) invocation.Result[WebSocketResult] {
	t.Helper()
	cancel(context.Canceled)
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		return awaitSocketResult(t, result.receipt)
	case <-time.After(3 * time.Second):
		t.Fatal("receiver did not join")
		return invocation.Result[WebSocketResult]{}
	}
}
func eventFrame(id string, payload []byte) larkws.Frame {
	return larkws.Frame{SeqID: 7, LogID: 8, Service: 42, Method: 1, Headers: []larkws.Header{{Key: "type", Value: "event"}, {Key: "message_id", Value: id}}, Payload: payload}
}
func socketACK(t testing.TB, peer *socketPeer) (larkws.Frame, larkws.Response) {
	t.Helper()
	select {
	case frame := <-peer.acks:
		var response larkws.Response
		if json.Unmarshal(frame.Payload, &response) != nil {
			t.Fatal("invalid ACK payload")
		}
		return frame, response
	case <-time.After(2 * time.Second):
		t.Fatal("ACK missing")
		return larkws.Frame{}, larkws.Response{}
	}
}
func drainSocketResults(t testing.TB, fixture *receiverFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for fixture.results.Usage().Outstanding > 0 {
		record, err := fixture.results.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = record.Receipt().WaitReleased(ctx); err != nil {
			t.Fatal(err)
		}
		if err = record.Release(); err != nil {
			t.Fatal(err)
		}
	}
}
