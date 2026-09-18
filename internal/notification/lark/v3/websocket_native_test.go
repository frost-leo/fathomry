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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

type nativeSocketLogger struct {
	quietLogger
	panicked chan struct{}
}

func (logger nativeSocketLogger) Error(_ context.Context, args ...interface{}) {
	for _, arg := range args {
		if text, ok := arg.(string); ok && strings.Contains(text, "panicked") {
			select {
			case logger.panicked <- struct{}{}:
			default:
			}
		}
	}
}

// The native cache has no Close and retains a cron worker. Run the deliberately
// broken native controls in a subprocess rather than leak one in our test process.
func TestNativeWebSocketRejectingControls(t *testing.T) {
	if os.Getenv("FATHOMRY_NATIVE_WS_CONTROL") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeWebSocketRejectingControls$")
		command.Env = append(os.Environ(), "FATHOMRY_NATIVE_WS_CONTROL=1")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("native control failed: %v\n%s", err, output)
		}
		return
	}
	peer := newSocketPeer(t, false, nil)
	peer.bootstrap = func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": strings.Replace(peer.server.URL, "https:", "wss:", 1) + "/ws?service_id=42", "ClientConfig": map[string]int{"PingInterval": 1, "ReconnectCount": 0, "ReconnectInterval": 1}}})
	}
	var delivered atomic.Int32
	handler := dispatcher.NewEventDispatcher("", "").OnP2MessageReceiveV1(func(context.Context, *larkim.P2MessageReceiveV1) error { delivered.Add(1); return nil })
	settings := defaults(socketTestOptions(peer))
	trust, err := settings.tls()
	if err != nil {
		t.Fatal(err)
	}
	panicked := make(chan struct{}, 1)
	client := larkws.NewClient("app_fixture", "secret-app_fixture", larkws.WithDomain(peer.server.URL),
		larkws.WithHttpClient(peer.server.Client()), larkws.WithWebSocketDialer(&websocket.Dialer{TLSClientConfig: trust, HandshakeTimeout: time.Second}),
		larkws.WithEventHandler(handler), larkws.WithAutoReconnect(false), larkws.WithLogger(nativeSocketLogger{panicked: panicked}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Start(ctx) }()
	socket := nextSocket(t, peer)
	bad, _ := fragmentedFrame("native-invalid", 2, -1, notificationEvent())
	if err := socket.send(bad); err != nil {
		t.Fatal(err)
	}
	select {
	case <-panicked:
	case <-time.After(time.Second):
		t.Fatal("native invalid-index premise changed")
	}
	if delivered.Load() != 0 {
		t.Fatal("native bad frame unexpectedly dispatched")
	}
	cleanup, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	if err := client.CloseAndWait(cleanup); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	stack := make([]byte, 2<<20)
	count := runtime.Stack(stack, true)
	if !bytes.Contains(stack[:count], []byte("larksuite/oapi-sdk-go/v3/cache.(*cron).start")) {
		t.Fatal("native cache-retention premise changed; requalify")
	}
}
