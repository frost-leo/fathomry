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

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	lark "github.com/frost-leo/fathomry/internal/notification/lark/v3"
	"github.com/gorilla/websocket"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

type observedNativeLogger struct {
	*nativeProbeLogger
	observed chan struct{}
	once     sync.Once
}

func TestReceiverSummaryIncludesTerminalFailure(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case larkws.GenEndpointUri:
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": strings.Replace(server.URL, "https:", "wss:", 1) + "/ws?service_id=42"}})
		case "/ws":
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
			pong := larkws.Frame{Service: 42, Method: 0, Headers: []larkws.Header{{Key: "type", Value: "pong"}}}
			data, _ := pong.Marshal()
			if conn.WriteMessage(websocket.BinaryMessage, data) != nil {
				return
			}
			if conn.WriteMessage(websocket.BinaryMessage, []byte{0xff}) != nil {
				return
			}
			for {
				if _, _, err = conn.ReadMessage(); err != nil {
					return
				}
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	options := lark.OptionsV1{Name: "review-summary", Profile: "application", AppID: "fixture-app", AppSecret: "fixture-secret", BaseURL: server.URL, MaxActive: 1,
		RootCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})),
		WebSocket: &lark.WebSocketOptions{AllowedHosts: []string{strings.TrimPrefix(server.URL, "https://")}}}
	var output bytes.Buffer
	err := listen(context.Background(), options, time.Second, "", "", "", &output)
	if !errors.Is(err, lark.ErrProtocol) {
		t.Fatalf("rejecting control expected a terminal protocol failure, got %v", err)
	}
	decoder := json.NewDecoder(&output)
	for decoder.More() {
		var entry receiveEvidence
		if err := decoder.Decode(&entry); err != nil {
			t.Fatal(err)
		}
		if entry.Phase == "summary" {
			if !entry.Failed {
				t.Fatalf("fatal protocol failure was presented as failed=false (connections=%d pongs=%d)", entry.Connections, entry.Pongs)
			}
			return
		}
	}
	t.Fatal("no summary emitted")
}

func (logger *observedNativeLogger) Debug(ctx context.Context, args ...interface{}) {
	logger.nativeProbeLogger.Debug(ctx, args...)
	for _, arg := range args {
		values, ok := arg.([]interface{})
		if !ok {
			continue
		}
		for _, value := range values {
			if text, ok := value.(string); ok && strings.HasPrefix(text, "receive message,") {
				logger.once.Do(func() { close(logger.observed) })
			}
		}
	}
}

func TestNativeProbeCountsSDKLogs(t *testing.T) {
	if os.Getenv("FATHOMRY_NATIVE_PROBE_REGRESSION") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeProbeCountsSDKLogs$")
		command.Env = append(os.Environ(), "FATHOMRY_NATIVE_PROBE_REGRESSION=1")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("native probe regression: %v\n%s", err, output)
		}
		return
	}
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case larkws.GenEndpointUri:
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": strings.Replace(server.URL, "https:", "wss:", 1) + "/ws?service_id=42", "ClientConfig": map[string]int{"PingInterval": 1, "ReconnectCount": 0}}})
		case "/ws":
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			event := larkws.Frame{Service: 42, Method: 1, Headers: []larkws.Header{{Key: "type", Value: "event"}, {Key: "message_id", Value: "fixture-frame"}}, Payload: []byte(`{"schema":"2.0","header":{"event_id":"fixture-event","event_type":"im.message.receive_v1","app_id":"fixture-app","tenant_key":"fixture-tenant"},"event":{}}`)}
			data, _ := event.Marshal()
			if conn.WriteMessage(websocket.BinaryMessage, data) != nil {
				return
			}
			for {
				if _, _, err = conn.ReadMessage(); err != nil {
					return
				}
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	trust := x509.NewCertPool()
	trust.AddCert(server.Certificate())
	logger := &observedNativeLogger{nativeProbeLogger: &nativeProbeLogger{seen: make(chan struct{})}, observed: make(chan struct{})}
	client := larkws.NewClient("fixture-app", "fixture-secret", larkws.WithDomain(server.URL), larkws.WithHttpClient(server.Client()),
		larkws.WithWebSocketDialer(&websocket.Dialer{TLSClientConfig: &tls.Config{RootCAs: trust}, HandshakeTimeout: time.Second}), larkws.WithAutoReconnect(false), larkws.WithLogger(logger))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Start(ctx) }()
	select {
	case <-logger.observed:
		t.Log("The official SDK received the local event and logged it as a nested []interface{}.")
	case <-ctx.Done():
		t.Fatal("control did not observe the event receive log")
	}
	cancel()
	<-done
	if actual := logger.data.Load(); actual != 1 {
		t.Fatalf("nativeProbeLogger counted %d events; official SDK received 1", actual)
	}
}
