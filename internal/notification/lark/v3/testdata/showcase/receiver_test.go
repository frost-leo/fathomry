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
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lark "github.com/frost-leo/fathomry/internal/notification/lark/v3"
	"github.com/gorilla/websocket"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

func TestReceiverCallerPersistsBeforeExplicitAcknowledgement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "event.json")
	var server *httptest.Server
	var workers sync.WaitGroup
	acknowledged := make(chan bool, 1)
	var rejected atomic.Int32
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_, _ = io.WriteString(w, `{"code":0,"tenant_access_token":"fixture-token","expire":7200}`)
		case "/open-apis/contact/v3/users/batch_get_id":
			_, _ = io.WriteString(w, `{"code":0,"data":{"user_list":[{"email":"receiver@example.test","user_id":"ou_fixture"}]}}`)
		case larkws.GenEndpointUri:
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": strings.Replace(server.URL, "https:", "wss:", 1) + "/ws?service_id=42"}})
		case "/ws":
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				return
			}
			workers.Add(1)
			defer workers.Done()
			defer conn.Close()
			sent := false
			for {
				_, data, err := conn.ReadMessage()
				if err != nil {
					return
				}
				var frame larkws.Frame
				if frame.Unmarshal(data) != nil {
					return
				}
				if frame.Method == 0 {
					pong := larkws.Frame{Service: 42, Method: 0, Headers: []larkws.Header{{Key: "type", Value: "pong"}}}
					encoded, _ := pong.Marshal()
					_ = conn.WriteMessage(websocket.BinaryMessage, encoded)
					if sent {
						continue
					}
					sent = true
					event := larkws.Frame{Service: 42, Method: 1, Headers: []larkws.Header{{Key: "type", Value: "event"}, {Key: "message_id", Value: "frame_fixture"}},
						Payload: []byte(`{"schema":"2.0","header":{"event_id":"event_fixture","event_type":"im.message.receive_v1","app_id":"app_fixture","tenant_key":"tenant_fixture"},"event":{"sender":{"sender_id":{"open_id":"ou_fixture"}},"message":{"message_type":"text","content":"{\"text\":\"probe\"}"}}}`)}
					previous := event
					previous.Payload = bytes.ReplaceAll(event.Payload, []byte(`\"probe\"`), []byte(`\"earlier-text\"`))
					encoded, _ = previous.Marshal()
					_ = conn.WriteMessage(websocket.BinaryMessage, encoded)
					encoded, _ = event.Marshal()
					_ = conn.WriteMessage(websocket.BinaryMessage, encoded)
				} else {
					var response larkws.Response
					_ = json.Unmarshal(frame.Payload, &response)
					if response.StatusCode == 500 {
						rejected.Add(1)
						continue
					}
					stored, err := os.ReadFile(path)
					acknowledged <- response.StatusCode == 200 && err == nil && bytes.Contains(stored, []byte("event_fixture"))
				}
			}
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	options := lark.OptionsV1{Name: "receiver-test", Profile: "application", AppID: "app_fixture", AppSecret: "fixture-secret", BaseURL: server.URL, MaxActive: 1,
		RootCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})),
		WebSocket: &lark.WebSocketOptions{AllowedHosts: []string{strings.TrimPrefix(server.URL, "https://")}}}
	var output bytes.Buffer
	if err := listen(context.Background(), options, time.Second, "receiver@example.test", "probe", path, &output); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	if rejected.Load() != 1 {
		t.Fatal("unmatched text was not rejected before continuing")
	}
	select {
	case persisted := <-acknowledged:
		if !persisted {
			t.Fatal("ACK preceded the synced event file")
		}
	default:
		t.Fatal("ACK was not observed")
	}
	if strings.Contains(output.String(), "ou_fixture") || strings.Contains(output.String(), "event_fixture") {
		t.Fatal("private event identity entered diagnostics")
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("event file privacy changed")
	}
}
func TestListenerRequiresConsistentExplicitProbeInputs(t *testing.T) {
	for _, args := range [][]string{
		{"-listen"}, {"-listen", "-send"}, {"-listen", "-config", "/does-not-exist", "-duration=0"},
	} {
		if err := run(context.Background(), args, io.Discard); err == nil {
			t.Fatal("invalid listener invocation accepted")
		}
	}
}
