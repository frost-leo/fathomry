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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	lark "github.com/frost-leo/fathomry/adapters/notification/lark/v1"
	"github.com/gorilla/websocket"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

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
	options := lark.Settings{Name: "review-summary", Profile: "application", AppID: "fixture-app", AppSecret: "fixture-secret", BaseURL: server.URL, MaxActive: 1,
		RootCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})),
		WebSocket: &lark.WebSocketSettings{AllowedHosts: []string{strings.TrimPrefix(server.URL, "https://")}}}
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
