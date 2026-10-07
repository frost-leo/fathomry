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

package trino_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// wirePeer exercises the selected native client, not a Trino coordinator or a
// connector. Readiness requests are counted separately from application SQL.
type wirePeer struct {
	server    *httptest.Server
	readiness atomic.Int64
	posts     atomic.Int64
	gets      atomic.Int64
	deletes   atomic.Int64
	mu        sync.Mutex
	bodies    []string
	onReady   func(http.ResponseWriter, *http.Request)
}

func newWirePeer(t testing.TB, serve func(http.ResponseWriter, *http.Request, []byte)) *wirePeer {
	t.Helper()
	return makeWirePeer(t, false, serve)
}

func makeWirePeer(t testing.TB, secure bool, serve func(http.ResponseWriter, *http.Request, []byte)) *wirePeer {
	t.Helper()
	peer := new(wirePeer)
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(io.LimitReader(request.Body, (4<<20)+1))
		if err != nil || len(body) > 4<<20 {
			t.Error("bounded fixture request read failed")
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		if request.Method == http.MethodPost && string(body) == "SELECT version()" {
			peer.readiness.Add(1)
			if peer.onReady != nil {
				peer.onReady(writer, request)
				return
			}
			writePage(t, writer, map[string]any{"id": "ready", "columns": []any{wireColumn("version", "varchar", "varchar", longArgument(2147483647))}, "data": [][]string{{"483"}}})
			return
		}
		switch request.Method {
		case http.MethodPost:
			peer.posts.Add(1)
			peer.mu.Lock()
			peer.bodies = append(peer.bodies, string(body))
			peer.mu.Unlock()
			if request.Header.Get("X-Trino-Query-Data-Encoding") != "" || request.Header.Get("X-Trino-Prepared-Statement") != "" {
				t.Error("native submission enabled spooling or managed preparation")
			}
		case http.MethodGet:
			peer.gets.Add(1)
		case http.MethodDelete:
			peer.deletes.Add(1)
		}
		serve(writer, request, body)
	})
	if secure {
		peer.server = httptest.NewTLSServer(handler)
	} else {
		peer.server = httptest.NewServer(handler)
	}
	t.Cleanup(peer.server.Close)
	return peer
}

func (peer *wirePeer) next(query string, token int) string {
	return peer.server.URL + "/v1/statement/executing/" + query + "/slug/" + strconv.Itoa(token)
}

func (peer *wirePeer) statements() []string {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return append([]string(nil), peer.bodies...)
}

func writePage(t testing.TB, writer http.ResponseWriter, page any) {
	t.Helper()
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(page); err != nil {
		t.Error("fixture response write failed")
	}
}

func wireColumn(name, text, raw string, arguments ...any) map[string]any {
	if arguments == nil {
		arguments = []any{}
	}
	return map[string]any{"name": name, "type": text, "typeSignature": map[string]any{"rawType": raw, "arguments": arguments}}
}

func longArgument(value int64) map[string]any {
	return map[string]any{"kind": "LONG", "value": value}
}

func typeArgument(raw string, arguments ...any) map[string]any {
	if arguments == nil {
		arguments = []any{}
	}
	return map[string]any{"kind": "TYPE", "value": map[string]any{"rawType": raw, "arguments": arguments}}
}
