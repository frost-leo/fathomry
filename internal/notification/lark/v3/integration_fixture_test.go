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
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type capturedRequest struct {
	method, path, query, auth, contentType string
	body                                   []byte
}
type peer struct {
	server   *httptest.Server
	mu       sync.Mutex
	requests []capturedRequest
}

func newPeer(t testing.TB, handler http.HandlerFunc) *peer {
	t.Helper()
	peer := &peer{}
	peer.server = httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(io.LimitReader(request.Body, 2<<20))
		if err != nil {
			return
		}
		_ = request.Body.Close()
		request.Body = io.NopCloser(strings.NewReader(string(data)))
		peer.mu.Lock()
		peer.requests = append(peer.requests, capturedRequest{request.Method, request.URL.Path, request.URL.RawQuery, request.Header.Get("Authorization"), request.Header.Get("Content-Type"), data})
		peer.mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("X-Tt-Logid", "fixture-log")
		if handler != nil {
			handler(writer, request)
			return
		}
		defaultReply(writer, request)
	}))
	t.Cleanup(peer.server.Close)
	return peer
}
func defaultReply(writer http.ResponseWriter, request *http.Request) {
	if strings.HasSuffix(request.URL.Path, "/tenant_access_token/internal") {
		var body struct {
			ID     string `json:"app_id"`
			Secret string `json:"app_secret"`
		}
		_ = json.NewDecoder(request.Body).Decode(&body)
		if body.Secret != "secret-"+body.ID {
			_, _ = io.WriteString(writer, `{"code":10003,"msg":"invalid secret"}`)
			return
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "tenant_access_token": "token-" + body.ID, "expire": 7200})
		return
	}
	switch {
	case strings.HasPrefix(request.URL.Path, "/open-apis/im/v1/messages/") && request.Method == "GET" && strings.Count(strings.TrimPrefix(request.URL.Path, "/open-apis/im/v1/messages/"), "/") == 0:
		_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{"message_id": strings.TrimPrefix(request.URL.Path, "/open-apis/im/v1/messages/"), "chat_id": "oc_fixture", "msg_type": "text", "body": map[string]string{"content": `{"text":"Synthetic"}`}, "sender": map[string]string{"id": "ou_fixture"}, "deleted": false}}}})
	case strings.HasSuffix(request.URL.Path, "/resources/asset"):
		writer.Header().Set("Content-Type", "image/png")
		_, _ = io.WriteString(writer, "fixture-image")
	case strings.HasSuffix(request.URL.Path, "/images") && request.Method == "POST":
		_, _ = io.WriteString(writer, `{"code":0,"data":{"image_key":"img_fixture"}}`)
	case strings.HasSuffix(request.URL.Path, "/files") && request.Method == "POST":
		_, _ = io.WriteString(writer, `{"code":0,"data":{"file_key":"file_fixture"}}`)
	case request.URL.Path == "/open-apis/cardkit/v1/cards" && request.Method == "POST":
		_, _ = io.WriteString(writer, `{"code":0,"data":{"card_id":"card_fixture"}}`)
	case strings.HasSuffix(request.URL.Path, "/reactions") && request.Method == "POST":
		_, _ = io.WriteString(writer, `{"code":0,"data":{"reaction_id":"reaction_fixture"}}`)
	case request.Method == "GET":
		_, _ = io.WriteString(writer, `{"code":0,"data":{"items":[{"message_id":"om_existing","chat_id":"oc_fixture","msg_type":"text","body":{"content":"{\"text\":\"Synthetic\"}"},"sender":{"id":"ou_fixture"},"deleted":false}],"has_more":false}}`)
	case request.Method == "POST" && !strings.Contains(request.URL.Path, "cardkit"):
		_, _ = io.WriteString(writer, `{"code":0,"data":{"message_id":"om_created"}}`)
	default:
		_, _ = io.WriteString(writer, `{"code":0,"data":{}}`)
	}
}
func (peer *peer) snapshot() []capturedRequest {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return append([]capturedRequest(nil), peer.requests...)
}
func testOptions(peer *peer) OptionsV1 {
	return OptionsV1{Name: "feishu", Profile: "application", AppID: "app_fixture", AppSecret: "secret-app_fixture", BaseURL: peer.server.URL,
		RootCAPEM:     string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: peer.server.Certificate().Raw})),
		MaxAssetBytes: 256 << 10, MaxResponseBytes: 256 << 10, MaxRequestBytes: 64 << 10, Timeout: 2 * time.Second}
}

type bound struct {
	client   *Client
	selected resource.Selection[Source]
	assembly *resource.Assembly
	inbox    *invocation.Inbox[Result]
}

func bindTest(t testing.TB, options OptionsV1, capacity int) *bound {
	t.Helper()
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "feishu-test", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](capacity, EvidenceBytesV1(options)*int64(capacity))
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := &bound{client, selected, assembly, inbox}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := assembly.Close(ctx); err != nil {
			t.Error(err)
		}
		drain(t, inbox)
	})
	return result
}
func drain(t testing.TB, inbox *invocation.Inbox[Result]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for inbox.Usage().Outstanding > 0 {
		record, err := inbox.Next(ctx)
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
func resolved(t testing.TB, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func textContent(t testing.TB) Content {
	t.Helper()
	content, err := Text("Synthetic only.")
	if err != nil {
		t.Fatal(err)
	}
	return content
}
func jsonValue(t testing.TB, raw string) JSON {
	t.Helper()
	value, err := NewJSON([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func cardContentTest(t testing.TB) Content {
	t.Helper()
	value, err := Card([]byte(`{"schema":"2.0","body":{"elements":[{"tag":"markdown","element_id":"intro","content":"Synthetic only."}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func sendTest(t testing.TB, client *Client, call string) invocation.Result[Result] {
	t.Helper()
	receipt, err := client.Send(context.Background(), fault.Correlation{Call: call}, Recipient{Type: "email", ID: "recipient@example.test"}, call, textContent(t))
	return resolved(t, receipt, err)
}
