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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	lark "github.com/frost-leo/fathomry/adapters/notification/lark/v1"
)

func TestIndependentLifecycleExerciseAndFailureCleanup(t *testing.T) {
	for _, mode := range []string{"success", "stale-card", "corrupt-file", "denied-cardkit", "missing-send-identity"} {
		t.Run(mode, func(t *testing.T) {
			_, _, imageBytes, fileBytes, err := report("")
			if err != nil {
				t.Fatal(err)
			}
			type message struct {
				kind, content string
				deleted       bool
			}
			messages := map[string]*message{}
			counts := map[string]int{}
			var mutex sync.Mutex
			var sequence int
			server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				mutex.Lock()
				defer mutex.Unlock()
				counts[request.Method+" "+request.URL.Path]++
				writer.Header().Set("Content-Type", "application/json")
				respond := func(data any) { _ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "data": data}) }
				create := func(kind, content string) {
					sequence++
					id := fmt.Sprintf("om_fixture_%d", sequence)
					messages[id] = &message{kind: kind, content: content}
					if mode == "missing-send-identity" {
						respond(map[string]any{})
						return
					}
					respond(map[string]string{"message_id": id})
				}
				switch request.URL.Path {
				case "/open-apis/auth/v3/tenant_access_token/internal":
					_, _ = io.WriteString(writer, `{"code":0,"tenant_access_token":"fixture-token","expire":7200}`)
					return
				case "/open-apis/contact/v3/users/batch_get_id":
					_, _ = io.WriteString(writer, `{"code":0,"data":{"user_list":[{"email":"receiver@example.test","user_id":"ou_receiver"}]}}`)
					return
				case "/open-apis/im/v1/images", "/open-apis/im/v1/files":
					field, key, expected := "image", "image_key", imageBytes
					if strings.HasSuffix(request.URL.Path, "/files") {
						field, key, expected = "file", "file_key", fileBytes
					}
					if err := request.ParseMultipartForm(1 << 20); err != nil {
						t.Error(err)
						writer.WriteHeader(400)
						return
					}
					defer request.MultipartForm.RemoveAll()
					file, _, err := request.FormFile(field)
					if err != nil {
						t.Error(err)
						writer.WriteHeader(400)
						return
					}
					body, err := io.ReadAll(file)
					_ = file.Close()
					if err != nil || !bytes.Equal(body, expected) {
						t.Error("upload bytes changed")
					}
					respond(map[string]string{key: field + "_fixture"})
					return
				case "/open-apis/im/v1/messages":
					var input struct {
						ID      string `json:"receive_id"`
						Kind    string `json:"msg_type"`
						Content string `json:"content"`
						UUID    string `json:"uuid"`
					}
					if request.Method != http.MethodPost || json.NewDecoder(request.Body).Decode(&input) != nil || input.ID != "ou_receiver" || input.UUID == "" {
						t.Error("invalid message request")
						writer.WriteHeader(400)
						return
					}
					create(input.Kind, input.Content)
					return
				case "/open-apis/cardkit/v1/cards":
					if mode == "denied-cardkit" {
						_, _ = io.WriteString(writer, `{"code":99991672,"msg":"synthetic-private-cardkit-denied"}`)
					} else {
						respond(map[string]string{"card_id": "card_fixture"})
					}
					return
				case "/open-apis/cardkit/v1/cards/card_fixture/elements/intro", "/open-apis/cardkit/v1/cards/card_fixture":
					body, _ := io.ReadAll(request.Body)
					if !json.Valid(body) {
						t.Error("invalid CardKit body")
					}
					respond(nil)
					return
				}
				const prefix = "/open-apis/im/v1/messages/"
				if !strings.HasPrefix(request.URL.Path, prefix) {
					t.Error("unexpected route")
					writer.WriteHeader(404)
					return
				}
				parts := strings.Split(strings.TrimPrefix(request.URL.Path, prefix), "/")
				entry := messages[parts[0]]
				if entry == nil {
					t.Error("unknown message identity")
					writer.WriteHeader(404)
					return
				}
				if len(parts) == 3 && parts[1] == "resources" {
					if request.Method != http.MethodGet || parts[2] != "file_fixture" || request.URL.Query().Get("type") != "file" {
						t.Error("wrong resource route")
					}
					writer.Header().Set("Content-Type", "application/octet-stream")
					body := fileBytes
					if mode == "corrupt-file" {
						body = []byte("corrupt")
					}
					_, _ = writer.Write(body)
					return
				}
				if len(parts) == 2 && parts[1] == "reply" {
					var input struct {
						Kind    string `json:"msg_type"`
						Content string `json:"content"`
					}
					if json.NewDecoder(request.Body).Decode(&input) != nil || input.Kind != "post" {
						t.Error("rich-post reply changed")
					}
					create(input.Kind, input.Content)
					return
				}
				if len(parts) != 1 {
					t.Error("unexpected message route")
					writer.WriteHeader(404)
					return
				}
				switch request.Method {
				case http.MethodGet:
					respond(map[string]any{"items": []any{map[string]any{"message_id": parts[0], "msg_type": entry.kind, "chat_id": "oc_fixture", "body": map[string]string{"content": entry.content}, "deleted": entry.deleted}}})
				case http.MethodPatch, http.MethodPut:
					var input struct {
						Content string `json:"content"`
					}
					if json.NewDecoder(request.Body).Decode(&input) != nil || input.Content == "" {
						t.Error("empty update")
					}
					if !(mode == "stale-card" && request.Method == http.MethodPatch) {
						entry.content = input.Content
					}
					respond(nil)
				case http.MethodDelete:
					if entry.deleted {
						t.Error("duplicate recall")
					}
					entry.deleted = true
					respond(nil)
				default:
					t.Error("unexpected method")
					writer.WriteHeader(405)
				}
			}))
			defer server.Close()
			options := lark.Settings{Name: "exercise-fixture", Profile: "application", AppID: "fixture-app", AppSecret: "fixture-secret", BaseURL: server.URL,
				RootCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))}
			var output bytes.Buffer
			err = deliver(context.Background(), options, lark.Recipient{Type: "email", ID: "receiver@example.test"}, true, nil, &output)
			if (err == nil) != (mode == "success") {
				t.Fatal("wrong exercise disposition", err)
			}
			mutex.Lock()
			defer mutex.Unlock()
			for _, entry := range messages {
				if entry.deleted != (mode != "missing-send-identity") {
					t.Error("cleanup lost a known message or invented an unknown identity")
				}
			}
			if mode == "success" && (len(messages) != 5 || counts["POST /open-apis/im/v1/messages"] != 4 || counts["PUT /open-apis/cardkit/v1/cards/card_fixture"] != 1) {
				t.Fatal("full lifecycle did not run")
			}
			if mode == "missing-send-identity" && (len(messages) != 1 || counts["POST /open-apis/im/v1/messages"] != 1) {
				t.Fatal("unknown send was repeated")
			}
			if strings.Contains(output.String(), "fixture-secret") || strings.Contains(output.String(), "receiver@example.test") || strings.Contains(output.String(), "synthetic-private") {
				t.Fatal("private data escaped diagnostics")
			}
		})
	}
}
