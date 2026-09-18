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
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	lark "github.com/frost-leo/fathomry/internal/notification/lark/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

func TestIndependentCallerUploadsAndSendsNativeReport(t *testing.T) {
	var calls []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/open-apis/contact/v3/users/batch_get_id":
			_, _ = io.WriteString(w, `{"code":0,"data":{"user_list":[{"email":"receiver@example.test","user_id":"ou_receiver"}]}}`)
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_, _ = io.WriteString(w, `{"code":0,"tenant_access_token":"fixture-token","expire":7200}`)
		case "/open-apis/im/v1/images":
			_, _ = io.WriteString(w, `{"code":0,"data":{"image_key":"img_fixture"}}`)
		case "/open-apis/im/v1/files":
			_, _ = io.WriteString(w, `{"code":0,"data":{"file_key":"file_fixture"}}`)
		case "/open-apis/im/v1/messages":
			var envelope struct {
				ID      string `json:"receive_id"`
				Type    string `json:"msg_type"`
				Content string `json:"content"`
			}
			if json.NewDecoder(r.Body).Decode(&envelope) != nil || envelope.ID != "ou_receiver" {
				t.Error("wrong target or message")
			}
			if envelope.Type == "interactive" {
				for _, part := range []string{`"chart_spec"`, `"tag":"table"`, `"img_key":"img_fixture"`} {
					if !strings.Contains(envelope.Content, part) {
						t.Error("native report content lost")
					}
				}
			}
			_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_fixture"}}`)
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	options := lark.OptionsV1{Name: "caller", Profile: "application", AppID: "fixture-app", AppSecret: "fixture-secret", BaseURL: server.URL,
		RootCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))}
	var evidence bytes.Buffer
	if err := deliver(context.Background(), options, lark.Recipient{Type: "email", ID: "receiver@example.test"}, false, nil, &evidence); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 6 || bytes.Count(evidence.Bytes(), []byte("\n")) != 5 {
		t.Fatal("unexpected remote/evidence operation count")
	}
	if bytes.Contains(evidence.Bytes(), []byte("fixture-secret")) || bytes.Contains(evidence.Bytes(), []byte("receiver@example.test")) {
		t.Fatal("private data escaped diagnostics")
	}
}

// TestLiveApplication is deliberately opt-in. It performs real test-owned writes,
// recalls all acknowledged messages and reports any retained/unknown effects.
// Uploaded IM assets have no delete endpoint; no physical-purge claim is made.
func TestLiveApplication(t *testing.T) {
	path, recipient := os.Getenv("FATHOMRY_FEISHU_CONFIG"), os.Getenv("FATHOMRY_FEISHU_RECIPIENT")
	if path == "" || recipient == "" {
		t.Skip("requires explicitly authorized private config and recipient")
	}
	options, err := readSettings(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var evidence bytes.Buffer
	kind := os.Getenv("FATHOMRY_FEISHU_RECIPIENT_TYPE")
	if kind == "" {
		kind = "email"
	}
	err = deliver(ctx, options, lark.Recipient{Type: kind, ID: recipient}, true, nil, &evidence)
	t.Log("\n" + evidence.String())
	if err != nil {
		t.Log(liveFailureHint(err))
		t.Fatal("live exercise failed; bounded evidence above retains the failure")
	}
}
func liveFailureHint(err error) string {
	if errors.Is(err, errUnresolvedRecipient) {
		return "The exact supplied email did not resolve to an application-visible recipient."
	}
	var native *larkcore.CodeError
	if errors.As(err, &native) {
		if native.Code == 230001 && strings.Contains(native.Msg, "invalid receive_id") {
			return "The supplied recipient is invalid for this application."
		}
		if native.Code == 99991672 && strings.Contains(native.Msg, "cardkit:card:write") {
			return "The application lacks cardkit:card:write."
		}
	}
	return "No public error detail; inspect the bounded numeric evidence."
}
func TestLiveHintsNeverEchoNativeMessages(t *testing.T) {
	secret := "private-dynamic-token"
	for _, cause := range []*larkcore.CodeError{{Code: 230001, Msg: "invalid receive_id " + secret}, {Code: 99991672, Msg: "cardkit:card:write " + secret}, {Code: 999, Msg: secret}} {
		if strings.Contains(liveFailureHint(cause), secret) {
			t.Fatal("native error leaked into live diagnostics")
		}
	}
}
