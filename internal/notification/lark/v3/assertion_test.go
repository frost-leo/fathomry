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

package lark

import (
	"context"
	"encoding/json"
	"github.com/frost-leo/fathomry/internal/fault"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestAssertionAudienceAndExplicitProxyRouting(t *testing.T) {
	peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/proxy/oauth/v3/token" {
			var body map[string]string
			if json.NewDecoder(r.Body).Decode(&body) != nil || body["client_assertion"] != "api-assertion" || r.Header.Get("X-Target-Service") != "accounts.invalid" || r.Header.Get("Authorization") != "" {
				t.Error("assertion proxy/audience custody lost")
			}
			_, _ = io.WriteString(w, `{"code":0,"access_token":"api-token","expires_in":7200}`)
			return
		}
		if r.URL.Path != "/open-apis/im/v1/messages" || r.Header.Get("X-Target-Service") != "" || r.Header.Get("Authorization") != "Bearer api-token" {
			t.Error("assertion route leaked into operation")
		}
		_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_fixture"}}`)
	})
	options := testOptions(peer)
	options.Profile = "client-assertion"
	options.AppSecret = ""
	options.ClientAssertion = "initial"
	options.OAuthBaseURL = "https://accounts.invalid"
	options.AssertionProxyURL = peer.server.URL
	options.AssertionProxyPrefix = "/proxy"
	options.Native.Credentials = func(_ context.Context, request CredentialRequest) (Credentials, error) {
		if request.Purpose != "api" || request.Audience != "accounts.invalid" {
			t.Error("wrong API assertion audience")
		}
		return Credentials{AppID: options.AppID, ClientAssertion: "api-assertion"}, nil
	}
	bound := bindTest(t, options, 1)
	receipt, err := bound.client.Send(context.Background(), fault.Correlation{Call: "proxy"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "", textContent(t))
	if result := resolved(t, receipt, err); result.Err() != nil || result.Attempts.Observed != 2 {
		t.Fatal("proxy API", result.Err())
	}
}
func TestWebSocketClientAssertionKeepsNativeAudience(t *testing.T) {
	var peer *socketPeer
	peer = newSocketPeer(t, false, func(w http.ResponseWriter, r *http.Request) {
		var body struct{ AppID, AppSecret, ClientAssertion string }
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.AppSecret != "" || body.ClientAssertion != "ws-assertion" || body.AppID != "app_fixture" || r.Header.Get("X-Target-Service") != "open.feishu.cn" {
			t.Error("mixed WSS assertion tuple")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": strings.Replace(peer.server.URL, "https:", "wss:", 1) + "/ws?service_id=42"}})
	})
	options := socketTestOptions(peer)
	options.Profile = "client-assertion"
	options.AppSecret = ""
	options.ClientAssertion = "initial"
	options.BaseURL = "https://open.feishu.cn"
	options.AssertionProxyURL = peer.server.URL
	options.Native.Credentials = func(_ context.Context, request CredentialRequest) (Credentials, error) {
		if request.Purpose != "websocket" || request.Audience != "open.feishu.cn" {
			t.Error("wrong WSS assertion audience")
		}
		return Credentials{AppID: options.AppID, TenantKey: options.TenantKey, ClientAssertion: "ws-assertion"}, nil
	}
	fixture := bindReceiverTest(t, options, 8, 4)
	cancel, done := startReceiver(t, fixture)
	_ = nextSocket(t, peer)
	waitRegressionState(t, fixture, done, func(stats WebSocketStats) bool { return stats.Connected && stats.Pongs > 0 })
	result := stopReceiver(t, cancel, done)
	if result.Outcome.Value.Stats().Connections != 1 {
		t.Fatal("assertion WSS did not connect")
	}
	drainSocketResults(t, fixture)
}
