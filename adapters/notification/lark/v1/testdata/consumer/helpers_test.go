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

package consumer

import (
	"context"
	"encoding/pem"
	lark "github.com/frost-leo/fathomry/adapters/notification/lark/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"net/http/httptest"
	"testing"
	"time"
)

// These aliases are all public types; they do not provide test replacements.
type Settings = lark.Settings
type WebSocketSettings = lark.WebSocketSettings
type Result = lark.Result
type Owner = lark.Owner
type Client = lark.Client
type Incoming = lark.Incoming
type NativeOptions = lark.NativeOptions
type JSON = lark.JSON
type Content = lark.Content
type Recipient = lark.Recipient

const WebSocketWritten = lark.WebSocketWritten

func fixtureSettings(server *httptest.Server) Settings {
	return Settings{Name: "fixture", Profile: "tenant-token", AppID: "app_fixture", TenantKey: "tenant_fixture", TenantToken: "fixture-token", TokenExpiresUnix: time.Now().Add(time.Hour).Unix(),
		BaseURL: server.URL, RootCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})),
		MaxActive: 2, QueuedCalls: 1, MaxRequestBytes: 16 << 10, MaxAssetBytes: 16 << 10, MaxResponseBytes: 32 << 10, Timeout: time.Second}
}
func testOwner(t testing.TB, settings Settings, hooks NativeOptions) (*Owner, *adapters.Runtime, *adapters.Inbox[Result]) {
	t.Helper()
	prepared, err := lark.Prepare(settings, hooks)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := prepared.Open(context.Background(), lark.Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := owner.Close(ctx); err != nil {
			t.Error(err)
		}
		if !owner.ShutdownComplete() {
			t.Error("source not released")
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		for {
			state, _ := inbox.Inspect()
			if state.Outstanding == 0 {
				break
			}
			delivery, err := inbox.NextReleased(ctx)
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
				break
			}
		}
	})
	return owner, runtime, inbox
}
func consume(t testing.TB, inbox *adapters.Inbox[Result]) adapters.Delivery[Result] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	value, err := inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func fixtureJSON(t testing.TB, value string) JSON {
	t.Helper()
	result, err := lark.NewJSON([]byte(value))
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func fixtureText(t testing.TB) Content {
	t.Helper()
	result, err := lark.Text("synthetic")
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func callbackBody(kind string) []byte {
	return []byte(`{"schema":"2.0","header":{"event_id":"event_fixture","event_type":"` + kind + `","app_id":"app_fixture","tenant_key":"tenant_fixture","create_time":"1700000000000"},"event":{"n":9007199254740993}}`)
}
