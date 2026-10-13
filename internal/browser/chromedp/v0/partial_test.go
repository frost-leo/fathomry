/*
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

package chromedp

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/chromedp/cdproto"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/gorilla/websocket"
)

// This is a protocol peer, not real Chrome qualification.
func TestPartialContextCreationFailureKeepsCleanupEvidence(t *testing.T) {
	var disposed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close()
		for {
			var request struct {
				ID     int64           `json:"id"`
				Method string          `json:"method"`
				Params json.RawMessage `json:"params"`
			}
			if err := connection.ReadJSON(&request); err != nil {
				return
			}
			response := map[string]any{"id": request.ID}
			switch request.Method {
			case "Browser.getVersion":
				response["result"] = map[string]any{"protocolVersion": "1.3", "product": "Chrome/150.0.0.0"}
			case "Target.createBrowserContext":
				response["result"] = map[string]any{"browserContextId": "owned-fixture"}
			case "Target.createTarget":
				response["error"] = map[string]any{"code": -32000, "message": "synthetic target creation failure"}
			case "Target.getBrowserContexts":
				ids := []string{"owned-fixture"}
				if disposed.Load() {
					ids = nil
				}
				response["result"] = map[string]any{"browserContextIds": ids}
			case "Target.disposeBrowserContext":
				var params struct {
					ID string `json:"browserContextId"`
				}
				if err := json.Unmarshal(request.Params, &params); err != nil || params.ID != "owned-fixture" {
					t.Error("wrong disposal identity")
				}
				disposed.Store(true)
				response["result"] = map[string]any{}
			default:
				t.Error("unexpected protocol method", request.Method)
				response["error"] = map[string]any{"code": -32601, "message": "unknown fixture method"}
			}
			if err := connection.WriteJSON(response); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	f := bindFixture(t, OptionsV1{Name: "partial", RemoteURL: "ws" + strings.TrimPrefix(server.URL, "http") + "/devtools/browser/fixture", NewWindow: true}, 1)
	receipt, err := f.client.Run(deadline(t), deadline(t), fault.Correlation{Call: "partial"}, func(*Session) error { t.Error("failed target entered callback"); return nil })
	var native *cdproto.Error
	if !errors.As(err, &native) || native.Code != -32000 || !disposed.Load() {
		t.Fatal("original failure or disposal evidence missing", err)
	}
	settle(t, f, fault.Correlation{Call: "partial"}, receipt, ErrNative, nil, func(t testing.TB, result Result) {
		if !result.ContextReleased() || result.CallbackCompleted() {
			t.Fatal("false partial-acquisition outcome")
		}
	})
}
