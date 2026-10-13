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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/gorilla/websocket"
)

// This protocol peer supplies malformed successful creation replies. It is not
// live browser evidence and does not stand in for an actual Chromium context.
func TestMissingCreationIdentityNeverFallsBackToDefault(t *testing.T) {
	for _, missing := range []string{"context", "target"} {
		t.Run(missing, func(t *testing.T) {
			var createdTargets, attached atomic.Int64
			var disposed atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				for {
					var request struct {
						ID     int64  `json:"id"`
						Method string `json:"method"`
					}
					if err := conn.ReadJSON(&request); err != nil {
						return
					}
					result := map[string]any{}
					switch request.Method {
					case "Browser.getVersion":
						result = map[string]any{"product": "Chrome/150.0.0.0", "protocolVersion": "1.3"}
					case "Target.createBrowserContext":
						if missing != "context" {
							result["browserContextId"] = "owned-context"
						}
					case "Target.createTarget":
						createdTargets.Add(1)
					case "Target.attachToTarget":
						attached.Add(1)
						t.Error("missing identity reached native attachment")
					case "Target.getBrowserContexts":
						ids := []string{"owned-context"}
						if disposed.Load() {
							ids = nil
						}
						result["browserContextIds"] = ids
					case "Target.disposeBrowserContext":
						disposed.Store(true)
					default:
						t.Error("unexpected protocol method", request.Method)
					}
					if err := conn.WriteJSON(map[string]any{"id": request.ID, "result": result}); err != nil {
						return
					}
				}
			}))
			defer server.Close()
			life, cancel := context.WithCancel(context.Background())
			defer cancel()
			options := OptionsV1{Name: "missing-identity", RemoteURL: "ws" + strings.TrimPrefix(server.URL, "http") + "/devtools/browser/fixture"}
			prepared, err := PrepareV1(options)
			if err != nil {
				t.Fatal(err)
			}
			selected, err := prepared.Select(life)
			if err != nil {
				t.Fatal(err)
			}
			selected = resource.WithLimits(selected, prepared.Limits())
			assembly, err := resource.Assemble(deadline(t), deadline(t), "identity-fixture", selected)
			if err != nil {
				t.Fatal(err)
			}
			inbox, err := invocation.NewInbox[Result](1, prepared.Budget().EvidenceBytes)
			if err != nil {
				t.Fatal(err)
			}
			client, err := Bind(assembly, selected, inbox, nil)
			if err != nil {
				t.Fatal(err)
			}
			// The context-negative case must preserve unknown custody. End only
			// the task-owned fake connection without fabricating Released.
			defer func() {
				cancel()
				if client.owner.closeRoot != nil {
					client.owner.closeRoot()
				}
				if client.owner.closeAllocator != nil {
					client.owner.closeAllocator()
					client.owner.allocator.Wait()
				}
			}()
			retained, err := client.Retain(deadline(t), fault.Correlation{Call: "missing"})
			if retained == nil || !errors.Is(err, ErrNative) {
				t.Fatal("malformed success accepted", err)
			}
			if attached.Load() != 0 || missing == "context" && createdTargets.Load() != 0 {
				t.Fatal("native default fallback attempted")
			}
			if missing == "context" {
				if page, err := retained.NewPage(deadline(t)); page != nil || !errors.Is(err, ErrState) || createdTargets.Load() != 0 {
					t.Fatal("partial context created a child in the shared default context", err)
				}
			}
			closeErr := retained.Close(deadline(t))
			snapshot, err := retained.Receipt().WaitReleased(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			if missing == "context" {
				if !errors.Is(closeErr, ErrCleanup) || snapshot.Outcome.Value.ContextReleased() {
					t.Fatal("unknown context falsely disposed", closeErr)
				}
				if err := assembly.Close(deadline(t)); err == nil {
					t.Fatal("unknown source cleanup claimed success")
				}
				state := assembly.Snapshot().Sources[0]
				if state.Released || !state.Pending {
					t.Fatal("unknown native context lost custody")
				}
			} else {
				if !snapshot.Outcome.Value.ContextReleased() || !disposed.Load() {
					t.Fatal("known context failed to contain unknown child", closeErr)
				}
				if err := assembly.Close(deadline(t)); err != nil {
					t.Fatal(err)
				}
			}
			delivery, err := inbox.Next(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			if err := delivery.Release(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
