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
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestCardAndElementOperations(t *testing.T) {
	peer := newPeer(t, nil)
	bound := bindTest(t, testOptions(peer), 1)
	client := bound.client
	ctx := context.Background()
	id := fault.Correlation{Call: "card"}
	rev := Revision{Sequence: 3, UUID: "revision"}
	tests := []struct {
		name, method, path string
		run                func() (*invocation.Receipt[Result], error)
	}{
		{"create", "POST", "/cards", func() (*invocation.Receipt[Result], error) { return client.CreateCard(ctx, id, cardContentTest(t)) }},
		{"update", "PUT", "/cards/card_fixture", func() (*invocation.Receipt[Result], error) {
			return client.UpdateCard(ctx, id, "card_fixture", rev, cardContentTest(t))
		}},
		{"settings", "PATCH", "/cards/card_fixture/settings", func() (*invocation.Receipt[Result], error) {
			return client.CardSettings(ctx, id, "card_fixture", rev, jsonValue(t, `{"config":{"streaming_mode":true}}`))
		}},
		{"batch", "POST", "/cards/card_fixture/batch_update", func() (*invocation.Receipt[Result], error) {
			return client.BatchUpdateCard(ctx, id, "card_fixture", rev, jsonValue(t, `[{"action":"partial_update_element","params":{"element_id":"intro","partial_element":{"content":"New"}}}]`))
		}},
		{"insert", "POST", "/cards/card_fixture/elements", func() (*invocation.Receipt[Result], error) {
			return client.InsertElements(ctx, id, "card_fixture", rev, "append", "", jsonValue(t, `[{"tag":"markdown","element_id":"extra","content":"New"}]`))
		}},
		{"element", "PUT", "/cards/card_fixture/elements/intro", func() (*invocation.Receipt[Result], error) {
			return client.UpdateElement(ctx, id, "card_fixture", "intro", rev, jsonValue(t, `{"tag":"markdown","element_id":"intro","content":"Replacement"}`))
		}},
		{"patch", "PATCH", "/cards/card_fixture/elements/intro", func() (*invocation.Receipt[Result], error) {
			return client.PatchElement(ctx, id, "card_fixture", "intro", rev, jsonValue(t, `{"content":"Partial"}`))
		}},
		{"content", "PUT", "/cards/card_fixture/elements/intro/content", func() (*invocation.Receipt[Result], error) {
			return client.ElementContent(ctx, id, "card_fixture", "intro", rev, "Streaming full content")
		}},
		{"delete", "DELETE", "/cards/card_fixture/elements/intro", func() (*invocation.Receipt[Result], error) {
			return client.DeleteElement(ctx, id, "card_fixture", "intro", rev)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			receipt, err := test.run()
			got := resolved(t, receipt, err)
			if got.Err() != nil {
				t.Fatal(got.Err())
			}
			capture := peer.snapshot()
			request := capture[len(capture)-1]
			if request.method != test.method || request.path != "/open-apis/cardkit/v1"+test.path {
				t.Fatal("wrong CardKit route")
			}
			var fields map[string]json.RawMessage
			if json.Unmarshal(request.body, &fields) != nil {
				t.Fatal("malformed body")
			}
			if test.name == "create" {
				if string(fields["type"]) != `"card_json"` || got.Outcome.Value.CardID() != "card_fixture" {
					t.Fatal("entity mode or ID lost")
				}
			} else if string(fields["sequence"]) != "3" || string(fields["uuid"]) != `"revision"` {
				t.Fatal("native revision lost")
			}
			if test.name == "batch" {
				var actions string
				if json.Unmarshal(fields["actions"], &actions) != nil {
					t.Fatal("native actions must be encoded once at the request boundary")
				}
				var decoded []struct {
					Action string `json:"action"`
					Params struct {
						ElementID      string         `json:"element_id"`
						PartialElement map[string]any `json:"partial_element"`
					} `json:"params"`
				}
				if json.Unmarshal([]byte(actions), &decoded) != nil || len(decoded) != 1 ||
					decoded[0].Action != "partial_update_element" || decoded[0].Params.ElementID != "intro" ||
					decoded[0].Params.PartialElement["content"] != "New" {
					t.Fatal("batch action must use the native enum and object-valued fields")
				}
			}
			drain(t, bound.inbox)
		})
	}
}
func TestStaleCardUpdatePreservesRejection(t *testing.T) {
	peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/auth/") {
			defaultReply(w, r)
			return
		}
		_, _ = io.WriteString(w, `{"code":300317,"msg":"stale update"}`)
	})
	bound := bindTest(t, testOptions(peer), 1)
	receipt, err := bound.client.UpdateCard(context.Background(), fault.Correlation{Call: "stale"}, "card_fixture", Revision{Sequence: 1}, cardContentTest(t))
	got := resolved(t, receipt, err)
	if got.Err() == nil || got.Outcome.Value.Effect() != Rejected || got.Outcome.Value.Target() != "card_fixture" {
		t.Fatal("stale update changed identity or retried")
	}
	if len(peer.snapshot()) != 2 {
		t.Fatal("stale update retried")
	}
}
