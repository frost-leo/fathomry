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
	"strings"
	"testing"
)

func TestMessageOperationsUseNativeRoutesAndBodies(t *testing.T) {
	peer := newPeer(t, nil)
	bound := bindTest(t, testOptions(peer), 1)
	client := bound.client
	ctx := context.Background()
	id := fault.Correlation{Call: "message"}
	tests := []struct {
		name, method, path string
		run                func() (*invocation.Receipt[Result], error)
	}{
		{"send", "POST", "/open-apis/im/v1/messages", func() (*invocation.Receipt[Result], error) {
			return client.Send(ctx, id, Recipient{Type: "email", ID: "receiver@example.test"}, "send-uuid", textContent(t))
		}},
		{"reply", "POST", "/open-apis/im/v1/messages/om_fixture/reply", func() (*invocation.Receipt[Result], error) {
			return client.Reply(ctx, id, "om_fixture", "reply-uuid", true, textContent(t))
		}},
		{"update", "PUT", "/open-apis/im/v1/messages/om_fixture", func() (*invocation.Receipt[Result], error) {
			return client.UpdateMessage(ctx, id, "om_fixture", textContent(t))
		}},
		{"patch", "PATCH", "/open-apis/im/v1/messages/om_fixture", func() (*invocation.Receipt[Result], error) {
			return client.PatchMessage(ctx, id, "om_fixture", cardContentTest(t))
		}},
		{"get", "GET", "/open-apis/im/v1/messages/om_fixture", func() (*invocation.Receipt[Result], error) { return client.GetMessage(ctx, id, "om_fixture") }},
		{"list", "GET", "/open-apis/im/v1/messages", func() (*invocation.Receipt[Result], error) {
			return client.ListMessages(ctx, id, "chat", "oc_fixture", 1, 9, Page{Size: 2, Token: "private-page"})
		}},
		{"read", "GET", "/open-apis/im/v1/messages/om_fixture/read_users", func() (*invocation.Receipt[Result], error) { return client.ReadUsers(ctx, id, "om_fixture", Page{}) }},
		{"forward", "POST", "/open-apis/im/v1/messages/om_fixture/forward", func() (*invocation.Receipt[Result], error) {
			return client.Forward(ctx, id, "om_fixture", "forward-uuid", Recipient{Type: "open_id", ID: "ou_fixture"})
		}},
		{"recall", "DELETE", "/open-apis/im/v1/messages/om_fixture", func() (*invocation.Receipt[Result], error) { return client.Recall(ctx, id, "om_fixture") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			receipt, err := test.run()
			got := resolved(t, receipt, err)
			if got.Err() != nil || got.Outcome.Value.Effect() != Accepted {
				t.Fatal(got.Err())
			}
			captured := peer.snapshot()
			request := captured[len(captured)-1]
			if request.method != test.method || request.path != test.path || request.auth != "Bearer token-app_fixture" {
				t.Fatal("wrong route or auth")
			}
			if test.name == "send" || test.name == "reply" {
				var body map[string]json.RawMessage
				_ = json.Unmarshal(request.body, &body)
				var content string
				if json.Unmarshal(body["content"], &content) != nil || content != textContent(t).json.value {
					t.Fatal("native content encoding changed")
				}
				if test.name == "reply" && string(body["reply_in_thread"]) != "true" {
					t.Fatal("thread reply lost")
				}
			}
			if test.name == "list" && !strings.Contains(request.query, "page_token=private-page") {
				t.Fatal("explicit page token lost")
			}
			if test.name == "get" {
				messages := got.Outcome.Value.Messages()
				if len(messages) != 1 || messages[0].ID() != "om_fixture" || messages[0].SenderID() != "ou_fixture" {
					t.Fatal("message inspection lost")
				}
				messages[0].id = "mutated"
				if got.Outcome.Value.Messages()[0].ID() != "om_fixture" {
					t.Fatal("message snapshot aliases")
				}
			}
			drain(t, bound.inbox)
		})
	}
}
func TestMessageInputBoundary(t *testing.T) {
	peer := newPeer(t, nil)
	bound := bindTest(t, testOptions(peer), 1)
	if _, err := bound.client.Recall(context.Background(), fault.Correlation{Call: "invalid"}, "../messages"); err == nil {
		t.Fatal("path injection accepted")
	}
	if _, err := bound.client.Send(context.Background(), fault.Correlation{Call: "invalid"}, Recipient{Type: "email", ID: "Name <receiver@example.test>"}, "uuid", textContent(t)); err == nil {
		t.Fatal("noncanonical recipient accepted")
	}
	if len(peer.snapshot()) != 0 {
		t.Fatal("invalid inputs performed I/O")
	}
}
