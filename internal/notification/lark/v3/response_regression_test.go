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
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
)

func TestPaginationRejectsAliasesAndInvalidTokens(t *testing.T) {
	for _, data := range []string{
		`{"items":[],"has_more":false,"Has_More":true}`,
		`{"items":[],"has_more":true,"page_token":"validated","Page_Token":"not-validated"}`,
		`{"items":[],"has_more":false,"page_token":null}`,
		`{"items":[],"has_more":false,"page_token":123}`,
		`{"items":[],"has_more":true,"page_token":""}`,
		`{"items":[],"has_more":true}`,
	} {
		for _, kind := range []string{"page", "message-page"} {
			result := Result{data: data}
			err := result.readIdentity(kind)
			token, more := result.NextPage()
			if !errors.Is(err, ErrResponse) || token != "" || more {
				t.Fatal("malformed pagination escaped through the validated snapshot")
			}
		}
	}
	for _, more := range []bool{true, false} {
		data, _ := json.Marshal(map[string]any{"items": []any{}, "has_more": more, "page_token": "validated"})
		result := Result{data: string(data)}
		if err := result.readIdentity("message-page"); err != nil {
			t.Fatal(err)
		}
		token, actual := result.NextPage()
		if token != "validated" || actual != more {
			t.Fatal("valid continuation was not retained")
		}
	}
}

func TestRecipientCaseAliasesCannotAuthorizeWrongTarget(t *testing.T) {
	for _, data := range []string{
		`{"user_list":[{"user_id":"ou_wrong","email":"wrong@example.test","Email":"target@example.test"}]}`,
		`{"user_list":[],"User_List":[{"user_id":"ou_wrong","email":"target@example.test"}]}`,
		`{"user_list":[{"user_id":"ou_original","User_ID":"ou_wrong","email":"target@example.test"}]}`,
		`{"user_list":[null]}`,
		`{"user_list":null}`,
	} {
		peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/auth/") {
				defaultReply(w, r)
				return
			}
			_, _ = io.WriteString(w, `{"code":0,"data":`+data+`}`)
		})
		bound := bindTest(t, testOptions(peer), 1)
		receipt, err := bound.client.ResolveEmail(context.Background(), fault.Correlation{Call: "recipient-alias"}, "target@example.test")
		got := resolved(t, receipt, err)
		if _, present := got.Outcome.Value.ResolvedRecipient(); !errors.Is(got.Err(), ErrResponse) || present || got.Outcome.Value.Effect() != Unknown {
			t.Fatal("ambiguous recipient response authorized a target")
		}
	}
}

func TestMergedACKAliasesCannotReplaceIdentity(t *testing.T) {
	for _, data := range []string{
		`{"message":{"message_id":"om_original"},"Message":{"message_id":"om_wrong"}}`,
		`{"message":{"message_id":"om_original","Message_ID":"om_wrong"}}`,
		`{"message":{"message_id":"om_original"},"invalid_message_id_list":[],"Invalid_Message_ID_List":["om_child"]}`,
	} {
		peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/auth/") {
				defaultReply(w, r)
				return
			}
			_, _ = io.WriteString(w, `{"code":0,"data":`+data+`}`)
		})
		bound := bindTest(t, testOptions(peer), 1)
		receipt, err := bound.client.MergeForward(context.Background(), fault.Correlation{Call: "merged-alias"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "merge-alias", []string{"om_child"})
		got := resolved(t, receipt, err)
		if !errors.Is(got.Err(), ErrResponse) || got.Outcome.Value.MessageID() != "" || got.Outcome.Value.Effect() != Unknown {
			t.Fatal("ambiguous merged-message identity or partial effect accepted")
		}
	}
}

func TestTokenCaseAliasesCannotAuthorizeSend(t *testing.T) {
	for _, body := range []string{
		`{"code":0,"tenant_access_token":"token-fixture","expire":0,"Expire":7200}`,
		`{"code":0,"tenant_access_token":"token-fixture","Tenant_Access_Token":"wrong-token","expire":7200}`,
	} {
		peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/auth/") {
				_, _ = io.WriteString(w, body)
				return
			}
			defaultReply(w, r)
		})
		bound := bindTest(t, testOptions(peer), 1)
		got := sendTest(t, bound.client, "token-alias")
		if !errors.Is(got.Err(), ErrAuth) || len(peer.snapshot()) != 1 || got.Outcome.Value.Effect() != NotAttempted {
			t.Fatal("ambiguous authentication response authorized a send")
		}
	}
}

func TestResponseCaseAliasingRefused(t *testing.T) {
	for _, body := range []string{
		`{"code":230013,"Code":0,"data":{"message_id":"om_fixture"}}`,
		`{"Code":0,"data":{"message_id":"om_fixture"}}`,
		`{"code":0,"data":{"message_id":"om_fixture"},"Data":{}}`,
	} {
		peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/auth/") {
				defaultReply(w, r)
				return
			}
			_, _ = io.WriteString(w, body)
		})
		bound := bindTest(t, testOptions(peer), 1)
		got := sendTest(t, bound.client, "case-code")
		code, known := got.Outcome.Value.Exchange().APICode()
		t.Logf("body=%s effect=%v code=%d known=%t error=%v", body, got.Outcome.Value.Effect(), code, known, got.Err())
		if got.Err() == nil || got.Outcome.Value.Effect() == Accepted {
			t.Error("a missing/conflicting exact code must not acknowledge a send")
		}
	}
}

func TestMessageFamilyRequiresRelatedDistinctMessages(t *testing.T) {
	valid := `[
		{"message_id":"om_leaf","msg_type":"text","upper_message_id":"om_nested","body":{"content":"text"}},
		{"message_id":"om_parent","msg_type":"merge_forward"},
		{"message_id":"om_nested","msg_type":"merge_forward","upper_message_id":"om_parent"}
	]`
	var entries []json.RawMessage
	if err := json.Unmarshal([]byte(valid), &entries); err != nil {
		t.Fatal(err)
	}
	snapshots, err := messageSnapshots(entries, "om_parent")
	if err != nil || len(snapshots) != 3 || snapshots[0].ID() != "om_leaf" {
		t.Fatal("valid nested merge lost native order or identity")
	}
	for _, raw := range []string{
		`[{"message_id":"om_parent"},{"message_id":"om_other","upper_message_id":"om_parent"}]`,
		`[{"message_id":"om_parent","msg_type":"merge_forward"},{"message_id":"om_other","upper_message_id":"om_foreign"}]`,
		`[{"message_id":"om_parent","msg_type":"merge_forward"},{"message_id":"om_other","msg_type":"merge_forward","upper_message_id":"om_other"}]`,
		`[{"message_id":"om_parent"},{"message_id":"om_parent"}]`,
		`[{"message_id":"om_parent","Message_ID":"om_other"}]`,
		`[{"message_id":"om_parent","body":{"content":"x","Content":"y"}}]`,
		`[{"message_id":"om_parent","sender":false}]`,
	} {
		if err := json.Unmarshal([]byte(raw), &entries); err != nil {
			t.Fatal(err)
		}
		if _, err := messageSnapshots(entries, "om_parent"); err == nil {
			t.Fatal("unrelated, cyclic, duplicate or malformed message family accepted")
		}
	}
}

func TestMalformedMessageEntriesRefused(t *testing.T) {
	for _, items := range []string{`[true]`, `[null]`, `[{}]`, `[{"message_id":"om_fixture","body":false}]`} {
		peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/auth/") {
				defaultReply(w, r)
				return
			}
			_, _ = io.WriteString(w, `{"code":0,"data":{"has_more":false,"items":`+items+`}}`)
		})
		bound := bindTest(t, testOptions(peer), 1)
		receipt, err := bound.client.ListMessages(context.Background(), fault.Correlation{Call: "items"}, "chat", "oc_fixture", 0, 0, Page{})
		got := resolved(t, receipt, err)
		t.Logf("items=%s effect=%v messages=%#v error=%v", items, got.Outcome.Value.Effect(), got.Outcome.Value.Messages(), got.Err())
		if got.Err() == nil || got.Outcome.Value.Effect() == Accepted {
			t.Error("malformed message page must not return successful reads")
		}
	}
}

func TestGetMergedMessageChildren(t *testing.T) {
	peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/auth/") {
			defaultReply(w, r)
			return
		}
		_, _ = io.WriteString(w, `{"code":0,"data":{"items":[{"message_id":"om_merged","msg_type":"merge_forward","chat_id":"oc_fixture","body":{"content":"{}"}},{"message_id":"om_child","msg_type":"text","chat_id":"oc_source","upper_message_id":"om_merged","body":{"content":"{\"text\":\"Child\"}"}}]}}`)
	})
	bound := bindTest(t, testOptions(peer), 1)
	receipt, err := bound.client.GetMessage(context.Background(), fault.Correlation{Call: "merged-get"}, "om_merged")
	got := resolved(t, receipt, err)
	t.Logf("effect=%v returnedMessages=%d error=%v", got.Outcome.Value.Effect(), len(got.Outcome.Value.Messages()), got.Err())
	if got.Err() != nil || got.Outcome.Value.Effect() != Accepted || len(got.Outcome.Value.Messages()) != 2 {
		t.Error("GetMessage must accept documented merge_forward response: one parent plus N children")
	}
}
