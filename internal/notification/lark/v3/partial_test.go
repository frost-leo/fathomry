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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	larkcard "github.com/larksuite/oapi-sdk-go/v3/card"
)

func TestNativePartialScopesRemainDistinct(t *testing.T) {
	cases := []struct {
		operation    Operation
		path         []string
		field, value string
	}{
		{IMV1ChatMembersCreate, []string{"oc_fixture"}, "invalid_id_list", `["ou_bad"]`},
		{IMV1ChatMembersCreate, []string{"oc_fixture"}, "not_existed_id_list", `["ou_missing"]`},
		{IMV1ChatMembersCreate, []string{"oc_fixture"}, "pending_approval_id_list", `["ou_pending"]`},
		{IMV1ChatMembersDelete, []string{"oc_fixture"}, "invalid_id_list", `["ou_bad"]`},
		{IMV1MessageReactionBatchQuery, nil, "fail_msg_reaction_details", `[{"message_id":"om_bad","fail_reason":"synthetic","extension":9007199254740993}]`},
		{IMV2AppFeedCardCreate, nil, "failed_cards", `[{"user_id":"ou_bad","reason":"synthetic"}]`},
		{IMV2AppFeedCardBatchDelete, nil, "failed_cards", `[{"user_id":"ou_bad","reason":"synthetic"}]`},
		{IMV2AppFeedCardBatchUpdate, nil, "failed_cards", `[{"user_id":"ou_bad","reason":"synthetic"}]`},
		{IMV2ChatButtonUpdate, nil, "failed_user_reasons", `[{"user_id":"ou_bad","error_code":1}]`},
		{IMV2FeedCardBotTimeSentive, nil, "failed_user_reasons", `[{"user_id":"ou_bad","error_code":1}]`},
		{IMV2FeedCardPatch, []string{"feed_fixture"}, "failed_user_reasons", `[{"user_id":"ou_bad","error_code":1}]`},
		{IMV2TagCreate, nil, "create_tag_fail_reason", `{"duplicate_id":"tag_duplicate"}`},
		{IMV2TagPatch, []string{"tag_fixture"}, "patch_tag_fail_reason", `{"duplicate_id":"tag_duplicate"}`},
	}
	for _, item := range cases {
		t.Run(string(item.operation)+"/"+item.field, func(t *testing.T) {
			peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, `{"code":0,"data":{"`+item.field+`":`+item.value+`}}`)
			})
			options := testOptions(peer)
			options.Profile = "tenant-token"
			options.AppSecret = ""
			options.TenantKey = "tenant_fixture"
			options.TenantToken = "token"
			options.TokenExpiresAt = time.Now().Add(time.Hour)
			bound := bindTest(t, options, 1)
			request, err := NewRequest(item.operation, item.path, nil, jsonValue(t, "{}"))
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := bound.client.Do(context.Background(), fault.Correlation{Call: "partial"}, request, RequestOptions{})
			result := resolved(t, receipt, err)
			if result.Err() != nil || result.Outcome.Value.Effect() != Partial || string(result.Outcome.Value.PartialFields()[item.field]) != item.value {
				t.Fatal("partial scope promoted or transformed", result.Err())
			}
			fields := result.Outcome.Value.PartialFields()
			fields[item.field][0] = 'X'
			if string(result.Outcome.Value.PartialFields()[item.field]) != item.value {
				t.Fatal("partial field alias")
			}
		})
	}
	for _, body := range []string{`{"code":0,"data":{"invalid_id_list":[],"pending_approval_id_list":null}}`, `{"code":0,"data":{"invalid_id_list":{}}}`} {
		peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) })
		options := testOptions(peer)
		options.Profile = "tenant-token"
		options.AppSecret = ""
		options.TenantKey = "tenant_fixture"
		options.TenantToken = "token"
		options.TokenExpiresAt = time.Now().Add(time.Hour)
		bound := bindTest(t, options, 1)
		request, _ := NewRequest(IMV1ChatMembersCreate, []string{"oc_fixture"}, nil, jsonValue(t, "{}"))
		receipt, err := bound.client.Do(context.Background(), fault.Correlation{Call: "shape"}, request, RequestOptions{})
		result := resolved(t, receipt, err)
		if bytes.Contains([]byte(body), []byte(`"invalid_id_list":{}`)) {
			if !errors.Is(result.Err(), ErrResponse) || result.Outcome.Value.Effect() != Unknown {
				t.Fatal("malformed partial accepted")
			}
			continue
		}
		fields := result.Outcome.Value.PartialFields()
		if result.Err() != nil || string(fields["invalid_id_list"]) != "[]" || string(fields["pending_approval_id_list"]) != "null" {
			t.Fatal("empty/null scope collapsed")
		}
		if _, present := fields["not_existed_id_list"]; present {
			t.Fatal("absent scope invented")
		}
	}
}
func TestPartialScopeMemberTypes(t *testing.T) {
	cases := []struct {
		name, raw string
	}{
		{"invalid_id_list", `[17]`},
		{"not_existed_id_list", `[false]`},
		{"pending_approval_id_list", `[{}]`},
		{"fail_msg_reaction_details", `[false]`},
		{"fail_msg_reaction_details", `[{"message_id":7}]`},
		{"fail_msg_reaction_details", `[{"fail_reason":42}]`},
		{"failed_cards", `[false]`},
		{"failed_cards", `[{"biz_id":7}]`},
		{"failed_cards", `[{"user_id":false}]`},
		{"failed_cards", `[{"reason":42}]`},
		{"failed_user_reasons", `[false]`},
		{"failed_user_reasons", `[{"error_code":"1"}]`},
		{"failed_user_reasons", `[{"error_message":7}]`},
		{"failed_user_reasons", `[{"user_id":false}]`},
		{"create_tag_fail_reason", `{"duplicate_id":42}`},
		{"patch_tag_fail_reason", `{"duplicate_id":false}`},
	}
	for _, item := range cases {
		t.Run(item.name+"/"+item.raw, func(t *testing.T) {
			result := Result{data: `{"` + item.name + `":` + item.raw + `}`}
			if err := result.readScopes([]string{item.name}); !errors.Is(err, ErrResponse) {
				t.Fatal("malformed native partial detail accepted", err)
			}
			if string(result.PartialFields()[item.name]) != item.raw {
				t.Fatal("malformed native evidence lost")
			}
		})
	}
}

func TestLocalizedNativeCardSignedWebhookWire(t *testing.T) {
	builder := larkcard.NewMessageCard().I18nElements(larkcard.NewMessageCardI18nElements().EnUS([]larkcard.MessageCardElement{larkcard.NewMessageCardMarkdown().Content("synthetic").Build()}).Build()).Build()
	encoded, err := builder.String()
	if err != nil {
		t.Fatal(err)
	}
	content, err := Card([]byte(encoded))
	if err != nil {
		t.Fatal("native localized card rejected", err)
	}
	if !bytes.Equal(content.JSON().Bytes(), []byte(encoded)) || bytes.Contains(content.JSON().Bytes(), []byte(`"elements":`)) {
		t.Fatal("native representation changed")
	}
	peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
		var input map[string]json.RawMessage
		if json.NewDecoder(r.Body).Decode(&input) != nil || string(input["card"]) != encoded {
			t.Error("localized card changed on signed-bot wire")
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("webhook borrowed tenant token")
		}
		_, _ = io.WriteString(w, `{"code":0}`)
	})
	options := testOptions(peer)
	options.Profile = "webhook"
	options.AppID = ""
	options.AppSecret = ""
	options.WebhookURL = peer.server.URL + "/open-apis/bot/v2/hook/fixture"
	options.WebhookSecret = "synthetic"
	bound := bindTest(t, options, 1)
	receipt, err := bound.client.SendWebhook(context.Background(), fault.Correlation{Call: "localized"}, content)
	if result := resolved(t, receipt, err); result.Err() != nil || result.Outcome.Value.Effect() != Accepted {
		t.Fatal(result.Err())
	}
}
