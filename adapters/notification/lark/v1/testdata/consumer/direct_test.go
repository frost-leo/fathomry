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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	lark "github.com/frost-leo/fathomry/adapters/notification/lark/v1"
)

func TestPublicNativeFamiliesAndHTTPCallback(t *testing.T) {
	captures := make(chan string, 16)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captures <- r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/open-apis/im/v1/images" {
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Error(err)
			}
			if r.FormValue("image_type") != "avatar" {
				t.Error("image purpose changed")
			}
			_, _ = io.WriteString(w, `{"code":0,"data":{"image_key":"img_fixture"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"code":0,"bot":{"open_id":"ou_bot","activate_status":0},"data":{"message_id":"om_fixture","chat_id":"oc_fixture","card_id":"card_fixture","card_ids":["card_fixture"],"user_list":[{"email":"fixture@example.invalid","user_id":"user_fixture","status":{"is_resigned":true}}],"n":9007199254740993,"empty":[],"nil":null,"flag":false}}`)
	}))
	defer server.Close()
	configuration := fixtureSettings(server)
	configuration.VerificationToken = "verification"
	owner, _, inbox := testOwner(t, configuration, lark.NativeOptions{})
	for _, item := range []struct {
		operation lark.Operation
		path      []string
		query     url.Values
		body      string
		wire      string
	}{
		{lark.IMV1MessageCreate, nil, url.Values{"receive_id_type": {"open_id"}}, `{"receive_id":"ou_fixture","msg_type":"text","content":"{\"text\":\"synthetic\"}"}`, "/open-apis/im/v1/messages"},
		{lark.CardKitV1CardIdConvert, nil, nil, `{"message_id":"om_fixture"}`, "/open-apis/cardkit/v1/cards/id_convert"},
		{lark.IMV1ChatCreate, nil, url.Values{"user_id_type": {"union_id"}}, `{"name":"synthetic","set_bot_manager":false}`, "/open-apis/im/v1/chats"},
		{lark.IMV2AppFeedCardCreate, nil, url.Values{"user_id_type": {"user_id"}}, `{"app_feed_card":{"title":"synthetic"},"user_ids":["user_fixture"]}`, "/open-apis/im/v2/app_feed_card"},
		{lark.ContactV3UserBatchGetId, nil, url.Values{"user_id_type": {"user_id"}}, `{"emails":["fixture@example.invalid"],"mobiles":[],"include_resigned":true}`, "/open-apis/contact/v3/users/batch_get_id"},
		{lark.BotInfo, nil, nil, "", "/open-apis/bot/v3/info"},
	} {
		var body lark.JSON
		if item.body != "" {
			body = fixtureJSON(t, item.body)
		}
		request, err := lark.NewRequest(item.operation, item.path, item.query, body)
		if err != nil {
			t.Fatal(err)
		}
		result, err := owner.Client().Do(context.Background(), request, lark.RequestOptions{})
		if err != nil || result.Effect() != lark.Accepted {
			t.Fatal("public route", err)
		}
		if got := <-captures; got != item.wire {
			t.Fatal("wire route changed", got)
		}
		if !json.Valid(result.ResponseJSON()) || !bytes.Contains(result.ResponseJSON(), []byte("9007199254740993")) {
			t.Fatal("native envelope lost")
		}
		_ = consume(t, inbox).Ack()
	}
	result, err := owner.Client().UploadAsset(context.Background(), true, lark.AssetInput{Name: "fixture.png", ImageType: "avatar", Reader: bytes.NewBufferString("synthetic")}, lark.RequestOptions{})
	if err != nil || result.AssetKey() != "img_fixture" {
		t.Fatal("public media", err)
	}
	<-captures
	_ = consume(t, inbox).Ack()
	callback := []byte(`{"schema":"2.0","header":{"event_id":"http_fixture","event_type":"url.preview.get","app_id":"app_fixture","tenant_key":"tenant_fixture","token":"verification"},"event":{"context":{"url":"https://fixture.invalid"}}}`)
	writer := httptest.NewRecorder()
	result, err = owner.Client().HandleHTTP(context.Background(), httptest.NewRequest("POST", "https://callback.invalid", bytes.NewReader(callback)), writer, false,
		func(context.Context, lark.Event) (lark.HTTPResponse, error) {
			return lark.HTTPResponse{Body: []byte(`{"inline":{"title":"synthetic"}}`)}, nil
		})
	if err != nil || !result.HTTPWrite().Written || writer.Code != 200 {
		t.Fatal("public callback", err)
	}
	_ = consume(t, inbox).Ack()
}

func TestCatalogBinaryDownloads(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="fixture.bin"`)
		w.Header().Set("X-Fixture", "copied")
		_, _ = w.Write([]byte{0, 1, 2, 255})
	}))
	defer server.Close()
	owner, _, inbox := testOwner(t, fixtureSettings(server), lark.NativeOptions{})
	for _, input := range []struct {
		operation lark.Operation
		path      []string
		query     url.Values
	}{
		{lark.IMV1FileGet, []string{"file_fixture"}, nil},
		{lark.IMV1ImageGet, []string{"image_fixture"}, nil},
		{lark.IMV1MessageResourceGet, []string{"om_fixture", "file_fixture"}, url.Values{"type": {"file"}}},
	} {
		t.Run(string(input.operation), func(t *testing.T) {
			request, err := lark.NewRequest(input.operation, input.path, input.query, lark.JSON{})
			if err != nil {
				t.Fatal(err)
			}
			result, err := owner.Client().Do(context.Background(), request, lark.RequestOptions{})
			if err != nil || !bytes.Equal(result.Bytes(), []byte{0, 1, 2, 255}) || result.FileName() != "fixture.bin" || result.Headers().Get("X-Fixture") != "copied" {
				t.Fatal("binary catalog route did not preserve native download", err)
			}
			_ = consume(t, inbox).Ack()
		})
	}
}

func TestCatalogPartialAndRequiredACKs(t *testing.T) {
	for _, item := range []struct {
		operation lark.Operation
		path      []string
		data      string
		effect    lark.Effect
		invalid   bool
	}{
		{lark.IMV1ChatMembersCreate, []string{"oc_fixture"}, `{"invalid_id_list":["ou_bad"],"pending_approval_id_list":null}`, lark.Partial, false},
		{lark.IMV2AppFeedCardCreate, nil, `{"failed_cards":[{"user_id":"ou_bad","reason":"synthetic"}]}`, lark.Partial, false},
		{lark.IMV2ChatButtonUpdate, nil, `{"failed_user_reasons":[{"user_id":"ou_bad","error_code":0}]}`, lark.Partial, false},
		{lark.IMV1MessageUrgentApp, []string{"om_fixture"}, `{"invalid_user_id_list":["bad/id"]}`, lark.Partial, false},
		{lark.IMV1ChatMembersCreate, []string{"oc_fixture"}, `{"invalid_id_list":[17]}`, lark.Unknown, true},
		{lark.IMV2AppFeedCardCreate, nil, `{"failed_cards":[false]}`, lark.Unknown, true},
		{lark.IMV2TagCreate, nil, `{"create_tag_fail_reason":{"duplicate_id":42}}`, lark.Unknown, true},
		{lark.CardKitV1CardIdConvert, nil, `{}`, lark.Unknown, true},
		{lark.IMV1ChatCreate, nil, `{}`, lark.Unknown, true},
		{lark.CardKitV1CardIdConvert, nil, `{"card_id":"card_fixture"}`, lark.Accepted, false},
		{lark.IMV1ChatCreate, nil, `{"chat_id":"oc_fixture"}`, lark.Accepted, false},
	} {
		t.Run(string(item.operation)+"/"+item.data, func(t *testing.T) {
			body := `{"code":0,"data":` + item.data + `}`
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			owner, _, inbox := testOwner(t, fixtureSettings(server), lark.NativeOptions{})
			input := fixtureJSON(t, "{}")
			if item.operation == lark.IMV1MessageUrgentApp {
				input = fixtureJSON(t, `{"user_id_list":["ou_good","bad/id"]}`)
			}
			request, err := lark.NewRequest(item.operation, item.path, nil, input)
			if err != nil {
				t.Fatal(err)
			}
			result, err := owner.Client().Do(context.Background(), request, lark.RequestOptions{})
			if (err != nil) != item.invalid || errors.Is(err, lark.ErrResponse) != item.invalid || result.Effect() != item.effect {
				t.Fatal("native outcome incorrectly promoted", err, result.Effect())
			}
			if string(result.JSONData()) != item.data || string(result.ResponseJSON()) != body {
				t.Fatal("native evidence was transformed")
			}
			_ = consume(t, inbox).Ack()
		})
	}
}
