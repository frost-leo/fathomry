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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
)

func TestSignedWebhookSeparateAuthorityAndNativeCards(t *testing.T) {
	peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{"code":0,"msg":"success"}`) })
	options := testOptions(peer)
	options.Profile = "webhook"
	options.AppID = ""
	options.AppSecret = ""
	options.WebhookURL = peer.server.URL + "/open-apis/bot/v2/hook/fixture"
	options.WebhookSecret = "fixture-secret"
	bound := bindTest(t, options, 2)
	chart, err := Chart("chart_one", jsonValue(t, `{"type":"line","data":{"values":[{"x":"a","y":1}]},"xField":"x","yField":"y"}`))
	if err != nil {
		t.Fatal(err)
	}
	content, err := ComposeCard("Native webhook chart", chart)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := bound.client.SendWebhook(context.Background(), fault.Correlation{Call: "webhook"}, content)
	got := resolved(t, receipt, err)
	if got.Err() != nil || got.Outcome.Value.Effect() != Accepted || got.Outcome.Value.MessageID() != "" {
		t.Fatal("webhook acknowledgement invented message identity")
	}
	captured := peer.snapshot()
	if len(captured) != 1 || captured[0].auth != "" {
		t.Fatal("webhook used application authority")
	}
	var payload struct {
		Timestamp string          `json:"timestamp"`
		Sign      string          `json:"sign"`
		Card      json.RawMessage `json:"card"`
		Content   json.RawMessage `json:"content"`
	}
	if json.Unmarshal(captured[0].body, &payload) != nil {
		t.Fatal("invalid webhook body")
	}
	stamp, err := strconv.ParseInt(payload.Timestamp, 10, 64)
	if err != nil || time.Since(time.Unix(stamp, 0)) > time.Minute {
		t.Fatal("timestamp invalid")
	}
	mac := hmac.New(sha256.New, []byte(payload.Timestamp+"\nfixture-secret"))
	expected := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if payload.Sign != expected || len(payload.Card) == 0 || len(payload.Content) != 0 {
		t.Fatal("wrong webhook signature or card envelope")
	}
	if _, err = bound.client.Recall(context.Background(), fault.Correlation{Call: "recall"}, "om_fixture"); !errors.Is(err, ErrUnsupported) {
		t.Fatal("webhook gained recall authority")
	}
	if _, err = bound.client.UploadImage(context.Background(), fault.Correlation{Call: "upload"}, "chart.png", []byte("image")); !errors.Is(err, ErrUnsupported) {
		t.Fatal("webhook gained upload authority")
	}
	big, err := Text(strings.Repeat("x", 21000))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err = bound.client.SendWebhook(context.Background(), fault.Correlation{Call: "oversize"}, big)
	rejected := resolved(t, receipt, err)
	if !errors.Is(rejected.Err(), ErrLimit) || len(peer.snapshot()) != 1 {
		t.Fatal("oversized signed body sent")
	}
}
