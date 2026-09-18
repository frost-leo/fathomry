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
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

func TestHTTPAPIAndUnknownOutcomes(t *testing.T) {
	cases := []struct {
		name, body string
		status     int
		effect     Effect
		kind       error
	}{
		{"success", `{"code":0,"data":{"message_id":"om_fixture"}}`, 200, Accepted, nil},
		{"api-error", `{"code":230013,"msg":"private-refusal"}`, 200, Rejected, ErrAPI},
		{"missing-code", `{"data":{"message_id":"om_fixture"}}`, 200, Unknown, ErrResponse},
		{"null-code", `{"code":null,"data":{"message_id":"om_fixture"}}`, 200, Unknown, ErrResponse},
		{"empty", `{}`, 200, Unknown, ErrResponse},
		{"null", `null`, 200, Unknown, ErrResponse},
		{"missing-id", `{"code":0,"data":{}}`, 200, Unknown, ErrResponse},
		{"duplicate", `{"code":230013,"code":0,"data":{"message_id":"om_fixture"}}`, 200, Unknown, ErrResponse},
		{"http-error", `{"code":0,"data":{"message_id":"om_fixture"}}`, 503, Unknown, ErrHTTP},
		{"gateway-timeout", `{"code":0}`, 504, Unknown, ErrHTTP},
		{"retry-advice", `{"code":99991400,"msg":"limited"}`, 429, Rejected, ErrAPI},
		{"native-ticket-hook", `{"code":10012,"msg":"ticket"}`, 200, Rejected, ErrAPI},
		{"oversized", strings.Repeat("x", 300<<10), 200, Unknown, ErrLimit},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "/auth/") {
					defaultReply(w, r)
					return
				}
				w.Header().Set("Retry-After", "12")
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			})
			bound := bindTest(t, testOptions(peer), 1)
			got := sendTest(t, bound.client, "call")
			if test.kind == nil && got.Err() != nil || test.kind != nil && !errors.Is(got.Err(), test.kind) || got.Outcome.Value.Effect() != test.effect {
				t.Fatalf("unexpected classification/effect: err=%v effect=%d", got.Err(), got.Outcome.Value.Effect())
			}
			if len(peer.snapshot()) != 2 || got.Attempts != (invocation.Attempts{Observed: 2, Exact: true}) {
				t.Fatal("hidden retry/auth traffic")
			}
			if got.Outcome.Value.Exchange().HTTPStatus() != test.status || got.Outcome.Value.Exchange().RequestID() != "fixture-log" {
				t.Fatal("HTTP evidence lost")
			}
			if test.name == "api-error" {
				if errors.Is(got.Err(), ErrHTTP) {
					t.Fatal("HTTP 200 API rejection mislabeled as HTTP failure")
				}
				conformance.Cause(t, got.Err(), func(native *larkcore.CodeError) bool { return native.Code == 230013 })
				conformance.Private(t, got.Err(), "private-refusal")
			}
		})
	}
}
func TestLostAcknowledgementAndNoRetry(t *testing.T) {
	peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/auth/") {
			defaultReply(w, r)
			return
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	})
	bound := bindTest(t, testOptions(peer), 1)
	got := sendTest(t, bound.client, "lost")
	if got.Err() == nil || got.Outcome.Value.Effect() != Unknown || len(peer.snapshot()) != 2 {
		t.Fatal("lost acknowledgement was retried or declared absent")
	}
}
func TestCompositionEvidenceSaturationAndAliasing(t *testing.T) {
	peer := newPeer(t, nil)
	options := testOptions(peer)
	bound := bindTest(t, options, 1)
	got := sendTest(t, bound.client, "composed")
	want := conformance.Expected[Result]{Context: fault.Context{Provider: ProviderID, Source: "feishu", Scope: "feishu-test", Operation: "send", Correlation: fault.Correlation{Call: "composed"}},
		Source: bound.client.access.Info(), Limits: LimitsV1(options), Shape: invocation.Finite, Present: true, Final: true, Released: true,
		Attempts: invocation.Attempts{Observed: 2, Exact: true}, Value: func(t testing.TB, value Result) {
			if value.MessageID() != "om_created" || value.Effect() != Accepted || value.Target() != "email:recipient@example.test" {
				t.Error("independent operation oracle differs")
			}
		}}
	conformance.Result(t, got, want)
	data := got.Outcome.Value.JSONData()
	data[0] = 'X'
	if _, err := bound.client.Send(context.Background(), fault.Correlation{Call: "saturated"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "saturated", textContent(t)); !errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("evidence saturation permitted sending")
	}
	if len(peer.snapshot()) != 2 {
		t.Fatal("rejected call performed I/O")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2e9)
	defer cancel()
	conformance.Receive(t, ctx, bound.inbox, []conformance.Expected[Result]{want})
	if second := sendTest(t, bound.client, "cached"); second.Attempts.Observed != 1 || second.Err() != nil {
		t.Fatal("source token cache not used")
	}
}
func TestEncodedLimitBeforeAuthenticationAndCanceledInput(t *testing.T) {
	peer := newPeer(t, nil)
	options := testOptions(peer)
	options.MaxRequestBytes = 1024
	bound := bindTest(t, options, 1)
	content, err := Text(strings.Repeat("\"", 400))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := bound.client.Send(context.Background(), fault.Correlation{Call: "oversize"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "oversize", content)
	got := resolved(t, receipt, err)
	if !errors.Is(got.Err(), ErrLimit) || got.Outcome.Value.Effect() != NotAttempted || len(peer.snapshot()) != 0 {
		t.Fatal("encoded expansion reached authentication")
	}
	drain(t, bound.inbox)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = bound.client.Send(ctx, fault.Correlation{Call: "canceled"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "canceled", textContent(t)); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled call admitted")
	}
	if len(peer.snapshot()) != 0 {
		t.Fatal("canceled call performed I/O")
	}
}
