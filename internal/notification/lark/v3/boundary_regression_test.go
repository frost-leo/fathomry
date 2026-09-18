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
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestBoundaryRegressionEventAliases(t *testing.T) {
	options := OptionsV1{AppID: "app_fixture", TenantKey: "tenant_fixture", VerificationToken: "verification-fixture", EncryptKey: "encrypt-fixture"}
	value := defaults(options)
	cases := []struct {
		name, old, replacement string
		socket                 bool
	}{
		{"schema", `"schema":"2.0"`, `"schema":"1.0","Schema":"2.0"`, true},
		{"app", `"app_id":"app_fixture"`, `"app_id":"app_other","App_Id":"app_fixture"`, true},
		{"tenant", `"tenant_key":"tenant_fixture"`, `"tenant_key":"tenant_other","Tenant_Key":"tenant_fixture"`, true},
		{"event-id", `"event_id":"event_fixture"`, `"event_id":"event_fixture","Event_Id":"event_substituted"`, true},
		{"event-type", `"event_type":"im.message.receive_v1"`, `"event_type":"contact.user.created_v3","Event_Type":"im.message.receive_v1"`, true},
		{"token", `"token":"verification-fixture"`, `"token":"wrong-token","Token":"verification-fixture"`, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			payload := []byte(strings.Replace(string(notificationEvent()), test.old, test.replacement, 1))
			for _, encrypted := range []bool{false, true} {
				event, err := value.receive(signedCallback(t, options, payload, encrypted), time.Now())
				t.Logf("HTTP encrypted=%t eventID=%q app=%q tenant=%q type=%q error=%v", encrypted, event.ID(), event.AppID(), event.TenantKey(), event.Type(), err)
				if err == nil {
					t.Error("HTTP event alias must not replace validated native fields")
				}
			}
			if test.socket {
				event, err := value.socketEvent(payload)
				t.Logf("WS eventID=%q app=%q tenant=%q type=%q error=%v", event.ID(), event.AppID(), event.TenantKey(), event.Type(), err)
				if err == nil {
					t.Error("WS event alias must not replace validated native fields")
				}
			}
		})
	}
}

func TestBoundaryRegressionEventObjectAndWrapperAliases(t *testing.T) {
	options := OptionsV1{AppID: "app_fixture", TenantKey: "tenant_fixture", VerificationToken: "verification-fixture", EncryptKey: "encrypt-fixture"}
	value := defaults(options)
	payload := strings.TrimSuffix(string(notificationEvent()), "}") + `,"Event":{"substituted":true}}`
	event, err := value.socketEvent([]byte(payload))
	t.Logf("WS data=%s error=%v", event.JSONData(), err)
	if err == nil {
		t.Error("Event alias must not replace the signed native event object")
	}
	event, err = value.receive(signedCallback(t, options, []byte(payload), false), time.Now())
	t.Logf("HTTP data=%s error=%v", event.JSONData(), err)
	if err == nil {
		t.Error("Event alias must not replace the signed native event object")
	}
	encrypted := signedCallback(t, options, notificationEvent(), true)
	var wrapper map[string]json.RawMessage
	if err := json.Unmarshal(encrypted.Body, &wrapper); err != nil {
		t.Fatal(err)
	}
	aliased := []byte(`{"encrypt":"not-base64","Encrypt":` + string(wrapper["encrypt"]) + `}`)
	event, err = value.receive(signedCallback(t, options, aliased, false), time.Now())
	t.Logf("wrapper eventID=%q error=%v", event.ID(), err)
	if err == nil {
		t.Error("Encrypt alias must not replace invalid lowercase ciphertext")
	}
	challenge := Callback{Body: []byte(`{"type":"url_verification","token":"wrong","Token":"verification-fixture","challenge":"synthetic"}`)}
	event, err = value.receive(challenge, time.Now())
	t.Logf("challenge=%q error=%v", event.Challenge(), err)
	if err == nil {
		t.Error("Token alias must not replace native challenge authentication field")
	}
}

func TestBoundaryRegressionBootstrapAliases(t *testing.T) {
	for _, mode := range []string{"URL", "ClientConfig"} {
		t.Run(mode, func(t *testing.T) {
			var peer *socketPeer
			peer = newSocketPeer(t, false, func(w http.ResponseWriter, r *http.Request) {
				address, _ := json.Marshal(strings.Replace(peer.server.URL, "https:", "wss:", 1) + "/ws?service_id=42")
				body := `{"code":0,"data":{"URL":"wss://unapproved.invalid/ws?service_id=42","url":` + string(address) + `,"ClientConfig":{}}}`
				if mode == "ClientConfig" {
					body = `{"code":0,"data":{"URL":` + string(address) + `,"ClientConfig":{"PingInterval":0},"clientconfig":{}}}`
				}
				_, _ = io.WriteString(w, body)
			})
			options := socketTestOptions(peer)
			options.WebSocket.MaxConnectAttempts = 1
			fixture := bindReceiverTest(t, options, 8, 2)
			cancel, done := startReceiver(t, fixture)
			select {
			case <-peer.connected:
				result := stopReceiver(t, cancel, done)
				t.Logf("bootstrapAlias=%s connections=%d error=%v", mode, result.Outcome.Value.Stats().Connections, result.Err())
				t.Error("ambiguous bootstrap reached WebSocket Upgrade")
			case terminal := <-done:
				result := awaitSocketResult(t, terminal.receipt)
				if result.Err() == nil || result.Outcome.Value.Stats().Connections != 0 {
					t.Fatal("invalid bootstrap accepted")
				}
			case <-time.After(2 * time.Second):
				stopReceiver(t, cancel, done)
				t.Fatal("bootstrap test did not complete")
			}
			drainSocketResults(t, fixture)
		})
	}
}

func TestBoundaryRegressionCardChartAliases(t *testing.T) {
	for _, raw := range []string{
		`{"schema":"1.0","Schema":"2.0","body":{"elements":[]}}`,
		`{"Schema":"2.0","body":{"elements":[]}}`,
		`{"schema":"2.0","body":{"elements":null,"Elements":[]}}`,
		`{"type":"card","Type":"template","data":{"card_id":"card_original"},"Data":{"template_id":"template_other"}}`,
	} {
		content, err := Card([]byte(raw))
		t.Logf("raw=%s acceptedType=%q error=%v", raw, content.Type(), err)
		if err == nil {
			native, nativeErr := entity(content)
			if nativeErr == nil {
				t.Logf("entity type=%q data=%s", deref(native.Type), deref(native.Data))
			}
			t.Error("card aliases must not authorize a different schema/reference than exact native fields")
		}
	}
	for _, raw := range []string{`{"type":"","Type":"line"}`, `{"Type":"line"}`} {
		spec := jsonValue(t, raw)
		chart, err := Chart("chart", spec)
		t.Logf("raw=%s emitted=%s error=%v", raw, chart.Bytes(), err)
		if err == nil {
			t.Error("chart type alias must not authorize missing/invalid lowercase type")
		}
	}
}
