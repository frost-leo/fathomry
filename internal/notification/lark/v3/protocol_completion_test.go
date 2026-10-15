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
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
)

func TestNotificationProtocolEventLedger(t *testing.T) {
	kinds := []string{"im.chat.disbanded_v1", "im.chat.updated_v1", "im.chat.access_event.bot_p2p_chat_entered_v1",
		"im.chat.member.bot.added_v1", "im.chat.member.bot.deleted_v1", "im.chat.member.user.added_v1", "im.chat.member.user.deleted_v1", "im.chat.member.user.withdrawn_v1",
		"im.message.receive_v1", "im.message.message_read_v1", "im.message.recalled_v1", "im.message.reaction.created_v1", "im.message.reaction.deleted_v1",
		"card.action.trigger", "url.preview.get", "application.bot.menu_v6", "auth.user_access_token.revoked_v4"}
	peer := newPeer(t, nil)
	options := callbackOptions(peer)
	bound := bindTest(t, options, 1)
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			body := bytes.ReplaceAll(notificationEvent(), []byte("im.message.receive_v1"), []byte(kind))
			input := signedCallback(t, options, body, true)
			digest := sha256.Sum256(append([]byte(input.Timestamp+input.Nonce+options.EncryptKey), input.Body...))
			input.Signature = hex.EncodeToString(digest[:])
			receipt, err := bound.client.Receive(context.Background(), fault.Correlation{Call: "event"}, input)
			result := resolved(t, receipt, err)
			event, ok := result.Outcome.Value.Event()
			if result.Err() != nil || !ok || event.Type() != kind || event.Schema() != "2.0" {
				t.Fatal("HTTP event rejected", result.Err())
			}
			socket := defaults(options)
			socket.WebSocket = socketDefaults(&WebSocketOptions{}, options.BaseURL)
			received, err := socket.socketEvent(body)
			if err != nil || received.Type() != kind {
				t.Fatal("WSS event rejected", err)
			}
			drain(t, bound.inbox)
		})
	}
}
func TestTokenOnlyAndLegacyCardProtocols(t *testing.T) {
	peer := newPeer(t, nil)
	options := callbackOptions(peer)
	options.EncryptKey = ""
	bound := bindTest(t, options, 1)
	receipt, err := bound.client.Receive(context.Background(), fault.Correlation{Call: "token-only"}, Callback{Body: notificationEvent()})
	got := resolved(t, receipt, err)
	if got.Err() != nil {
		t.Fatal("token-only event rejected", got.Err())
	}
	drain(t, bound.inbox)
	wrong := bytes.ReplaceAll(notificationEvent(), []byte(options.VerificationToken), []byte("wrong"))
	receipt, err = bound.client.Receive(context.Background(), fault.Correlation{Call: "wrong-token"}, Callback{Body: wrong})
	if got := resolved(t, receipt, err); !errors.Is(got.Err(), ErrAuth) {
		t.Fatal("token-only fail-open")
	}
	drain(t, bound.inbox)
	body := []byte(`{"open_id":"ou_fixture","open_message_id":"om_fixture","tenant_key":"tenant_fixture","token":"action-update-token","action":{"value":{"number":9007199254740993},"checked":false}}`)
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	input := Callback{Timestamp: stamp, Nonce: "nonce", Body: body}
	digest := sha1.Sum(append([]byte(stamp+"nonce"+options.VerificationToken), body...))
	input.Signature = hex.EncodeToString(digest[:])
	receipt, err = bound.client.ReceiveLegacy(context.Background(), fault.Correlation{Call: "legacy"}, input)
	got = resolved(t, receipt, err)
	event, ok := got.Outcome.Value.Event()
	if got.Err() != nil || !ok || event.Type() != "legacy.card.action" || !bytes.Equal(event.JSONData(), body) {
		t.Fatal("legacy identity/fields", got.Err())
	}
	drain(t, bound.inbox)
	input.Signature = strings.Repeat("0", 64)
	receipt, err = bound.client.ReceiveLegacy(context.Background(), fault.Correlation{Call: "wrong-protocol"}, input)
	if got := resolved(t, receipt, err); !errors.Is(got.Err(), ErrAuth) {
		t.Fatal("modern signature accepted as legacy")
	}
	drain(t, bound.inbox)
	ticket := Callback{Body: []byte(`{"ts":"123","uuid":"ticket_event","token":"verification-fixture","type":"event_callback","event":{"app_id":"app_fixture","type":"app_ticket","app_ticket":"private-ticket"}}`)}
	receipt, err = bound.client.Receive(context.Background(), fault.Correlation{Call: "ticket"}, ticket)
	got = resolved(t, receipt, err)
	event, ok = got.Outcome.Value.Event()
	if got.Err() != nil || !ok || event.Type() != "app_ticket" || event.TenantKey() != "" {
		t.Fatal("app ticket was lost or tenant invented", got.Err())
	}
}

func TestWebhookCredentialSnapshotsFailClosedAndRotate(t *testing.T) {
	for _, mode := range []string{"empty", "error", "panic", "downgrade", "identity", "canceled", "valid"} {
		t.Run(mode, func(t *testing.T) {
			peer := newPeer(t, nil)
			options := callbackOptions(peer)
			var calls atomic.Int32
			options.Native.WebhookCredentials = func(ctx context.Context) (WebhookCredentials, error) {
				calls.Add(1)
				value := WebhookCredentials{AppID: options.AppID, TenantKey: options.TenantKey, VerificationToken: options.VerificationToken, EncryptKey: options.EncryptKey}
				switch mode {
				case "empty":
					return WebhookCredentials{}, nil
				case "error":
					return value, errors.New("private-provider")
				case "panic":
					panic(errors.New("private-provider"))
				case "downgrade":
					value.EncryptKey = ""
				case "identity":
					value.AppID = "other"
				case "canceled":
					<-ctx.Done()
				}
				return value, nil
			}
			if _, err := PrepareV1(options); err != nil || calls.Load() != 0 {
				t.Fatal("offline callback invocation")
			}
			bound := bindTest(t, options, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			receipt, err := bound.client.Receive(ctx, fault.Correlation{Call: "callback"}, signedCallback(t, options, notificationEvent(), false))
			result := resolved(t, receipt, err)
			_, present := result.Outcome.Value.Event()
			if mode == "valid" {
				if result.Err() != nil || !present {
					t.Fatal(result.Err())
				}
			} else if result.Err() == nil || present {
				t.Fatal("callback authentication failed open")
			}
			if strings.Contains(fmt.Sprint(result.Err()), "private-provider") {
				t.Fatal("provider error leaked")
			}
		})
	}
	peer := newPeer(t, nil)
	options := callbackOptions(peer)
	options.MaxActive = 8
	var sequence atomic.Int32
	options.Native.WebhookCredentials = func(context.Context) (WebhookCredentials, error) {
		number := sequence.Add(1) % 2
		return WebhookCredentials{AppID: options.AppID, TenantKey: options.TenantKey, VerificationToken: fmt.Sprint("token-", number), EncryptKey: fmt.Sprint("key-", number)}, nil
	}
	bound := bindTest(t, options, 8)
	// Deliberately mismatched tuple: neither coherent snapshot can authenticate it.
	mismatched := options
	mismatched.VerificationToken = "token-0"
	mismatched.EncryptKey = "key-1"
	body := bytes.ReplaceAll(notificationEvent(), []byte(options.VerificationToken), []byte(mismatched.VerificationToken))
	var wait sync.WaitGroup
	for index := 0; index < 8; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			receipt, err := bound.client.Receive(context.Background(), fault.Correlation{Call: fmt.Sprint("rotate-", index)}, signedCallback(t, mismatched, body, true))
			if err != nil {
				t.Error(err)
				return
			}
			result, _ := receipt.WaitReleased(context.Background())
			if result.Err() == nil {
				t.Error("mixed credential tuple authenticated")
			}
		}(index)
	}
	wait.Wait()
}

func FuzzCallbackProtocols(f *testing.F) {
	f.Add([]byte(`{"elements":[]}`), "", "")
	f.Add(notificationEvent(), "1700000000", "nonce")
	f.Fuzz(func(t *testing.T, body []byte, stamp, nonce string) {
		if len(body) > 1<<20 || len(stamp) > 20 || len(nonce) > 256 {
			return
		}
		value := defaults(OptionsV1{Profile: "application", AppID: "app_fixture", AppSecret: "secret", TenantKey: "tenant_fixture", VerificationToken: "verification-fixture", EncryptKey: "encrypt-fixture"})
		modern := sha256.Sum256(append([]byte(stamp+nonce+value.EncryptKey), body...))
		legacy := sha1.Sum(append([]byte(stamp+nonce+value.VerificationToken), body...))
		_, _ = value.receive(Callback{Timestamp: stamp, Nonce: nonce, Signature: hex.EncodeToString(modern[:]), Body: body}, time.Unix(1700000000, 0))
		_, _ = value.receiveLegacy(Callback{Timestamp: stamp, Nonce: nonce, Signature: hex.EncodeToString(legacy[:]), Body: body}, time.Unix(1700000000, 0))
	})
}
