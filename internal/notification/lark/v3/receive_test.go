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
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
)

func callbackOptions(peer *peer) OptionsV1 {
	value := testOptions(peer)
	value.TenantKey = "tenant_fixture"
	value.VerificationToken = "verification-fixture"
	value.EncryptKey = "encrypt-fixture"
	return value
}
func signedCallback(t testing.TB, options OptionsV1, plain []byte, encrypted bool) Callback {
	t.Helper()
	body := bytes.Clone(plain)
	if encrypted {
		key := sha256.Sum256([]byte(options.EncryptKey))
		block, err := aes.NewCipher(key[:])
		if err != nil {
			t.Fatal(err)
		}
		padding := aes.BlockSize - len(body)%aes.BlockSize
		body = append(body, bytes.Repeat([]byte{byte(padding)}, padding)...)
		wire := make([]byte, aes.BlockSize+len(body))
		cipher.NewCBCEncrypter(block, wire[:aes.BlockSize]).CryptBlocks(wire[aes.BlockSize:], body)
		body, _ = json.Marshal(map[string]string{"encrypt": base64.StdEncoding.EncodeToString(wire)})
	}
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := "fixture-nonce"
	return Callback{Timestamp: stamp, Nonce: nonce, Signature: larkevent.Signature(stamp, nonce, options.EncryptKey, string(body)), Body: body}
}
func notificationEvent() []byte {
	return []byte(`{"schema":"2.0","header":{"event_id":"event_fixture","event_type":"im.message.receive_v1","app_id":"app_fixture","tenant_key":"tenant_fixture","token":"verification-fixture","create_time":"1700000000000"},"event":{"message":{"message_id":"om_incoming","message_type":"text","content":"{\"text\":\"Synthetic\"}"}}}`)
}
func TestReceiveSignatureDecryptionAndIndependentEvent(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		peer := newPeer(t, nil)
		options := callbackOptions(peer)
		bound := bindTest(t, options, 2)
		input := signedCallback(t, options, notificationEvent(), encrypted)
		receipt, err := bound.client.Receive(context.Background(), fault.Correlation{Call: "received"}, input)
		got := resolved(t, receipt, err)
		event, present := got.Outcome.Value.Event()
		if got.Err() != nil || !present || event.ID() != "event_fixture" || event.Type() != "im.message.receive_v1" || event.TenantKey() != "tenant_fixture" {
			t.Fatal("authenticated event lost")
		}
		if bytes.Contains(event.JSONData(), []byte(options.VerificationToken)) {
			t.Fatal("event retained verification token")
		}
		input.Body[0] = 'X'
		body := event.JSONData()
		body[0] = 'X'
		again, _ := got.Outcome.Value.Event()
		if !json.Valid(again.JSONData()) {
			t.Fatal("event aliases caller data")
		}
		if len(peer.snapshot()) != 0 || got.Attempts.Observed != 0 || !got.Attempts.Exact {
			t.Fatal("receiving performed unexpected network work")
		}
		input = signedCallback(t, options, notificationEvent(), encrypted)
		receipt, err = bound.client.Receive(context.Background(), fault.Correlation{Call: "redelivery"}, input)
		duplicate := resolved(t, receipt, err)
		if duplicate.Err() != nil {
			t.Fatal("provider claimed durable deduplication authority")
		}
	}
}
func TestCallbackRejectsForgeryWrongIdentityAndStaleInput(t *testing.T) {
	peer := newPeer(t, nil)
	options := callbackOptions(peer)
	bound := bindTest(t, options, 1)
	for _, mode := range []string{"signature", "timestamp", "tenant", "app", "token", "type", "duplicate", "padding"} {
		t.Run(mode, func(t *testing.T) {
			plain := notificationEvent()
			switch mode {
			case "tenant":
				plain = bytes.ReplaceAll(plain, []byte("tenant_fixture"), []byte("tenant_other"))
			case "app":
				plain = bytes.ReplaceAll(plain, []byte("app_fixture"), []byte("app_other"))
			case "token":
				plain = bytes.ReplaceAll(plain, []byte("verification-fixture"), []byte("wrong-token"))
			case "type":
				plain = bytes.ReplaceAll(plain, []byte("im.message.receive_v1"), []byte("contact.user.created_v3"))
			case "duplicate":
				plain = bytes.Replace(plain, []byte(`"schema":"2.0"`), []byte(`"schema":"1.0","schema":"2.0"`), 1)
			}
			input := signedCallback(t, options, plain, mode == "padding")
			if mode == "signature" {
				input.Signature = strings.Repeat("0", 64)
			}
			if mode == "timestamp" {
				input.Timestamp = strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10)
				input.Signature = larkevent.Signature(input.Timestamp, input.Nonce, options.EncryptKey, string(input.Body))
			}
			if mode == "padding" {
				var wrapper map[string]string
				_ = json.Unmarshal(input.Body, &wrapper)
				data, _ := base64.StdEncoding.DecodeString(wrapper["encrypt"])
				data[len(data)-1] ^= 0xff
				wrapper["encrypt"] = base64.StdEncoding.EncodeToString(data)
				input.Body, _ = json.Marshal(wrapper)
				input.Signature = larkevent.Signature(input.Timestamp, input.Nonce, options.EncryptKey, string(input.Body))
			}
			receipt, err := bound.client.Receive(context.Background(), fault.Correlation{Call: "rejected"}, input)
			got := resolved(t, receipt, err)
			if !errors.Is(got.Err(), ErrReceive) {
				t.Fatal("invalid callback accepted")
			}
			if _, present := got.Outcome.Value.Event(); present {
				t.Fatal("untrusted callback escaped")
			}
			drain(t, bound.inbox)
		})
	}
}
func TestURLChallengeEscapingAndNativeDecryptCounterexample(t *testing.T) {
	peer := newPeer(t, nil)
	options := callbackOptions(peer)
	bound := bindTest(t, options, 1)
	plain := []byte(`{"type":"url_verification","token":"verification-fixture","challenge":"quote\"challenge"}`)
	input := signedCallback(t, options, plain, true)
	input.Signature = ""
	input.Timestamp = ""
	input.Nonce = ""
	receipt, err := bound.client.Receive(context.Background(), fault.Correlation{Call: "challenge"}, input)
	got := resolved(t, receipt, err)
	event, present := got.Outcome.Value.Event()
	if got.Err() != nil || !present || event.Challenge() != `quote"challenge` {
		t.Fatal("token-authenticated challenge failed")
	}
	// Native decryption accepts junk surrounding an object; our full envelope
	// parser must reject it rather than silently cropping authenticated plaintext.
	malformed := signedCallback(t, options, []byte("junk"+string(notificationEvent())+"junk"), true)
	var wrapper struct {
		Encrypt string `json:"encrypt"`
	}
	_ = json.Unmarshal(malformed.Body, &wrapper)
	native, nativeErr := larkevent.EventDecrypt(wrapper.Encrypt, options.EncryptKey)
	if nativeErr != nil || !json.Valid(native) {
		t.Fatal("native rejecting-control premise changed")
	}
	drain(t, bound.inbox)
	receipt, err = bound.client.Receive(context.Background(), fault.Correlation{Call: "framing"}, malformed)
	if result := resolved(t, receipt, err); result.Err() == nil {
		t.Fatal("native malformed framing was accepted")
	}
}
func FuzzCallbackDecrypt(f *testing.F) {
	f.Add("")
	f.Add("AAAAAAAAAAAAAAAAAAAAAA==")
	f.Fuzz(func(t *testing.T, encoded string) {
		if len(encoded) > 1<<20 {
			return
		}
		_, _ = decryptEvent(encoded, "fixture")
	})
}
func TestSignaturePrecedesDecryptionAndUnsignedFailuresAreUniform(t *testing.T) {
	peer := newPeer(t, nil)
	options := callbackOptions(peer)
	value := defaults(options)
	input := Callback{Timestamp: strconv.FormatInt(time.Now().Unix(), 10), Nonce: "nonce", Signature: strings.Repeat("0", 64), Body: []byte(`{"encrypt":"not-base64"}`)}
	if _, err := value.receive(input, time.Now()); !errors.Is(err, ErrAuth) {
		t.Fatal("unauthenticated ciphertext reached decryption")
	}
	for _, body := range []string{`{"encrypt":"not-base64"}`, `{"type":"url_verification","token":"wrong","challenge":"x"}`, string(notificationEvent())} {
		if _, err := value.receive(Callback{Body: []byte(body)}, time.Now()); !errors.Is(err, ErrAuth) || errors.Is(err, ErrReceive) {
			t.Fatal("unsigned validation exposed an oracle or accepted an event")
		}
	}
}
