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
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
)

// Callback borrows the exact HTTP body and three X-Lark header values during
// Receive. The HTTP adapter must reject duplicate signature/timestamp/nonce
// headers, cap its body before reading, and own HTTP deadlines and response writes.
type Callback struct {
	private
	Timestamp, Nonce, Signature string
	Body                        []byte
}

// Event is immutable authenticated notification data, not a handled/committed
// event. It never contains verification tokens or encrypt keys. Event IDs can be
// redelivered: caller-owned durable deduplication is required before side effects.
type Event struct {
	private
	id, kind, app, tenant, created, data, challenge string
}

func (value Event) ID() string         { return value.id }
func (value Event) Type() string       { return value.kind }
func (value Event) AppID() string      { return value.app }
func (value Event) TenantKey() string  { return value.tenant }
func (value Event) CreateTime() string { return value.created }
func (value Event) JSONData() []byte   { return []byte(value.data) }
func (value Event) Challenge() string  { return value.challenge }

// Event returns a value copy and presence, including authenticated URL challenges.
func (value Result) Event() (Event, bool) {
	if value.event == nil {
		return Event{}, false
	}
	return *value.event, true
}

// Receive verifies/decrypts one callback and reserves independently owned event
// evidence. It starts no HTTP server, WebSocket, dispatcher or application code.
// Only schema 2.0 IM message events and card.action.trigger are in this profile.
// Signature freshness is enforced, but replay/durable processing is caller-owned.
// URL verification follows Feishu's separate token-authenticated challenge path.
func (client *Client) Receive(ctx context.Context, id fault.Correlation, input Callback) (*invocation.Receipt[Result], error) {
	if err := client.ready("receive", false); err != nil {
		return nil, err
	}
	value := client.owner.settings
	if value.VerificationToken == "" {
		return nil, failure(ErrUnsupported, "receive")
	}
	if ctx == nil || len(input.Body) == 0 || len(input.Body) > value.MaxRequestBytes || len(input.Timestamp) > 20 || len(input.Nonce) > 256 || len(input.Signature) > 128 {
		return nil, failure(ErrInput, "callback")
	}
	call, err := invocation.Begin(ctx, client.access, invocation.Request{Name: "receive", Correlation: id, Shape: invocation.Finite,
		Bytes: value.reservation(), EvidenceBytes: value.evidenceBytes(), Admission: invocation.Budget{Limit: value.Timeout}, AttemptsKnown: true, MaxAttempts: 1}, client.inbox, client.observer)
	if err != nil {
		return nil, err
	}
	result := Result{effect: Rejected}
	work, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(ctx, invocation.Execute)
	if err == nil {
		defer cancel()
		var event Event
		event, err = value.receive(input, time.Now())
		if err == nil && work.Err() != nil {
			err = work.Err()
		}
		if err == nil {
			result.effect = Accepted
			result.event = &event
		}
	}
	if err != nil {
		err = failure(ErrReceive, "receive", err)
	}
	call.Complete(invocation.Outcome[Result]{Present: true, Value: result, Primary: err})
	return call.Receipt(), nil
}
func (value settings) receive(input Callback, now time.Time) (event Event, err error) {
	unsigned := input.Signature == "" && input.Timestamp == "" && input.Nonce == ""
	if !unsigned {
		if err := value.verifySignature(input, now); err != nil {
			return Event{}, err
		}
	} else {
		// Only URL verification may omit signatures. Its failures deliberately share
		// one authentication result rather than expose CBC/JSON validation details.
		defer func() {
			if err != nil {
				event = Event{}
				err = failure(ErrAuth, "challenge")
			}
		}()
	}
	if err := checkJSON(input.Body, value.MaxRequestBytes); err != nil {
		return Event{}, err
	}
	if _, err := exactFields(input.Body, "encrypt"); err != nil {
		return Event{}, failure(ErrReceive, "envelope", err)
	}
	var wrapper struct {
		Encrypt string `json:"encrypt"`
	}
	if json.Unmarshal(input.Body, &wrapper) != nil {
		return Event{}, failure(ErrReceive, "envelope")
	}
	plain := input.Body
	if wrapper.Encrypt != "" {
		var err error
		plain, err = decryptEvent(wrapper.Encrypt, value.EncryptKey)
		if err != nil {
			return Event{}, err
		}
		if err = checkJSON(plain, value.MaxRequestBytes); err != nil {
			return Event{}, err
		}
	}
	if err := exactEventFields(plain); err != nil {
		return Event{}, failure(ErrReceive, "envelope", err)
	}
	var envelope struct {
		Schema    string                 `json:"schema"`
		Header    *larkevent.EventHeader `json:"header"`
		Event     json.RawMessage        `json:"event"`
		Type      string                 `json:"type"`
		Token     string                 `json:"token"`
		Challenge string                 `json:"challenge"`
	}
	if json.Unmarshal(plain, &envelope) != nil {
		return Event{}, failure(ErrReceive, "envelope")
	}
	if envelope.Type == "url_verification" {
		if subtle.ConstantTimeCompare([]byte(envelope.Token), []byte(value.VerificationToken)) != 1 || envelope.Challenge == "" || !shortText(envelope.Challenge, 2048) {
			return Event{}, failure(ErrAuth, "challenge")
		}
		return Event{kind: "url_verification", challenge: envelope.Challenge}, nil
	}
	if unsigned {
		return Event{}, failure(ErrAuth, "signature")
	}
	header := envelope.Header
	if envelope.Schema != "2.0" || header == nil || !identifier(header.EventID) || header.AppID != value.AppID || header.TenantKey != value.TenantKey ||
		subtle.ConstantTimeCompare([]byte(header.Token), []byte(value.VerificationToken)) != 1 {
		return Event{}, failure(ErrAuth, "event-identity")
	}
	if !strings.HasPrefix(header.EventType, "im.message.") && header.EventType != "card.action.trigger" {
		return Event{}, failure(ErrUnsupported, "event-type")
	}
	if !shortText(header.EventType, 128) || !shortText(header.CreateTime, 32) || len(envelope.Event) == 0 || envelope.Event[0] != '{' {
		return Event{}, failure(ErrReceive, "event")
	}
	return Event{id: header.EventID, kind: header.EventType, app: header.AppID, tenant: header.TenantKey, created: header.CreateTime, data: string(envelope.Event)}, nil
}

func exactEventFields(data []byte) error {
	fields, err := exactFields(data, "schema", "header", "event", "type", "token", "challenge", "encrypt")
	if err != nil {
		return err
	}
	if header, present := fields["header"]; present && string(header) != "null" {
		_, err = exactFields(header, "event_id", "event_type", "app_id", "tenant_key", "create_time", "token")
	}
	return err
}

func (value settings) verifySignature(input Callback, now time.Time) error {
	stamp, err := strconv.ParseInt(input.Timestamp, 10, 64)
	if err != nil || stamp <= 0 || !secretValid(input.Nonce, 256) {
		return failure(ErrAuth, "signature")
	}
	sent := time.Unix(stamp, 0)
	if sent.Before(now.Add(-value.CallbackMaxAge)) || sent.After(now.Add(value.CallbackMaxAge)) {
		return failure(ErrAuth, "stale-callback")
	}
	actual, err := hex.DecodeString(input.Signature)
	if err != nil || len(actual) != sha256.Size {
		return failure(ErrAuth, "signature")
	}
	expected, _ := hex.DecodeString(larkevent.Signature(input.Timestamp, input.Nonce, value.EncryptKey, string(input.Body)))
	if subtle.ConstantTimeCompare(actual, expected) != 1 {
		return failure(ErrAuth, "signature")
	}
	return nil
}
func decryptEvent(encoded, key string) ([]byte, error) {
	data, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(data) < 2*aes.BlockSize || len(data)%aes.BlockSize != 0 {
		return nil, failure(ErrReceive, "ciphertext")
	}
	hash := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(hash[:])
	if err != nil {
		return nil, failure(ErrReceive, "cipher", err)
	}
	plain := data[aes.BlockSize:]
	cipher.NewCBCDecrypter(block, data[:aes.BlockSize]).CryptBlocks(plain, plain)
	padding := int(plain[len(plain)-1])
	if padding < 1 || padding > aes.BlockSize || !bytes.Equal(plain[len(plain)-padding:], bytes.Repeat([]byte{byte(padding)}, padding)) {
		return nil, failure(ErrReceive, "padding")
	}
	// Native EventDecrypt searches for braces and silently discards other plaintext.
	// Strict PKCS#7 and complete-JSON validation retain the actual envelope boundary.
	return plain[:len(plain)-padding], nil
}
