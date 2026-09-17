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

package mail

import (
	"context"
	"errors"
	"log/slog"
	"net/textproto"
	"os"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
)

func TestPrivateRuntimeAndInspectableCauses(t *testing.T) {
	secret := "private-mail-canary"
	conformance.Runtime(t, OptionsV1{Password: secret}, new(OptionsV1), secret)
	conformance.Runtime(t, Content{Text: secret}, new(Content), secret)
	conformance.Runtime(t, Inline{Data: []byte(secret)}, new(Inline), secret)
	conformance.Runtime(t, Attachment{Data: []byte(secret)}, new(Attachment), secret)
	conformance.Runtime(t, Message{content: &Content{Text: secret}}, new(Message), secret)
	conformance.Runtime(t, Result{deliveries: []Delivery{{id: secret, sender: secret, recipients: []Recipient{{address: secret}}}}}, new(Result), secret)
	conformance.Runtime(t, Delivery{id: secret}, new(Delivery), secret)
	conformance.Runtime(t, Recipient{address: secret}, new(Recipient), secret)
	cause := &textproto.Error{Code: 550, Msg: "5.1.1 " + secret}
	err := failure(ErrSend, "send", cause, context.Canceled)
	if !errors.Is(err, ErrSend) || !errors.Is(err, context.Canceled) {
		t.Fatal("error identity lost")
	}
	conformance.Cause(t, err, func(native *textproto.Error) bool { return native == cause })
	conformance.Private(t, err, secret)
	for _, value := range []any{(*Client)(nil), (*OptionsV1)(nil), (*Message)(nil), (*Content)(nil), (*Inline)(nil), (*Attachment)(nil), (*Result)(nil), (*Source)(nil), (*Recipient)(nil), (*Delivery)(nil)} {
		projected := slog.AnyValue(value).Resolve()
		if projected.Kind() != slog.KindString || projected.String() != "mail[restricted]" {
			t.Fatal("typed nil diagnostics are unsafe")
		}
	}
}
func TestEnhancedStatusProjection(t *testing.T) {
	for _, text := range []string{"5.1.1 private message", "4.7.123 throttled", "2.0.0 accepted"} {
		if enhancedCode(text) == "" {
			t.Fatal("valid enhanced code missing")
		}
	}
	for _, text := range []string{"secret", "5.9999.1 text", "5.1.1-secret", "5.+1.1 text", "3.0.0 text", "5.1.1\nsecret"} {
		if enhancedCode(text) != "" {
			t.Fatal("malformed/private enhanced code accepted")
		}
	}
}

func TestPhaseFailurePreservesCausesWithoutInventingExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	err := phaseFailure(ErrSend, "test", ctx, os.ErrDeadlineExceeded)
	if errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("an unrelated timeout was presented as an expired phase")
	}
	if phaseFailure(ErrSend, "test", ctx, nil) != nil {
		t.Fatal("successful completion gained an invented error")
	}
	cause := errors.New("fixture cancellation cause")
	canceled, cancelCause := context.WithCancelCause(context.Background())
	cancelCause(cause)
	err = phaseFailure(ErrCleanup, "test", canceled, os.ErrClosed)
	if !errors.Is(err, cause) || !errors.Is(err, context.Canceled) || !errors.Is(err, os.ErrClosed) {
		t.Fatal("cleanup cancellation or native cause lost")
	}
	expired, end := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer end()
	if !errors.Is(phaseFailure(ErrSend, "test", expired, os.ErrDeadlineExceeded), context.DeadlineExceeded) {
		t.Fatal("owned deadline was not preserved")
	}
}
