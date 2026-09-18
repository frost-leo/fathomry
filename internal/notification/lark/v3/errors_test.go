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
	"log/slog"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

func TestPrivateRuntimeAndInspectableErrors(t *testing.T) {
	secret := "feishu-private-canary"
	conformance.Runtime(t, OptionsV1{AppSecret: secret}, new(OptionsV1), secret)
	conformance.Runtime(t, Content{json: JSON{value: secret}}, new(Content), secret)
	conformance.Runtime(t, JSON{value: secret}, new(JSON), secret)
	conformance.Runtime(t, Recipient{ID: secret}, new(Recipient), secret)
	conformance.Runtime(t, Upload{Data: []byte(secret)}, new(Upload), secret)
	conformance.Runtime(t, Result{data: secret, binary: []byte(secret)}, new(Result), secret)
	conformance.Runtime(t, Event{data: secret}, new(Event), secret)
	conformance.Runtime(t, Callback{Body: []byte(secret)}, new(Callback), secret)
	conformance.Runtime(t, Exchange{requestID: secret}, new(Exchange), secret)
	conformance.Runtime(t, Revision{UUID: secret}, new(Revision), secret)
	conformance.Runtime(t, Page{Token: secret}, new(Page), secret)
	cause := &larkcore.CodeError{Code: 230001, Msg: secret}
	err := failure(ErrAPI, "send", cause, context.Canceled)
	if !errors.Is(err, ErrAPI) || !errors.Is(err, context.Canceled) {
		t.Fatal("error identity lost")
	}
	conformance.Cause(t, err, func(native *larkcore.CodeError) bool { return native == cause })
	conformance.Private(t, err, secret)
	for _, value := range []any{(*OptionsV1)(nil), (*Client)(nil), (*Content)(nil), (*Result)(nil), (*JSON)(nil), (*Event)(nil), (*Source)(nil)} {
		result := slog.AnyValue(value).Resolve()
		if result.Kind() != slog.KindString || result.String() != "lark[restricted]" {
			t.Fatal("nil diagnostic unsafe")
		}
	}
}
