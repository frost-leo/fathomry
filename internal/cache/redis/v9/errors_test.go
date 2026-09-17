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

package redis

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	sdk "github.com/redis/go-redis/v9"
)

func TestRuntimePrivacyAndOriginalCauses(t *testing.T) {
	secret := "private-redis-canary"
	conformance.Runtime(t, OptionsV1{Password: secret}, new(OptionsV1), secret)
	conformance.Runtime(t, NewCommand("SET", secret, secret), new(Command), secret)
	conformance.Runtime(t, freeze([]any{secret}), new(Value), secret)
	conformance.Runtime(t, Result{replies: []Reply{{value: freeze(secret)}}}, new(Result), secret)
	cause := sdk.ErrNoScript
	err := failure(ErrCommand, "test", cause)
	if !errors.Is(err, cause) {
		t.Fatal("native identity lost")
	}
	conformance.Private(t, err, secret)
	for _, value := range []any{(*Client)(nil), (*Session)(nil), (*Subscription)(nil), (*Command)(nil), (*Password)(nil)} {
		if projected := slog.AnyValue(value).Resolve(); projected.Kind() != slog.KindString || projected.String() != "redis[restricted]" {
			t.Fatal("typed nil slog projection is unsafe")
		}
	}
}
