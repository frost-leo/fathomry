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

package duckdb

import (
	"errors"
	"log/slog"
	"path/filepath"
	"testing"

	sdk "github.com/duckdb/duckdb-go/v2"
	"github.com/frost-leo/fathomry/internal/conformance"
)

func TestRuntimePrivacyAndNativeCauses(t *testing.T) {
	const secret = "gh40-private-value-canary"
	fixture := openFixture(t, OptionsV1{})
	result := fixture.run(t, Request{Mode: Execute, SQL: "INSERT INTO missing_gh40_" + secret + " VALUES (1)"})
	if !errors.Is(result.Err(), ErrNative) {
		t.Fatal("native failure identity lost")
	}
	conformance.Private(t, result.Err(), secret)
	conformance.Runtime(t, OptionsV1{Name: "gh40", Path: filepath.Join("/tmp", secret)}, new(OptionsV1), secret)
	conformance.Runtime(t, Request{Mode: Query, SQL: secret, Args: []any{secret}}, new(Request), secret)
	conformance.Runtime(t, result.Outcome.Value, new(Result), secret)
	native := &sdk.Error{Type: sdk.ErrorTypeConstraint, Msg: secret}
	adapted := failure(ErrNative, "query", native)
	if !errors.Is(adapted, native) {
		t.Fatal("original native identity lost")
	}
	conformance.Cause[*sdk.Error](t, adapted, func(actual *sdk.Error) bool { return actual == native })
	conformance.Private(t, adapted, secret)
}

func TestNilProgressLogging(t *testing.T) {
	for _, value := range []any{(*Result)(nil), (*Step)(nil), (*Progress)(nil)} {
		resolved := slog.AnyValue(value).Resolve()
		if resolved.Kind() != slog.KindString || resolved.String() != "duckdb[restricted]" {
			t.Fatal("nil runtime pointer emitted a panic diagnostic")
		}
	}
}
