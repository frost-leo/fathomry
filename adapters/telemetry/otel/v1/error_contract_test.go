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

package otel

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	native "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
)

type cycleError struct{}

func (*cycleError) Error() string       { return "private-canary" }
func (value *cycleError) Unwrap() error { return value }

func TestErrorIdentityPrivacyAndNativeCause(t *testing.T) {
	const canary = "private-canary"
	for kind, code := range map[fault.Kind]failure.Code{
		native.ErrInput: ErrInput, native.ErrUnsupported: ErrUnsupported, native.ErrEnvironment: ErrEnvironment,
		native.ErrLimit: ErrLimit, native.ErrState: ErrState, native.ErrExport: ErrExport,
		native.ErrPartial: ErrPartial, native.ErrProtocol: ErrProtocol, native.ErrCleanup: ErrCleanup,
		native.ErrUndelivered: ErrUndelivered, native.ErrRecursion: ErrRecursion,
	} {
		cause := errors.New(canary)
		original := kind.New(fault.Context{}, cause)
		mapped := translate(original, "test")
		if !errors.Is(mapped, code) || !errors.Is(mapped, original) || !errors.Is(mapped, cause) {
			t.Fatal("lost native identity", code)
		}
		conformance.Private(t, mapped, canary)
	}
	original := fail(ErrExport, "export", errors.New(canary))
	if translate(original, "next") != original {
		t.Fatal("public occurrence reclassified")
	}
	wrapped := translate(fmt.Errorf(canary+": %w", original), "wrapped")
	conformance.Private(t, wrapped, canary)
	if !errors.Is(wrapped, original) {
		t.Fatal("public wrapper identity lost")
	}
	mixed := translate(errors.Join(original, adapters.ErrClosed), "mixed")
	if !errors.Is(mixed, original) || !errors.Is(mixed, adapters.ErrClosed) {
		t.Fatal("mixed public causes lost")
	}
	conformance.Private(t, mixed, canary)
	conformance.Private(t, translate(&cycleError{}, "cycle"), canary)
}

func TestOfflineDefinitionsAndLocales(t *testing.T) {
	definitions := Definitions()
	definitions[0].Message = "changed"
	if Definitions()[0].Message == "changed" {
		t.Fatal("offline definitions were mutable")
	}
	catalog, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "telemetry_otel", BaseLocale: "en",
		Resources: Resources(), Directory: "resources", Definitions: Definitions()})
	if err != nil {
		t.Fatal(err)
	}
	for _, definition := range Definitions() {
		for _, locale := range []string{"en", "zh-CN"} {
			explanation, found, err := catalog.Explain(definition.Code, locale)
			if err != nil || !found || explanation.Message.Locale != locale ||
				locale == "en" && explanation.Message.Text != definition.Message ||
				locale == "zh-CN" && explanation.Message.Text == definition.Message {
				t.Fatal("missing or inconsistent offline explanation", definition.Code, locale, explanation, err)
			}
		}
	}
}

func TestExportRuntimeBoundaries(t *testing.T) {
	for _, value := range []any{ExportLoop{}, ExportStatus{LastError: errors.New("private-canary")}} {
		conformance.Runtime(t, value, reflect.New(reflect.TypeOf(value)).Interface())
		conformance.Private(t, value, "private-canary")
	}
}

func FuzzErrorBoundary(f *testing.F) {
	f.Add("private-canary", uint8(0))
	f.Fuzz(func(t *testing.T, text string, kind uint8) {
		cause := errors.New(text)
		var err error
		switch kind % 4 {
		case 0:
			err = native.ErrExport.New(fault.Context{}, cause)
		case 1:
			err = errors.Join(fail(ErrPartial, "flush"), cause)
		case 2:
			err = fmt.Errorf("wrapped: %w", fail(ErrState, "state", cause))
		case 3:
			err = &cycleError{}
		}
		if translate(err, "fuzz") == nil {
			t.Fatal("non-nil failure disappeared")
		}
	})
}
