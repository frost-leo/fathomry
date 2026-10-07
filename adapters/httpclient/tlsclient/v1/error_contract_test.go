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

package tlsclient

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	native "github.com/frost-leo/fathomry/internal/httpclient/tlsclient/v1"
)

type cycleError struct{}

func (*cycleError) Error() string       { return "private-canary" }
func (value *cycleError) Unwrap() error { return value }
func TestErrorIdentityPrivacyAndNativeCause(t *testing.T) {
	const canary = "private-canary"
	for kind, code := range map[fault.Kind]failure.Code{native.ErrInput: ErrInput, native.ErrState: ErrState, native.ErrUnsupported: ErrUnsupported,
		native.ErrTransport: ErrTransport, native.ErrRead: ErrRead, native.ErrIntegrity: ErrIntegrity, native.ErrLimit: ErrLimit, native.ErrCleanup: ErrCleanup} {
		cause := errors.New(canary)
		original := kind.New(fault.Context{}, cause)
		mapped := translate(original, "test")
		if !errors.Is(mapped, code) || !errors.Is(mapped, original) || !errors.Is(mapped, cause) {
			t.Fatal("lost native identity", code)
		}
		conformance.Private(t, mapped, canary)
	}
	original := fail(ErrRead, "read", errors.New(canary))
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
	conformance.Private(t, translate(&cycleError{}, "cycle"), canary)
	first := Definitions()
	first[0].Message = "changed"
	if Definitions()[0].Message == "changed" {
		t.Fatal("mutable offline definitions")
	}
}

func TestRuntimeBoundariesRefuseSerialization(t *testing.T) {
	for _, value := range []any{Prepared{}, testNative(), Result{}, Metadata{}, RequestOptions{}, Stream{}, Bandwidth{}, Profile{}, BuildInfo{}, ModuleInfo{}, Fact{}} {
		conformance.Runtime(t, value, reflect.New(reflect.TypeOf(value)).Interface())
	}
}
func FuzzErrorBoundary(f *testing.F) {
	f.Add("private-canary", uint8(0))
	f.Fuzz(func(t *testing.T, text string, kind uint8) {
		cause := errors.New(text)
		var err error = cause
		switch kind % 4 {
		case 0:
			err = native.ErrTransport.New(fault.Context{}, cause)
		case 1:
			err = errors.Join(fail(ErrRead, "read"), cause)
		case 2:
			err = fmt.Errorf("wrapped: %w", fail(ErrState, "state", cause))
		case 3:
			err = &cycleError{}
		}
		mapped := translate(err, "fuzz")
		if mapped == nil {
			t.Fatal("non-nil failure vanished")
		}
	})
}

func FuzzPreparationPreservesExactData(f *testing.F) {
	f.Add("synthetic", int64(0))
	f.Fuzz(func(t *testing.T, text string, wait int64) {
		if len(text) > 4096 {
			return
		}
		duration := time.Duration(wait)
		value := Settings{Name: "fuzz", Timeout: &duration, ServerName: text}
		_, err := Prepare(value, testNative())
		if !utf8.ValidString(text) && err == nil {
			t.Fatal("invalid UTF8 was rewritten")
		}
		if (duration < time.Millisecond || duration > 24*time.Hour) && err == nil {
			t.Fatal("invalid continue wait admitted")
		}
	})
}
