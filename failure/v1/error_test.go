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

package failure_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
)

const sampleCode failure.Code = 0xA4400001

func definition() failure.Definition {
	return failure.Definition{
		Code: sampleCode, Identifier: "example.source.read_failed", Module: "example", Component: "source",
		Revision: 1, Message: "The selected configuration could not be read.",
		Description: "The operation did not obtain a complete document; native causes retain the observed failure.",
	}
}

func mustError(t testing.TB, definition failure.Definition, where failure.Location, causes ...error) *failure.Error {
	t.Helper()
	value, err := failure.New(definition, where, causes...)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestOccurrence(t *testing.T) {
	t.Run("native evidence and semantic layers", func(t *testing.T) {
		native := &fs.PathError{Op: "open", Path: "private-native-path", Err: fs.ErrPermission}
		d := definition()
		where := failure.Location{Operation: "read", Instance: "007-Worker"}
		slots := []error{native, nil}
		inner := mustError(t, d, where, slots...)
		slots[0] = errors.New("replacement")
		d.Message, where.Operation = "mutated", "mutated"
		outerDefinition := definition()
		outerDefinition.Code++
		outerDefinition.Identifier = "example.source.preparation_failed"
		outer := mustError(t, outerDefinition, failure.Location{Operation: "prepare"}, inner)
		if !errors.Is(outer, outerDefinition.Code) || !errors.Is(outer, sampleCode) || !errors.Is(outer, fs.ErrPermission) {
			t.Fatal("semantic or native matching lost")
		}
		var original *fs.PathError
		if !errors.As(outer, &original) || original != native || original.Path != "private-native-path" {
			t.Fatal("native pointer/type/message lost")
		}
		if errors.Is(inner, mustError(t, definition(), failure.Location{})) {
			t.Fatal("different occurrences matched by identity")
		}
		core, ok := failure.Inspect(outer)
		if !ok || core != outer || core.Diagnostic().Location.Operation != "prepare" {
			t.Fatal("wrong current occurrence selected")
		}
		if got := inner.Diagnostic(); got.Definition.Message != definition().Message || got.Location.Operation != "read" || got.Location.Instance != "007-Worker" || got.CauseCount != 1 {
			t.Fatal("metadata mutation or direct cause count changed")
		}
		causes := inner.Unwrap()
		causes[0] = nil
		if inner.Unwrap()[0] != native {
			t.Fatal("cause-slice storage escaped")
		}
		if errors.Unwrap(inner) != nil {
			t.Fatal("multi-cause contract unexpectedly became single unwrap")
		}
		for _, wrapped := range []error{fmt.Errorf("outer: %w", inner), errors.Join(inner, outer), sampleCode, errors.New("native")} {
			if current, ok := failure.Inspect(wrapped); ok || current != nil {
				t.Fatal("guessed an occurrence inside a wrapper or join")
			}
		}
	})
	t.Run("admission and zero values", func(t *testing.T) {
		bad := definition()
		bad.Code = 0
		if value, err := failure.New(bad, failure.Location{}); value != nil || !errors.Is(err, failure.ErrCode) {
			t.Fatal(value, err)
		}
		bad = definition()
		bad.Identifier = "different.source.reason"
		if value, err := failure.New(bad, failure.Location{}); value != nil || !errors.Is(err, failure.ErrDefinition) {
			t.Fatal(value, err)
		}
		if value, err := failure.New(definition(), failure.Location{Operation: "private operation\n"}); value != nil || !errors.Is(err, failure.ErrLocation) {
			t.Fatal("invalid location retained", err)
		}
		if value, err := failure.New(definition(), failure.Location{}, make([]error, failure.MaxCauses+1)...); value != nil || !errors.Is(err, failure.ErrLimit) {
			t.Fatal("cause-slot budget ignored", err)
		}
		var absent *failure.Error
		if absent.Error() != "<nil>" || absent.Unwrap() != nil || absent.Is(sampleCode) || absent.Diagnostic().Definition.Code != 0 {
			t.Fatal("nil semantics changed")
		}
		for _, input := range []error{nil, absent, new(failure.Error)} {
			if core, ok := failure.Inspect(input); core != nil || ok {
				t.Fatal("invalid occurrence acquired identity")
			}
		}
		var nilNative *fs.PathError
		value := mustError(t, definition(), failure.Location{}, nilNative)
		if value.Diagnostic().CauseCount != 1 || reflect.TypeOf(value.Unwrap()[0]) != reflect.TypeFor[*fs.PathError]() {
			t.Fatal("typed nil native interface was rewritten")
		}
		if current, ok := failure.Inspect(failure.ErrCode); ok || current != nil {
			t.Fatal("bare code became an occurrence")
		}
	})
}

type hostileCause struct{}

func (*hostileCause) Error() string { panic("foreign formatter called") }

func assertPrivate(t testing.TB, value any, secret string) {
	t.Helper()
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		text := fmt.Sprintf(format, value)
		if strings.Contains(text, secret) || strings.Contains(text, "PANIC") {
			t.Fatalf("unsafe runtime presentation with %s", format)
		}
	}
	for _, handler := range []func(*bytes.Buffer) slog.Handler{
		func(buffer *bytes.Buffer) slog.Handler { return slog.NewTextHandler(buffer, nil) },
		func(buffer *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(buffer, nil) },
	} {
		var buffer bytes.Buffer
		slog.New(handler(&buffer)).Info("event", slog.Any("error", value))
		if strings.Contains(buffer.String(), secret) || strings.Contains(buffer.String(), "PANIC") {
			t.Fatal("unsafe structured diagnostic")
		}
	}
}

func TestDiagnostics(t *testing.T) {
	secret := "private-payload-canary"
	value := mustError(t, definition(), failure.Location{Operation: "read", Instance: "alpha"},
		&hostileCause{}, &fs.PathError{Op: "read", Path: secret, Err: errors.New(secret)})
	for _, form := range []any{value, *value, (*failure.Error)(nil), failure.Error{}} {
		assertPrivate(t, form, secret)
	}
	text := value.Error()
	if !strings.Contains(text, sampleCode.String()) || !strings.Contains(text, string(definition().Identifier)) || !strings.Contains(text, definition().Message) {
		t.Fatal("code, readable identity or explanation missing")
	}
	var output bytes.Buffer
	slog.New(slog.NewJSONHandler(&output, nil)).Error("read failed", slog.Any("error", value))
	for _, name := range []string{`"code"`, `"identifier"`, `"module"`, `"component"`, `"operation"`, `"instance"`, `"message"`, `"cause_count"`} {
		if !strings.Contains(output.String(), name) {
			t.Fatal("missing structured diagnostic field", name)
		}
	}
	for _, form := range []any{value, *value} {
		if _, err := json.Marshal(form); !errors.Is(err, failure.ErrSerialization) {
			t.Fatal("runtime error serialized", err)
		}
	}
	for _, raw := range []string{"{}", "null", `{"Code":7}`, "[]", "true"} {
		var zero failure.Error
		if err := json.Unmarshal([]byte(raw), &zero); !errors.Is(err, failure.ErrSerialization) {
			t.Fatal("runtime error reconstructed", err)
		}
	}
	if _, err := json.Marshal(value.Diagnostic()); err != nil {
		t.Fatal("safe scalar diagnostic refused", err)
	}
}

func TestConcurrentOccurrenceReads(t *testing.T) {
	value := mustError(t, definition(), failure.Location{Operation: "read"}, fs.ErrPermission)
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			for range 100 {
				if !errors.Is(value, sampleCode) || !errors.Is(value, fs.ErrPermission) {
					t.Error("immutable matching changed")
				}
				copy := value.Diagnostic()
				copy.Definition.Message = "changed"
				_ = value.Error()
				_ = value.LogValue()
				value.Unwrap()[0] = nil
			}
		})
	}
	workers.Wait()
}
