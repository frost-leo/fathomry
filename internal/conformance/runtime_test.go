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

package conformance_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type scalarRuntime string

func (scalarRuntime) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("runtime")) }
func (scalarRuntime) LogValue() slog.Value           { return slog.StringValue("runtime") }
func (scalarRuntime) MarshalJSON() ([]byte, error)   { return nil, errors.New("JSON refused") }

type guardedScalar scalarRuntime

func (guardedScalar) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("runtime")) }
func (guardedScalar) LogValue() slog.Value           { return slog.StringValue("runtime") }
func (guardedScalar) MarshalJSON() ([]byte, error)   { return nil, errors.New("JSON refused") }
func (*guardedScalar) UnmarshalJSON([]byte) error    { return errors.New("JSON refused") }

type pointerRuntime struct{ safeFormatting }

func (*pointerRuntime) MarshalJSON() ([]byte, error) { return nil, errors.New("JSON refused") }
func (*pointerRuntime) UnmarshalJSON([]byte) error   { return errors.New("JSON refused") }

type textRuntimeRefusal struct{ safeFormatting }

func (textRuntimeRefusal) MarshalText() ([]byte, error) { return nil, errors.New("JSON refused") }
func (*textRuntimeRefusal) UnmarshalJSON([]byte) error  { return errors.New("JSON refused") }

type pointerTextRuntime struct{ safeFormatting }

func (*pointerTextRuntime) MarshalText() ([]byte, error) { return nil, errors.New("JSON refused") }
func (*pointerTextRuntime) UnmarshalJSON([]byte) error   { return errors.New("JSON refused") }

type objectOnlyGuard struct{ runtimeRefusal }

func (*objectOnlyGuard) UnmarshalJSON(data []byte) error {
	if len(data) != 0 && data[0] == '{' {
		return errors.New("object refused")
	}
	return nil
}

type partialGuard[T any] struct{ runtimeRefusal }

func (*partialGuard[T]) UnmarshalJSON(data []byte) error {
	var value T
	if string(data) != "null" && json.Unmarshal(data, &value) == nil {
		return nil
	}
	return errors.New("JSON refused")
}

type nullOnlyGuard struct{ runtimeRefusal }

func (*nullOnlyGuard) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	return errors.New("JSON refused")
}

type mismatchGuard struct{ runtimeRefusal }

func (*mismatchGuard) UnmarshalJSON(data []byte) error {
	var value chan int
	return json.Unmarshal(data, &value)
}

type malformedGuard struct{ runtimeRefusal }

func (*malformedGuard) UnmarshalJSON([]byte) error {
	var value any
	return json.Unmarshal([]byte("{"), &value)
}

type invalidTargetGuard struct{ runtimeRefusal }

func (*invalidTargetGuard) UnmarshalJSON(data []byte) error {
	var target *int
	return json.Unmarshal(data, target)
}

type malformedEncoding struct{ runtimeRefusal }

func (malformedEncoding) MarshalJSON() ([]byte, error) { return []byte("{"), nil }

type emptyStreamGuard struct{ runtimeRefusal }

func (*emptyStreamGuard) UnmarshalJSON([]byte) error {
	return json.NewDecoder(strings.NewReader("")).Decode(new(any))
}

type truncatedStreamGuard struct{ runtimeRefusal }

func (*truncatedStreamGuard) UnmarshalJSON([]byte) error {
	return json.NewDecoder(strings.NewReader("{")).Decode(new(any))
}

type wrappedStreamGuard struct{ runtimeRefusal }

func (*wrappedStreamGuard) UnmarshalJSON([]byte) error {
	return fmt.Errorf("decode: %w", json.NewDecoder(strings.NewReader("{")).Decode(new(any)))
}

type streamEncodingFailure struct{ runtimeRefusal }

func (streamEncodingFailure) MarshalJSON() ([]byte, error) {
	return nil, json.NewDecoder(strings.NewReader("{")).Decode(new(any))
}

type wrappedRuntimeRefusal struct{ runtimeRefusal }

func (*wrappedRuntimeRefusal) UnmarshalJSON([]byte) error {
	return fmt.Errorf("decode: %w", errors.New("runtime JSON refused"))
}

type valueReceiverGuard struct{ runtimeRefusal }

func (valueReceiverGuard) UnmarshalJSON([]byte) error { return errors.New("JSON refused") }

type unsafeDecodeError struct{ runtimeRefusal }

func (*unsafeDecodeError) UnmarshalJSON([]byte) error { return errors.New(diagnosticCanary) }

type decodeErrorPanic struct{ runtimeRefusal }

func (*decodeErrorPanic) UnmarshalJSON([]byte) error { return panicError{} }

type freshTargetGuard struct{ state int }

func (freshTargetGuard) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("runtime")) }
func (freshTargetGuard) LogValue() slog.Value           { return slog.StringValue("runtime") }
func (freshTargetGuard) MarshalJSON() ([]byte, error)   { return nil, errors.New("JSON refused") }
func (value *freshTargetGuard) UnmarshalJSON([]byte) error {
	if value.state != 0 {
		panic(diagnosticCanary)
	}
	value.state++
	return errors.New("JSON refused")
}

func runtimeShapeCases() []checkCase {
	var cases []checkCase
	for _, fixture := range []struct {
		name   string
		value  any
		reject string
	}{
		{"unguarded-scalar", scalarRuntime("live"), "JSON reconstruction guard"},
		{"guarded-scalar", guardedScalar("live"), ""},
		{"pointer-method-not-on-value", pointerRuntime{}, "JSON encoding guard"},
		{"pointer-method", &pointerRuntime{}, ""},
		{"text-guard", textRuntimeRefusal{}, ""},
		{"text-guard-pointer", &textRuntimeRefusal{}, ""},
		{"text-pointer-method", &pointerTextRuntime{}, ""},
		{"text-pointer-method-not-on-value", pointerTextRuntime{}, "JSON encoding guard"},
		{"object-only", objectOnlyGuard{}, "accepted JSON reconstruction"},
		{"accepts-string", partialGuard[string]{}, "accepted JSON reconstruction"},
		{"accepts-number", partialGuard[float64]{}, "accepted JSON reconstruction"},
		{"accepts-boolean", partialGuard[bool]{}, "accepted JSON reconstruction"},
		{"accepts-array", partialGuard[[]any]{}, "accepted JSON reconstruction"},
		{"accepts-object", partialGuard[map[string]any]{}, "accepted JSON reconstruction"},
		{"accepts-null", nullOnlyGuard{}, "accepted JSON reconstruction"},
		{"type-mismatch", mismatchGuard{}, "not a JSON reconstruction refusal"},
		{"malformed-input", malformedGuard{}, "not a JSON reconstruction refusal"},
		{"invalid-input-target", invalidTargetGuard{}, "not a JSON reconstruction refusal"},
		{"malformed-encoding", malformedEncoding{}, "not a JSON encoding refusal"},
		{"empty-stream", emptyStreamGuard{}, "not a JSON reconstruction refusal"},
		{"truncated-stream", truncatedStreamGuard{}, "not a JSON reconstruction refusal"},
		{"wrapped-truncated-stream", wrappedStreamGuard{}, "not a JSON reconstruction refusal"},
		{"encoding-stream-failure", streamEncodingFailure{}, "not a JSON encoding refusal"},
		{"wrapped-refusal", wrappedRuntimeRefusal{}, ""},
		{"value-receiver", valueReceiverGuard{}, ""},
		{"decode-error-canary", unsafeDecodeError{}, "forbidden"},
		{"decode-error-panic", decodeErrorPanic{}, "panicked"},
		{"fresh-target", freshTargetGuard{}, ""},
		{"pointer-chain", new(*runtimeRefusal), "JSON reconstruction guard"},
	} {
		cases = append(cases, checkCase{fixture.name, fixture.reject, func(t *testing.T) {
			target := reflect.New(reflect.Indirect(reflect.ValueOf(fixture.value)).Type()).Interface()
			conformance.Runtime(t, fixture.value, target, diagnosticCanary)
		}})
	}
	return cases
}

func TestRuntimeNativeReconstructionOracle(t *testing.T) {
	var scalar scalarRuntime
	if _, guarded := any(&scalar).(json.Unmarshaler); guarded {
		t.Fatal("unguarded scalar unexpectedly implements a JSON guard")
	}
	var mismatch *json.UnmarshalTypeError
	if err := json.Unmarshal([]byte("{}"), &scalar); !errors.As(err, &mismatch) {
		t.Fatal("object probe did not produce a native type mismatch")
	}
	if err := json.Unmarshal([]byte(`"reconstructed"`), &scalar); err != nil || scalar != "reconstructed" {
		t.Fatal("valid string did not reconstruct the unguarded runtime")
	}
	if _, guarded := any(pointerRuntime{}).(json.Marshaler); guarded {
		t.Fatal("unaddressable value unexpectedly has pointer methods")
	}
	if _, guarded := any(new(pointerRuntime)).(json.Marshaler); !guarded {
		t.Fatal("pointer lost its JSON guard")
	}
}

func TestRuntimeNativeStreamFailureOracle(t *testing.T) {
	for _, fixture := range []struct {
		target any
		want   error
	}{
		{new(emptyStreamGuard), io.EOF},
		{new(truncatedStreamGuard), io.ErrUnexpectedEOF},
		{new(wrappedStreamGuard), io.ErrUnexpectedEOF},
	} {
		if err := json.Unmarshal([]byte(`{"valid":true}`), fixture.target); !errors.Is(err, fixture.want) {
			t.Fatal("valid outer JSON did not expose the native stream failure")
		}
		conformance.Private(t, fixture.want, diagnosticCanary)
	}
	if _, err := json.Marshal(streamEncodingFailure{}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("encoding wrapper lost the native stream failure")
	}
}

func TestRuntimeNativeTextGuardOracle(t *testing.T) {
	value := textRuntimeRefusal{}
	if _, guarded := any(value).(json.Marshaler); guarded {
		t.Fatal("text-only encoding control unexpectedly implements a JSON marshaler")
	}
	if _, err := json.Marshal(value); err == nil {
		t.Fatal("native JSON bypassed the intentional text encoding refusal")
	}
	for _, document := range []string{`{}`, `[]`, `"value"`, `0`, `true`, `false`, `null`} {
		if err := json.Unmarshal([]byte(document), new(textRuntimeRefusal)); err == nil {
			t.Fatal("native JSON bypassed the reconstruction guard")
		}
	}
}

func TestRuntimeExistingGuardsAndAbsence(t *testing.T) {
	for _, value := range []any{
		compatibility.Build{}, compatibility.Module{}, compatibility.Report{},
		invocation.Result[int]{}, &fault.Error{}, resource.Prepared[settingsForRuntime]{},
		resource.Access{}, resource.Lease{},
	} {
		kind := reflect.Indirect(reflect.ValueOf(value)).Type()
		t.Run(kind.String(), func(t *testing.T) {
			conformance.Runtime(t, value, reflect.New(kind).Interface(), diagnosticCanary)
			for _, document := range []string{"{}", "[]", `"reconstructed"`, "1", "1.5", "true", "false", "null"} {
				if err := json.Unmarshal([]byte(document), reflect.New(kind).Interface()); err == nil {
					t.Fatal("existing non-nil runtime target accepted reconstruction")
				}
			}
			nilPointer := reflect.Zero(reflect.PointerTo(kind)).Interface()
			data, err := json.Marshal(nilPointer)
			if err != nil || string(data) != "null" {
				t.Fatal("native nil-pointer encoding lost absence semantics")
			}
			absent := reflect.New(reflect.PointerTo(kind))
			if err := json.Unmarshal([]byte("null"), absent.Interface()); err != nil || !absent.Elem().IsNil() {
				t.Fatal("native null reconstructed a non-nil runtime handle")
			}
		})
	}
}

type settingsForRuntime struct {
	Limit int `json:"limit"`
}
