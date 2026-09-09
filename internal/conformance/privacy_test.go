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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
)

type panicLog struct{}

func (panicLog) LogValue() slog.Value { panic(diagnosticCanary) }

type pointerPanicLog struct{}

func (*pointerPanicLog) LogValue() slog.Value { panic(diagnosticCanary) }

type nilPanicLog struct{}

func (nilPanicLog) LogValue() slog.Value { panic(nil) }

type safeFormatting struct{}

func (safeFormatting) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("safe")) }

type logProjection struct {
	safeFormatting
	value slog.Value
}

func (value logProjection) LogValue() slog.Value { return value.value }

type logChain int

func (depth logChain) LogValue() slog.Value {
	if depth == 0 {
		return slog.StringValue("safe")
	}
	return slog.AnyValue(depth - 1)
}

type loopLog struct{}

func (loopLog) LogValue() slog.Value { return slog.AnyValue(loopLog{}) }

type phaseLog struct {
	safeFormatting
	calls   *int
	panicAt int
}

func (value phaseLog) LogValue() slog.Value {
	*value.calls++
	if *value.calls == value.panicAt {
		panic(diagnosticCanary)
	}
	return slog.StringValue("safe")
}

type panicString struct{}

func (panicString) String() string { panic(diagnosticCanary) }

type panicError struct{}

func (panicError) Error() string { panic(diagnosticCanary) }

type panicGoString struct{}

func (panicGoString) GoString() string { panic(diagnosticCanary) }

type panicFormat struct{}

func (panicFormat) Format(fmt.State, rune) { panic(diagnosticCanary) }

type pointerPanicFormat struct{}

func (*pointerPanicFormat) Format(fmt.State, rune) { panic(diagnosticCanary) }

type nilSafeString struct{}

func (*nilSafeString) String() string { return "safe nil" }

type literalDiagnostic string

func (value literalDiagnostic) String() string       { return string(value) }
func (value literalDiagnostic) GoString() string     { return string(value) }
func (value literalDiagnostic) LogValue() slog.Value { return slog.StringValue(string(value)) }

type textPanic struct{ safeFormatting }

func (textPanic) MarshalText() ([]byte, error) { panic(diagnosticCanary) }
func (textPanic) MarshalJSON() ([]byte, error) { return []byte(`"safe"`), nil }

type jsonPanic struct{ safeFormatting }

func (jsonPanic) MarshalJSON() ([]byte, error) { panic(diagnosticCanary) }

type pointerJSONPanic struct{ safeFormatting }

func (*pointerJSONPanic) MarshalJSON() ([]byte, error) { panic(diagnosticCanary) }

type jsonLeak struct{ safeFormatting }

func (jsonLeak) MarshalJSON() ([]byte, error) { return json.Marshal(diagnosticCanary) }

type textLeak struct{ safeFormatting }

func (textLeak) MarshalText() ([]byte, error) { return []byte(diagnosticCanary), nil }

type textErrorPanic struct{ safeFormatting }

func (textErrorPanic) MarshalText() ([]byte, error) { return nil, panicError{} }
func (textErrorPanic) MarshalJSON() ([]byte, error) { return []byte(`"safe"`), nil }

type jsonErrorPanic struct{ safeFormatting }

func (jsonErrorPanic) MarshalJSON() ([]byte, error) { return nil, panicError{} }

type formatState struct{}

func (formatState) Format(state fmt.State, verb rune) {
	if verb == 'v' && state.Flag('#') {
		panic(diagnosticCanary)
	}
	_, _ = state.Write([]byte("safe"))
}

type redactedContainer struct {
	safeFormatting
	Hidden panicFormat
}

type namedLogBytes []byte

func (namedLogBytes) String() string { panic(diagnosticCanary) }

type namedLogByte byte

func (namedLogByte) String() string { panic(diagnosticCanary) }

type namedLogByteSlice []namedLogByte

func (namedLogByteSlice) String() string { panic(diagnosticCanary) }

type textLogBytes []byte

func (textLogBytes) String() string               { panic(diagnosticCanary) }
func (textLogBytes) MarshalText() ([]byte, error) { return []byte("marshaled"), nil }

type panicTextLogBytes []byte

func (panicTextLogBytes) MarshalText() ([]byte, error) { panic(diagnosticCanary) }
func (panicTextLogBytes) MarshalJSON() ([]byte, error) { return []byte(`"safe"`), nil }

type signedLogByte int8

func (signedLogByte) String() string { panic(diagnosticCanary) }

type byteLogCase struct {
	name      string
	value     any
	textValue string
	jsonValue string
	reject    string
}

func byteLogCases() []byteLogCase {
	const payload = "safe-bytes"
	elements := make([]namedLogByte, len(payload))
	for index := range payload {
		elements[index] = namedLogByte(payload[index])
	}
	named := namedLogBytes(payload)
	array := [1]namedLogByte{'s'}
	encoded := `"` + base64.StdEncoding.EncodeToString([]byte(payload)) + `"`
	return []byteLogCase{
		{"plain", []byte(payload), `"safe-bytes"`, encoded, ""},
		{"named-slice", named, `"safe-bytes"`, encoded, ""},
		{"named-element", elements, `"safe-bytes"`, encoded, ""},
		{"named-both", namedLogByteSlice(elements), `"safe-bytes"`, encoded, ""},
		{"nil-named", namedLogBytes(nil), `""`, "null", ""},
		{"empty-named", namedLogBytes{}, `""`, `""`, ""},
		{"nil-plain", []byte(nil), `""`, "null", ""},
		{"text-marshaler", textLogBytes(payload), "marshaled", `"marshaled"`, ""},
		{"text-marshaler-panic", panicTextLogBytes(payload), "!PANIC:", `"safe"`, "panicked"},
		{"array", array, "PANIC=String method:", "[115]", "panicked"},
		{"pointer-slice", &named, "PANIC=String method:", encoded, "panicked"},
		{"pointer-array", &array, "PANIC=String method:", "[115]", "panicked"},
		{"other-slice", []signedLogByte{'s'}, "PANIC=String method:", "[115]", "panicked"},
	}
}

func privateCases() []checkCase {
	var cases []checkCase
	var deepFormat any = "safe"
	deepLog := slog.StringValue("safe")
	for range 102 {
		deepFormat = []any{deepFormat}
		deepLog = slog.GroupValue(slog.Any("child", deepLog))
	}
	for _, fixture := range []struct {
		name   string
		value  any
		reject string
	}{
		{"valid", logProjection{value: slog.StringValue("safe")}, ""},
		{"nil", nil, ""},
		{"safe-nil-receiver", (*nilSafeString)(nil), ""},
		{"safe-panic-text", "PANIC is ordinary text; LogValue panicked is a quoted diagnostic", ""},
		{"safe-marker-stringer", literalDiagnostic("%!v(PANIC=String method: safe) !PANIC: safe LogValue panicked"), ""},
		{"safe-marker-error", errors.New("LogValue panicked\ncalled from example (example.go:1)\nPANIC"), ""},
		{"safe-nested-text", map[string]any{"value": []any{literalDiagnostic("PANIC"), "LogValue panicked"}}, ""},
		{"redacted-container", redactedContainer{}, ""},
		{"unresolved-struct-member", struct{ Value panicLog }{}, ""},
		{"log-panic", panicLog{}, "panicked"},
		{"log-panic-address", &panicLog{}, "panicked"},
		{"log-pointer-receiver", &pointerPanicLog{}, "panicked"},
		{"log-pointer-method-not-on-value", pointerPanicLog{}, ""},
		{"log-nil-receiver", (*pointerPanicLog)(nil), "panicked"},
		{"log-panic-nil", nilPanicLog{}, "panicked"},
		{"log-returned-valuer", logProjection{value: slog.AnyValue(panicLog{})}, "panicked"},
		{"log-group", logProjection{value: slog.GroupValue(slog.Any("child", panicLog{}))}, "panicked"},
		{"log-nested-group", slog.GroupValue(slog.Group("outer", slog.Any("child", &pointerPanicLog{}))), "panicked"},
		{"log-attrs", []slog.Attr{slog.Any("child", panicLog{})}, "panicked"},
		{"log-safe-group", slog.GroupValue(slog.Group("outer", slog.Any("child", logProjection{value: slog.StringValue("safe")}))), ""},
		{"log-chain-limit", logChain(98), ""},
		{"log-chain-exhausted", logChain(99), "bound"},
		{"log-loop", loopLog{}, "bound"},
		{"log-text-only-panic", phaseLog{calls: new(int), panicAt: 1}, "panicked"},
		{"log-json-only-panic", phaseLog{calls: new(int), panicAt: 2}, "panicked"},
		{"fmt-depth-bound", deepFormat, "bound"},
		{"log-depth-bound", logProjection{value: deepLog}, "bound"},
		{"fmt-string", panicString{}, "panicked"},
		{"fmt-error", panicError{}, "panicked"},
		{"fmt-go-string", panicGoString{}, "panicked"},
		{"fmt-format", panicFormat{}, "panicked"},
		{"fmt-pointer-format", &pointerPanicFormat{}, "panicked"},
		{"fmt-pointer-method-not-on-value", pointerPanicFormat{}, ""},
		{"fmt-nil-format", (*pointerPanicFormat)(nil), "panicked"},
		{"fmt-state", formatState{}, "panicked"},
		{"fmt-nested-struct", struct{ Value any }{panicString{}}, "panicked"},
		{"fmt-nested-map", map[any]any{panicGoString{}: "safe"}, "panicked"},
		{"fmt-nested-slice", []any{&pointerPanicFormat{}}, "panicked"},
		{"fmt-nested-array", [1]any{panicError{}}, "panicked"},
		{"text-marshaler", textPanic{}, "panicked"},
		{"text-marshaler-address", &textPanic{}, "panicked"},
		{"text-marshaler-group", logProjection{value: slog.GroupValue(slog.Any("child", textPanic{}))}, "panicked"},
		{"json-marshaler", jsonPanic{}, "panicked"},
		{"json-marshaler-address", &jsonPanic{}, "panicked"},
		{"json-marshaler-nested", struct{ Value any }{jsonPanic{}}, "panicked"},
		{"json-pointer-method-not-on-value", pointerJSONPanic{}, ""},
		{"json-pointer-method", &pointerJSONPanic{}, "panicked"},
		{"json-addressable-slice-element", []pointerJSONPanic{{}}, "panicked"},
		{"json-nonaddressable-field", struct{ Value pointerJSONPanic }{}, ""},
		{"text-error-panic", textErrorPanic{}, "panicked"},
		{"json-error-panic", jsonErrorPanic{}, "panicked"},
		{"fmt-canary", diagnosticCanary, "forbidden"},
		{"log-canary", logProjection{value: slog.StringValue(diagnosticCanary)}, "forbidden"},
		{"log-group-canary", logProjection{value: slog.GroupValue(slog.String("child", diagnosticCanary))}, "forbidden"},
		{"text-canary", textLeak{}, "forbidden"},
		{"json-canary", jsonLeak{}, "forbidden"},
		{"fmt-size", strings.Repeat("x", 1<<20+1), "bound"},
		{"log-size", logProjection{value: slog.StringValue(strings.Repeat("x", 1<<20+1))}, "bound"},
	} {
		cases = append(cases, checkCase{fixture.name, fixture.reject, func(t *testing.T) {
			conformance.Private(t, fixture.value, diagnosticCanary)
		}})
	}
	cases = append(cases, checkCase{"empty-canary", "forbidden", func(t *testing.T) {
		conformance.Private(t, "safe", "")
	}})
	for _, fixture := range byteLogCases() {
		cases = append(cases, checkCase{"log-bytes-" + fixture.name, fixture.reject, func(t *testing.T) {
			conformance.Private(t, logProjection{value: slog.AnyValue(fixture.value)}, diagnosticCanary)
		}})
	}
	return cases
}

func TestPrivateContracts(t *testing.T) { runCheckCases(t, "private", privateCases()) }

func TestPrivateNativeByteLoggingDispatch(t *testing.T) {
	for _, fixture := range byteLogCases() {
		t.Run(fixture.name, func(t *testing.T) {
			value := logProjection{value: slog.AnyValue(fixture.value)}
			for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
				if fmt.Sprintf(format, value) != "safe" {
					t.Fatal("native fmt did not select the safe outer formatter")
				}
			}
			var textOutput, jsonOutput bytes.Buffer
			slog.New(slog.NewTextHandler(&textOutput, nil)).Info("probe", "value", value)
			slog.New(slog.NewJSONHandler(&jsonOutput, nil)).Info("probe", "value", value)
			if fixture.reject == "" {
				if !strings.HasSuffix(textOutput.String(), "value="+fixture.textValue+"\n") {
					t.Fatal("native text logging did not render the expected safe projection")
				}
			} else if !strings.Contains(textOutput.String(), fixture.textValue) ||
				!strings.Contains(textOutput.String(), diagnosticCanary) {
				t.Fatal("native text logging did not select the expected failing hook")
			}
			var record struct{ Value json.RawMessage }
			if json.Unmarshal(jsonOutput.Bytes(), &record) != nil || string(record.Value) != fixture.jsonValue {
				t.Fatal("native JSON logging did not render the expected projection")
			}
		})
	}
}

func TestNativeLogPanicPaths(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		var output bytes.Buffer
		var handler slog.Handler = slog.NewTextHandler(&output, nil)
		if jsonOutput {
			handler = slog.NewJSONHandler(&output, nil)
		}
		slog.New(handler).Info("probe", "value", panicLog{})
		if !strings.Contains(output.String(), "LogValue panicked") || strings.Contains(output.String(), diagnosticCanary) {
			t.Fatal("native logger did not produce the expected payload-free panic fallback")
		}
	}
}

type countedLog struct {
	safeFormatting
	calls *int
}

func (value countedLog) LogValue() slog.Value {
	*value.calls++
	return slog.StringValue("safe")
}

func TestPrivatePreservesGroupValues(t *testing.T) {
	calls := 0
	attrs := []slog.Attr{slog.Any("child", countedLog{calls: &calls})}
	value := logProjection{value: slog.GroupValue(attrs...)}
	conformance.Private(t, value, diagnosticCanary)
	if calls != 2 || attrs[0].Value.Kind() != slog.KindLogValuer || value.value.Group()[0].Value.Kind() != slog.KindLogValuer {
		t.Fatal("logging resolved hooks more than once per handler or mutated caller-owned attributes")
	}
}

type runtimeRefusal struct{ state int }

func (runtimeRefusal) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("runtime")) }
func (runtimeRefusal) LogValue() slog.Value           { return slog.StringValue("runtime") }
func (runtimeRefusal) MarshalJSON() ([]byte, error)   { return nil, errors.New("runtime JSON refused") }
func (*runtimeRefusal) UnmarshalJSON([]byte) error    { return errors.New("runtime JSON refused") }

type reconstructionAccepted struct{ safeFormatting }

func (reconstructionAccepted) MarshalJSON() ([]byte, error) {
	return nil, errors.New("runtime JSON refused")
}

type runtimeEncodePanic struct{ runtimeRefusal }

func (runtimeEncodePanic) MarshalJSON() ([]byte, error) { panic(diagnosticCanary) }

type runtimeDecodePanic struct{ runtimeRefusal }

func (*runtimeDecodePanic) UnmarshalJSON([]byte) error { panic(diagnosticCanary) }

func runtimeCases() []checkCase {
	return []checkCase{
		{"valid", "", func(t *testing.T) { conformance.Runtime(t, runtimeRefusal{}, new(runtimeRefusal), diagnosticCanary) }},
		{"valid-pointer", "", func(t *testing.T) { conformance.Runtime(t, &runtimeRefusal{}, new(runtimeRefusal), diagnosticCanary) }},
		{"encoding-accepted", "JSON encoding", func(t *testing.T) { conformance.Runtime(t, struct{}{}, new(struct{}), diagnosticCanary) }},
		{"reconstruction-accepted", "JSON reconstruction", func(t *testing.T) {
			conformance.Runtime(t, reconstructionAccepted{}, new(reconstructionAccepted), diagnosticCanary)
		}},
		{"nil-target", "invalid reconstruction test target", func(t *testing.T) {
			conformance.Runtime(t, runtimeRefusal{}, (*runtimeRefusal)(nil), diagnosticCanary)
		}},
		{"wrong-target", "invalid reconstruction test target", func(t *testing.T) {
			conformance.Runtime(t, runtimeRefusal{}, new(string), diagnosticCanary)
		}},
		{"nonzero-target", "invalid reconstruction test target", func(t *testing.T) {
			conformance.Runtime(t, runtimeRefusal{}, &runtimeRefusal{state: 1}, diagnosticCanary)
		}},
		{"encoding-panic", "panicked", func(t *testing.T) {
			conformance.Runtime(t, runtimeEncodePanic{}, new(runtimeEncodePanic), diagnosticCanary)
		}},
		{"reconstruction-panic", "panicked", func(t *testing.T) {
			conformance.Runtime(t, runtimeDecodePanic{}, new(runtimeDecodePanic), diagnosticCanary)
		}},
	}
}

func TestRuntimeContracts(t *testing.T) { runCheckCases(t, "runtime", runtimeCases()) }

func FuzzPrivateLiteralDiagnostics(f *testing.F) {
	for _, seed := range []string{"safe", "PANIC", "%!v(PANIC=String method: safe)", "LogValue panicked\ncalled from example", "!PANIC: safe", "\x00\xff\n\""} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 4096 {
			return
		}
		conformance.Private(t, value)
		conformance.Private(t, literalDiagnostic(value))
	})
}
