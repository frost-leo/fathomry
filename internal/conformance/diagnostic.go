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

package conformance

import (
	"encoding"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"testing"
)

const diagnosticLimit = 1 << 20
const diagnosticDepth = 100

func project(t testing.TB, projection func()) (completed bool) {
	t.Helper()
	defer func() {
		_ = recover()
		if !completed {
			t.Errorf("conformance: diagnostic projection panicked")
		}
	}()
	projection()
	return true
}

type formatProbe func(fmt.State, rune)

func (probe formatProbe) Format(state fmt.State, verb rune) { probe(state, verb) }

func probeFormat(t testing.TB, format string, value any) bool {
	t.Helper()
	completed := false
	output := fmt.Sprintf(format, formatProbe(func(state fmt.State, verb rune) {
		remaining := diagnosticLimit
		withinBound := false
		completed = project(t, func() {
			withinBound = probeFormatValue(t, reflect.ValueOf(value), state, verb, 0, &remaining)
		}) && withinBound
	}))
	if len(output) > diagnosticLimit {
		t.Errorf("conformance: diagnostic projection exceeded the test bound")
		return false
	}
	return completed
}

// Follow fmt's hook precedence and traversal, not the words in its output. A
// redacting outer hook suppresses descent; inaccessible fields have no hooks.
func probeFormatValue(t testing.TB, value reflect.Value, state fmt.State, verb rune, depth int, remaining *int) bool {
	t.Helper()
	if !value.IsValid() || !value.CanInterface() {
		return true
	}
	if !diagnosticBudget(t, depth, remaining) {
		return false
	}
	if wrapped, ok := value.Interface().(reflect.Value); ok && depth == 0 {
		value = wrapped
		if !value.IsValid() || !value.CanInterface() {
			return true
		}
	}
	input := value.Interface()
	if formatter, ok := input.(fmt.Formatter); ok {
		formatter.Format(state, verb)
		return true
	}
	if verb == 'v' && state.Flag('#') {
		if stringer, ok := input.(fmt.GoStringer); ok {
			_ = stringer.GoString()
			return true
		}
	} else {
		switch stringer := input.(type) {
		case error:
			_ = stringer.Error()
			return true
		case fmt.Stringer:
			_ = stringer.String()
			return true
		}
	}
	visit := func(child reflect.Value) bool { return probeFormatValue(t, child, state, verb, depth+1, remaining) }
	switch value.Kind() {
	case reflect.Struct:
		for index := range value.NumField() {
			if !visit(value.Field(index)) {
				return false
			}
		}
	case reflect.Interface:
		return visit(value.Elem())
	case reflect.Map:
		for entries := value.MapRange(); entries.Next(); {
			if !visit(entries.Key()) || !visit(entries.Value()) {
				return false
			}
		}
	case reflect.Array, reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 && (verb == 's' || verb == 'q') {
			return true
		}
		for index := range value.Len() {
			if !visit(value.Index(index)) {
				return false
			}
		}
	case reflect.Pointer:
		if depth == 0 && !value.IsNil() {
			switch value.Elem().Kind() {
			case reflect.Array, reflect.Slice, reflect.Struct, reflect.Map:
				return visit(value.Elem())
			}
		}
	}
	return true
}

func diagnosticBudget(t testing.TB, depth int, remaining *int) bool {
	t.Helper()
	if depth > diagnosticDepth || *remaining <= 0 {
		t.Errorf("conformance: diagnostic traversal exceeded the test bound")
		return false
	}
	*remaining--
	return true
}

type logProbe struct {
	t     testing.TB
	value slog.LogValuer
	calls *int
	valid *bool
}

func (probe logProbe) LogValue() (value slog.Value) {
	probe.t.Helper()
	if !project(probe.t, func() { value = probe.value.LogValue() }) {
		*probe.valid = false
		return slog.Value{}
	}
	*probe.calls++
	// Go 1.26's Resolve rejects even a terminal value returned by call 100.
	if *probe.calls == 100 {
		probe.t.Errorf("conformance: diagnostic LogValue resolution exceeded the test bound")
		*probe.valid = false
	}
	if value.Kind() == slog.KindLogValuer {
		probe.value = value.LogValuer()
		return slog.AnyValue(probe)
	}
	return value
}

func probeLog(t testing.TB, value slog.Value, jsonOutput bool, depth int, remaining *int) (slog.Value, bool) {
	t.Helper()
	if !diagnosticBudget(t, depth, remaining) {
		return slog.Value{}, false
	}
	valid := true
	if value.Kind() == slog.KindLogValuer {
		calls := 0
		value = slog.AnyValue(logProbe{t, value.LogValuer(), &calls, &valid}).Resolve()
		if !valid {
			return slog.Value{}, false
		}
	}
	if value.Kind() == slog.KindGroup {
		attrs := slices.Clone(value.Group())
		for index := range attrs {
			var ok bool
			attrs[index].Value, ok = probeLog(t, attrs[index].Value, jsonOutput, depth+1, remaining)
			if !ok {
				return slog.Value{}, false
			}
		}
		return slog.GroupValue(attrs...), true
	}
	if value.Kind() != slog.KindAny {
		return value, true
	}
	input := value.Any()
	var encodingError error
	if jsonOutput {
		valid = project(t, func() {
			if err, ok := input.(error); ok {
				if _, marshaler := input.(json.Marshaler); !marshaler {
					_ = err.Error()
					return
				}
			}
			encoder := json.NewEncoder(io.Discard)
			encoder.SetEscapeHTML(false)
			// An ordinary encoding refusal is allowed, notably for Runtime.
			encodingError = encoder.Encode(input)
		})
	} else if marshaler, ok := input.(encoding.TextMarshaler); ok {
		valid = project(t, func() { _, encodingError = marshaler.MarshalText() })
	} else if kind := reflect.TypeOf(input); kind != nil && kind.Kind() == reflect.Slice && kind.Elem().Kind() == reflect.Uint8 {
		// TextHandler renders byte slices without selecting fmt hooks.
		return value, true
	} else {
		valid = probeFormat(t, "%+v", input)
	}
	if valid && encodingError != nil {
		valid = probeFormat(t, "%v", encodingError)
	}
	return value, valid
}
