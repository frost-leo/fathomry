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

package resource

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestJSONVariablesUseExistingPreparationSemantics(t *testing.T) {
	schema := testSchema()
	schema.Validate = func(value testSettings) error { value.Headers["default"] = "changed"; return nil }
	prepared, err := PrepareData(schema, []Layer{
		JSONVariables([]byte(`{"connection":{"host":"a\u0085b"},"items":[],"optional":null,"headers":{"json":"😀"},"enabled":false}`)),
		{Kind: Base, Content: []byte("connection: {port: 123}\nheaders: {base: kept}")},
	})
	if err != nil {
		t.Fatal(err)
	}
	value, err := prepared.ValueCopy()
	if err != nil || value.Connection != (testNested{Host: "a\u0085b", Port: 123}) ||
		value.Items == nil || len(value.Items) != 0 || value.Optional != nil || value.Enabled ||
		!reflect.DeepEqual(value.Headers, map[string]string{"default": "value", "base": "kept", "json": "😀"}) {
		t.Fatal("JSON path diverged from shared merge/copy/validation")
	}
	for _, raw := range []string{
		`{"connection":{"unknown":1}}`, `{"limit":1.0}`, `{"limit":1e0}`, `{"limit":256}`,
		`{"limit":null}`, `{"items":[1]}`, `{"headers":{"a":"first","\u0061":"last"}}`,
		`{"headers":{"a":"\ud800"}}`, `{} {}`, `[]`, "headers: {}",
	} {
		if _, err := PrepareData(schema, []Layer{JSONVariables([]byte(raw))}); !errors.Is(err, ErrConfiguration) {
			t.Fatalf("invalid JSON variable admitted: %q", raw)
		}
	}
	layer := JSONVariables([]byte("{}"))
	layer.Kind = Base
	if _, err := PrepareData(schema, []Layer{layer}); !errors.Is(err, ErrConfiguration) {
		t.Fatal("generated-JSON marker usable outside Variables")
	}
	if _, err := PrepareData(schema, []Layer{JSONVariables([]byte("{}")), {Kind: Variables, Content: []byte("{}")}}); !errors.Is(err, ErrConfiguration) {
		t.Fatal("duplicate Variables layer admitted")
	}
	// Default Layer literals retain YAML behavior; no auto-detection or global
	// reinterpretation of external JSON-as-YAML is introduced.
	ordinary, err := PrepareData(schema, []Layer{{Kind: Variables, Content: []byte("connection: {host: 'a\u0085b'}")}})
	if err != nil {
		t.Fatal(err)
	}
	yamlValue, _ := ordinary.ValueCopy()
	if yamlValue.Connection.Host != "a b" {
		t.Fatal("existing YAML semantics changed")
	}
}

func TestJSONVariablesBoundsAndExactNumbers(t *testing.T) {
	for _, text := range []string{"18446744073709551615", "1.0", "1e0", "1E+2", "-0", "1e4000"} {
		value, err := parseJSONMapping([]byte(`{"number":` + text + `}`))
		if err != nil || value["number"] != json.Number(text) {
			t.Fatal("number lexeme changed", text, err)
		}
	}
	type value struct {
		Number uint64 `json:"number"`
	}
	prepared, err := PrepareData(Schema[value]{Format: 1}, []Layer{JSONVariables([]byte(`{"number":18446744073709551615}`))})
	if err != nil {
		t.Fatal(err)
	}
	actual, _ := prepared.ValueCopy()
	if actual.Number != math.MaxUint64 {
		t.Fatal("integer lost precision")
	}
	for _, text := range []string{"18446744073709551616", "1.0", "1e0", "1e4000"} {
		if _, err := PrepareData(Schema[value]{Format: 1}, []Layer{JSONVariables([]byte(`{"number":` + text + `}`))}); err == nil {
			t.Fatal("inexact integer admitted", text)
		}
	}
	for _, depth := range []int{63, 64} {
		raw := `{"v":` + strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth) + "}"
		_, err := parseJSONMapping([]byte(raw))
		if (err == nil) != (depth == 63) {
			t.Fatal("root-zero depth profile changed", depth, err)
		}
	}
	for _, size := range []int{1 << 20, 1<<20 + 1} {
		raw := []byte(`{"v":"` + strings.Repeat("x", size-8) + `"}`)
		_, err := parseJSONMapping(raw)
		if (err == nil) != (size == 1<<20) {
			t.Fatal("document byte profile changed", size, err)
		}
	}
	// Variables do not inherit the remote wire-protocol node quota.
	if _, err := parseJSONMapping([]byte(`{"v":[` + strings.Repeat("0,", 33000) + `0]}`)); err != nil {
		t.Fatal(err)
	}
}

func FuzzJSONVariables(f *testing.F) {
	for _, seed := range []string{`{}`, `{"a":"\ud83d\ude00"}`, `{"a":"\ud800"}`, `{"a":18446744073709551615}`, `{"a":null,"b":[]}`, "{\"a\u0085b\":\"\u007f\"}", `{"a":1,"\u0061":2}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20+1 {
			t.Skip()
		}
		mapping, err := parseJSONMapping(raw)
		if err != nil {
			return
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var expected map[string]any
		if err := decoder.Decode(&expected); err != nil || !reflect.DeepEqual(mapping, expected) {
			t.Fatal("JSON values changed", err)
		}
		if _, err := json.Marshal(mapping); err != nil {
			t.Fatal(fmt.Errorf("unrepresentable mapping: %w", err))
		}
	})
}
