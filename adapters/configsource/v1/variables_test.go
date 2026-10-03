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

package configsource

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestDotenv(t *testing.T) {
	t.Setenv("BOUNDARY_ENV", "ambient")
	values, err := ParseDotenv([]byte("# explicit values\nBOUNDARY_ENV=literal-$HOME\nEMPTY=\nQUOTED='a # b' # comment\nJSON=\"one\\ntwo\"\n"))
	if err != nil || !reflect.DeepEqual(values, map[string]string{"BOUNDARY_ENV": "literal-$HOME", "EMPTY": "", "QUOTED": "a # b", "JSON": "one\ntwo"}) {
		t.Fatal("literal values changed", err)
	}
	for _, raw := range []string{"A=1\nA=2", "export A=1", "1BAD=value", "BAD-KEY=value", "A", "A='unclosed", "A=\"\\ud800\"", "A=\"one\ntwo\"", "A='one' trailing", "A=bad\x00value", "A=\xff", strings.Repeat(" ", MaxDotenvBytes+1)} {
		if _, err := ParseDotenv([]byte(raw)); err == nil {
			t.Fatal("invalid variable document admitted")
		}
	}
	if values, err := ParseDotenv(nil); err != nil || len(values) != 0 {
		t.Fatal("empty document changed", err)
	}
	t.Run("inclusive_document_limit", func(t *testing.T) {
		for _, ending := range []string{"", "\n", "\r\n"} {
			for _, size := range []int{MaxDotenvBytes - 1, MaxDotenvBytes, MaxDotenvBytes + 1} {
				value := strings.Repeat("x", size-len("A=")-len(ending))
				parsed, err := ParseDotenv([]byte("A=" + value + ending))
				if size > MaxDotenvBytes {
					if !errors.Is(err, ErrLimit) || parsed != nil {
						t.Fatal("oversized document admitted")
					}
				} else if err != nil || parsed["A"] != value {
					t.Fatalf("valid document refused: size=%d ending=%q: %v", size, ending, err)
				}
			}
		}
		for _, raw := range []string{strings.Repeat(" ", MaxDotenvBytes), "#" + strings.Repeat("x", MaxDotenvBytes-1)} {
			if parsed, err := ParseDotenv([]byte(raw)); err != nil || len(parsed) != 0 {
				t.Fatal("maximum empty/comment-only document refused", err)
			}
		}
	})
}

func FuzzDotenv(f *testing.F) {
	for _, raw := range []string{"", "A=literal-$HOME", "A='quoted'", "A=\"escaped\\ntext\"", "A=unterminated'"} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > MaxDotenvBytes+1 {
			return
		}
		values, err := ParseDotenv([]byte(raw))
		if err != nil {
			return
		}
		if len(values) > MaxVariables {
			t.Fatal("unbounded variable count")
		}
		for name, value := range values {
			if !dotenvKey(name) || !dotenvText(value) {
				t.Fatal("invalid literal output")
			}
		}
	})
}

func TestVariables(t *testing.T) {
	values := []Variable{
		{Path: "/text", Value: "", Present: true},
		{Path: "/count", Value: "9007199254740993", Present: true, JSON: true},
		{Path: "/map", Value: `{"New":{"first":8},"Secret-Key":{"first":9}}`, Present: true, JSON: true},
		{Path: "/pointer", Value: "null", Present: true, JSON: true},
		{Path: "/nested/second", Value: "null", Present: true},
		{Path: "/enabled", JSON: true},
	}
	layer, err := BindVariables[testData](values)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := Prepare(context.Background(), Schema[testData]{Version: 1, Defaults: defaults()}, []Layer{layer})
	if err != nil {
		t.Fatal(err)
	}
	result, _ := prepared.ValueCopy()
	if result.Text != "" || result.Count != 9007199254740993 || result.Map["New"].First != 8 || result.Map["Secret-Key"].Second != "map" || result.Map["Secret-Key"].First != 9 || result.Pointer != nil || result.Nested.Second != "null" || !result.Enabled {
		t.Fatal("variable semantics changed")
	}
	for _, variables := range [][]Variable{
		{{Path: "/nested", JSON: true}, {Path: "/nested/first", JSON: true}},
		{{Path: "/text"}, {Path: "/text"}},
		{{Path: "/map/key", JSON: true}},
		{{Path: "/list/0", JSON: true}},
		{{Path: ""}}, {{Path: "/unknown"}}, {{Path: "/te~xt"}}, {{Path: "/count"}},
		{{Path: "/count", Value: "1e0", Present: true, JSON: true}},
		{{Path: "/enabled", Value: "", Present: true, JSON: true}},
		{{Path: "/text", Value: string([]byte{255}), Present: true}},
		{{Path: "/text", Value: `"\ud800"`, Present: true, JSON: true}},
		{{Path: "/nested", Value: `{"first":1,"first":2}`, Present: true, JSON: true}},
	} {
		if _, err := BindVariables[testData](variables); err == nil {
			t.Fatal("invalid variable declaration accepted")
		}
	}
	if _, err := BindVariables[testData](make([]Variable, MaxVariables+1)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := BindVariables[testData]([]Variable{{Path: "/text", Value: strings.Repeat("x", MaxDocumentBytes+1), Present: true}}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if layer, err := BindVariables[struct{}](nil); err != nil || string(layer.Content) != "{}" {
		t.Fatal("empty schema variables", err)
	}
}

func FuzzVariables(f *testing.F) {
	for _, seed := range []struct {
		path, value string
		json        bool
	}{
		{"/text", "literal", false}, {"/count", "9007199254740993", true}, {"/map", `{"key":{"first":1}}`, true}, {"/pointer", "null", true},
	} {
		f.Add(seed.path, seed.value, seed.json)
	}
	f.Fuzz(func(t *testing.T, path, value string, jsonMode bool) {
		if len(value) > 2*MaxDocumentBytes || len(path) > 8192 {
			return
		}
		layer, err := BindVariables[testData]([]Variable{{Path: path, Value: value, Present: true, JSON: jsonMode}})
		if err != nil {
			return
		}
		if layer.Kind != Variables || layer.Encoding != JSON || len(layer.Content) > MaxDocumentBytes {
			t.Fatal("invalid variable result")
		}
		if _, err := Prepare(context.Background(), Schema[testData]{Version: 1}, []Layer{layer}); err != nil {
			t.Fatal("binding did not satisfy its admitted schema", err)
		}
	})
}
