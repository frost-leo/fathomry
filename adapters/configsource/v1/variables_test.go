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
	"strings"
	"testing"
)

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
