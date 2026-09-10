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

package viper

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"

	sdk "github.com/spf13/viper"
)

func value(t testing.TB, document *Document, key string) any {
	t.Helper()
	result, err := document.ValueCopy(key)
	if err != nil {
		t.Fatal("fixture query failed", err)
	}
	return result
}

func TestNativeProfileAndCopiedObservations(t *testing.T) {
	for _, encoding := range []string{"yaml", "json"} {
		t.Run(encoding, func(t *testing.T) {
			content := `{"Nested":{"Keep":"base","Items":[{"MiXeD":"first"}]},"optional":null,"enabled":false,"zero":0,"empty":""}`
			input := readerInput(encoding, content)
			input.Options.Defaults = []Default{{Key: "optional", Value: "fallback"}, {Key: "absent", Value: 23}}
			document := loadOne(t, input)
			native := sdk.New()
			native.SetConfigType(encoding)
			for _, entry := range input.Options.Defaults {
				native.SetDefault(entry.Key, entry.Value)
			}
			if err := native.ReadConfig(strings.NewReader(content)); err != nil {
				t.Fatal("direct native positive control failed")
			}
			zero := any(0)
			if encoding == "json" {
				zero = float64(0)
			}
			for key, expected := range map[string]any{
				"NESTED.keep": "base", "optional": "fallback", "absent": 23,
				"enabled": false, "zero": zero, "empty": "", "missing": nil,
				"nested.items": []any{map[string]any{"mixed": "first"}},
			} {
				if got := value(t, document, key); !reflect.DeepEqual(got, expected) || !reflect.DeepEqual(got, native.Get(key)) {
					t.Error("supported native lookup changed")
				}
			}
			copied := value(t, document, "nested").(map[string]any)
			copied["keep"] = "changed"
			copied["items"].([]any)[0].(map[string]any)["mixed"] = "changed"
			if value(t, document, "nested.keep") != "base" ||
				value(t, document, "nested.items").([]any)[0].(map[string]any)["mixed"] != "first" {
				t.Fatal("returned native collection aliases SDK state")
			}
			raw := document.RawCopy()
			raw[0] = '!'
			if string(document.RawCopy()) != content || document.Encoding() != encoding {
				t.Fatal("raw input was changed or normalized")
			}
			if _, preserved := native.AllSettings()["optional"]; !preserved || native.AllSettings()["optional"] != "fallback" {
				t.Fatal("native null-loss control changed")
			}
		})
	}
	document := loadOne(t, readerInput("json", `{"large":9007199254740993}`))
	if value(t, document, "large") != float64(9007199254740992) ||
		!bytes.Contains(document.RawCopy(), []byte("9007199254740993")) {
		t.Fatal("native JSON rounding was hidden or original number lost")
	}
	document = loadOne(t, readerInput("json", `{"value":1,"value":2}`))
	if value(t, document, "value") != float64(2) {
		t.Fatal("native JSON duplicate-key behavior was silently redefined")
	}
}

func TestExplicitLiveEnvironmentAndDefaults(t *testing.T) {
	const first, second = "FATHOMRY_VIPER_FIRST", "FATHOMRY_VIPER_SECOND"
	t.Setenv(first, "before")
	t.Setenv(second, "second")
	t.Setenv("FATHOMRY_VIPER_UNSELECTED", "unselected-canary")
	input := readerInput("yaml", "value: file")
	input.Options.Defaults = []Default{{Key: "value", Value: "default"}, {Key: "other", Value: "fallback"}}
	input.Options.Environment = []Binding{{Key: "value", Name: first}, {Key: "value", Name: second}}
	document := loadOne(t, input)
	input.Options.Environment[0].Name = "FATHOMRY_VIPER_UNSELECTED"
	input.Options.Defaults[1].Value = "changed"
	if value(t, document, "VALUE") != "before" || value(t, document, "other") != "fallback" ||
		value(t, document, "FATHOMRY_VIPER_UNSELECTED") != nil {
		t.Fatal("explicit bootstrap ownership changed")
	}
	t.Setenv(first, "after")
	if value(t, document, "value") != "after" {
		t.Fatal("live binding was frozen")
	}
	t.Setenv(first, "")
	if value(t, document, "value") != "second" {
		t.Fatal("native binding order / empty fallback changed")
	}
	t.Setenv(second, "")
	if value(t, document, "value") != "file" {
		t.Fatal("native file precedence changed")
	}
	input = readerInput("yaml", "value: null")
	input.Options.Environment = []Binding{{Key: "value", Name: first}}
	input.Options.Defaults = []Default{{Key: "value", Value: "default"}}
	input.Options.AllowEmptyEnv = true
	allowEmpty := loadOne(t, input)
	if value(t, allowEmpty, "value") != "" {
		t.Fatal("explicit empty environment setting changed")
	}
	t.Setenv(first, strings.Repeat("x", MaxDocumentBytes+1))
	if got, err := allowEmpty.ValueCopy("value"); got != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("oversized live environment was exposed or replaced by a fallback")
	}
}

func TestIndependentLoadsAndConcurrentCopies(t *testing.T) {
	shared := loadOne(t, readerInput("yaml", "items: [{key: original}]"))
	var group sync.WaitGroup
	for index := range 24 {
		group.Go(func() {
			for range 10 {
				content := "value: independent"
				document := loadOne(t, readerInput("yaml", content))
				if value(t, document, "value") != "independent" || string(document.RawCopy()) != content {
					t.Error("independent load contaminated")
				}
				copied := value(t, shared, "items").([]any)
				copied[0].(map[string]any)["key"] = index
				raw := shared.RawCopy()
				raw[0] = '!'
			}
		})
	}
	group.Wait()
	if value(t, shared, "items").([]any)[0].(map[string]any)["key"] != "original" {
		t.Fatal("concurrent returned copies alias native state")
	}
}

func sameNativeValue(left, right any) bool {
	switch left := left.(type) {
	case float64:
		right, ok := right.(float64)
		return ok && (left == right || math.IsNaN(left) && math.IsNaN(right))
	case map[string]any:
		right, ok := right.(map[string]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for key, child := range left {
			other, found := right[key]
			if !found || !sameNativeValue(child, other) {
				return false
			}
		}
		return true
	case []any:
		right, ok := right.([]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for index, child := range left {
			if !sameNativeValue(child, right[index]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(left, right)
	}
}

func TestNativeQueryRestrictions(t *testing.T) {
	content := `{"items":["first",{"nested":["child"]}],"empty":[],"negative":{"-1":"literal","-01":"padded","-name":"named"},"literal.-1":"dotted","-1":"root"}`
	for _, encoding := range []string{"yaml", "json"} {
		document := loadOne(t, readerInput(encoding, content))
		native := sdk.New()
		native.SetConfigType(encoding)
		if err := native.ReadConfig(strings.NewReader(content)); err != nil {
			t.Fatal("native positive control failed")
		}
		for key, want := range map[string]any{
			"items.0": "first", "items.+0": "first", "items.-0": "first",
			"items.-9223372036854775809": nil,
			"items.9":                    nil, "empty.0": nil, "items.1.nested.0": "child",
			"negative.-name": "named", "-1": "root",
		} {
			if got := value(t, document, key); !reflect.DeepEqual(got, want) || !reflect.DeepEqual(native.Get(key), want) {
				t.Fatal("supported native index/map query changed")
			}
		}
		for _, key := range []string{"items.-1", "items.-01", "empty.-1", "items.1.nested.-1", "negative.-1", "negative.-01", "literal.-1"} {
			if got, err := document.ValueCopy(key); got != nil || !errors.Is(err, ErrInput) {
				t.Fatal("negative native-integer path component was not refused")
			}
		}
		for key, want := range map[string]any{"negative.-1": "literal", "negative.-01": "padded", "literal.-1": "dotted"} {
			if native.Get(key) != want {
				t.Fatal("native valid-but-excluded map-key control changed")
			}
		}
		if value(t, document, "negative").(map[string]any)["-1"] != "literal" || string(document.RawCopy()) != content {
			t.Fatal("technical query restriction erased source data")
		}
	}
}

func FuzzQuery(f *testing.F) {
	raw := []byte(`{"items":["first",{"nested":["child"]}],"empty":[],"map":{"-1":"literal"},"-1":"root"}`)
	for _, key := range []string{"items.-1", "items.-01", "items.0", "items.-0", "items.+0", "items.1.nested.-1", "empty.-1", "map.-1", "-1", "", "missing"} {
		f.Add(raw, key, false)
		f.Add(raw, key, true)
	}
	f.Fuzz(func(t *testing.T, raw []byte, key string, jsonEncoding bool) {
		if len(raw) > 64<<10 || len(key) > MaxKeyBytes+1 {
			return
		}
		encoding := "yaml"
		if jsonEncoding {
			encoding = "json"
		}
		documents, err := Load(context.Background(), []LoadInput{{Options: OptionsV1{Encoding: encoding}, Reader: bytes.NewReader(raw)}})
		if err != nil {
			return
		}
		got, err := documents[0].ValueCopy(key)
		if err != nil {
			if got != nil || !(errors.Is(err, ErrInput) || errors.Is(err, ErrLimit)) {
				t.Fatal("query failure exposed a value or an unknown fault")
			}
			return
		}
		again, err := documents[0].ValueCopy(key)
		if err != nil || !sameNativeValue(got, again) {
			t.Fatal("unchanged query was unstable")
		}
	})
}

func TestAbsentDocumentQueries(t *testing.T) {
	for _, document := range []*Document{nil, {}} {
		if document.RawCopy() != nil || document.Encoding() != "" {
			t.Fatal("absent document invented source data")
		}
		if got, err := document.ValueCopy("value"); got != nil || !errors.Is(err, ErrInput) {
			t.Fatal("absent document admitted a query")
		}
	}
}
