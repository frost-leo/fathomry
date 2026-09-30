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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/settings/v1"
)

type nested struct {
	First  int    `json:"first"`
	Second string `json:"second"`
}
type testData struct {
	Count    int64             `json:"count"`
	Unsigned uint64            `json:"unsigned"`
	Enabled  bool              `json:"enabled"`
	Text     string            `json:"text"`
	Delay    time.Duration     `json:"delay_ns"`
	Nested   nested            `json:"nested"`
	Pointer  *nested           `json:"pointer"`
	Map      map[string]nested `json:"map"`
	List     []nested          `json:"list"`
}

func defaults() testData {
	return testData{Count: 5, Enabled: true, Text: "inherited", Nested: nested{1, "keep"}, Pointer: &nested{2, "pointer"}, Map: map[string]nested{"Secret-Key": {3, "map"}}, List: []nested{{4, "list"}}}
}
func TestPrepare(t *testing.T) {
	t.Run("layering precision zero null and detached validation", func(t *testing.T) {
		input := defaults()
		schema := Schema[testData]{Version: 7, Defaults: input, Validate: func(_ context.Context, value testData) error {
			if value.Count != 9007199254740993 || value.Unsigned != math.MaxUint64 || value.Nested.First != 9 || value.Nested.Second != "keep" || value.Map["Secret-Key"].Second != "map" || value.Map["Secret-Key"].First != 8 || value.Enabled || value.Text != "" || value.Pointer != nil || len(value.List) != 0 || value.List == nil {
				return errors.New("wrong effective value")
			}
			value.Map["Secret-Key"] = nested{99, "validator-private"}
			return nil
		}}
		result, err := Prepare(context.Background(), schema, []Layer{
			{Kind: Variables, Encoding: JSON, Content: []byte(`{"unsigned":18446744073709551615,"count":9007199254740993}`)},
			{Kind: Local, Encoding: YAML, Content: []byte("nested: {first: 9}\nmap: {Secret-Key: {first: 8}}\n")},
			{Kind: Base, Encoding: JSON, Content: []byte(`{"count":7,"enabled":false,"text":"","pointer":null,"list":[]}`)},
			{Kind: Environment, Encoding: YAML, Content: []byte("nested: {}\nmap: {}\n")},
		})
		if err != nil {
			t.Fatal(err)
		}
		input.Map["Secret-Key"] = nested{}
		value, err := result.ValueCopy()
		if err != nil {
			t.Fatal(err)
		}
		if value.Map["Secret-Key"].Second != "map" {
			t.Fatal("validator or input alias")
		}
		value.Map["Secret-Key"] = nested{}
		snapshot, err := result.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		store := settings.NewStore[testData]()
		if err := store.Publish(snapshot); err != nil {
			t.Fatal(err)
		}
		view, err := store.Reader().Capture()
		if err != nil {
			t.Fatal(err)
		}
		stored, err := settings.As[testData](view)
		if err != nil {
			t.Fatal(err)
		}
		copied, _ := stored.ValueCopy()
		if copied.Map["Secret-Key"].Second != "map" {
			t.Fatal("snapshot alias")
		}
		description := result.Description()
		if description.Version != 7 || len(description.Revision) != 32 || len(description.Layers) != 4 {
			t.Fatal(description)
		}
		description.Layers[0].Fields[0] = "mutated"
		if strings.Contains(fmt.Sprint(result.Description()), "Secret-Key") || strings.Contains(fmt.Sprint(result.Description()), "mutated") {
			t.Fatal("unsafe/aliased provenance")
		}
		next, err := Prepare(context.Background(), schema, []Layer{{Kind: Base, Encoding: JSON, Content: []byte("{}")}})
		if err == nil || next.state != nil {
			t.Fatal("validation failure gave prepared value")
		}
	})
	t.Run("null map list and pointer reintroduction", func(t *testing.T) {
		result, err := Prepare(context.Background(), Schema[testData]{Version: 1, Defaults: defaults()}, []Layer{
			{Base, JSON, []byte(`{"pointer":null,"map":null,"list":null}`)},
			{Local, JSON, []byte(`{"pointer":{"second":"new"},"map":{"Other":{"first":1}}}`)},
		})
		if err != nil {
			t.Fatal(err)
		}
		value, _ := result.ValueCopy()
		if value.Pointer.First != 0 || value.Pointer.Second != "new" || len(value.Map) != 1 || value.List != nil {
			t.Fatal("wrong clear/overlay")
		}
	})
	t.Run("empty schema", func(t *testing.T) {
		if _, err := Prepare(context.Background(), Schema[struct{}]{Version: 1}, nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("failure and cancellation preserve causes", func(t *testing.T) {
		marker := errors.New("private-validation-canary")
		value, err := Prepare(context.Background(), Schema[testData]{Version: 1, Validate: func(context.Context, testData) error { return marker }}, nil)
		if !errors.Is(err, marker) || value.state != nil || strings.Contains(fmt.Sprintf("%+v", err), "private-validation-canary") {
			t.Fatal("unsafe validation")
		}
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(marker)
		if _, err := Prepare(ctx, Schema[testData]{Version: 1}, nil); !errors.Is(err, context.Canceled) || !errors.Is(err, marker) {
			t.Fatal(err)
		}
		ctx, cancel = context.WithCancelCause(context.Background())
		_, err = Prepare(ctx, Schema[testData]{Version: 1, Validate: func(context.Context, testData) error { cancel(marker); return nil }}, nil)
		if !errors.Is(err, marker) {
			t.Fatal(err)
		}
	})
}
func TestPrepareRejects(t *testing.T) {
	cases := []struct {
		name   string
		format Encoding
		raw    string
	}{
		{"empty", JSON, ""}, {"empty yaml", YAML, "# comment"},
		{"unknown", JSON, `{"COUNT":1}`}, {"duplicate escaped", JSON, `{"text":"a","te\u0078t":"b"}`},
		{"trailing", JSON, "{} {}"}, {"root list", JSON, "[]"}, {"null scalar", JSON, `{"count":null}`},
		{"fraction integer", JSON, `{"count":1.0}`}, {"exponent integer", JSON, `{"count":1e0}`}, {"signed overflow", JSON, `{"count":9223372036854775808}`},
		{"unsigned overflow", JSON, `{"unsigned":18446744073709551616}`}, {"weak conversion", JSON, `{"count":"1"}`},
		{"surrogate high", JSON, `{"text":"\ud800"}`}, {"surrogate low", JSON, `{"text":"\udc00"}`},
		{"duplicate yaml", YAML, "text: a\ntext: b\n"}, {"alias", YAML, "map: &m {}\npointer: *m"},
		{"anchor", YAML, "text: &x value"}, {"merge", YAML, "map: {<<: {}}"},
		{"bare tag", YAML, "text: ! value"}, {"tagged", YAML, "text: !!str value"}, {"root tag", YAML, "! {}"},
		{"key tag", YAML, "! text: value"}, {"timestamp", YAML, "text: 2026-09-30"}, {"non string key", YAML, "map: {1: {}}"},
		{"multiple", YAML, "{}\n---\n{}"}, {"hex", YAML, "count: 0x10"}, {"nan", YAML, "count: .nan"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			result, err := Prepare(context.Background(), Schema[testData]{Version: 1}, []Layer{{Base, test.format, []byte(test.raw)}, {Local, JSON, []byte(`{"count":4,"text":"override"}`)}})
			if err == nil || result.state != nil {
				t.Fatal("accepted invalid lower layer")
			}
		})
	}
	for _, raw := range []string{`{"text":"\ud83d\ude03"}`, "text: '! literal'", "text: |\n  ! literal\n", "\ufefftext: '! literal'"} {
		format := YAML
		if strings.HasPrefix(raw, "{") {
			format = JSON
		}
		if _, err := Prepare(context.Background(), Schema[testData]{Version: 1}, []Layer{{Base, format, []byte(raw)}}); err != nil {
			t.Fatal(raw, err)
		}
	}
	t.Run("declarations and bounds", func(t *testing.T) {
		for _, layers := range [][]Layer{
			{{Base, JSON, []byte("{}")}, {Base, JSON, []byte("{}")}},
			{{0, JSON, []byte("{}")}}, {{5, JSON, []byte("{}")}}, {{Base, "toml", []byte("{}")}},
			{{Base, JSON, []byte(strings.Repeat(" ", MaxDocumentBytes) + "{}")}},
			{{Base, JSON, []byte("{\"text\":\"" + string([]byte{255}) + "\"}")}},
		} {
			if _, err := Prepare(context.Background(), Schema[testData]{Version: 1}, layers); err == nil {
				t.Fatal("accepted invalid declaration")
			}
		}
		if _, err := Prepare[testData](nil, Schema[testData]{Version: 1}, nil); err == nil {
			t.Fatal("nil context")
		}
		if _, err := Prepare(context.Background(), Schema[testData]{}, nil); err == nil {
			t.Fatal("zero version")
		}
		if _, err := Prepare(context.Background(), Schema[int]{Version: 1}, nil); err == nil {
			t.Fatal("nonstruct root")
		}
		schema := Schema[testData]{Version: 1, Defaults: defaults()}
		schema.Defaults.Text = strings.Repeat("x", MaxDocumentBytes)
		if _, err := Prepare(context.Background(), schema, nil); !errors.Is(err, ErrLimit) {
			t.Fatal(err)
		}
		for _, raw := range []string{strings.Repeat("[", MaxDepth+2) + strings.Repeat("]", MaxDepth+2), "{\"text\":[" + strings.Repeat("0,", MaxNodes) + "0]}"} {
			if _, err := parse([]byte(raw), JSON); !errors.Is(err, ErrLimit) {
				t.Fatal(err)
			}
		}
	})
}
func TestSchema(t *testing.T) {
	type recursive struct {
		Next *recursive `json:"next"`
	}
	for _, kind := range []reflect.Type{
		reflect.TypeFor[recursive](), reflect.TypeFor[struct {
			Value any `json:"value"`
		}](),
		reflect.TypeFor[struct {
			Value []byte `json:"value"`
		}](), reflect.TypeFor[struct {
			Value time.Time `json:"value"`
		}](),
		reflect.TypeFor[struct{ Value int }](), reflect.TypeFor[struct {
			Value int `json:"value,omitempty"`
		}](),
		reflect.TypeFor[struct {
			Value func() `json:"value"`
		}](), reflect.TypeFor[struct {
			Value map[int]string `json:"value"`
		}](),
	} {
		if _, err := compile(kind); err == nil {
			t.Fatal("accepted", kind)
		}
	}
	kind := reflect.TypeFor[struct{}]()
	for range 20 {
		kind = reflect.StructOf([]reflect.StructField{{Name: "First", Type: kind, Tag: `json:"first"`}, {Name: "Second", Type: kind, Tag: `json:"second"`}})
	}
	shape, err := compile(kind)
	if err != nil {
		t.Fatal(err)
	}
	budget := newBudget()
	if _, err := fromGo(shape, reflect.New(kind).Elem(), &budget, 0); !errors.Is(err, ErrLimit) {
		t.Fatal("unbounded defaults traversal", err)
	}
}
func TestPrepared(t *testing.T) {
	schema := Schema[testData]{Version: 1, Defaults: defaults(), Validate: func(context.Context, testData) error { return nil }}
	for _, value := range []any{schema, Layer{Content: []byte("Secret-Key")}, Variable{Value: "Secret-Key"}} {
		if strings.Contains(fmt.Sprintf("%+v %#v", value, value), "Secret-Key") {
			t.Fatal("declaration diagnostics exposed data")
		}
	}
	if _, err := json.Marshal(schema); !errors.Is(err, ErrSerialization) {
		t.Fatal("schema callback declaration serialized", err)
	}
	if err := json.Unmarshal([]byte("{}"), &schema); !errors.Is(err, ErrSerialization) || schema.Validate == nil {
		t.Fatal("schema validation silently reconstructed", err)
	}
	result, err := Prepare(context.Background(), Schema[testData]{Version: 1, Defaults: defaults()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(fmt.Sprintf("%#v", result), "Secret-Key") {
		t.Fatal("format leaked")
	}
	if _, err := json.Marshal(result); err == nil {
		t.Fatal("serialized runtime")
	}
	if err := json.Unmarshal([]byte("{}"), &result); err == nil {
		t.Fatal("reconstructed runtime")
	}
	var zero Prepared[testData]
	if _, err := zero.ValueCopy(); err == nil {
		t.Fatal("zero value")
	}
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			for range 30 {
				value, err := result.ValueCopy()
				if err != nil {
					t.Error(err)
					return
				}
				value.Map["Secret-Key"] = nested{}
				_, _ = result.Snapshot()
			}
		})
	}
	group.Wait()
}
func FuzzPrepare(f *testing.F) {
	for _, seed := range []string{"{}", `{"count":3}`, "text: hello", `{"text":"\ud800"}`, "text: ! value"} {
		f.Add(seed, true)
		f.Add(seed, false)
	}
	f.Fuzz(func(t *testing.T, raw string, jsonFormat bool) {
		format := YAML
		if jsonFormat {
			format = JSON
		}
		prepared, err := Prepare(context.Background(), Schema[testData]{Version: 1}, []Layer{{Base, format, []byte(raw)}})
		if err != nil {
			if prepared.state != nil {
				t.Fatal("prepared failure")
			}
			return
		}
		first, err := prepared.ValueCopy()
		if err != nil {
			t.Fatal(err)
		}
		second, err := prepared.ValueCopy()
		if err != nil || !reflect.DeepEqual(first, second) {
			t.Fatal("unstable copy")
		}
	})
}
