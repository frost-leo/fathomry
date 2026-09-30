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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type testNested struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func TestSharedTypeDAGAdmission(t *testing.T) {
	kind := reflect.TypeFor[admissionDAG28]()
	if !plainType(kind, make(map[reflect.Type]bool)) {
		t.Fatal("bounded shared plain-data type rejected")
	}
}

type admissionDAG0 struct {
	Value string `json:"value"`
}
type admissionDAG1 struct {
	Left  *admissionDAG0 `json:"left"`
	Right *admissionDAG0 `json:"right"`
}
type admissionDAG2 struct {
	Left  *admissionDAG1 `json:"left"`
	Right *admissionDAG1 `json:"right"`
}
type admissionDAG3 struct {
	Left  *admissionDAG2 `json:"left"`
	Right *admissionDAG2 `json:"right"`
}
type admissionDAG4 struct {
	Left  *admissionDAG3 `json:"left"`
	Right *admissionDAG3 `json:"right"`
}
type admissionDAG5 struct {
	Left  *admissionDAG4 `json:"left"`
	Right *admissionDAG4 `json:"right"`
}
type admissionDAG6 struct {
	Left  *admissionDAG5 `json:"left"`
	Right *admissionDAG5 `json:"right"`
}
type admissionDAG7 struct {
	Left  *admissionDAG6 `json:"left"`
	Right *admissionDAG6 `json:"right"`
}
type admissionDAG8 struct {
	Left  *admissionDAG7 `json:"left"`
	Right *admissionDAG7 `json:"right"`
}
type admissionDAG9 struct {
	Left  *admissionDAG8 `json:"left"`
	Right *admissionDAG8 `json:"right"`
}
type admissionDAG10 struct {
	Left  *admissionDAG9 `json:"left"`
	Right *admissionDAG9 `json:"right"`
}
type admissionDAG11 struct {
	Left  *admissionDAG10 `json:"left"`
	Right *admissionDAG10 `json:"right"`
}
type admissionDAG12 struct {
	Left  *admissionDAG11 `json:"left"`
	Right *admissionDAG11 `json:"right"`
}
type admissionDAG13 struct {
	Left  *admissionDAG12 `json:"left"`
	Right *admissionDAG12 `json:"right"`
}
type admissionDAG14 struct {
	Left  *admissionDAG13 `json:"left"`
	Right *admissionDAG13 `json:"right"`
}
type admissionDAG15 struct {
	Left  *admissionDAG14 `json:"left"`
	Right *admissionDAG14 `json:"right"`
}
type admissionDAG16 struct {
	Left  *admissionDAG15 `json:"left"`
	Right *admissionDAG15 `json:"right"`
}
type admissionDAG17 struct {
	Left  *admissionDAG16 `json:"left"`
	Right *admissionDAG16 `json:"right"`
}
type admissionDAG18 struct {
	Left  *admissionDAG17 `json:"left"`
	Right *admissionDAG17 `json:"right"`
}
type admissionDAG19 struct {
	Left  *admissionDAG18 `json:"left"`
	Right *admissionDAG18 `json:"right"`
}
type admissionDAG20 struct {
	Left  *admissionDAG19 `json:"left"`
	Right *admissionDAG19 `json:"right"`
}
type admissionDAG21 struct {
	Left  *admissionDAG20 `json:"left"`
	Right *admissionDAG20 `json:"right"`
}
type admissionDAG22 struct {
	Left  *admissionDAG21 `json:"left"`
	Right *admissionDAG21 `json:"right"`
}
type admissionDAG23 struct {
	Left  *admissionDAG22 `json:"left"`
	Right *admissionDAG22 `json:"right"`
}
type admissionDAG24 struct {
	Left  *admissionDAG23 `json:"left"`
	Right *admissionDAG23 `json:"right"`
}
type admissionDAG25 struct {
	Left  *admissionDAG24 `json:"left"`
	Right *admissionDAG24 `json:"right"`
}
type admissionDAG26 struct {
	Left  *admissionDAG25 `json:"left"`
	Right *admissionDAG25 `json:"right"`
}
type admissionDAG27 struct {
	Left  *admissionDAG26 `json:"left"`
	Right *admissionDAG26 `json:"right"`
}
type admissionDAG28 struct {
	Left  *admissionDAG27 `json:"left"`
	Right *admissionDAG27 `json:"right"`
}
type testSettings struct {
	Connection testNested        `json:"connection"`
	Headers    map[string]string `json:"headers"`
	Items      []string          `json:"items"`
	Optional   *testNested       `json:"optional"`
	Enabled    bool              `json:"enabled"`
	Limit      uint8             `json:"limit"`
}

func testSchema() Schema[testSettings] {
	return Schema[testSettings]{Format: 1, Defaults: testSettings{
		Connection: testNested{Host: "default", Port: 443},
		Headers:    map[string]string{"default": "value"}, Items: []string{"default"},
		Optional: &testNested{Host: "optional", Port: 44}, Enabled: true, Limit: 5,
	}}
}
func testInput(content string) Input {
	return Input{Identity: Identity{Provider: "example.memory", Name: "one"}, Format: 1,
		Layers: []Layer{{Kind: Base, Content: []byte(content)}}}
}

func TestPreparationPrecedenceNullEmptyAndIsolation(t *testing.T) {
	schema := testSchema()
	schema.Validate = func(value testSettings) error { value.Headers["default"] = "validator"; return nil }
	input := testInput("{}")
	input.Layers = []Layer{
		{Kind: Variables, Content: []byte("connection: {host: variables}\nenabled: false")},
		{Kind: Local, Content: []byte("items: []\noptional: null\nheaders: {local: ''}")},
		{Kind: Base, Content: []byte("connection: {host: base}\nitems: [base]\nheaders: {base: first}")},
		{Kind: Environment, Content: []byte("connection: {host: environment}\nheaders: {base: last}")},
	}
	prepared, err := Prepare(schema, input)
	if err != nil {
		t.Fatal(err)
	}
	schema.Defaults.Headers["default"] = "caller"
	schema.Defaults.Optional.Host = "caller"
	input.Layers[0].Content[0] = 'X'
	value, err := prepared.settings()
	if err != nil {
		t.Fatal(err)
	}
	if value.Connection != (testNested{Host: "variables", Port: 443}) || value.Enabled ||
		value.Items == nil || len(value.Items) != 0 || value.Optional != nil ||
		!reflect.DeepEqual(value.Headers, map[string]string{"default": "value", "base": "last", "local": ""}) {
		t.Fatalf("incorrect resolved settings: %#v", value)
	}
	value.Headers["default"] = "changed"
	value.Connection.Host = "changed"
	again, _ := prepared.settings()
	if again.Headers["default"] != "value" || again.Connection.Host != "variables" {
		t.Fatal("prepared settings aliased")
	}
	description := prepared.Description()
	description.Provenance[0].Fields[0] = "changed"
	if prepared.Description().Provenance[0].Fields[0] == "changed" {
		t.Fatal("provenance aliased")
	}
	for index, layer := range prepared.Description().Provenance {
		if layer.Kind != LayerKind(index) {
			t.Fatal("precedence not canonical")
		}
	}
	second, err := Prepare(testSchema(), testInput("{}"))
	if err != nil || second.Description().Revision == prepared.Description().Revision {
		t.Fatal("revision reused")
	}
}

func TestConfigurationRejectsEveryInvalidLayer(t *testing.T) {
	cases := []string{"", "[]", "null", "connection: [wrong]", "enabled: null", "limit: 256",
		"limit: -1", "limit: 1.5", "limit: '1'", "enabled: yes", "Enabled: true",
		"unknown: private-token", "connection: {unknown: private-token}", "enabled: true\nenabled: false",
		"connection: {host: a, host: b}", "items: &items [a]", "items: *items",
		"headers: {<<: {a: b}}", "enabled: !!bool true", "connection: {host: 2026-09-08}",
		"headers: {1: value}", "headers: {a: .nan}", "{}\n---\n{}", "{",
		strings.Repeat(" ", 1<<20+1),
	}
	for index, content := range cases {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			prepared, err := Prepare(testSchema(), testInput(content))
			if !errors.Is(err, ErrConfiguration) || prepared.state != nil {
				t.Fatal("invalid configuration accepted")
			}
			if strings.Contains(fmt.Sprintf("%#v", err), "private-token") {
				t.Fatal("configuration disclosed")
			}
		})
	}
	input := testInput("unknown: value")
	input.Layers = append(input.Layers, Layer{Kind: Variables, Content: []byte("{}")})
	if _, err := Prepare(testSchema(), input); err == nil {
		t.Fatal("invalid lower layer masked")
	}
	for _, kind := range []LayerKind{Defaults, Base, LayerKind(99)} {
		input := testInput("{}")
		input.Layers = append(input.Layers, Layer{Kind: kind, Content: []byte("{}")})
		if _, err := Prepare(testSchema(), input); err == nil {
			t.Fatal("invalid or duplicate kind accepted")
		}
	}
	input = testInput("{}")
	input.Format = 2
	if _, err := Prepare(testSchema(), input); err == nil {
		t.Fatal("unknown format accepted")
	}
	input = testInput("{}")
	input.Identity.Name = "private/path"
	if _, err := Prepare(testSchema(), input); err == nil {
		t.Fatal("invalid identity accepted")
	}
}

func TestIntegerLexemesDoNotPassThroughFloatingPoint(t *testing.T) {
	type integerSettings struct {
		Value uint64 `json:"value"`
	}
	schema := Schema[integerSettings]{Format: 1}
	for _, content := range []string{"value: 9007199254740993.0", "value: 1.0", "value: 1e1"} {
		if _, err := Prepare(schema, testInput(content)); err == nil {
			t.Errorf("floating syntax accepted for integer: %s", content)
		}
	}
	prepared, err := Prepare(schema, testInput("value: 18446744073709551615"))
	if err != nil {
		t.Fatal(err)
	}
	value, err := prepared.settings()
	if err != nil || value.Value != ^uint64(0) {
		t.Fatal("integer precision lost")
	}
}

type customSettings struct{}

func (customSettings) MarshalJSON() ([]byte, error) { panic("custom marshaler invoked") }

type recursiveSettings struct {
	Next *recursiveSettings `json:"next"`
}

func rejectedType[T any](t *testing.T, defaults T) {
	t.Helper()
	if _, err := Prepare(Schema[T]{Format: 1, Defaults: defaults}, testInput("{}")); err == nil {
		t.Fatalf("accepted unsupported type %T", defaults)
	}
}
func TestPlainDataBoundary(t *testing.T) {
	rejectedType(t, customSettings{})
	rejectedType(t, recursiveSettings{})
	rejectedType(t, struct{ MissingTag string }{})
	rejectedType(t, struct {
		Private string `json:"private,omitempty"`
	}{})
	rejectedType(t, struct {
		Callback func() `json:"callback"`
	}{})
	rejectedType(t, struct {
		Value any `json:"value"`
	}{})
	rejectedType(t, struct {
		Data []byte `json:"data"`
	}{})
	rejectedType(t, struct {
		Time time.Time `json:"time"`
	}{})
	rejectedType(t, struct{ private string }{})
	schema := testSchema()
	schema.Validate = func(testSettings) error { return context.Canceled }
	if _, err := Prepare(schema, testInput("{}")); !errors.Is(err, context.Canceled) {
		t.Fatal("validator cause lost")
	}
}

func TestPreparedDiagnosticsNeverContainSecrets(t *testing.T) {
	prepared, err := Prepare(testSchema(), testInput("headers: {private-key: private-token}"))
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		text := fmt.Sprintf(format, prepared)
		if strings.Contains(text, "private-token") || strings.Contains(text, "private-key") {
			t.Fatal("prepared data leaked")
		}
	}
	if _, err := json.Marshal(prepared); err == nil {
		t.Fatal("runtime settings serialized")
	}
	data, err := json.Marshal(prepared.Description())
	if err != nil || bytes.Contains(data, []byte("private")) {
		t.Fatal("unsafe provenance")
	}
}

func TestDefaultsRejectInvalidUTF8WithoutChangingData(t *testing.T) {
	for _, defaults := range []testSettings{
		{Connection: testNested{Host: "\xff"}},
		{Headers: map[string]string{"\xff": "first", "\xfe": "second"}},
		{Items: []string{"\xff"}},
		{Optional: &testNested{Host: "\xff"}},
	} {
		if _, err := Prepare(Schema[testSettings]{Format: 1, Defaults: defaults}, testInput("{}")); !errors.Is(err, ErrConfiguration) {
			t.Error("invalid UTF-8 was normalized instead of rejected")
		}
	}
}

func TestPreparationConcurrentReuse(t *testing.T) {
	prepared, err := Prepare(testSchema(), testInput("{}"))
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for index := 0; index < 32; index++ {
		group.Go(func() {
			for iteration := 0; iteration < 50; iteration++ {
				value, err := prepared.settings()
				if err != nil || value.Headers["default"] != "value" {
					t.Error("shared configuration")
				}
				value.Headers["default"] = "local"
				info := prepared.Description()
				info.Provenance[0].Fields[0] = "local"
			}
		})
	}
	group.Wait()
}

func FuzzPrepare(f *testing.F) {
	for _, seed := range []string{"{}", "enabled: null", "connection: {host: x}", "headers: {a: b, a: c}", "items: &a [*a]",
		"enabled: ! true", "! {enabled: true}", "! enabled: true", "items: ! [value]",
		"connection: {host: 'é😀'}, items: [! value]", "headers: {'! key': '! value'}", "items: ['! value']"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, content string) {
		prepared, err := Prepare(testSchema(), testInput(content))
		if err == nil {
			if _, err := prepared.settings(); err != nil || prepared.Description().Revision == "" {
				t.Fatal("invalid success")
			}
			if _, err := Prepare(testSchema(), testInput("! "+content)); !errors.Is(err, ErrConfiguration) {
				t.Fatal("explicitly tagged document accepted")
			}
		} else if prepared.state != nil || !errors.Is(err, ErrConfiguration) {
			t.Fatal("invalid failure")
		}
	})
}
