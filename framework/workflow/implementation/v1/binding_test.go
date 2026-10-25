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

package implementation

import (
	"context"
	"encoding"
	jsonv1 "encoding/json"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
	"github.com/frost-leo/fathomry/i18n/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type metricInput struct {
	Target string `json:"target"`
}
type metricOutput struct {
	Value int64 `json:"value"`
}
type summaryInput struct {
	CPU    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
}
type summaryOutput struct {
	Text string `json:"text"`
}

func testBundle(t *testing.T, source declaration.Declaration) *declaration.Bundle {
	t.Helper()
	value, err := declaration.Compile(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func healthIdentity() declaration.Identity {
	return declaration.Identity{Namespace: "example", Code: "server-health", Version: "1"}
}
func healthBindings(calls *int) []Binding {
	metric := Activity(func(context.Context, metricInput) (metricOutput, error) {
		if calls != nil {
			*calls++
		}
		return metricOutput{42}, nil
	})
	summary := Activity(func(context.Context, summaryInput) (summaryOutput, error) {
		if calls != nil {
			*calls++
		}
		return summaryOutput{"ok"}, nil
	})
	return []Binding{{Key: "read_cpu", Callable: metric}, {Key: "read_memory", Callable: metric}, {Key: "summarize", Callable: summary}}
}
func TestPrepare(t *testing.T) {
	source := health.Declarations()
	bundle := testBundle(t, source)
	count := 0
	bindings := healthBindings(&count)
	registry, err := Prepare(context.Background(), bundle, healthIdentity(), bindings)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("preparation called handler")
	}
	bindings[0].Key = "changed"
	first, err := registry.ManifestCopy()
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Bindings) != 3 || len(first.Calls) != 3 {
		t.Fatalf("counts: %+v", first)
	}
	if err := ValidateManifest(context.Background(), bundle, first); err != nil {
		t.Fatal(err)
	}
	first.Bindings[0].Key = "corrupt"
	second, _ := registry.ManifestCopy()
	if second.Bindings[0].Key == "corrupt" {
		t.Fatal("aliased manifest")
	}
	fn, err := registry.Lookup("read_cpu")
	if err != nil {
		t.Fatal(err)
	}
	value, err := fn.(func(context.Context, metricInput) (metricOutput, error))(context.Background(), metricInput{"example"})
	if err != nil || value.Value != 42 || count != 1 {
		t.Fatal("borrowed callable changed")
	}
	if strings.Contains(fmt.Sprintf("%#v", registry), "read_cpu") {
		t.Fatal("diagnostic leaked")
	}
	if _, err := jsonv1.Marshal(registry); !errors.Is(err, ErrHandle) {
		t.Fatalf("serialization: %v", err)
	}
	var reconstructed Registry
	raw, _ := json.Marshal(second)
	if err := json.Unmarshal(raw, &reconstructed); !errors.Is(err, ErrHandle) {
		t.Fatalf("reconstructed runtime: %v", err)
	}
	var wait sync.WaitGroup
	for range 8 {
		wait.Go(func() {
			for range 20 {
				manifest, err := registry.ManifestCopy()
				if err != nil {
					t.Error(err)
				}
				if err = ValidateManifest(context.Background(), bundle, manifest); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wait.Wait()
}
func TestBindingRefusals(t *testing.T) {
	bundle := testBundle(t, health.Declarations())
	var absent func(context.Context, metricInput) (metricOutput, error)
	for _, test := range []struct {
		name   string
		change func([]Binding) []Binding
		code   error
	}{
		{"missing", func(values []Binding) []Binding { return values[:2] }, ErrInvalid},
		{"duplicate", func(values []Binding) []Binding { return append(values, values[0]) }, ErrInvalid},
		{"foreign", func(values []Binding) []Binding { values[0].Key = "foreign"; return values }, ErrInvalid},
		{"nil", func(values []Binding) []Binding { values[0].Callable = Activity(absent); return values }, ErrInvalid},
		{"zero", func(values []Binding) []Binding { values[0].Callable = Callable{}; return values }, ErrInvalid},
		{"wrong_result", func(values []Binding) []Binding {
			values[0].Callable = Activity(func(context.Context, metricInput) (summaryOutput, error) { panic("not called") })
			return values
		}, ErrInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, err := Prepare(context.Background(), bundle, healthIdentity(), test.change(healthBindings(nil)))
			if value != nil || !errors.Is(err, test.code) {
				t.Fatalf("got %v %v", value, err)
			}
		})
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("private-cancel")
	cancel(cause)
	if value, err := Prepare(ctx, bundle, healthIdentity(), healthBindings(nil)); value != nil || !errors.Is(err, ErrCanceled) || !errors.Is(err, cause) {
		t.Fatal("cancellation lost")
	}
	if _, err := Prepare(nil, bundle, healthIdentity(), nil); !errors.Is(err, ErrHandle) {
		t.Fatal(err)
	}
	if _, err := Prepare(context.Background(), nil, healthIdentity(), nil); !errors.Is(err, ErrHandle) {
		t.Fatal(err)
	}
}
func TestReusedBinding(t *testing.T) {
	source := health.Declarations()
	spec := &source.Workflows[0].Spec
	node := spec.Nodes["sample"]
	call := node.Activities["memory"]
	call.Binding = "read_cpu"
	node.Activities["memory"] = call
	spec.Nodes["sample"] = node
	delete(spec.Bindings, "read_memory")
	bindings := healthBindings(nil)
	bindings = append(bindings[:1], bindings[2])
	bundle := testBundle(t, source)
	value, err := Prepare(context.Background(), bundle, healthIdentity(), bindings)
	if err != nil {
		t.Fatal(err)
	}
	manifest, _ := value.ManifestCopy()
	if len(manifest.Bindings) != 2 || len(manifest.Calls) != 3 {
		t.Fatal("call/binding units conflated")
	}
}
func TestManifestRefusals(t *testing.T) {
	bundle := testBundle(t, health.Declarations())
	registry, err := Prepare(context.Background(), bundle, healthIdentity(), healthBindings(nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*Manifest)
	}{
		{"profile", func(v *Manifest) { v.ABIProfile = "unknown" }},
		{"missing", func(v *Manifest) { v.Calls = v.Calls[:2] }},
		{"duplicate", func(v *Manifest) { v.Bindings[1] = v.Bindings[0] }},
		{"digest", func(v *Manifest) { v.Specification.SHA256 = strings.Repeat("0", 64) }},
		{"foreign", func(v *Manifest) { v.Specification.Identity.Version = "other" }},
		{"mode", func(v *Manifest) { v.Specification.Identity.Mode = "standard"; v.Specification.Identity.Revision = "1" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, _ := registry.ManifestCopy()
			test.change(&value)
			if err := ValidateManifest(context.Background(), bundle, value); err == nil {
				t.Fatal("accepted invalid manifest")
			}
		})
	}
}

type jsonHook string

func (jsonHook) MarshalJSON() ([]byte, error) { panic("codec invoked") }

type pointerHook string

func (*pointerHook) UnmarshalText([]byte) error { panic("codec invoked") }

type appendHook string

func (appendHook) AppendText([]byte) ([]byte, error) { panic("codec invoked") }

var _ encoding.TextAppender = appendHook("")

type protoHook struct {
	Value int64 `json:"value"`
}

func (protoHook) ProtoReflect() protoreflect.Message { panic("codec invoked") }

type legacyHook struct {
	Value int64 `json:"value"`
}

func (*legacyHook) Reset()         { panic("codec invoked") }
func (*legacyHook) String() string { panic("codec invoked") }
func (*legacyHook) ProtoMessage()  { panic("codec invoked") }

type recursive struct {
	Next *recursive `json:"next"`
}

func scalar(t string) declaration.DataSchema {
	min, max := int64(0), int64(100)
	switch t {
	case "string":
		return declaration.DataSchema{Type: t, MinLength: &min, MaxLength: &max}
	case "integer":
		return declaration.DataSchema{Type: t, Minimum: &min, Maximum: &max}
	default:
		return declaration.DataSchema{Type: t}
	}
}
func object(properties map[string]declaration.DataSchema) declaration.DataSchema {
	no := false
	return declaration.DataSchema{Type: "object", Properties: properties, Required: sortedKeys(properties), AdditionalProperties: &no}
}
func prepareTarget[Target any](t *testing.T, schema declaration.DataSchema) error {
	t.Helper()
	type request struct {
		Target Target `json:"target"`
	}
	source := health.Declarations()
	source.Workflows[0].Spec.Contracts["target"] = schema
	bindings := healthBindings(nil)
	callable := Activity(func(context.Context, request) (metricOutput, error) { panic("handler invoked") })
	bindings[0].Callable, bindings[1].Callable = callable, callable
	_, err := Prepare(context.Background(), testBundle(t, source), healthIdentity(), bindings)
	return err
}
func TestShapeProfile(t *testing.T) {
	if err := prepareTarget[string](t, scalar("string")); err != nil {
		t.Fatal(err)
	}
	if err := prepareTarget[int64](t, scalar("integer")); err != nil {
		t.Fatal(err)
	}
	if err := prepareTarget[bool](t, scalar("boolean")); err != nil {
		t.Fatal(err)
	}
	type record struct {
		Name string `json:"name"`
	}
	if err := prepareTarget[record](t, object(map[string]declaration.DataSchema{"name": scalar("string")})); err != nil {
		t.Fatal(err)
	}
	min, max := int64(0), int64(4)
	item := scalar("string")
	if err := prepareTarget[[]string](t, declaration.DataSchema{Type: "array", Items: &item, MinItems: &min, MaxItems: &max}); err != nil {
		t.Fatal(err)
	}
	nullable := declaration.DataSchema{AnyOf: []declaration.DataSchema{scalar("string"), {Type: "null"}}}
	if err := prepareTarget[*string](t, nullable); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		run  func() error
	}{
		{"json", func() error { return prepareTarget[jsonHook](t, scalar("string")) }},
		{"pointer", func() error { return prepareTarget[pointerHook](t, scalar("string")) }},
		{"append", func() error { return prepareTarget[appendHook](t, scalar("string")) }},
		{"proto", func() error {
			return prepareTarget[protoHook](t, object(map[string]declaration.DataSchema{"value": scalar("integer")}))
		}},
		{"legacy", func() error {
			return prepareTarget[legacyHook](t, object(map[string]declaration.DataSchema{"value": scalar("integer")}))
		}},
		{"number", func() error { return prepareTarget[jsonv1.Number](t, scalar("string")) }},
		{"int", func() error { return prepareTarget[int](t, scalar("integer")) }},
		{"map", func() error {
			return prepareTarget[map[string]string](t, object(map[string]declaration.DataSchema{"name": scalar("string")}))
		}},
		{"recursive", func() error {
			return prepareTarget[recursive](t, object(map[string]declaration.DataSchema{"next": nullable}))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); err == nil {
				t.Fatal("unsupported shape accepted")
			}
		})
	}
}
func TestResources(t *testing.T) {
	components := append(i18n.CoreComponents(), Component())
	if _, err := i18n.Prepare(components...); err != nil {
		t.Fatal(err)
	}
}
