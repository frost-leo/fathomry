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
	jsonv1 "encoding/json"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"reflect"
	"strings"
	"testing"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
)

type v2Hook string

func (*v2Hook) MarshalJSONTo(*jsontext.Encoder) error { panic("hook called") }

func TestAdditionalProfileBoundaries(t *testing.T) {
	if err := prepareTarget[map[string]string](t, object(map[string]declaration.DataSchema{"name": scalar("string")})); !errors.Is(err, ErrUnsupported) {
		t.Fatal("map was not classified as an unsupported representation", err)
	}
	if err := prepareTarget[v2Hook](t, scalar("string")); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	optional := object(map[string]declaration.DataSchema{"name": scalar("string")})
	optional.Required = []string{}
	type record struct {
		Name string `json:"name"`
	}
	if err := prepareTarget[record](t, optional); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	type omitted struct {
		Name string `json:"name,omitempty"`
	}
	if err := prepareTarget[omitted](t, object(map[string]declaration.DataSchema{"name": scalar("string")})); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	type ignored struct {
		Name string `json:"-"`
	}
	if err := prepareTarget[ignored](t, object(map[string]declaration.DataSchema{"name": scalar("string")})); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	type embedded struct{ record }
	if err := prepareTarget[embedded](t, object(map[string]declaration.DataSchema{"record": object(map[string]declaration.DataSchema{"name": scalar("string")})})); err == nil {
		t.Fatal("embedded field accepted")
	}
}

func TestDuplicateJSONNames(t *testing.T) {
	// Dynamic invalid tags keep Go vet's own struct-tag checks enabled.
	callable := func(input, output reflect.Type) Callable {
		functionType := reflect.FuncOf([]reflect.Type{reflect.TypeFor[context.Context](), input}, []reflect.Type{output, reflect.TypeFor[error]()}, false)
		function := reflect.MakeFunc(functionType, func([]reflect.Value) []reflect.Value { panic("handler invoked") })
		return Callable{function: function.Interface(), input: input, output: output}
	}
	for _, nested := range []bool{false, true} {
		source := health.Declarations()
		bindings := healthBindings(nil)
		if nested {
			collision := reflect.StructOf([]reflect.StructField{
				{Name: "First", Type: reflect.TypeFor[string](), Tag: `json:"a"`},
				{Name: "Second", Type: reflect.TypeFor[string](), Tag: `json:"a"`},
			})
			input := reflect.StructOf([]reflect.StructField{{Name: "Target", Type: collision, Tag: `json:"target"`}})
			source.Workflows[0].Spec.Contracts["target"] = object(map[string]declaration.DataSchema{"a": scalar("string"), "b": scalar("string")})
			bindings[0].Callable = callable(input, reflect.TypeFor[metricOutput]())
		} else {
			input := reflect.StructOf([]reflect.StructField{
				{Name: "First", Type: reflect.TypeFor[int64](), Tag: `json:"cpu"`},
				{Name: "Second", Type: reflect.TypeFor[int64](), Tag: `json:"cpu"`},
			})
			bindings[2].Callable = callable(input, reflect.TypeFor[summaryOutput]())
		}
		_, err := Prepare(context.Background(), testBundle(t, source), healthIdentity(), bindings)
		diagnostic, ok := Diagnose(err)
		if !ok || diagnostic.Reason != "duplicate_json_name" {
			t.Fatalf("nested=%v: %+v %v", nested, diagnostic, err)
		}
	}
}
func TestManifestDecoderBounds(t *testing.T) {
	atCount := []byte(`{"format":"x","abi_profile":"x","specification":{},"bindings":[` + strings.Repeat("{},", 4095) + `{}],"calls":[]}`)
	raw := []byte(`{"format":"x","abi_profile":"x","specification":{},"bindings":[` + strings.Repeat("{},", 4096) + `{}],"calls":[]}`)
	for _, decode := range []func([]byte, any) error{
		func(raw []byte, value any) error { return json.Unmarshal(raw, value) },
		jsonv1.Unmarshal,
	} {
		var value Manifest
		if err := decode(atCount, &value); err != nil || len(value.Bindings) != 4096 {
			t.Fatalf("exact count limit rejected: %v", err)
		}
		if err := decode(raw, &value); !errors.Is(err, ErrLimit) || errors.Is(err, ErrInvalid) {
			t.Fatalf("count limit lost: %v", err)
		}
	}
	oversized := []byte(`{"format":"` + strings.Repeat("x", MaxManifestBytes) + `"}`)
	var value Manifest
	if err := json.Unmarshal(oversized, &value); !errors.Is(err, ErrLimit) || errors.Is(err, ErrInvalid) {
		t.Fatal("v2 bytes limit", err)
	}
	if err := jsonv1.Unmarshal(oversized, &value); !errors.Is(err, ErrLimit) || errors.Is(err, ErrInvalid) {
		t.Fatal("v1 bytes limit", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ValidateManifest(ctx, nil, Manifest{}); !errors.Is(err, ErrCanceled) {
		t.Fatal(err)
	}
}

func TestManifestExactBytes(t *testing.T) {
	base := `{"format":"x","abi_profile":"x","specification":{},"bindings":[],"calls":[]}`
	raw := []byte("{" + strings.Repeat(" ", MaxManifestBytes-len(base)) + base[1:])
	var value Manifest
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal("exact encoded bound refused", err)
	}
	raw = append([]byte("{ "), raw[1:]...)
	if err := json.Unmarshal(raw, &value); !errors.Is(err, ErrLimit) {
		t.Fatal("over encoded bound accepted", err)
	}
}

func TestTypeTraversalLimits(t *testing.T) {
	matcher := typeMatcher{ctx: context.Background(), visits: MaxTypeVisits - 1}
	if err := matcher.visit(MaxTypeDepth); err != nil {
		t.Fatal("exact limits refused", err)
	}
	if err := matcher.visit(0); !errors.Is(err, ErrLimit) {
		t.Fatal("visit budget not enforced", err)
	}
	matcher.visits = 0
	if err := matcher.visit(MaxTypeDepth + 1); !errors.Is(err, ErrLimit) {
		t.Fatal("depth budget not enforced", err)
	}
}
func FuzzManifest(f *testing.F) {
	bundle, err := declaration.Compile(context.Background(), health.Declarations())
	if err != nil {
		f.Fatal(err)
	}
	registry, err := Prepare(context.Background(), bundle, healthIdentity(), healthBindings(nil))
	if err != nil {
		f.Fatal(err)
	}
	manifest, _ := registry.ManifestCopy()
	raw, _ := json.Marshal(manifest)
	f.Add(raw)
	f.Add([]byte("{}"))
	f.Add([]byte(`{"bindings":null}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			return
		}
		var value Manifest
		if err := json.Unmarshal(raw, &value); err != nil {
			return
		}
		_ = ValidateManifest(context.Background(), bundle, value)
	})
}
