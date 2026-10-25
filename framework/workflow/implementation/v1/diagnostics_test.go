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
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
)

func TestManifestDecodeDiagnostics(t *testing.T) {
	const canary = "private_manifest_marker"
	base := "{\"format\":\"x\",\"abi_profile\":\"x\",\"specification\":{},\"bindings\":[],\"calls\":[]}"
	for _, test := range []struct {
		name    string
		raw     string
		unknown bool
		syntax  bool
	}{
		{"unknown_root", base[:len(base)-1] + ",\"" + canary + "\":true}", true, false},
		{"unknown_binding", strings.Replace(base, "\"bindings\":[]", "\"bindings\":[{\""+canary+"\":true}]", 1), true, false},
		{"unknown_call", strings.Replace(base, "\"calls\":[]", "\"calls\":[{\""+canary+"\":true}]", 1), true, false},
		{"duplicate", base[:len(base)-1] + ",\"format\":\"" + canary + "\"}", false, true},
		{"invalid_value", strings.Replace(base, "\"format\":\"x\"", "\"format\":{\""+canary+"\":true}", 1), false, false},
	} {
		for _, decoder := range []struct {
			name   string
			decode func([]byte, any) error
		}{
			{"v1", jsonv1.Unmarshal},
			{"v2", func(raw []byte, out any) error { return json.Unmarshal(raw, out) }},
		} {
			t.Run(test.name+"/"+decoder.name, func(t *testing.T) {
				var value Manifest
				err := decoder.decode([]byte(test.raw), &value)
				if !errors.Is(err, ErrInvalid) || strings.Contains(fmt.Sprint(err), canary) {
					t.Fatalf("unsafe or unclassified decode error: %v", err)
				}
				if test.unknown && !errors.Is(err, json.ErrUnknownName) {
					t.Fatal("unknown-member cause lost", err)
				}
				var syntax *jsontext.SyntacticError
				if test.syntax && !errors.As(err, &syntax) {
					t.Fatal("duplicate-member cause lost", err)
				}
				var semantic *json.SemanticError
				if !test.syntax && !errors.As(err, &semantic) {
					t.Fatal("native semantic cause lost", err)
				}
			})
		}
	}
}

type manifestErrorReader struct{ err error }

func (reader manifestErrorReader) Read([]byte) (int, error) { return 0, reader.err }

func TestManifestOwnedDecodeFailures(t *testing.T) {
	cause := errors.New("private_reader_failure")
	var value Manifest
	err := value.UnmarshalJSONFrom(jsontext.NewDecoder(manifestErrorReader{cause}))
	if !errors.Is(err, ErrInvalid) || !errors.Is(err, cause) || strings.Contains(fmt.Sprint(err), cause.Error()) {
		t.Fatal("owned reader failure lost classification, cause or privacy", err)
	}
	err = value.UnmarshalJSON([]byte("{\"private_manifest_marker\":"))
	var syntax *jsontext.SyntacticError
	if !errors.Is(err, ErrInvalid) || (!errors.As(err, &syntax) && !errors.Is(err, io.ErrUnexpectedEOF)) || strings.Contains(fmt.Sprint(err), "private_manifest_marker") {
		t.Fatal("direct owned malformed-input failure differs", err)
	}
	var absent *Manifest
	if err := absent.UnmarshalJSON([]byte("{}")); !errors.Is(err, ErrHandle) || errors.Is(err, ErrInvalid) {
		t.Fatal("nil handle classification changed", err)
	}
	if err := absent.UnmarshalJSONFrom(jsontext.NewDecoder(strings.NewReader("{}"))); !errors.Is(err, ErrHandle) || errors.Is(err, ErrInvalid) {
		t.Fatal("nil streaming handle classification changed", err)
	}
	if err := value.UnmarshalJSONFrom(nil); !errors.Is(err, ErrHandle) || errors.Is(err, ErrInvalid) {
		t.Fatal("nil decoder was not rejected as a handle error", err)
	}
}

func TestDiagnoseIdentity(t *testing.T) {
	for _, code := range []failure.Code{ErrInvalid, ErrUnsupported, ErrLimit, ErrCanceled, ErrHandle} {
		err := problem(code, "control", "/bindings/0")
		diagnostic, ok := Diagnose(err)
		want := code == ErrInvalid || code == ErrUnsupported || code == ErrLimit
		if ok != want || ok && diagnostic.Code != code {
			t.Fatalf("code=%s: %+v, completed=%v", code, diagnostic, ok)
		}
		if _, ok := Diagnose(fmt.Errorf("wrapped: %w", err)); ok {
			t.Fatal("wrapped cause became direct validation")
		}
		if _, ok := Diagnose(errors.Join(err, errors.New("other"))); ok {
			t.Fatal("joined cause became direct validation")
		}
	}
	future := definition(ErrHandle + 1)
	future.Identifier = "fathomry.workflow_implementation.future"
	for _, test := range []struct {
		name   string
		core   failure.Definition
		detail failure.Code
	}{
		{"different_code", definition(ErrInvalid), ErrUnsupported},
		{"canceled_core", definition(ErrCanceled), ErrUnsupported},
		{"foreign_core", declaration.Definitions()[0], ErrInvalid},
		{"future_code", future, future.Code},
		{"zero_detail", definition(ErrInvalid), 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			err, creation := failure.NewDetailed(test.core, failure.Location{}, Diagnostic{Code: test.detail, Reason: "caller-provided"}, func(value Diagnostic) Diagnostic { return value })
			if creation != nil {
				t.Fatal(creation)
			}
			if _, ok := Diagnose(err); ok {
				t.Fatal("inconsistent, foreign or unsupported identity admitted")
			}
		})
	}
	for _, err := range []error{nil, errors.New("native"), new(failure.Detailed[Diagnostic])} {
		if _, ok := Diagnose(err); ok {
			t.Fatal("non-validation error admitted")
		}
	}
}

func TestBindingDiagnosticAuthoredIndex(t *testing.T) {
	bundle := testBundle(t, health.Declarations())
	calls := 0
	original := healthBindings(&calls)
	var canonical Manifest
	for _, order := range [][3]int{{0, 1, 2}, {2, 0, 1}, {1, 2, 0}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			bindings := []Binding{original[order[0]], original[order[1]], original[order[2]]}
			registry, err := Prepare(context.Background(), bundle, healthIdentity(), bindings)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := registry.ManifestCopy()
			if err != nil {
				t.Fatal(err)
			}
			if canonical.Format == "" {
				canonical = manifest
			}
			if !reflect.DeepEqual(manifest, canonical) {
				t.Fatal("manifest order follows authoring order")
			}
			for index := range bindings {
				bad := append([]Binding(nil), bindings...)
				bad[index].Callable = Callable{}
				registry, err := Prepare(context.Background(), bundle, healthIdentity(), bad)
				diagnostic, ok := Diagnose(err)
				if registry != nil || !ok || diagnostic.Reason != "nil_callable" || diagnostic.Path != fmt.Sprintf("/bindings/%d", index) {
					t.Errorf("authored=%d: %+v, %v", index, diagnostic, err)
				}
			}
		})
	}
	if calls != 0 {
		t.Fatal("inspection invoked business code")
	}
}
