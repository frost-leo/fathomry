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

package build

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"errors"
	"strings"
	"testing"

	workflows "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow"
	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
	implementation "github.com/frost-leo/fathomry/framework/workflow/implementation/v1"
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

func boundDefinition() Definition {
	metric := implementation.Activity(func(context.Context, metricInput) (metricOutput, error) { panic("handler called") })
	summary := implementation.Activity(func(context.Context, summaryInput) (summaryOutput, error) { panic("handler called") })
	return Definition{Name: "health", Workflows: []workflows.Workflow{{Declaration: health.Declarations(), Implementations: []implementation.VersionBindings{{Version: "1", Bindings: []implementation.Binding{
		{Key: "read_cpu", Callable: metric}, {Key: "read_memory", Callable: metric}, {Key: "summarize", Callable: summary},
	}}}}}}
}
func TestImplementationInspection(t *testing.T) {
	ctx := context.Background()
	definition := boundDefinition()
	var output bytes.Buffer
	if err := ExportImplementations(ctx, definition, &output); err != nil {
		t.Fatal(err)
	}
	document, report, records, err := inspectImplementations(ctx, output.Bytes())
	if err != nil || document.Schema != implementationInspectionSchema || !report.Passed || len(records) != 1 || records[0].Status != "complete" {
		t.Fatal(document, report, records, err)
	}
	output.Reset()
	if err := Export(ctx, definition, &output); err != nil {
		t.Fatal(err)
	}
	if _, _, err := inspect(ctx, output.Bytes()); err != nil {
		t.Fatal(err)
	}
	definition.Workflows[0].Implementations[0].Bindings = nil
	output.Reset()
	if err := Export(ctx, definition, &output); err == nil || output.Len() != 0 {
		t.Fatal("legacy export bypassed invalid claim")
	}
	output.Reset()
	if err := ExportImplementations(ctx, definition, &output); err == nil || output.Len() != 0 {
		t.Fatal("v2 export bypassed invalid claim")
	}
	for _, definition := range []Definition{{Name: "empty"}, {Name: "data", Declarations: []declaration.Declaration{health.Declarations()}}} {
		output.Reset()
		if err := ExportImplementations(ctx, definition, &output); err != nil {
			t.Fatal(err)
		}
		_, _, records, err := inspectImplementations(ctx, output.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			if record.Status != "not_supplied" || record.Manifest != nil {
				t.Fatal("data certified as code")
			}
		}
	}
}
func TestImplementationInspectionRejects(t *testing.T) {
	ctx := context.Background()
	var output bytes.Buffer
	if err := ExportImplementations(ctx, boundDefinition(), &output); err != nil {
		t.Fatal(err)
	}
	raw := bytes.Clone(output.Bytes())
	for _, test := range []struct {
		name   string
		change func(*inspectionV2)
	}{
		{"missing", func(v *inspectionV2) { v.Implementations = nil }},
		{"empty", func(v *inspectionV2) { v.Implementations = implementationList{} }},
		{"duplicate", func(v *inspectionV2) { v.Implementations = append(v.Implementations, v.Implementations[0]) }},
		{"status", func(v *inspectionV2) { v.Implementations[0].Status = "ready" }},
		{"count", func(v *inspectionV2) { v.Implementations[0].CallSites++ }},
		{"manifest", func(v *inspectionV2) { v.Implementations[0].Manifest = nil }},
		{"scope", func(v *inspectionV2) { v.Implementations[0].Selected = false }},
		{"profile", func(v *inspectionV2) { v.Implementations[0].Manifest.ABIProfile = "unknown" }},
		{"fingerprint", func(v *inspectionV2) { v.Implementations[0].Manifest.Specification.SHA256 = "bad" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			var value inspectionV2
			if err := json.Unmarshal(raw, &value); err != nil {
				t.Fatal(err)
			}
			test.change(&value)
			bad, _ := json.Marshal(value)
			if _, _, _, err := inspectImplementations(ctx, bad); err == nil {
				t.Fatal("invalid record accepted")
			}
		})
	}
	for _, bad := range [][]byte{
		bytes.Replace(raw, []byte(`"abi_profile":`), []byte(`"extra":true,"abi_profile":`), 1),
		bytes.Replace(raw, []byte(`"status":"complete"`), []byte(`"status":"complete","status":"complete"`), 1),
	} {
		if _, _, _, err := inspectImplementations(ctx, bad); err == nil {
			t.Fatal("unknown/duplicate member accepted")
		}
	}
}

func TestImplementationInspectionBounds(t *testing.T) {
	ctx := context.Background()
	prefix := `{"schema":"fathomry.project-inspection/v2","name":"empty","bundles":[],"implementations":`
	atBound := prefix + "[" + strings.Repeat(" ", implementation.MaxManifestBytes-2) + "]}"
	if _, _, _, err := inspectImplementations(ctx, []byte(atBound)); err != nil {
		t.Fatal("exact implementation array bound refused", err)
	}
	overBound := prefix + "[" + strings.Repeat(" ", implementation.MaxManifestBytes-1) + "]}"
	if _, _, _, err := inspectImplementations(ctx, []byte(overBound)); !errors.Is(err, ErrLimit) {
		t.Fatal("raw serialized array bound lost", err)
	}
	tooMany := prefix + "[" + strings.Repeat("{},", declaration.MaxInputs*declaration.MaxWorkflows) + "{}]}"
	if _, _, _, err := inspectImplementations(ctx, []byte(tooMany)); !errors.Is(err, ErrLimit) {
		t.Fatal("version count bound lost", err)
	}
	if maxImplementationInspectionBytes != 64<<20+16<<20+4096 {
		t.Fatal("whole response allowance drifted")
	}
}
