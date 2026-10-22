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

package declaration

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

func fixtureTree(t testing.TB) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("testdata/health.bundle.json")
	if err != nil {
		t.Fatal(err)
	}
	value, err := readJSON(context.Background(), raw, MaxBundleBytes, MaxBundleDepth)
	if err != nil {
		t.Fatal(err)
	}
	return value.(map[string]any)
}

func fixtureWorkflow(tree map[string]any) map[string]any {
	return tree["workflows"].([]any)[0].(map[string]any)
}
func fixtureSpec(tree map[string]any) map[string]any {
	return fixtureWorkflow(tree)["spec"].(map[string]any)
}

func TestClosedShape(t *testing.T) {
	if err := (validator{ctx: context.Background()}).shape(fixtureTree(t)); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"unknown", func(tree map[string]any) { tree["private_payload"] = "canary" }},
		{"combined_field", func(tree map[string]any) { tree["format workflows"] = "canary" }},
		{"case_alias", func(tree map[string]any) { tree["Format"] = tree["format"]; delete(tree, "format") }},
		{"wrong_kind_profile", func(tree map[string]any) { fixtureWorkflow(tree)["fingerprint_profile"] = ModeProfile }},
		{"namespace_control", func(tree map[string]any) { fixtureWorkflow(tree)["namespace"] = "demo\n" }},
		{"version_newline", func(tree map[string]any) { fixtureWorkflow(tree)["version"] = "1\n" }},
		{"id_newline", func(tree map[string]any) {
			nodes := fixtureSpec(tree)["nodes"].(map[string]any)
			nodes[strings.Repeat("a", 64)+"\n"] = nodes["sample"]
			delete(nodes, "sample")
		}},
		{"missing_required_boolean", func(tree map[string]any) {
			delete(fixtureSpec(tree)["inputs"].(map[string]any)["target"].(map[string]any), "required")
		}},
		{"null_boolean", func(tree map[string]any) {
			fixtureSpec(tree)["inputs"].(map[string]any)["target"].(map[string]any)["required"] = nil
		}},
		{"null_collection", func(tree map[string]any) { fixtureSpec(tree)["relations"] = nil }},
		{"duplicate_phase_member", func(tree map[string]any) {
			fixtureSpec(tree)["nodes"].(map[string]any)["sample"].(map[string]any)["phases"] = []any{[]any{"cpu", "cpu"}}
		}},
		{"inactive_source_field", func(tree map[string]any) {
			node := fixtureSpec(tree)["nodes"].(map[string]any)["sample"].(map[string]any)
			node["inputs"].(map[string]any)["target"].(map[string]any)["source"].(map[string]any)["edge"] = "sampled"
		}},
		{"empty_enum", func(tree map[string]any) {
			fixtureSpec(tree)["contracts"].(map[string]any)["target"].(map[string]any)["enum"] = []any{}
		}},
		{"schema_range", func(tree map[string]any) {
			fixtureSpec(tree)["contracts"].(map[string]any)["target"].(map[string]any)["minLength"] = int64(256)
		}},
		{"unknown_schema_keyword", func(tree map[string]any) {
			fixtureSpec(tree)["contracts"].(map[string]any)["target"].(map[string]any)["pattern"] = ".*"
		}},
		{"decimal_whitespace", func(tree map[string]any) {
			fixtureSpec(tree)["controls"].(map[string]any)["attempt_timeout"].(map[string]any)["minimum"] = " 1"
		}},
		{"decimal_overflow", func(tree map[string]any) {
			fixtureSpec(tree)["controls"].(map[string]any)["attempt_timeout"].(map[string]any)["maximum"] = "9223372036854775808"
		}},
		{"nullable_nested", func(tree map[string]any) {
			fixtureSpec(tree)["contracts"].(map[string]any)["target"] = map[string]any{
				"anyOf": []any{map[string]any{"anyOf": []any{}}, map[string]any{"type": "null"}},
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree := fixtureTree(t)
			test.mutate(tree)
			err := (validator{ctx: context.Background()}).shape(tree)
			if err == nil {
				t.Fatal("invalid shape admitted")
			}
			if _, ok := Diagnose(err); !ok {
				t.Fatal("validation lost diagnostic", err)
			}
			if strings.Contains(err.Error(), "canary") {
				t.Fatal("private value leaked")
			}
		})
	}
}

func TestObjectAuthoringPresence(t *testing.T) {
	for _, source := range []bool{false, true} {
		tree := fixtureTree(t)
		fixtureSpec(tree)["contracts"].(map[string]any)["metric"] = map[string]any{"type": "object"}
		err := (validator{ctx: context.Background(), authoring: source}).shape(tree)
		if source && err != nil || !source && err == nil {
			t.Fatal(source, err)
		}
	}
	tree := fixtureTree(t)
	fixtureSpec(tree)["contracts"].(map[string]any)["metric"] = map[string]any{"type": "object", "properties": nil}
	if err := (validator{ctx: context.Background(), authoring: true}).shape(tree); err == nil {
		t.Fatal("explicit null repaired")
	}
}

func TestShapeBounds(t *testing.T) {
	tree := fixtureTree(t)
	tree["workflows"] = make([]any, MaxWorkflows+1)
	if err := (validator{ctx: context.Background()}).shape(tree); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	schema := map[string]any{"type": "boolean"}
	for range 18 {
		schema = map[string]any{"type": "array", "items": schema, "minItems": int64(0), "maxItems": int64(1)}
	}
	if err := (validator{ctx: context.Background()}).schema(schema, "/schema", 0); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}

func TestAuthoringPointerPresence(t *testing.T) {
	for _, test := range []struct {
		name, reason, path string
		value              any
		change             func(*WorkflowDeclaration)
	}{
		{"empty_fixed_and_control", "setting", "nodes/sample/activities/cpu/policy/start_to_close_ms",
			map[string]any{"value": "", "control": "attempt_timeout"}, func(workflow *WorkflowDeclaration) {
				call := workflow.Spec.Nodes["sample"].Activities["cpu"]
				call.Policy.StartToCloseMS = Fixed("")
				call.Policy.StartToCloseMS.Control = "attempt_timeout"
				workflow.Spec.Nodes["sample"].Activities["cpu"] = call
			}},
		{"empty_default", "decimal", "controls/attempt_timeout/default", "", func(workflow *WorkflowDeclaration) {
			control := workflow.Spec.Controls["attempt_timeout"]
			control.Default = new(Decimal)
			workflow.Spec.Controls["attempt_timeout"] = control
		}},
		{"empty_heartbeat", "setting", "nodes/sample/activities/cpu/policy/heartbeat_ms", map[string]any{}, func(workflow *WorkflowDeclaration) {
			call := workflow.Spec.Nodes["sample"].Activities["cpu"]
			call.Policy.HeartbeatMS = &Setting{}
			workflow.Spec.Nodes["sample"].Activities["cpu"] = call
		}},
		{"empty_inactive_schema", "unknown_field", "contracts/metric/items", map[string]any{}, func(workflow *WorkflowDeclaration) {
			schema := workflow.Spec.Contracts["metric"]
			schema.Items = &DataSchema{}
			workflow.Spec.Contracts["metric"] = schema
		}},
		{"inactive_false", "unknown_field", "contracts/metric/additionalProperties", false, func(workflow *WorkflowDeclaration) {
			schema := workflow.Spec.Contracts["metric"]
			schema.AdditionalProperties = new(bool)
			workflow.Spec.Contracts["metric"] = schema
		}},
		{"inactive_zero", "unknown_field", "contracts/metric/minItems", int64(0), func(workflow *WorkflowDeclaration) {
			schema := workflow.Spec.Contracts["metric"]
			schema.MinItems = new(int64)
			workflow.Spec.Contracts["metric"] = schema
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := fixtureDeclaration(t)
			test.change(&input.Workflows[0])
			compiled, compileError := Compile(context.Background(), input)

			tree := fixtureTree(t)
			record := fixtureSpec(tree)
			parts := strings.Split(test.path, "/")
			for _, part := range parts[:len(parts)-1] {
				record = record[part].(map[string]any)
			}
			record[parts[len(parts)-1]] = test.value
			raw, err := marshal(context.Background(), tree, MaxBundleBytes)
			if err != nil {
				t.Fatal(err)
			}
			parsed, parseError := Parse(context.Background(), raw)
			fileDetail, fileOK := Diagnose(parseError)
			if parsed != nil || !fileOK || fileDetail.Code != ErrInvalid || fileDetail.Reason != test.reason {
				t.Fatalf("invalid wire control: %+v / %v", fileDetail, parseError)
			}
			sourceDetail, sourceOK := Diagnose(compileError)
			if compiled != nil || !sourceOK || sourceDetail != fileDetail {
				t.Fatalf("explicit authoring field lost: compile=%+v / %v; parse=%+v", sourceDetail, compileError, fileDetail)
			}
		})
	}
}

func TestValidOptionalPointers(t *testing.T) {
	for _, supplied := range []bool{false, true} {
		input := fixtureDeclaration(t)
		if supplied {
			call := input.Workflows[0].Spec.Nodes["sample"].Activities["cpu"]
			heartbeat := Fixed("100")
			call.Policy.HeartbeatMS = &heartbeat
			input.Workflows[0].Spec.Nodes["sample"].Activities["cpu"] = call
			input.Workflows[0].Spec.Contracts["metric"] = DataSchema{
				Type: "array", Items: &DataSchema{Type: "boolean"}, MinItems: new(int64), MaxItems: new(int64),
			}
		} else {
			control := input.Workflows[0].Spec.Controls["attempt_timeout"]
			control.Default = nil
			input.Workflows[0].Spec.Controls["attempt_timeout"] = control
		}
		bundle := mustCompile(t, input)
		raw, _ := bundle.BytesCopy()
		parsed, err := Parse(context.Background(), raw)
		if err != nil {
			t.Fatal(err)
		}
		roundTrip, _ := parsed.BytesCopy()
		if !bytes.Equal(raw, roundTrip) {
			t.Fatal("valid optional pointers did not round trip")
		}
		value, err := readJSON(context.Background(), raw, MaxBundleBytes, MaxBundleDepth)
		if err != nil {
			t.Fatal(err)
		}
		spec := fixtureSpec(value.(map[string]any))
		control := spec["controls"].(map[string]any)["attempt_timeout"].(map[string]any)
		policy := spec["nodes"].(map[string]any)["sample"].(map[string]any)["activities"].(map[string]any)["cpu"].(map[string]any)["policy"].(map[string]any)
		_, hasDefault := control["default"]
		_, hasHeartbeat := policy["heartbeat_ms"]
		if hasDefault != supplied || hasHeartbeat != supplied {
			t.Fatal("optional presence changed", supplied, hasDefault, hasHeartbeat)
		}
		if supplied && policy["heartbeat_ms"].(map[string]any)["value"] != "100" {
			t.Fatal("explicit heartbeat value changed")
		}
	}
}
