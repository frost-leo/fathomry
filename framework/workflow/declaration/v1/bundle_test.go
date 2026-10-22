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
	jsonv1 "encoding/json"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func fixtureDeclaration(t testing.TB) Declaration {
	t.Helper()
	raw, err := os.ReadFile("testdata/health.bundle.json")
	if err != nil {
		t.Fatal(err)
	}
	var document wireBundle
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	var result Declaration
	for _, entry := range document.Workflows {
		workflow := WorkflowDeclaration{Namespace: entry.Namespace, Code: entry.Code, Version: entry.Version, Spec: entry.Spec.Specification}
		for _, mode := range entry.Modes {
			workflow.Modes = append(workflow.Modes, ModeDeclaration{Code: mode.Code, Revision: mode.Revision, Values: mode.Config.Values})
		}
		result.Workflows = append(result.Workflows, workflow)
	}
	return result
}

func mustCompile(t testing.TB, input Declaration) *Bundle {
	t.Helper()
	bundle, err := Compile(context.Background(), input)
	if err != nil {
		detail, _ := Diagnose(err)
		t.Fatalf("compile: %v %+v", err, detail)
	}
	return bundle
}

func TestCompileParse(t *testing.T) {
	input := fixtureDeclaration(t)
	bundle := mustCompile(t, input)
	actual, err := bundle.BytesCopy()
	if err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile("testdata/health.bundle.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, bytes.TrimSpace(golden)) {
		t.Fatal("canonical export differs from independent reference fixture")
	}
	parsed, err := Parse(context.Background(), golden)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := parsed.BytesCopy()
	if !bytes.Equal(actual, again) {
		t.Fatal("parse/compile diverge")
	}
	input.Workflows[0].Modes[0].Values["attempt_timeout"] = "200"
	input.Workflows[0].Spec.Nodes["sample"].Phases[0][0] = "mutated"
	*input.Workflows[0].Spec.Controls["attempt_timeout"].Default = "300"
	actual[0] = '!'
	golden[0] = '!'
	retained, _ := bundle.BytesCopy()
	parsedBytes, _ := parsed.BytesCopy()
	if !bytes.Equal(retained, again) || !bytes.Equal(parsedBytes, again) {
		t.Fatal("caller mutation escaped")
	}
	if fmt.Sprintf("%+v", bundle) != "workflow.Bundle" {
		t.Fatal("bundle contents leaked")
	}
	var zero Bundle
	if _, err := zero.BytesCopy(); !errors.Is(err, ErrHandle) {
		t.Fatal(err)
	}
	if err := json.Unmarshal(again, bundle); !errors.Is(err, ErrHandle) {
		t.Fatal("immutable bundle overwritten", err)
	}
	var wait sync.WaitGroup
	for range 8 {
		wait.Go(func() {
			for range 8 {
				if copied, err := bundle.BytesCopy(); err != nil || !bytes.Equal(copied, again) {
					t.Error("concurrent snapshot changed", err)
				}
			}
		})
	}
	wait.Wait()
}

func TestCopiedBundlePresentation(t *testing.T) {
	bundle := mustCompile(t, fixtureDeclaration(t))
	expected, _ := bundle.BytesCopy()
	for _, value := range []any{bundle, *bundle} {
		for _, format := range []string{"%v", "%+v", "%#v"} {
			if fmt.Sprintf(format, value) != "workflow.Bundle" {
				t.Errorf("%T exposes contents through %s", value, format)
			}
		}
		if logged := slog.AnyValue(value).Resolve(); logged.Kind() != slog.KindString || logged.String() != "workflow.Bundle" {
			t.Errorf("%T lacks a redacted slog value", value)
		}
		for _, encode := range []func(any) ([]byte, error){jsonv1.Marshal, func(value any) ([]byte, error) { return json.Marshal(value) }} {
			raw, err := encode(value)
			if err != nil || !bytes.Equal(raw, expected) {
				t.Errorf("%T does not serialize the snapshot: %v", value, err)
			}
		}
	}
	for _, encode := range []func(any) ([]byte, error){jsonv1.Marshal, func(value any) ([]byte, error) { return json.Marshal(value) }} {
		for _, zero := range []any{Bundle{}, &Bundle{}} {
			if _, err := encode(zero); !errors.Is(err, ErrHandle) {
				t.Errorf("%T zero value serialized: %v", zero, err)
			}
		}
		if raw, err := encode((*Bundle)(nil)); err != nil || string(raw) != "null" {
			t.Errorf("unexpected native nil-pointer encoding: %v", err)
		}
	}
	if _, err := (*Bundle)(nil).BytesCopy(); !errors.Is(err, ErrHandle) {
		t.Fatal("explicit export admitted a nil handle", err)
	}
}

func TestSemanticRefusals(t *testing.T) {
	tests := []struct {
		name, reason string
		change       func(*WorkflowDeclaration)
	}{
		{"cycle", "cycle", func(w *WorkflowDeclaration) {
			w.Spec.Edges["reverse"] = Edge{From: "summary", To: "sample", On: []string{"continue"}}
		}},
		{"missing_endpoint", "edge_ref", func(w *WorkflowDeclaration) {
			edge := w.Spec.Edges["sampled"]
			edge.From = "missing"
			w.Spec.Edges["sampled"] = edge
		}},
		{"binding", "binding_ref", func(w *WorkflowDeclaration) {
			node := w.Spec.Nodes["sample"]
			call := node.Activities["cpu"]
			call.Binding = "missing"
			node.Activities["cpu"] = call
		}},
		{"control", "control_ref", func(w *WorkflowDeclaration) {
			node := w.Spec.Nodes["sample"]
			call := node.Activities["cpu"]
			call.Policy.StartToCloseMS = Tuned("missing")
			node.Activities["cpu"] = call
		}},
		{"unused_contract", "unused_contract", func(w *WorkflowDeclaration) { w.Spec.Contracts["unused"] = DataSchema{Type: "boolean"} }},
		{"unused_binding", "unused_binding", func(w *WorkflowDeclaration) {
			binding := w.Spec.Bindings["read_cpu"]
			binding.Type = "health/unused"
			w.Spec.Bindings["unused"] = binding
		}},
		{"phase", "phase_membership", func(w *WorkflowDeclaration) {
			node := w.Spec.Nodes["sample"]
			node.Phases = [][]string{{"cpu"}, {"cpu", "memory"}}
			w.Spec.Nodes["sample"] = node
		}},
		{"port", "port_contract", func(w *WorkflowDeclaration) {
			node := w.Spec.Nodes["summary"]
			input := node.Inputs["cpu"]
			input.Contract = "target"
			node.Inputs["cpu"] = input
		}},
		{"presence", "port_presence", func(w *WorkflowDeclaration) {
			port := w.Spec.Inputs["target"]
			port.Required = false
			w.Spec.Inputs["target"] = port
		}},
		{"read_only_effect", "retry_safety", func(w *WorkflowDeclaration) {
			binding := w.Spec.Bindings["read_cpu"]
			binding.Effects = map[string][]string{"stored": {"accepted"}}
			w.Spec.Bindings["read_cpu"] = binding
		}},
		{"gate", "gate_dependency", func(w *WorkflowDeclaration) {
			node := w.Spec.Nodes["sample"]
			node.Gate = &Gate{Scope: "run", Nodes: []string{"summary"}, Condition: "all_settled"}
			w.Spec.Nodes["sample"] = node
		}},
		{"route", "route_contract", func(w *WorkflowDeclaration) {
			node := w.Spec.Nodes["sample"]
			node.Route.Value = "other"
			w.Spec.Nodes["sample"] = node
		}},
		{"dynamic_route", "route_contract", func(w *WorkflowDeclaration) {
			node := w.Spec.Nodes["sample"]
			node.Route = Route{Kind: "activity", Activity: "cpu", Port: "value"}
			w.Spec.Nodes["sample"] = node
		}},
		{"control_kind", "control_kind", func(w *WorkflowDeclaration) {
			control := w.Spec.Controls["attempt_timeout"]
			control.Kind = "count"
			w.Spec.Controls["attempt_timeout"] = control
		}},
		{"control_default", "control_default", func(w *WorkflowDeclaration) { *w.Spec.Controls["attempt_timeout"].Default = "9999" }},
		{"no_default_fixed_budget", "budget", func(w *WorkflowDeclaration) {
			control := w.Spec.Controls["attempt_timeout"]
			control.Default = nil
			w.Spec.Controls["attempt_timeout"] = control
			w.Modes = nil
			w.Spec.Limits.RunTimeoutMS = Fixed("10")
			w.Spec.Limits.CleanupReserveMS = Fixed("20")
		}},
		{"duration_overflow", "slot_range", func(w *WorkflowDeclaration) {
			w.Spec.Limits.RunTimeoutMS = Fixed(Decimal(strconv.FormatInt(maxDurationMS+1, 10)))
		}},
		{"attempt_overflow", "slot_range", func(w *WorkflowDeclaration) {
			node := w.Spec.Nodes["sample"]
			call := node.Activities["cpu"]
			call.Policy.MaximumAttempts = Fixed("2147483648")
			node.Activities["cpu"] = call
		}},
		{"mode", "control_value", func(w *WorkflowDeclaration) { w.Modes[0].Values["attempt_timeout"] = "1501" }},
		{"completion", "completion", func(w *WorkflowDeclaration) { w.Spec.Completion.MaximumFailedItems = "1" }},
		{"item_scope", "override_scope", func(w *WorkflowDeclaration) {
			control := w.Spec.Controls["attempt_timeout"]
			control.Override = []string{"item"}
			w.Spec.Controls["attempt_timeout"] = control
			w.Spec.Limits.RunTimeoutMS = Tuned("attempt_timeout")
		}},
		{"non_retryable_control_range", "retry_safety", func(w *WorkflowDeclaration) {
			binding := w.Spec.Bindings["read_cpu"]
			binding.RetrySafety = "not_retryable"
			w.Spec.Bindings["read_cpu"] = binding
			one := Decimal("1")
			w.Spec.Controls["attempts"] = Control{Kind: "count", Minimum: "1", Maximum: "2", Default: &one}
			call := w.Spec.Nodes["sample"].Activities["cpu"]
			call.Policy.MaximumAttempts = Tuned("attempts")
			w.Spec.Nodes["sample"].Activities["cpu"] = call
		}},
		{"same_phase_dependency", "phase_dependency", func(w *WorkflowDeclaration) {
			call := w.Spec.Nodes["sample"].Activities["memory"]
			call.Inputs["target"] = Source{Kind: "activity", Activity: "cpu", Port: "value"}
		}},
		{"binding_identity", "binding_identity", func(w *WorkflowDeclaration) {
			w.Spec.Bindings["duplicate"] = w.Spec.Bindings["read_cpu"]
		}},
		{"root_export", "export_ref", func(w *WorkflowDeclaration) {
			w.Spec.Outputs["summary"] = Export{Node: "missing", Port: "text", Required: true}
		}},
		{"unknown_mode_control", "unknown_control", func(w *WorkflowDeclaration) {
			w.Modes[0].Values["missing"] = "1"
		}},
		{"missing_mode_default", "missing_control", func(w *WorkflowDeclaration) {
			control := w.Spec.Controls["attempt_timeout"]
			control.Default = nil
			w.Spec.Controls["attempt_timeout"] = control
			w.Modes[0].Values = nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := fixtureDeclaration(t)
			test.change(&input.Workflows[0])
			bundle, err := Compile(context.Background(), input)
			if err == nil || bundle != nil {
				t.Fatal("invalid declaration compiled")
			}
			detail, ok := Diagnose(err)
			if !ok || detail.Reason != test.reason {
				t.Fatalf("wanted %s, got %+v / %v", test.reason, detail, err)
			}
		})
	}
}

func TestModesAndCanonicalSeparation(t *testing.T) {
	input := fixtureDeclaration(t)
	base := mustCompile(t, input)
	input.Workflows[0].Modes[0].Values["attempt_timeout"] = "1200"
	changed := mustCompile(t, input)
	if base.entries[0].SHA256 != changed.entries[0].SHA256 || base.entries[1].SHA256 == changed.entries[1].SHA256 {
		t.Fatal("mode and spec fingerprints mixed")
	}
	input = fixtureDeclaration(t)
	input.Workflows[0].Modes[1].Values = nil
	if _, err := Compile(context.Background(), input); err != nil {
		t.Fatal("Go default not materialized", err)
	}
	tree := fixtureTree(t)
	mode := fixtureWorkflow(tree)["modes"].([]any)[0].(map[string]any)
	mode["config"].(map[string]any)["values"] = map[string]any{}
	raw, _ := marshal(context.Background(), tree, MaxBundleBytes)
	if _, err := Parse(context.Background(), raw); err == nil {
		t.Fatal("missing exported values repaired")
	} else if detail, _ := Diagnose(err); detail.Reason != "missing_control" {
		t.Fatal(detail, err)
	}
	input = fixtureDeclaration(t)
	node := input.Workflows[0].Spec.Nodes["sample"]
	node.Phases = [][]string{{"memory", "cpu"}}
	input.Workflows[0].Spec.Nodes["sample"] = node
	if altered := mustCompile(t, input); altered.entries[0].SHA256 != base.entries[0].SHA256 {
		t.Fatal("parallel set order changed identity")
	}
	node.Phases = [][]string{{"cpu"}, {"memory"}}
	input.Workflows[0].Spec.Nodes["sample"] = node
	if altered := mustCompile(t, input); altered.entries[0].SHA256 == base.entries[0].SHA256 {
		t.Fatal("phase sequence disappeared")
	}
}

func TestIdentityAndIntegrity(t *testing.T) {
	input := fixtureDeclaration(t)
	input.Workflows = append(input.Workflows, input.Workflows[0])
	if _, err := Compile(context.Background(), input); err == nil {
		t.Fatal("duplicate workflow admitted")
	}
	input.Workflows[1].Version = "2"
	changedBinding := input.Workflows[1].Spec.Bindings["read_cpu"]
	changedBinding.Revision = "2"
	// Detached authoring is unnecessary here: both identities may intentionally
	// share declarations, and compile must retain distinct portable parents.
	input.Workflows[1].Spec.Bindings["read_cpu"] = changedBinding
	bundle := mustCompile(t, input)
	if len(bundle.entries) != 6 {
		t.Fatal("equal/compatible content collapsed parent identities")
	}
	tree := fixtureTree(t)
	fixtureWorkflow(tree)["spec_sha256"] = strings.Repeat("0", 64)
	raw, _ := marshal(context.Background(), tree, MaxBundleBytes)
	if _, err := Parse(context.Background(), raw); !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Compile(ctx, fixtureDeclaration(t)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := Parse(ctx, raw); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := Parse(nil, raw); !errors.Is(err, ErrHandle) {
		t.Fatal(err)
	}
}

func TestPartialDefaultsAndOverflow(t *testing.T) {
	input := fixtureDeclaration(t)
	workflow := &input.Workflows[0]
	control := workflow.Spec.Controls["attempt_timeout"]
	control.Default = nil
	workflow.Spec.Controls["attempt_timeout"] = control
	workflow.Modes = nil
	mustCompile(t, input)
	maximum := Decimal("9223372036854775807")
	for _, name := range []string{"active", "chunk", "parallel"} {
		workflow.Spec.Controls[name] = Control{Kind: "count", Minimum: "1", Maximum: maximum, Default: &maximum}
	}
	workflow.Spec.Limits.ActiveChunks = Tuned("active")
	workflow.Spec.Limits.ItemsPerChunk = Tuned("chunk")
	workflow.Spec.Limits.ChunkItemConcurrency = Tuned("parallel")
	workflow.Spec.Relations["budget"] = Relation{Kind: "sum_le", Terms: []string{"chunk", "parallel"}, Limit: "active"}
	if _, err := Compile(context.Background(), input); err == nil {
		t.Fatal("overflowing sum admitted")
	} else if detail, _ := Diagnose(err); detail.Reason != "relation" {
		t.Fatal(detail, err)
	}
}

func TestSchemaObjectAndNullable(t *testing.T) {
	input := fixtureDeclaration(t)
	input.Workflows[0].Spec.Contracts["metric"] = DataSchema{Type: "object"}
	bundle := mustCompile(t, input)
	raw, _ := bundle.BytesCopy()
	if _, err := Parse(context.Background(), raw); err != nil {
		t.Fatal("normalized empty object lost presence", err)
	}
	input.Workflows[0].Spec.Contracts["metric"] = DataSchema{AnyOf: []DataSchema{{Type: "boolean"}, {Type: "null"}}}
	bundle = mustCompile(t, input)
	raw, _ = bundle.BytesCopy()
	if _, err := Parse(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
}

func TestModeConstraintDiagnosticPaths(t *testing.T) {
	for _, test := range []struct {
		name, control, reason string
		value                 Decimal
		prepare               func(*Specification)
	}{
		{"policy", "attempt_timeout", "budget", "2500", func(spec *Specification) {
			control := spec.Controls["attempt_timeout"]
			control.Maximum = "3000"
			spec.Controls["attempt_timeout"] = control
		}},
		{"limits", "run_budget", "budget", "500", func(spec *Specification) {
			value := Decimal("10000")
			spec.Controls["run_budget"] = Control{Kind: "duration_ms", Minimum: "100", Maximum: value, Default: &value}
			spec.Limits.RunTimeoutMS = Tuned("run_budget")
		}},
		{"relation", "active", "relation", "1", func(spec *Specification) {
			for name, value := range map[string]Decimal{"active": "3", "chunk": "1", "parallel": "1"} {
				spec.Controls[name] = Control{Kind: "count", Minimum: "1", Maximum: "3", Default: &value}
			}
			spec.Limits.ActiveChunks = Tuned("active")
			spec.Limits.ItemsPerChunk = Tuned("chunk")
			spec.Limits.ChunkItemConcurrency = Tuned("parallel")
			spec.Relations["budget"] = Relation{Kind: "sum_le", Terms: []string{"chunk", "parallel"}, Limit: "active"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := fixtureDeclaration(t)
			test.prepare(&input.Workflows[0].Spec)
			valid := mustCompile(t, input)
			input.Workflows[0].Modes[0].Values[test.control] = test.value
			_, compileError := Compile(context.Background(), input)

			raw, _ := valid.BytesCopy()
			var document wireBundle
			if err := json.Unmarshal(raw, &document); err != nil {
				t.Fatal(err)
			}
			mode := &document.Workflows[0].Modes[0]
			mode.Config.Values[test.control] = test.value
			config, err := canonical(context.Background(), mode.Config, MaxModeBytes)
			if err != nil {
				t.Fatal(err)
			}
			mode.Digest = fingerprint(ModeProfile, config)
			raw, err = marshal(context.Background(), document, MaxBundleBytes)
			if err != nil {
				t.Fatal(err)
			}
			_, parseError := Parse(context.Background(), raw)
			for _, err := range []error{compileError, parseError} {
				detail, ok := Diagnose(err)
				if !ok || detail.Reason != test.reason || detail.Path != "/workflows/0/modes/0/config/values" {
					t.Errorf("mode failure points outside its values: %+v / %v", detail, err)
				}
			}
		})
	}
}
