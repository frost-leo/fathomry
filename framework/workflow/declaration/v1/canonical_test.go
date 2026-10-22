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
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestReferenceCompatibility(t *testing.T) {
	for _, name := range []string{"orders", "branching", "stage-gate"} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("testdata", name+".bundle.json"))
			if err != nil {
				t.Fatal(err)
			}
			bundle, err := Parse(context.Background(), raw)
			if err != nil {
				detail, _ := Diagnose(err)
				t.Fatal(err, detail)
			}
			actual, _ := bundle.BytesCopy()
			if !bytes.Equal(actual, bytes.TrimSpace(raw)) {
				t.Fatal("candidate canonical contract changed")
			}
			report, err := Check(context.Background(), bundle)
			if err != nil || !report.Passed {
				t.Fatal(report, err)
			}
		})
	}
}

func TestIndependentCanonicalEncoding(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("independent JavaScript codec requires Node.js")
	}
	inputs := [][]byte{
		[]byte("{\"\\ue000\":2,\"\\ud800\\udc00\":1,\"2\":false,\"10\":null}"),
		[]byte("[9007199254740991,-9007199254740991,0,\"9223372036854775807\"]"),
		[]byte("{\"text\":\"<>&\\u2028\\u2029\\n\\t\\u0000\"}"),
	}
	for _, name := range []string{"health", "orders", "branching", "stage-gate"} {
		raw, err := os.ReadFile(filepath.Join("testdata", name+".bundle.json"))
		if err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, raw)
	}
	for index, input := range inputs {
		value, err := readJSON(context.Background(), input, MaxBundleBytes, MaxBundleDepth)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := canonical(context.Background(), value, MaxBundleBytes)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		process := exec.CommandContext(ctx, node, "testdata/canonical.mjs")
		process.Stdin = bytes.NewReader(input)
		result, err := process.Output()
		cancel()
		if err != nil || !bytes.Equal(encoded, result) {
			t.Fatalf("vector %d disagrees: %v", index, err)
		}
	}
}

func TestAdmittedGateEffectsAndCompleteBatch(t *testing.T) {
	input := fixtureDeclaration(t)
	spec := &input.Workflows[0].Spec
	binding := spec.Bindings["read_cpu"]
	binding.RetrySafety = "idempotent"
	binding.Effects = map[string][]string{"stored": {"committed", "accepted"}}
	spec.Bindings["read_cpu"] = binding
	sample := spec.Nodes["sample"]
	sample.RequiredEffects = map[string]EffectCheck{"cpu_stored": {Activity: "cpu", Effect: "stored", Guarantee: "committed"}}
	spec.Nodes["sample"] = sample
	summary := spec.Nodes["summary"]
	summary.Gate = &Gate{Scope: "chunk", Nodes: []string{"sample"}, Condition: "all_succeeded"}
	spec.Nodes["summary"] = summary
	mustCompile(t, input)
	binding.Outputs["value"] = Port{Contract: "metric", Delivery: "complete_batch", Required: true}
	spec.Bindings["read_cpu"] = binding
	target := summary.Inputs["cpu"]
	target.Delivery = "complete_batch"
	summary.Inputs["cpu"] = target
	summarize := spec.Bindings["summarize"]
	summarize.Inputs["cpu"] = Port{Contract: "metric", Delivery: "complete_batch", Required: true}
	spec.Bindings["summarize"] = summarize
	mustCompile(t, input)
}
