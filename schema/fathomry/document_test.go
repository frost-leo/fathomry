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

package fathomry_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
)

func maximumSpecification(t *testing.T) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	declared := health.Declarations()
	minimum, maximum := int64(0), int64(1024)
	choices := make([]string, 256)
	for index := range choices {
		choices[index] = fmt.Sprintf("%04d", index) + strings.Repeat("x", 1020)
	}
	properties := make(map[string]wf.DataSchema)
	for index := range 15 {
		properties[fmt.Sprintf("field%02d", index)] = wf.DataSchema{
			Type: "string", MinLength: &minimum, MaxLength: &maximum, Enum: choices,
		}
	}
	tail := wf.DataSchema{Type: "string", MinLength: &minimum, MaxLength: &maximum, Enum: []string{"0000"}}
	properties["field15"] = tail
	declared.Workflows[0].Spec.Contracts["target"] = wf.DataSchema{Type: "object", Properties: properties}
	compile := func() (*wf.Bundle, []byte) {
		t.Helper()
		bundle, err := wf.Compile(ctx, declared)
		if err != nil {
			t.Fatal("maximum-size valid declaration failed", err)
		}
		contents, err := bundle.ContentsCopy()
		if err != nil || len(contents) != 3 || contents[0].Entry.Identity.Mode != "" {
			t.Fatal("fixture content export failed", err)
		}
		return bundle, contents[0].Bytes
	}
	_, baseline := compile()
	remaining := wf.MaxSpecBytes - len(baseline)
	if remaining < 0 {
		t.Fatal("baseline exceeds the supported spec profile")
	}
	for remaining >= 1027 {
		tail.Enum = append(tail.Enum, fmt.Sprintf("%04d", len(tail.Enum))+strings.Repeat("x", 1020))
		remaining -= 1027
	}
	if remaining > 1020 {
		tail.Enum = append(tail.Enum, fmt.Sprintf("%04d", len(tail.Enum)))
		remaining -= 7
	}
	tail.Enum[0] += strings.Repeat("x", remaining)
	if len(tail.Enum) > 256 {
		t.Fatal("tail exceeds the supported enum profile")
	}
	properties["field15"] = tail
	bundle, document := compile()
	if len(document) != wf.MaxSpecBytes {
		t.Fatalf("fixture is not exactly at the spec boundary: %d", len(document))
	}
	raw, err := bundle.BytesCopy()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := wf.Parse(ctx, raw)
	if err != nil {
		t.Fatal("maximum document failed public Parse", err)
	}
	report, err := wf.Check(ctx, parsed)
	if err != nil || !report.Passed {
		t.Fatal("maximum document failed public Check", err)
	}
	contents, err := parsed.ContentsCopy()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wf.ParseContents(ctx, contents); err != nil {
		t.Fatal("maximum document failed stored-content reconstruction", err)
	}
	prepared, err := catalog.Prepare(ctx, catalog.Registration{Bundle: parsed, Resources: []catalog.Resource{
		{Target: catalog.Target{Kind: catalog.Definition}, Field: catalog.Name, Text: catalog.Text{English: "Health"}},
		{Target: catalog.Target{Kind: catalog.Mode, Version: "1", Mode: "standard"}, Field: catalog.Name, Text: catalog.Text{English: "Standard"}},
		{Target: catalog.Target{Kind: catalog.Mode, Version: "1", Mode: "patient"}, Field: catalog.Name, Text: catalog.Text{English: "Patient"}},
	}})
	if err != nil || prepared == nil {
		t.Fatal("catalog preparation shrank the valid 4 MiB document profile", err)
	}
	return document
}

func TestMaximumSpecificationFixture(t *testing.T) {
	if document := maximumSpecification(t); len(document) != wf.MaxSpecBytes {
		t.Fatal("maximum specification fixture changed")
	}
}

func maximumModeDeclaration(t *testing.T) wf.Declaration {
	t.Helper()
	spec := wf.Specification{
		Contracts: map[string]wf.DataSchema{"result": {Type: "boolean"}},
		Bindings: map[string]wf.Binding{"read": {Type: "example/read", Revision: "1", RetrySafety: "read_only",
			Outputs: map[string]wf.Port{"result": {Contract: "result", Delivery: "value", Required: true}}}},
		Nodes: map[string]wf.Node{}, Edges: map[string]wf.Edge{}, Controls: map[string]wf.Control{},
		Completion: wf.Completion{Item: "selected_paths_and_required_effects", Run: "all_succeeded", MaximumFailedItems: "0"},
	}
	next := 0
	control := func(kind string, value wf.Decimal) wf.Setting {
		name := fmt.Sprintf("c%04d", next) + strings.Repeat("x", 59)
		next++
		spec.Controls[name] = wf.Control{Kind: kind, Minimum: value, Maximum: value, Default: &value}
		return wf.Tuned(name)
	}
	spec.Limits = wf.Limits{
		ActiveChunks: control("count", "9223372036854775807"), ItemsPerChunk: control("count", "9223372036854775807"),
		ChunkItemConcurrency: control("count", "9223372036854775807"),
		RunTimeoutMS:         control("duration_ms", "9000000000000"), CleanupReserveMS: control("duration_ms", "1000000000000"),
	}
	duration := func() wf.Setting {
		if next == 4096 {
			return wf.Fixed("1000000000000")
		}
		return control("duration_ms", "1000000000000")
	}
	for index := 0; next < 4096; index++ {
		name := fmt.Sprintf("n%d", index/256)
		node, exists := spec.Nodes[name]
		if !exists {
			node = wf.Node{Activities: map[string]wf.Activity{}, Phases: [][]string{{}}, Join: "all",
				Routes: []string{"continue"}, Route: wf.Route{Kind: "fixed", Value: "continue"}}
			if index != 0 {
				spec.Edges[name] = wf.Edge{From: fmt.Sprintf("n%d", index/256-1), To: name, On: []string{"continue"}}
			}
		}
		call := fmt.Sprintf("a%d", index)
		heartbeat := duration()
		node.Activities[call] = wf.Activity{Binding: "read", Policy: wf.Policy{
			HeartbeatMS: &heartbeat, StartToCloseMS: duration(), ScheduleToCloseMS: duration(),
			InitialIntervalMS: duration(), MaximumIntervalMS: duration(), MaximumAttempts: wf.Fixed("1"), BackoffMilli: wf.Fixed("1000"),
		}}
		node.Phases[0] = append(node.Phases[0], call)
		spec.Nodes[name] = node
	}
	return wf.Declaration{Workflows: []wf.WorkflowDeclaration{{
		Namespace: strings.Repeat("n", 255), Code: strings.Repeat("w", 128), Version: strings.Repeat("v", 128), Spec: spec,
		Modes: []wf.ModeDeclaration{{Code: strings.Repeat("m", 128), Revision: strings.Repeat("r", 128)}},
	}}}
}

func maximumModeAggregate(t *testing.T) *wf.Bundle {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	declaration := maximumModeDeclaration(t)
	compile := func() (*wf.Bundle, int) {
		t.Helper()
		bundle, err := wf.Compile(ctx, declaration)
		if err != nil {
			detail, _ := wf.Diagnose(err)
			t.Fatal("maximum-control declaration failed", err, detail)
		}
		raw, err := bundle.BytesCopy()
		if err != nil {
			t.Fatal(err)
		}
		return bundle, len(raw)
	}
	first, firstBytes := compile()
	contents, err := first.ContentsCopy()
	if err != nil || len(contents) != 2 {
		t.Fatal("maximum mode export failed", err)
	}
	// Only the three global count slots admit 19-digit values. Every other
	// used control is bounded to at most 13 digits; unused controls are invalid.
	maximum := len(`{"format":"`+wf.ModeFormat+`","values":{}}`) + 4096*(64+13+6) - 1 + 3*(19-13)
	if len(contents[1].Bytes) != maximum || maximum >= wf.MaxModeBytes {
		t.Fatalf("maximum reachable mode size changed: got %d, want %d", len(contents[1].Bytes), maximum)
	}
	mode := declaration.Workflows[0].Modes[0]
	mode.Code = fmt.Sprintf("m%03d", 1) + strings.Repeat("m", 124)
	declaration.Workflows[0].Modes = append(declaration.Workflows[0].Modes, mode)
	_, twoBytes := compile()
	perMode := twoBytes - firstBytes
	count := min(wf.MaxModesPerWorkflow, 1+(wf.MaxBundleBytes-firstBytes)/perMode)
	for index := 2; index < count; index++ {
		mode.Code = fmt.Sprintf("m%03d", index) + strings.Repeat("m", 124)
		declaration.Workflows[0].Modes = append(declaration.Workflows[0].Modes, mode)
	}
	bundle, size := compile()
	mode.Code = "m999" + strings.Repeat("m", 124)
	declaration.Workflows[0].Modes = append(declaration.Workflows[0].Modes, mode)
	if rejected, err := wf.Compile(ctx, declaration); rejected != nil || !errors.Is(err, wf.ErrLimit) {
		t.Fatal("one more maximum-size mode did not exceed the aggregate limit", err)
	}
	contents, err = bundle.ContentsCopy()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wf.ParseContents(ctx, contents); err != nil {
		t.Fatal("maximum-mode aggregate reconstruction failed", err)
	}
	t.Logf("maximum reachable mode: %d bytes, 4096 controls; aggregate: %d modes, %d bytes; one more rejected", maximum, count, size)
	return bundle
}

func TestMaximumModeAggregateFixture(t *testing.T) {
	_ = maximumModeAggregate(t)
}
