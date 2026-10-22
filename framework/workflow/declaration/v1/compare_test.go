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
	json "encoding/json/v2"
	"errors"
	"testing"
)

func TestCheckIdentitiesAndConflicts(t *testing.T) {
	first := mustCompile(t, fixtureDeclaration(t))
	report, err := Check(context.Background(), first, first)
	if err != nil || !report.Passed || report.Workflows != 1 || report.Modes != 2 || len(report.Entries) != 3 {
		t.Fatal(report, err)
	}
	input := fixtureDeclaration(t)
	binding := input.Workflows[0].Spec.Bindings["read_cpu"]
	binding.Revision = "2"
	input.Workflows[0].Spec.Bindings["read_cpu"] = binding
	second := mustCompile(t, input)
	report, err = Check(context.Background(), first, second)
	if err != nil || report.Passed || len(report.Conflicts) != 1 || report.UncomparedModes != 2 {
		t.Fatal(report, err)
	}
	if got := report.Conflicts[0].Paths; len(got) != 1 || got[0] != "/spec/bindings/read_cpu/revision" {
		t.Fatal(got)
	}
	reversed, err := Check(context.Background(), second, first)
	if err != nil {
		t.Fatal(err)
	}
	left, _ := json.Marshal(report)
	right, _ := json.Marshal(reversed)
	if !bytes.Equal(left, right) {
		t.Fatal("file ordering changed report")
	}
	report.Conflicts[0].Paths[0] = "mutated"
	report.Entries = append(report.Entries, Entry{})
	again, _ := Check(context.Background(), first, second)
	if again.Conflicts[0].Paths[0] == "mutated" {
		t.Fatal("report aliases retained state")
	}
	input = fixtureDeclaration(t)
	input.Workflows[0].Version = "2"
	report, err = Check(context.Background(), first, mustCompile(t, input))
	if err != nil || !report.Passed || report.Workflows != 2 || report.Modes != 4 {
		t.Fatal("content collapsed identity", report, err)
	}
	input = fixtureDeclaration(t)
	input.Workflows[0].Modes = input.Workflows[0].Modes[:1]
	report, err = Check(context.Background(), first, mustCompile(t, input))
	if err != nil || !report.Passed || report.Modes != 2 {
		t.Fatal("mode subsets conflict", report, err)
	}
}

func TestModeConflictAndHashNegativeControl(t *testing.T) {
	first := mustCompile(t, fixtureDeclaration(t))
	input := fixtureDeclaration(t)
	input.Workflows[0].Modes[0].Values["attempt_timeout"] = "1200"
	second := mustCompile(t, input)
	report, err := Check(context.Background(), first, second)
	if err != nil || report.Passed || len(report.Conflicts) != 1 || report.Conflicts[0].Identity.Mode == "" {
		t.Fatal(report, err)
	}
	// Test-only corrupted metadata forces a digest match while content differs.
	// Equality must still compare bytes, even under a hypothetical collision.
	for index := range second.entries {
		second.entries[index].SHA256 = first.entries[index].SHA256
	}
	report, err = Check(context.Background(), first, second)
	if err != nil || report.Passed || len(report.Conflicts) != 1 {
		t.Fatal("hash-only equality", report, err)
	}
}

func TestCheckBoundsAndCancellation(t *testing.T) {
	if _, err := Check(context.Background()); !errors.Is(err, ErrHandle) {
		t.Fatal(err)
	}
	if _, err := Check(context.Background(), nil); !errors.Is(err, ErrHandle) {
		t.Fatal(err)
	}
	bundle := mustCompile(t, fixtureDeclaration(t))
	inputs := make([]*Bundle, MaxInputs+1)
	for index := range inputs {
		inputs[index] = bundle
	}
	if _, err := Check(context.Background(), inputs...); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Check(ctx, bundle); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestBoundedConflictPaths(t *testing.T) {
	left, right := map[string]any{}, map[string]any{}
	for index := range 32 {
		left[indexPath("", index)] = int64(1)
		right[indexPath("", index)] = int64(2)
	}
	paths := map[string]bool{}
	truncated := false
	if err := differentPaths(context.Background(), left, right, "/spec", paths, &truncated); err != nil {
		t.Fatal(err)
	}
	if !truncated || len(paths) != MaxConflictPaths {
		t.Fatal(paths, truncated)
	}
}
