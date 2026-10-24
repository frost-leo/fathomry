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

package workflow

import (
	"context"
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

func TestPublicationPreflightIsAllBeforeAnyWork(t *testing.T) {
	first, second := projectWorkflow("first"), projectWorkflow("second")
	second.Declaration.Workflows[0].Spec.Limits.ActiveChunks = wf.Fixed("0")
	input := ProjectInput{Workflows: []Workflow{first, second}}
	report, prepared, err := PrepareProject(context.Background(), input, Selection{})
	if !errors.Is(err, command.ErrCheck) || prepared != nil || report.Checked || report.Effect != "not_started" || report.Attempted != 0 || len(report.Workflows) != 2 {
		t.Fatal("partial preparation escaped failed aggregate checks", err)
	}
	selected, prepared, err := PrepareProject(context.Background(), input, Selection{Workflow: "first", Version: "1", Mode: "patient", Revision: "1"})
	if err != nil || !selected.Checked || selected.Selected != 1 || selected.Omitted != 1 || len(prepared) != 1 {
		t.Fatal("exact selection differs from vet", err)
	}
	info, err := prepared[0].Inspect()
	if err != nil || info.Code != "first" || info.Versions != 1 || info.Modes != 1 || info.Revisions != 1 || info.DocumentBytes == 0 {
		t.Fatal("selected prepared scope is wrong", err)
	}
	input.Workflows[0].Metadata.Resources[0].Text.English = "mutated"
	again, _ := prepared[0].Inspect()
	if again != info {
		t.Fatal("prepared snapshot changed")
	}
	second = projectWorkflow("second")
	second.Metadata = nil
	report, prepared, err = PrepareProject(context.Background(), ProjectInput{Workflows: []Workflow{projectWorkflow("first"), second}}, Selection{})
	if !errors.Is(err, command.ErrCheck) || prepared != nil || report.Checked || report.Workflows[1].Metadata != "not_supplied" {
		t.Fatal("missing metadata silently skipped", err)
	}
	if _, prepared, err := PrepareProject(context.Background(), ProjectInput{}, Selection{}); !errors.Is(err, command.ErrSelectionMissing) || prepared != nil {
		t.Fatal("empty project was published")
	}
	if _, prepared, err := PrepareProject(context.Background(), ProjectInput{Declarations: []wf.Declaration{first.Declaration}}, Selection{}); !errors.Is(err, command.ErrCheck) || prepared != nil {
		t.Fatal("legacy declarations fabricated metadata")
	}
}
func TestPublicationPreflightReportsOneDiagnosticWithoutDocuments(t *testing.T) {
	invalid := projectWorkflow("invalid")
	invalid.Declaration.Workflows[0].Spec.Limits.ActiveChunks = wf.Fixed("0")
	report, _, err := PrepareProject(context.Background(), ProjectInput{Workflows: []Workflow{invalid}}, Selection{})
	if !errors.Is(err, command.ErrCheck) || len(report.Workflows) != 1 || report.Workflows[0].CheckDiagnostic == nil || report.Workflows[0].Result != nil {
		t.Fatal("invalid declaration lost its bounded diagnostic", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, prepared, err := PrepareProject(ctx, ProjectInput{Workflows: []Workflow{projectWorkflow("valid")}}, Selection{}); !errors.Is(err, context.Canceled) || prepared != nil {
		t.Fatal("canceled preparation escaped", err)
	}
}

func TestPublicationBatchPolicy(t *testing.T) {
	ctx := context.Background()
	input := ProjectInput{Workflows: []Workflow{projectWorkflow("alpha"), projectWorkflow("beta"), projectWorkflow("gamma")}}
	before, prepared, err := PrepareProject(ctx, input, Selection{})
	if err != nil {
		t.Fatal(err)
	}
	_, detailed := catalog.Prepare(ctx, catalog.Registration{})
	diagnostic, known := catalog.Diagnose(detailed)
	if !known {
		t.Fatal("diagnostic control unavailable")
	}
	cause := errors.Join(errors.New("private-batch-canary"), detailed)
	for _, test := range []struct {
		name    string
		effects []catalog.Effect
		failAt  int
		want    string
	}{
		{"unchanged", []catalog.Effect{catalog.Unchanged, catalog.Unchanged, catalog.Unchanged}, -1, "unchanged"},
		{"applied", []catalog.Effect{catalog.Unchanged, catalog.Applied, catalog.Unchanged}, -1, "applied"},
		{"first_refused", []catalog.Effect{catalog.NotStarted}, 0, "not_started"},
		{"first_rollback", []catalog.Effect{catalog.RolledBack}, 0, "rolled_back"},
		{"partial", []catalog.Effect{catalog.Applied, catalog.RolledBack}, 1, "partial"},
		{"unknown_dominates", []catalog.Effect{catalog.Applied, catalog.Unknown}, 1, "unknown"},
		{"known_commit_with_error", []catalog.Effect{catalog.Applied}, 0, "partial"},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			report, err := PublishPrepared(ctx, before, prepared, func(given context.Context, request *catalog.PreparedRegistration) (catalog.Publication, error) {
				if given != ctx || request != prepared[calls] {
					t.Fatal("batch changed caller context or prepared selection")
				}
				index := calls
				calls++
				value := catalog.Publication{Effect: test.effects[index]}
				if index == test.failAt {
					return value, cause
				}
				return value, nil
			})
			wantCalls := len(prepared)
			if test.failAt >= 0 {
				wantCalls = test.failAt + 1
			}
			if calls != wantCalls || report.Attempted != wantCalls || report.Effect != test.want || report.Complete != (test.failAt < 0) || (err == nil) != (test.failAt < 0) {
				t.Fatal("batch effect or stopping policy changed", calls, report, err)
			}
			if test.failAt >= 0 && (!errors.Is(err, cause) || report.Workflows[test.failAt].Diagnostic == nil || *report.Workflows[test.failAt].Diagnostic != diagnostic) {
				t.Fatal("batch lost cause or bounded diagnostic", err)
			}
			for index, item := range report.Workflows {
				if (item.Result != nil) != (index < wantCalls) || before.Workflows[index].Result != nil || before.Workflows[index].Diagnostic != nil {
					t.Fatal("unattempted result fabricated or preflight input mutated")
				}
			}
		})
	}
}

func TestPublicationBatchRejectsUnpairedPreparation(t *testing.T) {
	ctx := context.Background()
	report, prepared, err := PrepareProject(ctx, ProjectInput{Workflows: []Workflow{projectWorkflow("alpha"), projectWorkflow("beta")}}, Selection{})
	if err != nil {
		t.Fatal(err)
	}
	for _, values := range [][]*catalog.PreparedRegistration{nil, {prepared[1], prepared[0]}, {prepared[0], nil}} {
		calls := 0
		result, err := PublishPrepared(ctx, report, values, func(context.Context, *catalog.PreparedRegistration) (catalog.Publication, error) {
			calls++
			return catalog.Publication{}, nil
		})
		if err == nil || calls != 0 || result.Attempted != 0 || result.Complete {
			t.Fatal("invalid batch reached an operation", err)
		}
	}
}
