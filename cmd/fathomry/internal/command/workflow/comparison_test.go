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
)

func TestPrepareComparisonMatchesPublicationSelection(t *testing.T) {
	input := ProjectInput{Workflows: []Workflow{projectWorkflow("alpha"), projectWorkflow("beta")}}
	selection := Selection{Namespace: "example", Workflow: "alpha", Version: "1", Mode: "patient", Revision: "1"}
	report, prepared, err := PrepareComparison(context.Background(), input, selection)
	if err != nil || !report.Checked || report.FullProject || report.Selected != 1 || report.Omitted != 1 || report.Complete || report.Compared != 0 || len(prepared) != 1 {
		t.Fatal("comparison preflight scope differs", err)
	}
	info, err := prepared[0].Inspect()
	if err != nil || info.Modes != 1 || info.Revisions != 1 {
		t.Fatal(err)
	}
	input.Workflows[1].Metadata = nil
	report, prepared, err = PrepareComparison(context.Background(), input, Selection{})
	if !errors.Is(err, command.ErrCheck) || report.Checked || prepared != nil || len(report.Workflows) != 2 {
		t.Fatal("incomplete metadata entered DB comparison", err)
	}
}

func TestComparisonBatchPolicy(t *testing.T) {
	ctx := context.Background()
	before, prepared, err := PrepareComparison(ctx, ProjectInput{Workflows: []Workflow{projectWorkflow("alpha"), projectWorkflow("beta"), projectWorkflow("gamma")}}, Selection{})
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("private-comparison-canary")
	for _, test := range []struct {
		name      string
		different int
		failAt    int
		wantEqual bool
		wantCount int
		wantCalls int
	}{
		{"equal", -1, -1, true, 3, 3},
		{"different_is_complete", 0, -1, false, 3, 3},
		{"first_error", -1, 0, false, 0, 1},
		{"later_error", 0, 1, false, 1, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			report, err := ComparePrepared(ctx, before, prepared, func(given context.Context, request *catalog.PreparedRegistration) (catalog.Comparison, error) {
				if given != ctx || request != prepared[calls] {
					t.Fatal("comparison changed context or prepared selection")
				}
				index := calls
				calls++
				if index == test.failAt {
					return catalog.Comparison{}, cause
				}
				return catalog.Comparison{Complete: true, Equal: index != test.different}, nil
			})
			if report.Equal != test.wantEqual || report.Compared != test.wantCount || calls != test.wantCalls || report.Complete != (test.failAt < 0) || (err == nil) != (test.failAt < 0) {
				t.Fatal("comparison aggregation changed", report, err)
			}
			if test.failAt >= 0 && !errors.Is(err, cause) {
				t.Fatal("comparison lost failure cause")
			}
			for index, item := range report.Workflows {
				if (item.Result != nil) != (index < test.wantCount) || before.Workflows[index].Result != nil {
					t.Fatal("comparison fabricated data or mutated preflight input")
				}
			}
		})
	}
}

func TestComparisonBatchRejectsUnpairedPreparation(t *testing.T) {
	ctx := context.Background()
	report, prepared, err := PrepareComparison(ctx, ProjectInput{Workflows: []Workflow{projectWorkflow("alpha"), projectWorkflow("beta")}}, Selection{})
	if err != nil {
		t.Fatal(err)
	}
	for _, values := range [][]*catalog.PreparedRegistration{nil, {prepared[1], prepared[0]}, {prepared[0], nil}} {
		calls := 0
		result, err := ComparePrepared(ctx, report, values, func(context.Context, *catalog.PreparedRegistration) (catalog.Comparison, error) {
			calls++
			return catalog.Comparison{}, nil
		})
		if err == nil || calls != 0 || result.Compared != 0 || result.Complete || result.Equal {
			t.Fatal("invalid comparison batch reached an operation", err)
		}
	}
}
