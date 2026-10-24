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
	"slices"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

// WorkflowComparison separates offline validation from a completed storage
// observation. Nil Result means this workflow has not been compared.
type WorkflowComparison struct {
	Namespace          string              `json:"namespace"`
	Code               string              `json:"code"`
	Checked            bool                `json:"checked"`
	Metadata           string              `json:"metadata"`
	CheckDiagnostic    *wf.Diagnostic      `json:"check_diagnostic,omitempty"`
	MetadataDiagnostic *catalog.Diagnostic `json:"metadata_diagnostic,omitempty"`
	Conflicts          int                 `json:"conflicts"`
	Result             *catalog.Comparison `json:"result,omitempty"`
}

// ComparisonReport is bounded project-scope data. Each workflow is one coherent
// comparison; Complete is not a cross-workflow or persistent database snapshot.
type ComparisonReport struct {
	Format      string               `json:"format"`
	Selection   Selection            `json:"selection"`
	FullProject bool                 `json:"full_project"`
	Total       int                  `json:"total_workflows"`
	Selected    int                  `json:"selected_workflows"`
	Omitted     int                  `json:"omitted_workflows"`
	Checked     bool                 `json:"checked"`
	Compared    int                  `json:"compared"`
	Complete    bool                 `json:"complete"`
	Equal       bool                 `json:"equal"`
	Workflows   []WorkflowComparison `json:"workflows"`
}

// PrepareComparison reuses the exact selected declaration/metadata preparation,
// with no database or resolver access and no separate project registry.
func PrepareComparison(ctx context.Context, input ProjectInput, selection Selection) (ComparisonReport, []*catalog.PreparedRegistration, error) {
	check, prepared, err := PrepareProject(ctx, input, selection)
	result := ComparisonReport{Format: "fathomry.project-comparison/v1", Selection: check.Selection, FullProject: check.FullProject, Total: check.Total, Selected: check.Selected, Omitted: check.Omitted, Checked: check.Checked, Workflows: []WorkflowComparison{}}
	for _, item := range check.Workflows {
		result.Workflows = append(result.Workflows, WorkflowComparison{Namespace: item.Namespace, Code: item.Code, Checked: item.Checked, Metadata: item.Metadata, CheckDiagnostic: item.CheckDiagnostic, MetadataDiagnostic: item.MetadataDiagnostic, Conflicts: item.Conflicts})
	}
	return result, prepared, err
}

// ComparePrepared aggregates the paired successful output of PrepareComparison.
// The caller supplies a borrowed catalog operation and owns its resource lifetime.
// Workflow result slots are copied; each callback transfers its detached result.
// Missing/different data remains a completed comparison, unlike an execution error.
func ComparePrepared(ctx context.Context, report ComparisonReport, prepared []*catalog.PreparedRegistration, compare func(context.Context, *catalog.PreparedRegistration) (catalog.Comparison, error)) (ComparisonReport, error) {
	if ctx == nil || compare == nil || !report.Checked || report.Complete || report.Compared != 0 || report.Selected == 0 || report.Selected != len(prepared) || len(report.Workflows) != len(prepared) || len(prepared) > wf.MaxInputs {
		return report, command.Fail(command.ErrOptions)
	}
	for index, request := range prepared {
		info, err := request.Inspect()
		if err != nil {
			return report, err
		}
		if report.Workflows[index].Namespace != info.Namespace || report.Workflows[index].Code != info.Code || !report.Workflows[index].Checked {
			return report, command.Fail(command.ErrOptions)
		}
	}
	report.Workflows = slices.Clone(report.Workflows)
	report.Equal = true
	for index, request := range prepared {
		result, err := compare(ctx, request)
		if err != nil {
			report.Equal = false
			return report, err
		}
		report.Compared++
		report.Workflows[index].Result = &result
		report.Equal = report.Equal && result.Equal
	}
	report.Complete = true
	return report, nil
}
