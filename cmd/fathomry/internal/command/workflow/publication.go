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
	"slices"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

// WorkflowPublication retains bounded per-workflow preflight and publication facts.
// A nil Result means this workflow was never passed to the catalog. Diagnostics
// show the first declaration occurrence; workflow vet remains the full check report.
type WorkflowPublication struct {
	Namespace          string               `json:"namespace"`
	Code               string               `json:"code"`
	Checked            bool                 `json:"checked"`
	Metadata           string               `json:"metadata"`
	CheckDiagnostic    *wf.Diagnostic       `json:"check_diagnostic,omitempty"`
	MetadataDiagnostic *catalog.Diagnostic  `json:"metadata_diagnostic,omitempty"`
	Conflicts          int                  `json:"conflicts"`
	Diagnostic         *catalog.Diagnostic  `json:"diagnostic,omitempty"`
	Result             *catalog.Publication `json:"result,omitempty"`
}

// PublicationReport is not a project-wide transaction. Earlier acknowledged
// aggregates remain published when a later workflow fails; unattempted items stay
// explicit. Complete means every selected workflow finished without an error.
type PublicationReport struct {
	Format      string                `json:"format"`
	Selection   Selection             `json:"selection"`
	FullProject bool                  `json:"full_project"`
	Total       int                   `json:"total_workflows"`
	Selected    int                   `json:"selected_workflows"`
	Omitted     int                   `json:"omitted_workflows"`
	Checked     bool                  `json:"checked"`
	Attempted   int                   `json:"attempted"`
	Complete    bool                  `json:"complete"`
	Effect      string                `json:"effect"`
	Workflows   []WorkflowPublication `json:"workflows"`
}

// PrepareProject performs the same selection/checks as vet, then snapshots every
// selected aggregate before any database resolver runs. Missing metadata rejects
// the whole selected set. This is distinct from build's full-project Bundle export.
func PrepareProject(ctx context.Context, input ProjectInput, selection Selection) (PublicationReport, []*catalog.PreparedRegistration, error) {
	checked, err := analyzeProject(ctx, input, selection, prepareProject)
	check := checked.report
	result := PublicationReport{Format: "fathomry.project-publication/v1", Selection: selection, FullProject: check.FullProject, Total: check.Total, Selected: check.Selected, Omitted: check.Omitted, Effect: "not_started", Workflows: []WorkflowPublication{}}
	for _, item := range check.Workflows {
		value := WorkflowPublication{Namespace: item.Namespace, Code: item.Code, Checked: item.Passed && item.Metadata == "passed", Metadata: item.Metadata, MetadataDiagnostic: item.MetadataDiagnostic, Conflicts: len(item.Declaration.Conflicts)}
		if len(item.Declaration.Diagnostics) > 0 {
			diagnostic := item.Declaration.Diagnostics[0].Diagnostic
			value.CheckDiagnostic = &diagnostic
		}
		result.Workflows = append(result.Workflows, value)
	}
	if err != nil {
		return result, nil, err
	}
	if check.Empty {
		return result, nil, command.Fail(command.ErrSelectionMissing)
	}
	result.Checked = check.Passed
	for _, item := range result.Workflows {
		result.Checked = result.Checked && item.Checked
	}
	if !result.Checked {
		return result, nil, command.Fail(command.ErrCheck)
	}
	if len(checked.prepared) != result.Selected {
		return result, nil, command.Fail(command.ErrCheck)
	}
	return result, checked.prepared, nil
}

// PublishPrepared executes the paired successful output of PrepareProject.
// The caller supplies a borrowed catalog operation and owns its resource lifetime.
// Workflow result slots are copied; each callback transfers its detached result.
// The first error stops the batch without replay or hiding earlier effects.
func PublishPrepared(ctx context.Context, report PublicationReport, prepared []*catalog.PreparedRegistration, publish func(context.Context, *catalog.PreparedRegistration) (catalog.Publication, error)) (PublicationReport, error) {
	if ctx == nil || publish == nil || !report.Checked || report.Complete || report.Attempted != 0 || report.Selected == 0 || report.Selected != len(prepared) || len(report.Workflows) != len(prepared) || len(prepared) > wf.MaxInputs {
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
	for index, request := range prepared {
		result, problem := publish(ctx, request)
		report.Attempted++
		report.Workflows[index].Result = &result
		if problem != nil {
			var details *failure.Detailed[catalog.Diagnostic]
			if errors.As(problem, &details) {
				if diagnostic, ok := details.Details(); ok {
					report.Workflows[index].Diagnostic = &diagnostic
				}
			}
			report.Effect = string(result.Effect)
			if result.Effect == catalog.Applied || result.Effect == catalog.Unchanged || index > 0 {
				report.Effect = "partial"
			}
			if result.Effect == catalog.Unknown {
				report.Effect = "unknown"
			}
			return report, problem
		}
		if result.Effect == catalog.Applied {
			report.Effect = "applied"
		} else if report.Effect == "not_started" {
			report.Effect = "unchanged"
		}
	}
	report.Complete = true
	return report, nil
}
