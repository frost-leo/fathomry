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

// Package workflow owns project selection, shared offline preflight and batch
// reporting through caller-supplied operations. The CLI entry owns command leaves
// and resources; this package never hosts a project or constructs database clients.
package workflow

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	implementation "github.com/frost-leo/fathomry/framework/workflow/implementation/v1"
	"github.com/spf13/cobra"
)

// Workflow registers one logical workflow's versions, optional publication
// metadata and Go implementation sets. Its Declaration must contain one Namespace/code. Metadata
// nil means declaration-only, not ready for catalog registration. Inputs are
// borrowed during a check; concurrent mutation is unsupported.
type Workflow struct {
	Declaration wf.Declaration
	Metadata    *catalog.Metadata
	// Implementations opts exact versions into Go binding checks; absence is data-only.
	Implementations []implementation.VersionBindings
}

func (Workflow) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("workflow.Registration")) }
func (Workflow) LogValue() slog.Value           { return slog.StringValue("workflow.Registration") }

// ProjectInput admits either the combined registry or the legacy declaration
// inputs, never both. Each form retains MaxInputs and MaxInputBytes project bounds.
type ProjectInput struct {
	Workflows    []Workflow
	Declarations []wf.Declaration
}

// Selection is exact and case-sensitive. Version requires Workflow; Mode requires
// Version; Revision requires Mode. Unspecified Version selects every version.
type Selection struct {
	Namespace string `json:"namespace,omitempty"`
	Workflow  string `json:"workflow,omitempty"`
	Version   string `json:"version,omitempty"`
	Mode      string `json:"mode,omitempty"`
	Revision  string `json:"revision,omitempty"`
}

// WorkflowReport names the selected logical workflow; Declaration counts are
// version/mode-revision counts. Metadata is not_supplied/not_checked/passed/failed.
type WorkflowReport struct {
	Namespace            string                 `json:"namespace"`
	Code                 string                 `json:"code"`
	Versions             []string               `json:"versions"`
	Passed               bool                   `json:"passed"`
	Declaration          wf.Report              `json:"declaration"`
	DiagnosticIdentities []wf.Identity          `json:"diagnostic_identities,omitempty"`
	Metadata             string                 `json:"metadata"`
	MetadataDiagnostic   *catalog.Diagnostic    `json:"metadata_diagnostic,omitempty"`
	Implementations      []ImplementationReport `json:"implementations"`
}

// ProjectReport reports only the selected scope, never DB state or readiness.
// Empty is an explicit zero-work outcome, not a successful workflow check.
type ProjectReport struct {
	Format           string           `json:"format"`
	Selection        Selection        `json:"selection"`
	FullProject      bool             `json:"full_project"`
	Empty            bool             `json:"empty"`
	Passed           bool             `json:"passed"`
	Total            int              `json:"total_workflows"`
	Selected         int              `json:"selected_workflows"`
	Omitted          int              `json:"omitted_workflows"`
	Workflows        []WorkflowReport `json:"workflows"`
	Truncated        bool             `json:"truncated"`
	TotalVersions    int              `json:"total_versions"`
	SelectedVersions int              `json:"selected_versions"`
	OmittedVersions  int              `json:"omitted_versions"`
}

type workflowIdentity struct{ namespace, code string }
type projectGroup struct {
	identity        workflowIdentity
	parts           []wf.Declaration
	sources         []int
	metadata        *catalog.Metadata
	originals       []wf.WorkflowDeclaration
	implementations []implementation.VersionBindings
}

type analysisMode uint8

const (
	inspectProject analysisMode = iota
	exportProject
	prepareProject
)

type projectAnalysis struct {
	report   ProjectReport
	bundles  []*wf.Bundle
	prepared []*catalog.PreparedRegistration
}

type checkedBundle struct {
	bundle *wf.Bundle
	bytes  int
}

type groupAnalysis struct {
	report   WorkflowReport
	parts    []checkedBundle
	prepared *catalog.PreparedRegistration
}

// InspectProject checks the selected scope without retaining build bundles or
// publication metadata. It performs no configuration or database work.
func InspectProject(ctx context.Context, input ProjectInput, selection Selection) (ProjectReport, error) {
	checked, err := analyzeProject(ctx, input, selection, inspectProject)
	return checked.report, err
}

// CheckProject uses declaration admission and catalog preparation without Boot,
// resources, files, project discovery or database access. Full successful checks
// also return complete inspection bundles for the build gate. A selected check
// never produces publication bundles, even if its selected workflows all pass.
func CheckProject(ctx context.Context, input ProjectInput, selection Selection) (ProjectReport, []*wf.Bundle, error) {
	checked, err := analyzeProject(ctx, input, selection, exportProject)
	return checked.report, checked.bundles, err
}

func analyzeProject(ctx context.Context, input ProjectInput, selection Selection, mode analysisMode) (result projectAnalysis, err error) {
	result.report = ProjectReport{Format: "fathomry.project-workflow-check/v2", Selection: selection, FullProject: selection == (Selection{}), Workflows: []WorkflowReport{}}
	report := &result.report
	defer func() {
		if err != nil || !result.report.Passed {
			result.bundles, result.prepared = nil, nil
		}
	}()
	if err := projectContext(ctx); err != nil {
		return result, err
	}
	if !validSelection(selection) {
		return result, command.Fail(command.ErrUsage)
	}
	groups, sources, err := projectGroups(input)
	if err != nil {
		return result, err
	}
	report.Total = len(groups)
	for _, group := range groups {
		report.TotalVersions += len(allVersions(group))
	}
	if selection.Workflow != "" && selection.Namespace == "" {
		namespaces := map[string]bool{}
		for _, group := range groups {
			if group.identity.code == selection.Workflow {
				namespaces[group.identity.namespace] = true
			}
		}
		if len(namespaces) > 1 {
			return result, command.Fail(command.ErrSelectionAmbiguous)
		}
	}
	selected := []projectGroup{}
	for _, group := range groups {
		if selection.Namespace != "" && selection.Namespace != group.identity.namespace || selection.Workflow != "" && selection.Workflow != group.identity.code {
			continue
		}
		group, found := selectGroup(group, selection)
		if found {
			selected = append(selected, group)
		}
	}
	if len(selected) == 0 {
		if report.FullProject && len(groups) == 0 {
			report.Empty = true
			return result, nil
		}
		return result, command.Fail(command.ErrSelectionMissing)
	}
	report.Selected, report.Omitted = len(selected), len(groups)-len(selected)
	report.Passed = true
	sourceSizes := make([]int, len(sources))
	if report.FullProject && mode == exportProject {
		result.bundles = make([]*wf.Bundle, len(sources))
	}
	preparedComplete := true
	remainingDiagnostics := wf.MaxDiagnostics
	checkedBytes := 0
	for _, group := range selected {
		if err := projectContext(ctx); err != nil {
			return projectAnalysis{}, err
		}
		checked, err := checkGroup(ctx, group, selection, &checkedBytes)
		if err != nil {
			return projectAnalysis{}, err
		}
		if err := checkImplementations(ctx, group, &checked, mode == exportProject); err != nil {
			return projectAnalysis{}, err
		}
		item := checked.report
		for _, version := range item.Implementations {
			if version.Selected {
				report.SelectedVersions++
			}
		}
		report.Passed = report.Passed && item.Passed
		if report.FullProject && report.Passed {
			for index, part := range checked.parts {
				source := group.sources[index]
				if len(group.parts[index].Workflows) == len(sources[source].Workflows) {
					sourceSizes[source] = part.bytes
					if mode == exportProject {
						result.bundles[source] = part.bundle
					}
				}
			}
		}
		if mode == prepareProject {
			preparedComplete = preparedComplete && item.Passed && checked.prepared != nil
			if preparedComplete {
				result.prepared = append(result.prepared, checked.prepared)
			} else {
				result.prepared = nil
			}
		}
		if !report.Passed {
			result.bundles = nil
		}
		if len(item.Declaration.Diagnostics) > remainingDiagnostics {
			item.Declaration.Diagnostics = item.Declaration.Diagnostics[:remainingDiagnostics]
			item.DiagnosticIdentities = item.DiagnosticIdentities[:remainingDiagnostics]
			report.Truncated = true
		}
		remainingDiagnostics -= len(item.Declaration.Diagnostics)
		if len(item.Declaration.Conflicts) > remainingDiagnostics {
			item.Declaration.Conflicts = item.Declaration.Conflicts[:remainingDiagnostics]
			report.Truncated = true
		}
		remainingDiagnostics -= len(item.Declaration.Conflicts)
		if item.MetadataDiagnostic != nil {
			if remainingDiagnostics == 0 {
				item.MetadataDiagnostic = nil
				report.Truncated = true
			} else {
				remainingDiagnostics--
			}
		}
		for index := range item.Implementations {
			if item.Implementations[index].Diagnostic != nil {
				if remainingDiagnostics == 0 {
					item.Implementations[index].Diagnostic = nil
					report.Truncated = true
				} else {
					remainingDiagnostics--
				}
			}
		}
		report.Truncated = report.Truncated || item.Declaration.Truncated
		for _, conflict := range item.Declaration.Conflicts {
			report.Truncated = report.Truncated || conflict.PathsTruncated
		}
		report.Workflows = append(report.Workflows, item)
	}
	if report.FullProject && report.Passed {
		total := 0
		for index, source := range sources {
			if sourceSizes[index] == 0 {
				// Split legacy groups do not prove the original source's complete
				// bundle envelope or aggregate authoring bounds.
				bundle, err := wf.Compile(ctx, source)
				if err != nil {
					return result, err
				}
				raw, err := bundle.BytesCopy()
				if err != nil {
					return result, err
				}
				sourceSizes[index] = len(raw)
				if mode == exportProject {
					result.bundles[index] = bundle
				}
			}
			if sourceSizes[index] > wf.MaxInputBytes-total {
				return result, command.Fail(command.ErrLimit)
			}
			total += sourceSizes[index]
		}
	}
	if err := projectContext(ctx); err != nil {
		return projectAnalysis{}, err
	}
	report.OmittedVersions = report.TotalVersions - report.SelectedVersions
	return result, nil
}

func projectGroups(input ProjectInput) ([]projectGroup, []wf.Declaration, error) {
	if len(input.Workflows) > 0 && len(input.Declarations) > 0 {
		return nil, nil, command.Fail(command.ErrUsage)
	}
	if len(input.Workflows) > wf.MaxInputs || len(input.Declarations) > wf.MaxInputs {
		return nil, nil, command.Fail(command.ErrLimit)
	}
	sources := input.Declarations
	if len(input.Workflows) > 0 {
		sources = make([]wf.Declaration, len(input.Workflows))
		for index, value := range input.Workflows {
			sources[index] = value.Declaration
		}
	}
	groups := []projectGroup{}
	positions := map[workflowIdentity]int{}
	for sourceIndex, source := range sources {
		if len(source.Workflows) > wf.MaxWorkflows {
			return nil, nil, command.Fail(command.ErrLimit)
		}
		modes := 0
		identities := map[workflowIdentity]bool{}
		for _, value := range source.Workflows {
			if len(value.Namespace) > 255 || len(value.Code) > 128 {
				return nil, nil, command.Fail(command.ErrLimit)
			}
			modes += len(value.Modes)
			identities[workflowIdentity{value.Namespace, value.Code}] = true
		}
		if modes > wf.MaxModes {
			return nil, nil, command.Fail(command.ErrLimit)
		}
		if len(input.Workflows) > 0 && len(identities) != 1 {
			return nil, nil, command.Fail(command.ErrUsage)
		}
		if len(input.Workflows) > 0 {
			if err := implementationStructure(source, input.Workflows[sourceIndex].Implementations); err != nil {
				return nil, nil, err
			}
		}
		if len(source.Workflows) == 0 {
			groups = append(groups, projectGroup{parts: []wf.Declaration{source}, sources: []int{sourceIndex}})
			continue
		}
		for _, value := range source.Workflows {
			key := workflowIdentity{value.Namespace, value.Code}
			index, exists := positions[key]
			if !exists {
				index = len(groups)
				positions[key] = index
				group := projectGroup{identity: key}
				if len(input.Workflows) > 0 {
					group.metadata = input.Workflows[sourceIndex].Metadata
					group.implementations = input.Workflows[sourceIndex].Implementations
				}
				groups = append(groups, group)
			} else if len(input.Workflows) > 0 && len(groups[index].sources) > 0 && groups[index].sources[0] != sourceIndex {
				return nil, nil, command.Fail(command.ErrUsage)
			}
			group := &groups[index]
			group.originals = append(group.originals, value)
			if len(group.sources) == 0 || group.sources[len(group.sources)-1] != sourceIndex {
				group.sources = append(group.sources, sourceIndex)
				group.parts = append(group.parts, wf.Declaration{})
			}
			group.parts[len(group.parts)-1].Workflows = append(group.parts[len(group.parts)-1].Workflows, value)
		}
	}
	slices.SortFunc(groups, func(left, right projectGroup) int {
		if value := strings.Compare(left.identity.namespace, right.identity.namespace); value != 0 {
			return value
		}
		return strings.Compare(left.identity.code, right.identity.code)
	})
	return groups, sources, nil
}

func selectGroup(group projectGroup, selection Selection) (projectGroup, bool) {
	result := group
	result.parts = nil
	result.sources = nil
	found := selection.Version == ""
	for index, part := range group.parts {
		chosen := wf.Declaration{}
		for _, version := range part.Workflows {
			if selection.Version != "" && version.Version != selection.Version {
				continue
			}
			if selection.Mode != "" {
				modes := []wf.ModeDeclaration{}
				for _, mode := range version.Modes {
					if mode.Code == selection.Mode && (selection.Revision == "" || mode.Revision == selection.Revision) {
						modes = append(modes, mode)
					}
				}
				if len(modes) > 0 {
					found = true
				}
				version.Modes = modes
			} else {
				found = true
			}
			chosen.Workflows = append(chosen.Workflows, version)
		}
		if len(chosen.Workflows) > 0 || len(part.Workflows) == 0 && selection == (Selection{}) {
			result.parts = append(result.parts, chosen)
			result.sources = append(result.sources, group.sources[index])
		}
	}
	return result, found
}

func checkGroup(ctx context.Context, group projectGroup, selection Selection, checkedBytes *int) (groupAnalysis, error) {
	checked := groupAnalysis{report: WorkflowReport{Namespace: displayIdentity(group.identity.namespace, 255), Code: displayIdentity(group.identity.code, 128), Versions: []string{}, Metadata: "not_supplied"}, parts: make([]checkedBundle, len(group.parts))}
	result := &checked.report
	bundles := []*wf.Bundle{}
	diagnostics := []wf.InputDiagnostic{}
	for index, source := range group.parts {
		for _, version := range source.Workflows {
			if identity := displayIdentity(version.Version, 128); identity != "" && !slices.Contains(result.Versions, identity) {
				result.Versions = append(result.Versions, identity)
			}
		}
		bundle, err := wf.Compile(ctx, source)
		if err != nil {
			if problem := projectContext(ctx); problem != nil {
				return checked, problem
			}
			diagnostic, completed := wf.Diagnose(err)
			if !completed {
				return checked, err
			}
			diagnostics = append(diagnostics, wf.InputDiagnostic{Input: group.sources[index], Diagnostic: diagnostic})
			result.DiagnosticIdentities = append(result.DiagnosticIdentities, diagnosticIdentity(source, diagnostic.Path))
			continue
		}
		raw, err := bundle.BytesCopy()
		if err != nil {
			return checked, err
		}
		if len(raw) > wf.MaxInputBytes-*checkedBytes {
			return checked, command.Fail(command.ErrLimit)
		}
		*checkedBytes += len(raw)
		bundles = append(bundles, bundle)
		checked.parts[index] = checkedBundle{bundle: bundle, bytes: len(raw)}
	}
	slices.Sort(result.Versions)
	result.Declaration = wf.Report{Inputs: len(group.parts), Entries: []wf.Entry{}, Conflicts: []wf.Conflict{}, Diagnostics: diagnostics}
	if len(bundles) > 0 {
		report, err := wf.Check(ctx, bundles...)
		if err != nil {
			return checked, err
		}
		report.Inputs, report.Diagnostics = len(group.parts), diagnostics
		report.Passed = report.Passed && len(diagnostics) == 0
		result.Declaration = report
	}
	result.Passed = result.Declaration.Passed
	if group.metadata != nil {
		if len(group.metadata.Resources) > catalog.MaxResources || len(group.metadata.Tags) > catalog.MaxTags {
			return checked, command.Fail(command.ErrLimit)
		}
		result.Metadata = "not_checked"
		if result.Passed {
			metadata := selectMetadata(*group.metadata, selection)
			prepared, err := catalog.Prepare(ctx, catalog.Registration{Bundle: bundles[0], Resources: metadata.Resources, Category: metadata.Category, Tags: metadata.Tags})
			if err != nil {
				if problem := projectContext(ctx); problem != nil {
					return checked, problem
				}
				diagnostic, completed := catalog.Diagnose(err)
				if !completed {
					return checked, err
				}
				result.MetadataDiagnostic = &diagnostic
				result.Metadata, result.Passed = "failed", false
			} else {
				result.Metadata = "passed"
				checked.prepared = prepared
			}
		}
	}
	return checked, nil
}

func selectMetadata(input catalog.Metadata, selection Selection) catalog.Metadata {
	if selection.Version == "" {
		return input
	}
	selected := input
	selected.Resources = nil
	for _, item := range input.Resources {
		if item.Target.Kind != catalog.Definition && item.Target.Version != selection.Version {
			continue
		}
		if (item.Target.Kind == catalog.Mode || item.Target.Kind == catalog.ModeRevision) && selection.Mode != "" {
			if item.Target.Mode != selection.Mode {
				continue
			}
			if item.Target.Kind == catalog.ModeRevision && selection.Revision != "" && item.Target.Revision != selection.Revision {
				continue
			}
		}
		selected.Resources = append(selected.Resources, item)
	}
	return selected
}
func validSelection(value Selection) bool {
	if value.Version != "" && value.Workflow == "" || value.Mode != "" && value.Version == "" || value.Revision != "" && value.Mode == "" {
		return false
	}
	for index, text := range []string{value.Namespace, value.Workflow, value.Version, value.Mode, value.Revision} {
		maximum := 128
		if index == 0 {
			maximum = 255
		}
		if len(text) > maximum || !utf8.ValidString(text) || strings.ContainsFunc(text, unicode.IsControl) {
			return false
		}
	}
	return true
}
func diagnosticIdentity(source wf.Declaration, path string) wf.Identity {
	parts := strings.Split(path, "/")
	if len(parts) < 3 || parts[1] != "workflows" {
		return wf.Identity{}
	}
	index, err := strconv.Atoi(parts[2])
	if err != nil || index < 0 || index >= len(source.Workflows) {
		return wf.Identity{}
	}
	workflow := source.Workflows[index]
	identity := wf.Identity{Namespace: displayIdentity(workflow.Namespace, 255), Code: displayIdentity(workflow.Code, 128), Version: displayIdentity(workflow.Version, 128)}
	if len(parts) >= 5 && parts[3] == "modes" {
		index, err := strconv.Atoi(parts[4])
		if err == nil && index >= 0 && index < len(workflow.Modes) {
			identity.Mode = displayIdentity(workflow.Modes[index].Code, 128)
			identity.Revision = displayIdentity(workflow.Modes[index].Revision, 128)
		}
	}
	return identity
}

func displayIdentity(value string, maximum int) string {
	if len(value) > maximum || !utf8.ValidString(value) || strings.ContainsFunc(value, unicode.IsControl) {
		return ""
	}
	return strings.Clone(value)
}

func projectContext(ctx context.Context) error {
	if ctx == nil {
		return command.Fail(command.ErrOptions)
	}
	if ctx.Err() != nil {
		return command.Fail(command.ErrCanceled, ctx.Err(), context.Cause(ctx))
	}
	return nil
}

// New declares the namespace; the shared CLI entry attaches its fixed leaf set.
func New(invocation *command.Invocation) *cobra.Command {
	return invocation.Group("workflow", "fathomry.workflow_commands.root")
}
