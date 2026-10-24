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
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
)

func projectWorkflow(code string) Workflow {
	declared := health.Declarations()
	declared.Workflows[0].Code = code
	metadata := &catalog.Metadata{Resources: []catalog.Resource{{Target: catalog.Target{Kind: catalog.Definition}, Field: catalog.Name, Text: catalog.Text{English: "Health"}}}}
	for _, mode := range declared.Workflows[0].Modes {
		metadata.Resources = append(metadata.Resources, catalog.Resource{Target: catalog.Target{Kind: catalog.Mode, Version: "1", Mode: mode.Code}, Field: catalog.Name, Text: catalog.Text{English: mode.Code}})
	}
	return Workflow{Declaration: declared, Metadata: metadata}
}
func TestProjectScopeAndIndependentFailures(t *testing.T) {
	good, bad := projectWorkflow("healthy"), projectWorkflow("broken")
	bad.Declaration.Workflows[0].Modes[0].Values = map[string]wf.Decimal{"attempt_timeout": "9999"}
	input := ProjectInput{Workflows: []Workflow{good, bad}}
	all, bundles, err := CheckProject(context.Background(), input, Selection{})
	if err != nil || all.Passed || !all.FullProject || all.Total != 2 || all.Selected != 2 || len(all.Workflows) != 2 || len(bundles) != 0 {
		t.Fatal("wrong full-project result", all, err)
	}
	if all.Workflows[0].Passed || !all.Workflows[1].Passed || all.Workflows[0].Metadata != "not_checked" || all.Workflows[1].Metadata != "passed" {
		t.Fatal("one failure prevented independent checks", all)
	}
	location := all.Workflows[0].DiagnosticIdentities[0]
	if location.Version != "1" || location.Mode != "standard" || location.Revision != "1" {
		t.Fatal("diagnostic lost version/mode identity", location)
	}
	selected, bundles, err := CheckProject(context.Background(), input, Selection{Workflow: "healthy"})
	if err != nil || !selected.Passed || selected.FullProject || selected.Selected != 1 || selected.Omitted != 1 || len(bundles) != 0 {
		t.Fatal("quick check claimed whole-project readiness", selected, err)
	}
	if len(good.Declaration.Workflows[0].Modes) != 2 || bad.Declaration.Workflows[0].Modes[0].Values["attempt_timeout"] != "9999" {
		t.Fatal("selection changed authoring data")
	}
}
func TestVersionModeAndMetadataSelection(t *testing.T) {
	value := projectWorkflow("health")
	version := health.Declarations().Workflows[0]
	version.Code, version.Version = "health", "2"
	value.Declaration.Workflows = append(value.Declaration.Workflows, version)
	for _, mode := range version.Modes {
		value.Metadata.Resources = append(value.Metadata.Resources, catalog.Resource{Target: catalog.Target{Kind: catalog.Mode, Version: "2", Mode: mode.Code}, Field: catalog.Name, Text: catalog.Text{English: mode.Code}})
	}
	value.Metadata.Resources = append(value.Metadata.Resources, catalog.Resource{Target: catalog.Target{Kind: catalog.Node, Version: "1", Node: "missing"}, Field: catalog.Name, Text: catalog.Text{English: "Bad owner"}})
	input := ProjectInput{Workflows: []Workflow{value}}
	all, _, err := CheckProject(context.Background(), input, Selection{})
	if err != nil || all.Passed || all.Workflows[0].Metadata != "failed" {
		t.Fatal("invalid metadata admitted", err)
	}
	selected, _, err := CheckProject(context.Background(), input, Selection{Workflow: "health", Version: "2", Mode: "patient", Revision: "1"})
	if err != nil || !selected.Passed || selected.Workflows[0].Declaration.Workflows != 1 || selected.Workflows[0].Declaration.Modes != 1 || selected.Workflows[0].Metadata != "passed" {
		t.Fatal("parent or metadata selection differs", selected, err)
	}
	if !reflect.DeepEqual(selected.Workflows[0].Versions, []string{"2"}) {
		t.Fatal("wrong selected versions")
	}
	selected, _, err = CheckProject(context.Background(), input, Selection{Workflow: "health", Version: "2"})
	if err != nil || !selected.Passed || selected.Workflows[0].Declaration.Modes != 2 {
		t.Fatal("version did not include all its modes", err)
	}
}
func TestProjectSelectionAdmission(t *testing.T) {
	first, second := projectWorkflow("same"), projectWorkflow("same")
	second.Declaration.Workflows[0].Namespace = "another"
	input := ProjectInput{Workflows: []Workflow{first, second}}
	for _, selection := range []Selection{{Workflow: "same"}, {Workflow: "same", Version: "1"}} {
		if _, _, err := CheckProject(context.Background(), input, selection); !errors.Is(err, command.ErrSelectionAmbiguous) {
			t.Fatal("Namespace was guessed", err)
		}
	}
	selected, _, err := CheckProject(context.Background(), input, Selection{Namespace: "example", Workflow: "same"})
	if err != nil || !selected.Passed || selected.Selected != 1 {
		t.Fatal(err)
	}
	for _, selection := range []Selection{
		{Workflow: "missing"}, {Namespace: "missing"}, {Namespace: "example", Workflow: "same", Version: "missing"},
		{Namespace: "example", Workflow: "same", Version: "1", Mode: "missing"},
		{Namespace: "example", Workflow: "same", Version: "1", Mode: "standard", Revision: "missing"},
	} {
		if _, _, err := CheckProject(context.Background(), input, selection); !errors.Is(err, command.ErrSelectionMissing) {
			t.Fatal("empty selection reported success", selection, err)
		}
	}
	for _, selection := range []Selection{{Version: "1"}, {Workflow: "same", Mode: "standard"}, {Workflow: "same", Version: "1", Revision: "1"}, {Namespace: "nul\x00"}} {
		if _, _, err := CheckProject(context.Background(), input, selection); !errors.Is(err, command.ErrUsage) {
			t.Fatal("invalid selection admitted", err)
		}
	}
	empty, _, err := CheckProject(context.Background(), ProjectInput{}, Selection{})
	if err != nil || !empty.Empty || empty.Passed || empty.Selected != 0 {
		t.Fatal("empty project claimed a workflow check", empty, err)
	}
	if _, _, err := CheckProject(context.Background(), ProjectInput{}, Selection{Workflow: "absent"}); !errors.Is(err, command.ErrSelectionMissing) {
		t.Fatal("empty targeted project accepted")
	}
}
func TestLegacyGroupingAndRetainedParent(t *testing.T) {
	first := health.Declarations()
	other := health.Declarations().Workflows[0]
	other.Code = "other"
	first.Workflows = append(first.Workflows, other)
	report, _, err := CheckProject(context.Background(), ProjectInput{Declarations: []wf.Declaration{first}}, Selection{Workflow: "other"})
	if err != nil || !report.Passed || report.Total != 2 || report.Omitted != 1 || report.Workflows[0].Metadata != "not_supplied" {
		t.Fatal(report, err)
	}
	original, conflicting := health.Declarations(), health.Declarations()
	conflicting.Workflows[0].Modes = nil
	limit := conflicting.Workflows[0].Spec.Limits
	limit.RunTimeoutMS = wf.Fixed("12000")
	conflicting.Workflows[0].Spec.Limits = limit
	input := ProjectInput{Declarations: []wf.Declaration{original, conflicting}}
	report, _, err = CheckProject(context.Background(), input, Selection{Workflow: "server-health", Version: "1", Mode: "standard"})
	if err != nil || report.Passed || len(report.Workflows[0].Declaration.Conflicts) == 0 || report.Workflows[0].Declaration.Conflicts[0].Identity.Mode != "" {
		t.Fatal("mode filtering discarded a conflicting parent", report, err)
	}
}
func TestProjectRegistryBoundsAndCopying(t *testing.T) {
	input := ProjectInput{Workflows: []Workflow{projectWorkflow("health")}}
	valid, bundles, err := CheckProject(context.Background(), input, Selection{})
	if err != nil || !valid.Passed || len(bundles) != 1 {
		t.Fatal(err)
	}
	before, _ := bundles[0].BytesCopy()
	input.Workflows[0].Declaration.Workflows[0].Modes[0].Values = map[string]wf.Decimal{"attempt_timeout": "1200"}
	after, _ := bundles[0].BytesCopy()
	if string(before) != string(after) {
		t.Fatal("returned inspection bundle aliases authoring")
	}
	if _, _, err := CheckProject(context.Background(), ProjectInput{Workflows: input.Workflows, Declarations: []wf.Declaration{health.Declarations()}}, Selection{}); !errors.Is(err, command.ErrUsage) {
		t.Fatal("two registries accepted")
	}
	if _, _, err := CheckProject(context.Background(), ProjectInput{Workflows: make([]Workflow, wf.MaxInputs+1)}, Selection{}); !errors.Is(err, command.ErrLimit) {
		t.Fatal("unbounded registry")
	}
	duplicate := projectWorkflow("health")
	if _, _, err := CheckProject(context.Background(), ProjectInput{Workflows: []Workflow{duplicate, duplicate}}, Selection{}); !errors.Is(err, command.ErrUsage) {
		t.Fatal("duplicate project registration accepted")
	}
	mixed := projectWorkflow("health")
	other := health.Declarations().Workflows[0]
	other.Code = "other"
	mixed.Declaration.Workflows = append(mixed.Declaration.Workflows, other)
	if _, _, err := CheckProject(context.Background(), ProjectInput{Workflows: []Workflow{mixed}}, Selection{}); !errors.Is(err, command.ErrUsage) {
		t.Fatal("one registration hid multiple workflows")
	}
	if _, _, err := CheckProject(nil, input, Selection{}); !errors.Is(err, command.ErrOptions) {
		t.Fatal("nil context accepted")
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("private-context")
	cancel(cause)
	if _, _, err := CheckProject(ctx, input, Selection{}); !errors.Is(err, command.ErrCanceled) || !errors.Is(err, cause) {
		t.Fatal("cancellation lost", err)
	}
}

func TestSelectedProjectByteAccounting(t *testing.T) {
	group := projectGroup{identity: workflowIdentity{"example", "server-health"}, parts: []wf.Declaration{health.Declarations()}, sources: []int{0}}
	used := wf.MaxInputBytes - 1
	if _, err := checkGroup(context.Background(), group, Selection{Workflow: "server-health"}, &used); !errors.Is(err, command.ErrLimit) || used != wf.MaxInputBytes-1 {
		t.Fatal("selected group bypassed aggregate budget", err)
	}
}

func TestSelectedMetadataCannotBypassInputCountBounds(t *testing.T) {
	value := projectWorkflow("health")
	value.Metadata.Resources = make([]catalog.Resource, catalog.MaxResources+1)
	if _, _, err := CheckProject(context.Background(), ProjectInput{Workflows: []Workflow{value}}, Selection{Workflow: "health", Version: "1"}); !errors.Is(err, command.ErrLimit) {
		t.Fatal("selector scanned an unbounded metadata input", err)
	}
}

func TestProjectAnalysisRetainsOnlyRequestedOutputs(t *testing.T) {
	input := ProjectInput{Workflows: []Workflow{projectWorkflow("zeta"), projectWorkflow("alpha")}}
	for _, mode := range []analysisMode{inspectProject, exportProject, prepareProject} {
		checked, err := analyzeProject(context.Background(), input, Selection{}, mode)
		if err != nil || !checked.report.Passed || checked.report.Workflows[0].Code != "alpha" {
			t.Fatal("shared project analysis changed", err)
		}
		if mode != exportProject && checked.bundles != nil || mode != prepareProject && checked.prepared != nil {
			t.Fatal("analysis retained another consumer's output")
		}
		if mode == exportProject {
			if len(checked.bundles) != len(input.Workflows) {
				t.Fatal("source bundles missing")
			}
			for index, source := range input.Workflows {
				want, err := wf.Compile(context.Background(), source.Declaration)
				if err != nil {
					t.Fatal(err)
				}
				expected, _ := want.BytesCopy()
				actual, _ := checked.bundles[index].BytesCopy()
				if string(actual) != string(expected) {
					t.Fatal("workflow sorting replaced original source order")
				}
			}
		}
		if mode == prepareProject {
			if len(checked.prepared) != 2 {
				t.Fatal("prepared outputs missing")
			}
			info, err := checked.prepared[0].Inspect()
			if err != nil || info.Code != "alpha" {
				t.Fatal("prepared/report order differs", err)
			}
		}
	}
	input.Workflows[1].Metadata = nil
	checked, err := analyzeProject(context.Background(), input, Selection{}, prepareProject)
	if err != nil || !checked.report.Passed || checked.prepared != nil {
		t.Fatal("missing metadata leaked a partial publication set", err)
	}
	input.Workflows[1].Declaration.Workflows[0].Spec.Limits.ActiveChunks = wf.Fixed("0")
	for _, mode := range []analysisMode{inspectProject, exportProject, prepareProject} {
		checked, err := analyzeProject(context.Background(), input, Selection{}, mode)
		if err != nil || checked.report.Passed || checked.bundles != nil || checked.prepared != nil || len(checked.report.Workflows) != 2 || !checked.report.Workflows[1].Passed {
			t.Fatal("failed group skipped later validation or retained output", err)
		}
	}
}

func TestLegacyOriginalSourceExport(t *testing.T) {
	first, second := projectWorkflow("zeta").Declaration, projectWorkflow("alpha").Declaration
	first.Workflows = append(first.Workflows, second.Workflows...)
	input := ProjectInput{Declarations: []wf.Declaration{first, second}}
	report, bundles, err := CheckProject(context.Background(), input, Selection{})
	if err != nil || !report.Passed || len(bundles) != len(input.Declarations) {
		t.Fatal("legacy source packaging changed", err)
	}
	for index, original := range input.Declarations {
		bundle, err := wf.Compile(context.Background(), original)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := bundle.BytesCopy()
		got, _ := bundles[index].BytesCopy()
		if string(got) != string(want) {
			t.Fatal("legacy inputs were regrouped or deduplicated during export")
		}
	}
}

func TestLegacyFullSourceAdmissionSurvivesGrouping(t *testing.T) {
	declared := health.Declarations()
	minimum, maximum := int64(0), int64(1024)
	choices := make([]string, 256)
	for index := range choices {
		choices[index] = fmt.Sprintf("%04d", index) + strings.Repeat("x", 1020)
	}
	properties := map[string]wf.DataSchema{}
	for index := range 14 {
		properties[fmt.Sprintf("field%02d", index)] = wf.DataSchema{Type: "string", MinLength: &minimum, MaxLength: &maximum, Enum: choices}
	}
	declared.Workflows[0].Spec.Contracts["target"] = wf.DataSchema{Type: "object", Properties: properties}
	declared.Workflows[0].Modes = nil
	full := wf.Declaration{}
	for index := range 5 {
		item := declared.Workflows[0]
		item.Code = fmt.Sprintf("workflow-%d", index)
		full.Workflows = append(full.Workflows, item)
	}
	input := ProjectInput{Declarations: []wf.Declaration{full}}
	selected, err := InspectProject(context.Background(), input, Selection{Workflow: "workflow-0"})
	if err != nil || !selected.Passed || selected.Selected != 1 || selected.Omitted != 4 {
		t.Fatal("legal selected group unexpectedly rejected", err)
	}
	for _, mode := range []analysisMode{inspectProject, exportProject} {
		checked, err := analyzeProject(context.Background(), input, Selection{}, mode)
		if !errors.Is(err, wf.ErrLimit) || checked.bundles != nil || checked.prepared != nil {
			t.Fatal("full source bypassed its original bundle bound", err)
		}
	}
}

type checkedContext struct {
	context.Context
	checks   int
	cancelAt int
	cancel   context.CancelFunc
}

func (ctx *checkedContext) Err() error {
	// Canonicalization may poll context in map-dependent order. Count only the
	// project pipeline's own checkpoints so the cancellation point is stable.
	var callers [1]uintptr
	if count := runtime.Callers(2, callers[:]); count > 0 {
		frame, _ := runtime.CallersFrames(callers[:count]).Next()
		if strings.HasSuffix(frame.Function, "/workflow.projectContext") {
			ctx.checks++
			if ctx.cancel != nil && ctx.checks == ctx.cancelAt {
				ctx.cancel()
			}
		}
	}
	return ctx.Context.Err()
}

func TestProjectFinalCancellationReturnsNoRetainedOutputs(t *testing.T) {
	input := ProjectInput{Workflows: []Workflow{projectWorkflow("healthy")}}
	for _, mode := range []analysisMode{inspectProject, exportProject, prepareProject} {
		baseline := &checkedContext{Context: context.Background()}
		if _, err := analyzeProject(baseline, input, Selection{}, mode); err != nil || baseline.checks < 2 {
			t.Fatal("context control failed", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		controlled := &checkedContext{Context: ctx, cancelAt: baseline.checks, cancel: cancel}
		checked, err := analyzeProject(controlled, input, Selection{}, mode)
		if ctx.Err() == nil {
			cancel()
			t.Fatal("final checkpoint cancellation probe did not fire")
		}
		cancel()
		if !errors.Is(err, context.Canceled) || checked.bundles != nil || checked.prepared != nil {
			t.Fatal("last cancellation checkpoint leaked retained outputs", err)
		}
	}
}

func FuzzProjectSelectionScope(f *testing.F) {
	for _, value := range [][5]string{
		{"", "", "", "", ""}, {"", "health", "", "", ""}, {"example", "health", "1", "patient", "1"}, {"", "missing", "", "", ""}, {"", "health", "", "patient", ""},
	} {
		f.Add(value[0], value[1], value[2], value[3], value[4])
	}
	input := ProjectInput{Workflows: []Workflow{projectWorkflow("health")}}
	f.Fuzz(func(t *testing.T, namespace, code, version, mode, revision string) {
		if len(namespace)+len(code)+len(version)+len(mode)+len(revision) > 1024 {
			return
		}
		selection := Selection{Namespace: namespace, Workflow: code, Version: version, Mode: mode, Revision: revision}
		report, bundles, err := CheckProject(context.Background(), input, selection)
		if err != nil {
			return
		}
		if !report.Passed || report.Total != 1 || report.Selected != 1 || len(report.Workflows) != 1 {
			t.Fatal("invalid successful scope")
		}
		if report.FullProject != (selection == (Selection{})) {
			t.Fatal("partial scope claimed full validation")
		}
		if !report.FullProject && len(bundles) != 0 {
			t.Fatal("partial scope returned build publication bundles")
		}
		if namespace != "" && namespace != "example" || code != "" && code != "health" || version != "" && version != "1" || mode != "" && mode != "standard" && mode != "patient" || revision != "" && revision != "1" {
			t.Fatal("unknown selector matched")
		}
	})
}
