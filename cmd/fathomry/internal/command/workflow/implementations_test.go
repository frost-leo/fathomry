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
	json "encoding/json/v2"
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
	implementation "github.com/frost-leo/fathomry/framework/workflow/implementation/v1"
)

type bindingInput struct {
	Target string `json:"target"`
}
type bindingOutput struct {
	Value int64 `json:"value"`
}
type bindingSummaryInput struct {
	CPU    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
}
type bindingSummaryOutput struct {
	Text string `json:"text"`
}

func testImplementations() []implementation.Binding {
	metric := implementation.Activity(func(context.Context, bindingInput) (bindingOutput, error) { panic("inspection invoked handler") })
	summary := implementation.Activity(func(context.Context, bindingSummaryInput) (bindingSummaryOutput, error) {
		panic("inspection invoked handler")
	})
	return []implementation.Binding{{Key: "read_cpu", Callable: metric}, {Key: "read_memory", Callable: metric}, {Key: "summarize", Callable: summary}}
}
func implementationProject() ProjectInput {
	first, second := health.Declarations(), health.Declarations()
	second.Workflows[0].Version = "2"
	source := declaration.Declaration{Workflows: append(first.Workflows, second.Workflows...)}
	return ProjectInput{Workflows: []Workflow{{Declaration: source, Implementations: []implementation.VersionBindings{{Version: "1", Bindings: testImplementations()}}}}}
}
func TestImplementationSelection(t *testing.T) {
	ctx := context.Background()
	input := implementationProject()
	input.Workflows[0].Implementations[0].Bindings[0].Callable = implementation.Callable{}
	report, err := InspectProject(ctx, input, Selection{Namespace: "example", Workflow: "server-health", Version: "2"})
	if err != nil || !report.Passed || report.TotalVersions != 2 || report.SelectedVersions != 1 || report.OmittedVersions != 1 {
		t.Fatalf("%+v %v", report, err)
	}
	rows := report.Workflows[0].Implementations
	if len(rows) != 2 || rows[0].Status != "not_checked" || !rows[0].Supplied || rows[0].RequirementsKnown || rows[1].Status != "not_supplied" || !rows[1].RequirementsKnown {
		t.Fatalf("%+v", rows)
	}
	if rows[1].RequiredBindings != 3 || rows[1].CallSites != 3 {
		t.Fatal("wrong obligations")
	}
	report, bundles, err := CheckProject(ctx, input, Selection{})
	if err != nil || report.Passed || bundles != nil || report.Workflows[0].Implementations[0].Status != "failed" {
		t.Fatalf("%+v %v", report, err)
	}
}
func TestImplementationStructureBeforeSelection(t *testing.T) {
	for _, kind := range []string{"foreign_version", "foreign_key", "duplicate_version", "duplicate_key"} {
		t.Run(kind, func(t *testing.T) {
			input := implementationProject()
			switch kind {
			case "foreign_version":
				input.Workflows[0].Implementations[0].Version = "3"
			case "foreign_key":
				input.Workflows[0].Implementations[0].Bindings[0].Key = "missing"
			case "duplicate_version":
				input.Workflows[0].Implementations = append(input.Workflows[0].Implementations, input.Workflows[0].Implementations[0])
			case "duplicate_key":
				input.Workflows[0].Implementations[0].Bindings = append(input.Workflows[0].Implementations[0].Bindings, input.Workflows[0].Implementations[0].Bindings[0])
			}
			if _, err := InspectProject(context.Background(), input, Selection{Namespace: "example", Workflow: "server-health", Version: "2"}); err == nil {
				t.Fatal("structural error hidden by selection")
			}
		})
	}
}
func TestImplementationCompleteAndEmptyClaim(t *testing.T) {
	input := implementationProject()
	ctx := context.Background()
	report, bundles, err := CheckProject(ctx, input, Selection{})
	if err != nil || !report.Passed || len(bundles) != 1 {
		t.Fatalf("%+v %v", report, err)
	}
	rows := report.Workflows[0].Implementations
	if rows[0].Status != "complete" || rows[0].Manifest == nil || rows[1].Status != "not_supplied" {
		t.Fatalf("%+v", rows)
	}
	ordinary, err := InspectProject(ctx, input, Selection{})
	if err != nil || !ordinary.Passed || ordinary.Workflows[0].Implementations[0].Manifest != nil {
		t.Fatal("vet retained build manifest", err)
	}
	input.Workflows[0].Implementations[0].Bindings = nil
	report, err = InspectProject(ctx, input, Selection{})
	if err != nil || report.Passed || report.Workflows[0].Implementations[0].Status != "failed" {
		t.Fatal("empty claim became absent", err)
	}
}

func TestImplementationReportUnvalidatedIdentities(t *testing.T) {
	source := health.Declarations()
	canary := strings.Repeat("private-version", 128)
	source.Workflows[0].Version = canary
	source.Workflows[0].Namespace = "private-namespace\x1b"
	report, err := InspectProject(context.Background(), ProjectInput{Declarations: []declaration.Declaration{source}}, Selection{})
	if err != nil || report.Passed || report.TotalVersions != 1 || report.SelectedVersions != 1 {
		t.Fatal("invalid declaration scope lost", err)
	}
	row := report.Workflows[0].Implementations[0]
	if row.Identity.Namespace != "" || row.Identity.Version != "" || row.Status != "not_checked" || row.RequirementsKnown {
		t.Fatal("unvalidated identity escaped the bounded display boundary")
	}
	raw, err := json.Marshal(report)
	if err != nil || strings.Contains(string(raw), canary) || strings.Contains(string(raw), "private-namespace") {
		t.Fatal("private identity leaked", err)
	}
}

type postBindingCancellationContext struct {
	context.Context
	checks int
	cancel context.CancelCauseFunc
	cause  error
}

func (ctx *postBindingCancellationContext) Err() error {
	// Cancel at the post-Prepare checkpoint without replacing the observed error.
	var callers [2]uintptr
	if count := runtime.Callers(2, callers[:]); count == 2 {
		frames := runtime.CallersFrames(callers[:count])
		first, _ := frames.Next()
		second, _ := frames.Next()
		if strings.HasSuffix(first.Function, "/workflow.projectContext") && strings.HasSuffix(second.Function, "/workflow.checkImplementations") {
			ctx.checks++
			if ctx.checks == 2 {
				ctx.cancel(ctx.cause)
			}
		}
	}
	return ctx.Context.Err()
}

func TestImplementationCancellationRetainsFailure(t *testing.T) {
	for _, test := range []struct {
		name     string
		callable implementation.Callable
		code     error
	}{
		{"invalid", implementation.Callable{}, implementation.ErrInvalid},
		{"unsupported", implementation.Activity(func(context.Context, struct {
			Target any `json:"target"`
		}) (bindingOutput, error) {
			panic("handler invoked")
		}), implementation.ErrUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			bindings := testImplementations()
			bindings[0].Callable = test.callable
			input := ProjectInput{Workflows: []Workflow{{Declaration: health.Declarations(), Implementations: []implementation.VersionBindings{{Version: "1", Bindings: bindings}}}}}
			control, err := InspectProject(context.Background(), input, Selection{})
			if err != nil || control.Passed || control.Workflows[0].Implementations[0].Diagnostic == nil || !errors.Is(control.Workflows[0].Implementations[0].Diagnostic.Code, test.code) {
				t.Fatal("control did not observe the binding failure", err)
			}
			base, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("private_post_binding_cancellation")
			ctx := &postBindingCancellationContext{Context: base, cancel: cancel, cause: cause}
			_, err = InspectProject(ctx, input, Selection{})
			if ctx.checks < 2 || !errors.Is(err, command.ErrCanceled) || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || !errors.Is(err, test.code) {
				t.Fatalf("cancellation or observed binding failure lost: %v", err)
			}
			if strings.Contains(err.Error(), cause.Error()) {
				t.Fatal("cancellation detail leaked")
			}
		})
	}
}
