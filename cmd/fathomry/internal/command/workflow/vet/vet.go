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

// Package vet presents offline project selection or explicit Bundle inspection,
// without project discovery, configuration loading or service access.
package vet

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	projectworkflows "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/spf13/cobra"
)

// New declares the workflow vet command without acquiring files or settings.
func New(invocation *command.Invocation, project func(context.Context, projectworkflows.Selection) (projectworkflows.ProjectReport, error)) *cobra.Command {
	var selection projectworkflows.Selection
	cmd := &cobra.Command{Use: "vet [BUNDLE...]", Short: "fathomry.workflow_vet.command", Args: cobra.MaximumNArgs(wf.MaxInputs)}
	cmd.Flags().StringVar(&selection.Namespace, "namespace", "", "fathomry.workflow_vet.flag_namespace")
	cmd.Flags().StringVar(&selection.Workflow, "workflow", "", "fathomry.workflow_vet.flag_workflow")
	cmd.Flags().StringVar(&selection.Version, "version", "", "fathomry.workflow_vet.flag_version")
	cmd.Flags().StringVar(&selection.Mode, "mode", "", "fathomry.workflow_vet.flag_mode")
	cmd.Flags().StringVar(&selection.Revision, "revision", "", "fathomry.workflow_vet.flag_revision")
	invocation.Bind(cmd, func(ctx context.Context, args []string) error {
		for _, flag := range []struct{ name, value string }{{"namespace", selection.Namespace}, {"workflow", selection.Workflow}, {"version", selection.Version}, {"mode", selection.Mode}, {"revision", selection.Revision}} {
			if cmd.Flags().Changed(flag.name) && (flag.value == "" || len(args) > 0) {
				return command.Fail(command.ErrUsage)
			}
		}
		if len(args) == 0 {
			if project == nil {
				return command.Fail(command.ErrUsage)
			}
			report, err := project(ctx, selection)
			if err != nil {
				return err
			}
			return invocation.CheckResult("workflow.vet", report, report.Passed, func(writer io.Writer) error { return renderProject(invocation, writer, report) })
		}
		report, err := inspect(ctx, args, readInput)
		if err != nil {
			return err
		}
		return invocation.CheckResult("workflow.vet", report, report.Passed, func(writer io.Writer) error {
			status := "fathomry.workflow_vet.failed"
			if report.Passed {
				status = "fathomry.workflow_vet.passed"
			}
			if _, err := fmt.Fprintln(writer, invocation.Text(status)); err != nil {
				return err
			}
			for _, entry := range report.Entries {
				id := entry.Identity
				if _, err := fmt.Fprintf(writer, "%q %q %q %q %q %s\n", id.Namespace, id.Code, id.Version, id.Mode, id.Revision, entry.SHA256); err != nil {
					return err
				}
			}
			for _, diagnostic := range report.Diagnostics {
				if _, err := fmt.Fprintf(writer, "%d %s %s %s\n", diagnostic.Input, diagnostic.Diagnostic.Code.String(), diagnostic.Diagnostic.Reason, diagnostic.Diagnostic.Path); err != nil {
					return err
				}
			}
			for _, conflict := range report.Conflicts {
				id := conflict.Identity
				if _, err := fmt.Fprintf(writer, "%q %q %q %q %q %s\n", id.Namespace, id.Code, id.Version, id.Mode, id.Revision, strings.Join(conflict.Paths, ", ")); err != nil {
					return err
				}
				if conflict.PathsTruncated {
					if _, err := fmt.Fprintln(writer, invocation.Text("fathomry.workflow_vet.truncated")); err != nil {
						return err
					}
				}
			}
			if report.Truncated {
				_, err := fmt.Fprintln(writer, invocation.Text("fathomry.workflow_vet.truncated"))
				return err
			}
			return nil
		})
	})
	return cmd
}

func renderProject(invocation *command.Invocation, writer io.Writer, report projectworkflows.ProjectReport) error {
	scope := "fathomry.workflow_vet.scope_selected"
	if report.FullProject {
		scope = "fathomry.workflow_vet.scope_all"
	}
	if _, err := fmt.Fprintln(writer, invocation.Text(scope)); err != nil {
		return err
	}
	if report.Empty {
		_, err := fmt.Fprintln(writer, invocation.Text("fathomry.workflow_vet.empty_project"))
		return err
	}
	if !report.FullProject {
		selection := report.Selection
		if _, err := fmt.Fprintf(writer, "namespace=%q workflow=%q version=%q mode=%q revision=%q\n", selection.Namespace, selection.Workflow, selection.Version, selection.Mode, selection.Revision); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(writer, "%s %d/%d; %s %d\n", invocation.Text("fathomry.workflow_vet.checked"), report.Selected, report.Total, invocation.Text("fathomry.workflow_vet.omitted"), report.Omitted); err != nil {
		return err
	}
	for _, item := range report.Workflows {
		status := "fathomry.workflow_vet.failed"
		if item.Passed {
			status = "fathomry.workflow_vet.passed"
		}
		if _, err := fmt.Fprintf(writer, "%s %q/%q %q; %s\n", invocation.Text(status), item.Namespace, item.Code, item.Versions, invocation.Text("fathomry.workflow_vet.metadata_"+item.Metadata)); err != nil {
			return err
		}
		for index, problem := range item.Declaration.Diagnostics {
			var identity wf.Identity
			if index < len(item.DiagnosticIdentities) {
				identity = item.DiagnosticIdentities[index]
			}
			if _, err := fmt.Fprintf(writer, "%d %q %q %q %s %s %s\n", problem.Input, identity.Version, identity.Mode, identity.Revision, problem.Diagnostic.Code.String(), problem.Diagnostic.Reason, problem.Diagnostic.Path); err != nil {
				return err
			}
		}
		for _, conflict := range item.Declaration.Conflicts {
			if _, err := fmt.Fprintf(writer, "%q %q %q %s\n", conflict.Identity.Version, conflict.Identity.Mode, conflict.Identity.Revision, strings.Join(conflict.Paths, ", ")); err != nil {
				return err
			}
		}
		if item.MetadataDiagnostic != nil {
			problem := item.MetadataDiagnostic
			if _, err := fmt.Fprintf(writer, "%s %s %s\n", problem.Code.String(), problem.Reason, problem.Path); err != nil {
				return err
			}
		}
		for _, binding := range item.Implementations {
			if _, err := fmt.Fprintf(writer, "%q: %s", binding.Identity.Version, invocation.Text("fathomry.workflow_vet.implementation_"+binding.Status)); err != nil {
				return err
			}
			if binding.RequirementsKnown {
				if _, err := fmt.Fprintf(writer, "; %s %d/%d/%d", invocation.Text("fathomry.workflow_vet.implementation_counts"), binding.SuppliedBindings, binding.RequiredBindings, binding.CallSites); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintln(writer); err != nil {
				return err
			}
			if binding.Diagnostic != nil {
				if _, err := fmt.Fprintf(writer, "%s %s %s\n", binding.Diagnostic.Code, binding.Diagnostic.Reason, binding.Diagnostic.Path); err != nil {
					return err
				}
			}
		}
	}
	if _, err := fmt.Fprintf(writer, "%s %d/%d; %s %d\n", invocation.Text("fathomry.workflow_vet.implementation_versions"), report.SelectedVersions, report.TotalVersions, invocation.Text("fathomry.workflow_vet.omitted"), report.OmittedVersions); err != nil {
		return err
	}
	if report.Truncated {
		_, err := fmt.Fprintln(writer, invocation.Text("fathomry.workflow_vet.truncated"))
		return err
	}
	return nil
}

func inspect(ctx context.Context, paths []string, read func(context.Context, string, int) ([]byte, error)) (wf.Report, error) {
	if len(paths) == 0 || len(paths) > wf.MaxInputs {
		return wf.Report{}, command.Fail(command.ErrUsage)
	}
	total := 0
	bundles := make([]*wf.Bundle, 0, len(paths))
	diagnostics := []wf.InputDiagnostic{}
	for index, path := range paths {
		raw, err := read(ctx, path, min(wf.MaxBundleBytes, wf.MaxInputBytes-total))
		if err != nil {
			return wf.Report{}, err
		}
		if len(raw) > wf.MaxInputBytes-total {
			return wf.Report{}, command.Fail(command.ErrLimit)
		}
		total += len(raw)
		bundle, err := wf.Parse(ctx, raw)
		if err != nil {
			if ctx.Err() != nil {
				return wf.Report{}, command.Fail(command.ErrCanceled, ctx.Err(), context.Cause(ctx), err)
			}
			diagnostic, completed := wf.Diagnose(err)
			if !completed {
				return wf.Report{}, err
			}
			diagnostics = append(diagnostics, wf.InputDiagnostic{Input: index, Diagnostic: diagnostic})
			continue
		}
		bundles = append(bundles, bundle)
	}
	report := wf.Report{Inputs: len(paths), Entries: []wf.Entry{}, Conflicts: []wf.Conflict{}}
	if len(bundles) > 0 {
		var err error
		report, err = wf.Check(ctx, bundles...)
		if err != nil {
			if ctx.Err() != nil {
				return wf.Report{}, command.Fail(command.ErrCanceled, ctx.Err(), context.Cause(ctx), err)
			}
			return wf.Report{}, err
		}
	}
	report.Inputs = len(paths)
	report.Diagnostics = diagnostics
	if len(diagnostics) > 0 {
		report.Passed = false
	}
	return report, nil
}
