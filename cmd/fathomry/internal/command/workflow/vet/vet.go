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

// Package vet implements explicit offline bundle inspection, not project hosting.
package vet

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/spf13/cobra"
)

// New declares the workflow vet command without acquiring files or settings.
func New(invocation *command.Invocation) *cobra.Command {
	cmd := &cobra.Command{Use: "vet BUNDLE [BUNDLE...]", Short: "fathomry.workflow_vet.command", Args: cobra.RangeArgs(1, wf.MaxInputs)}
	invocation.Bind(cmd, func(ctx context.Context, args []string) error {
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
