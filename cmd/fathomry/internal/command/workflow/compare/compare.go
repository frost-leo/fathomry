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

// Package compare presents bounded read-only comparisons with the stored catalog.
package compare

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	workflows "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow"
	"github.com/spf13/cobra"
)

// New borrows an entry-owned lazy operation. Help/construction do no work.
func New(invocation *command.Invocation, run func(context.Context, workflows.Selection) (workflows.ComparisonReport, error)) *cobra.Command {
	var selection workflows.Selection
	cmd := &cobra.Command{Use: "compare", Short: "fathomry.workflow_compare.command", Args: cobra.NoArgs}
	cmd.Flags().StringVar(&selection.Namespace, "namespace", "", "fathomry.workflow_compare.namespace")
	cmd.Flags().StringVar(&selection.Workflow, "workflow", "", "fathomry.workflow_compare.workflow")
	cmd.Flags().StringVar(&selection.Version, "version", "", "fathomry.workflow_compare.version")
	cmd.Flags().StringVar(&selection.Mode, "mode", "", "fathomry.workflow_compare.mode")
	cmd.Flags().StringVar(&selection.Revision, "revision", "", "fathomry.workflow_compare.revision")
	invocation.Bind(cmd, func(ctx context.Context, _ []string) error {
		for _, flag := range []struct{ name, value string }{{"namespace", selection.Namespace}, {"workflow", selection.Workflow}, {"version", selection.Version}, {"mode", selection.Mode}, {"revision", selection.Revision}} {
			if cmd.Flags().Changed(flag.name) && flag.value == "" {
				return command.Fail(command.ErrUsage)
			}
		}
		if run == nil {
			return command.Fail(command.ErrOptions)
		}
		report, err := run(ctx, selection)
		if err != nil && !(errors.Is(err, command.ErrCheck) && !report.Checked) {
			return err
		}
		return invocation.CheckResult("workflow.compare", report, report.Complete && report.Equal, func(writer io.Writer) error {
			if _, err := fmt.Fprintf(writer, "%s %d/%d; %s %d\n", invocation.Text("fathomry.workflow_compare.compared"), report.Compared, report.Selected, invocation.Text("fathomry.workflow_compare.omitted"), report.Omitted); err != nil {
				return err
			}
			if !report.Checked {
				_, err := fmt.Fprintln(writer, invocation.Text("fathomry.workflow_compare.preflight"))
				return err
			}
			for _, item := range report.Workflows {
				if item.Result == nil {
					continue
				}
				state := "fathomry.workflow_compare.different"
				if item.Result.Equal {
					state = "fathomry.workflow_compare.equal"
				}
				if _, err := fmt.Fprintf(writer, "%q/%q: %s; %s %d; %s %d\n", item.Namespace, item.Code, invocation.Text(state), invocation.Text("fathomry.workflow_compare.missing"), item.Result.Missing, invocation.Text("fathomry.workflow_compare.changed"), item.Result.Different); err != nil {
					return err
				}
				for _, problem := range item.Result.Problems {
					if _, err := fmt.Fprintf(writer, "%s %s\n", problem.Kind, problem.Path); err != nil {
						return err
					}
				}
				if item.Result.Truncated {
					if _, err := fmt.Fprintln(writer, invocation.Text("fathomry.workflow_compare.truncated")); err != nil {
						return err
					}
				}
			}
			return nil
		})
	})
	return cmd
}
