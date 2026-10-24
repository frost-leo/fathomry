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

// Package register implements explicit project workflow publication.
package register

import (
	"context"
	"fmt"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	workflows "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow"
	"github.com/spf13/cobra"
)

// New borrows a lazy entry-owned operation; constructing/help never resolves DBs.
func New(invocation *command.Invocation, run func(context.Context, workflows.Selection) (workflows.PublicationReport, error)) *cobra.Command {
	var selection workflows.Selection
	cmd := &cobra.Command{Use: "register", Short: "fathomry.workflow_register.command", Args: cobra.NoArgs}
	cmd.Flags().StringVar(&selection.Namespace, "namespace", "", "fathomry.workflow_register.flag_namespace")
	cmd.Flags().StringVar(&selection.Workflow, "workflow", "", "fathomry.workflow_register.flag_workflow")
	cmd.Flags().StringVar(&selection.Version, "version", "", "fathomry.workflow_register.flag_version")
	cmd.Flags().StringVar(&selection.Mode, "mode", "", "fathomry.workflow_register.flag_mode")
	cmd.Flags().StringVar(&selection.Revision, "revision", "", "fathomry.workflow_register.flag_revision")
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
		if report.Format == "" {
			return err
		}
		state := "incomplete"
		if report.Complete {
			state = "complete"
		} else if report.Attempted == 0 {
			state = "not_started"
		}
		output := invocation.MutationResult("workflow.register", report, command.MutationSummary{Effect: report.Effect, State: state}, func(writer io.Writer) error {
			if _, err := fmt.Fprintf(writer, "%s %d/%d; %s %d\n", invocation.Text("fathomry.workflow_register.attempted"), report.Attempted, report.Selected, invocation.Text("fathomry.workflow_register.omitted"), report.Omitted); err != nil {
				return err
			}
			for _, item := range report.Workflows {
				effect, ack := "not_started", "unobserved"
				if item.Result != nil {
					effect, ack = string(item.Result.Effect), string(item.Result.Acknowledgement)
				}
				if _, err := fmt.Fprintf(writer, "%q/%q: %s; %s %s\n", item.Namespace, item.Code, effect, invocation.Text("fathomry.workflow_register.acknowledgement"), ack); err != nil {
					return err
				}
			}
			if report.Effect == "unknown" {
				_, err := fmt.Fprintln(writer, invocation.Text("fathomry.workflow_register.unknown"))
				return err
			}
			if !report.Checked {
				_, err := fmt.Fprintln(writer, invocation.Text("fathomry.workflow_register.preflight"))
				return err
			}
			return nil
		})
		if output != nil {
			return command.Fail(command.ErrExecution, err, output)
		}
		return err
	})
	return cmd
}
