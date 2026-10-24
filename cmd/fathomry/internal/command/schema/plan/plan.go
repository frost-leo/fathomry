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

// Package plan implements the explicit schema plan leaf.
package plan

import (
	"context"

	"fmt"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	schemaapi "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema"
	"github.com/spf13/cobra"
)

// New borrows an invocation and a lazy entry-owned operation, without acquiring resources.
func New(invocation *command.Invocation, run func(context.Context) (schemaapi.Plan, error)) *cobra.Command {
	cmd := &cobra.Command{Use: "plan", Short: "fathomry.schema_plan.command", Args: cobra.NoArgs}
	invocation.Bind(cmd, func(ctx context.Context, _ []string) error {
		result, err := run(ctx)
		if err != nil {
			return err
		}
		return invocation.Result("schema.plan", result, func(writer io.Writer) error {
			if _, err := fmt.Fprintf(writer, "%s %s\n%s %t\n", invocation.Text("fathomry.schema_plan.state"), result.Status.State, invocation.Text("fathomry.schema_plan.applicable"), result.Applicable); err != nil {
				return err
			}
			for _, step := range result.Steps {
				if _, err := fmt.Fprintf(writer, "%s %s\n", step.ID, step.SHA256); err != nil {
					return err
				}
			}
			return nil
		})
	})
	return cmd
}
