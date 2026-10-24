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

// Package status implements the explicit schema status leaf.
package status

import (
	"context"

	"fmt"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	schemaapi "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema"
	"github.com/spf13/cobra"
)

// New borrows an invocation and a lazy entry-owned operation, without acquiring resources.
func New(invocation *command.Invocation, run func(context.Context) (schemaapi.Status, error)) *cobra.Command {
	cmd := &cobra.Command{Use: "status", Short: "fathomry.schema_status.command", Args: cobra.NoArgs}
	invocation.Bind(cmd, func(ctx context.Context, _ []string) error {
		result, err := run(ctx)
		if err != nil {
			return err
		}
		return invocation.Result("schema.status", result, func(writer io.Writer) error {
			_, err := fmt.Fprintf(writer, "%s %s\n", invocation.Text("fathomry.schema_status.state"), result.State)
			return err
		})
	})
	return cmd
}
