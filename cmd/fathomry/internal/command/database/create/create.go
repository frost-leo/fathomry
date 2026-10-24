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

// Package create implements the explicit database create leaf.
package create

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/spf13/cobra"
)

// New borrows entry-owned creation; no configuration/SQL is loaded by construction.
func New(invocation *command.Invocation, run func(context.Context, string) (DatabaseChange, error)) *cobra.Command {
	var path string
	cmd := &cobra.Command{Use: "create", Short: "fathomry.database_create.command", Args: cobra.NoArgs}
	cmd.Flags().StringVar(&path, "admin-config", "", "fathomry.database_create.admin_config")
	invocation.Bind(cmd, func(ctx context.Context, _ []string) error {
		extension := strings.ToLower(filepath.Ext(path))
		if path == "" || len(path) > 4096 || extension != ".json" && extension != ".yaml" && extension != ".yml" && extension != ".toml" {
			return command.Fail(command.ErrUsage)
		}
		result, err := run(ctx, path)
		if err != nil && result.Effect == "" {
			return err
		}
		output := invocation.MutationResult("database.create", result, command.MutationSummary{Effect: string(result.Effect), State: result.Stage}, func(writer io.Writer) error {
			_, err := fmt.Fprintf(writer, "%s %s\n%s %s\n%s %s\n%s %t\n", invocation.Text("fathomry.database_create.effect"), result.Effect, invocation.Text("fathomry.database_create.schema_effect"), result.SchemaEffect, invocation.Text("fathomry.database_create.stage"), result.Stage, invocation.Text("fathomry.database_create.ready"), result.Ready)
			return err
		})
		if output != nil {
			return command.Fail(command.ErrExecution, err, output)
		}
		return err
	})
	return cmd
}
