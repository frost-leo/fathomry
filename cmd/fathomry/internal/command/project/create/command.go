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

package create

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/spf13/cobra"
)

// New constructs the official-only creation leaf without filesystem work.
func New(invocation *command.Invocation) *cobra.Command {
	value := options{}
	leaf := &cobra.Command{Use: "new", Short: "fathomry.project_creation.new", Args: cobra.NoArgs}
	leaf.Flags().StringVar(&value.Name, "name", "", "fathomry.project_creation.flag_name")
	leaf.Flags().StringVar(&value.Module, "module", "", "fathomry.project_creation.flag_module")
	leaf.Flags().StringVar(&value.FrameworkVersion, "framework-version", "", "fathomry.project_creation.flag_version")
	for _, name := range []string{"name", "module", "framework-version"} {
		_ = leaf.MarkFlagRequired(name)
	}
	invocation.Bind(leaf, func(ctx context.Context, _ []string) error {
		selected, err := prepare(ctx, value)
		if errors.Is(err, ErrInput) {
			return command.Fail(command.ErrUsage, err)
		}
		if err != nil {
			return stage(invocation, newReport(value), err)
		}
		if err := platform(runtime.GOOS); err != nil {
			return stage(invocation, newReport(value), err)
		}
		result, err := create(ctx, selected)
		return stage(invocation, result, err)
	})
	return leaf
}

func stage(invocation *command.Invocation, result report, creationError error) error {
	if errors.Is(creationError, ErrCanceled) {
		creationError = command.Fail(command.ErrCanceled, creationError)
	}
	effect := "none"
	if len(result.Directories) > 0 {
		effect = "created"
	}
	err := invocation.MutationResult("project.new", result,
		command.MutationSummary{Effect: effect, State: result.State}, func(output io.Writer) error {
			if _, err := fmt.Fprintf(output, "%s: %s\n%s: %s\n%s: %s\n",
				invocation.Text("fathomry.project_creation.target"), result.Target,
				invocation.Text("fathomry.project_creation.module"), result.ModulePath,
				invocation.Text("fathomry.project_creation.state"), invocation.Text("fathomry.project_creation.state_"+result.State)); err != nil {
				return err
			}
			for _, directory := range result.Directories {
				if _, err := fmt.Fprintf(output, "%s: %s\n", invocation.Text("fathomry.project_creation.directory"), directory); err != nil {
					return err
				}
			}
			for _, file := range result.Files {
				if _, err := fmt.Fprintf(output, "%s: %s (%s=%d, %s=%t)\n",
					invocation.Text("fathomry.project_creation.file"), file.Path,
					invocation.Text("fathomry.project_creation.bytes"), file.BytesWritten,
					invocation.Text("fathomry.project_creation.complete"), file.Complete); err != nil {
					return err
				}
			}
			return nil
		})
	if err == nil {
		return creationError
	}
	if creationError == nil {
		return err
	}
	return command.Fail(command.ErrExecution, errors.Join(creationError, err))
}
