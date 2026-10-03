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

package project

import (
	"context"
	"fmt"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/spf13/cobra"
)

// New declares the official offline project-creation command.
func New(invocation *command.Invocation) *cobra.Command {
	input := request{}
	root := &cobra.Command{Use: "new PROJECT", Short: "fathomry.project_creation.new", Args: cobra.ExactArgs(1)}
	root.Flags().StringVar(&input.module, "module", "", "fathomry.project_creation.module")
	root.Flags().StringVar(&input.name, "name", "", "fathomry.project_creation.name")
	root.Flags().StringVar(&input.mode, "config-source", "local", "fathomry.project_creation.source")
	root.Flags().StringVar(&input.provider, "config-provider", "viper", "fathomry.project_creation.provider")
	root.Flags().StringVar(&input.encoding, "config-format", "yaml", "fathomry.project_creation.format")
	root.Flags().StringVar(&input.frameworkSource, "framework-source", "", "fathomry.project_creation.framework_source")
	invocation.Bind(root, func(ctx context.Context, args []string) error {
		input.destination = args[0]
		if root.Flags().Changed("config-provider") && input.provider == "" {
			return command.Fail(command.ErrUsage)
		}
		tree, err := prepare(input)
		if err != nil {
			return err
		}
		if err := create(ctx, tree, writeProjectFile); err != nil {
			return err
		}
		result := struct {
			Destination string `json:"destination"`
			Module      string `json:"module"`
			Name        string `json:"name"`
			Source      string `json:"config_source"`
			Provider    string `json:"config_provider"`
			Encoding    string `json:"config_format"`
			Files       int    `json:"files"`
			Status      string `json:"status"`
		}{tree.destination, tree.module, tree.name, tree.mode, tree.provider, tree.encoding, len(tree.files), "created"}
		return invocation.Result("new", result, func(writer io.Writer) error {
			_, err := fmt.Fprintf(writer, "%s %q\n", invocation.Text("fathomry.project_creation.created"), tree.destination)
			return err
		})
	})
	return root
}
