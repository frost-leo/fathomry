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

// Package errorcatalog implements the official offline error atlas commands.
package errorcatalog

import (
	"context"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/spf13/cobra"
)

// New declares the error catalog commands without acquiring resources.
func New(invocation *command.Invocation) *cobra.Command {
	root := invocation.Group("error", "fathomry.error_catalog.root")
	var input listOptions
	list := &cobra.Command{Use: "list", Short: "fathomry.error_catalog.list", Args: cobra.NoArgs}
	bindOwners(list, &input.owners)
	list.Flags().StringVar(&input.query, "query", "", "fathomry.error_catalog.flag_query")
	invocation.Bind(list, func(ctx context.Context, _ []string) error {
		request := input
		request.locale = invocation.Locale()
		result, err := listErrors(ctx, invocation.Catalogs(), request)
		if err != nil {
			return err
		}
		return invocation.Result("error.list", result, func(writer io.Writer) error {
			return writeList(ctx, writer, result)
		})
	})
	explain := &cobra.Command{Use: "explain CODE_OR_IDENTIFIER", Short: "fathomry.error_catalog.explain", Args: cobra.ExactArgs(1)}
	invocation.Bind(explain, func(ctx context.Context, args []string) error {
		result, err := explainError(ctx, invocation.Catalogs(), args[0], invocation.Locale())
		if err != nil {
			return err
		}
		return invocation.Result("error.explain", result, func(writer io.Writer) error {
			return writeExplanation(writer, invocation, result)
		})
	})
	var owners command.OwnerFilter
	components := &cobra.Command{Use: "components", Short: "fathomry.error_catalog.components", Args: cobra.NoArgs}
	bindOwners(components, &owners)
	invocation.Bind(components, func(ctx context.Context, _ []string) error {
		result, err := listComponents(ctx, invocation.Catalogs().Errors, owners)
		if err != nil {
			return err
		}
		return invocation.Result("error.components", result, func(writer io.Writer) error {
			return writeComponents(writer, result)
		})
	})
	root.AddCommand(list, explain, components)
	return root
}

func bindOwners(cmd *cobra.Command, owners *command.OwnerFilter) {
	cmd.Flags().StringVar(&owners.Module, "module", "", "fathomry.error_catalog.flag_module")
	cmd.Flags().StringVar(&owners.Component, "component", "", "fathomry.error_catalog.flag_component")
}
