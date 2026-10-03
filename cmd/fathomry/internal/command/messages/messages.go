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

// Package messages implements offline translation-resource inspection.
package messages

import (
	"context"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/spf13/cobra"
)

// New declares message commands without reading application settings or files.
func New(invocation *command.Invocation) *cobra.Command {
	root := invocation.Group("i18n", "fathomry.message_catalog.root")
	root.AddCommand(listCommand(invocation), showCommand(invocation), localesCommand(invocation),
		renderCommand(invocation), coverageCommand(invocation))
	return root
}

func bindOwners(cmd *cobra.Command, owners *command.OwnerFilter) {
	cmd.Flags().StringVar(&owners.Module, "module", "", "fathomry.message_catalog.flag_module")
	cmd.Flags().StringVar(&owners.Component, "component", "", "fathomry.message_catalog.flag_component")
}

func listCommand(invocation *command.Invocation) *cobra.Command {
	cmd := &cobra.Command{Use: "list", Short: "fathomry.message_catalog.list", Args: cobra.NoArgs}
	var input listOptions
	bindOwners(cmd, &input.owners)
	var locale string
	cmd.Flags().StringVar(&locale, "locale", "", "fathomry.message_catalog.flag_resource_locale")
	cmd.Flags().StringVar(&input.query, "query", "", "fathomry.message_catalog.flag_query")
	invocation.Bind(cmd, func(ctx context.Context, _ []string) error {
		request := input
		if cmd.Flags().Changed("locale") {
			request.locale = &locale
		}
		result, err := listMessages(ctx, invocation.Catalogs().Messages, request)
		if err != nil {
			return err
		}
		return invocation.Result("i18n.list", result, func(writer io.Writer) error {
			return writeList(ctx, writer, result)
		})
	})
	return cmd
}

func showCommand(invocation *command.Invocation) *cobra.Command {
	cmd := &cobra.Command{Use: "show MESSAGE_ID", Short: "fathomry.message_catalog.show", Args: cobra.ExactArgs(1)}
	var locale string
	cmd.Flags().StringVar(&locale, "locale", "", "fathomry.message_catalog.flag_locale")
	invocation.Bind(cmd, func(ctx context.Context, args []string) error {
		selected := invocation.Locale()
		if cmd.Flags().Changed("locale") {
			selected = locale
		}
		result, err := lookupMessage(ctx, invocation.Catalogs().Messages, args[0], selected)
		if err != nil {
			return err
		}
		return invocation.Result("i18n.show", result, func(writer io.Writer) error {
			return writeLookup(writer, invocation, args[0], result)
		})
	})
	return cmd
}

func localesCommand(invocation *command.Invocation) *cobra.Command {
	cmd := &cobra.Command{Use: "locales", Short: "fathomry.message_catalog.locales", Args: cobra.NoArgs}
	var owners command.OwnerFilter
	bindOwners(cmd, &owners)
	invocation.Bind(cmd, func(ctx context.Context, _ []string) error {
		result, err := inspectLocales(ctx, invocation.Catalogs().Messages, owners)
		if err != nil {
			return err
		}
		return invocation.Result("i18n.locales", result, func(writer io.Writer) error {
			return writeLocales(writer, invocation, result)
		})
	})
	return cmd
}

func coverageCommand(invocation *command.Invocation) *cobra.Command {
	cmd := &cobra.Command{Use: "coverage LOCALE", Short: "fathomry.message_catalog.coverage", Args: cobra.ExactArgs(1)}
	var owners command.OwnerFilter
	bindOwners(cmd, &owners)
	var strict bool
	cmd.Flags().BoolVar(&strict, "strict", false, "fathomry.message_catalog.flag_strict")
	invocation.Bind(cmd, func(ctx context.Context, args []string) error {
		result, err := inspectCoverage(ctx, invocation.Catalogs().Messages, owners, args[0])
		if err != nil {
			return err
		}
		return invocation.CheckResult("i18n.coverage", result, !strict || result.Complete, func(writer io.Writer) error {
			return writeCoverage(writer, invocation, result)
		})
	})
	return cmd
}

func renderCommand(invocation *command.Invocation) *cobra.Command {
	cmd := &cobra.Command{Use: "render MESSAGE_ID", Short: "fathomry.message_catalog.render", Args: cobra.ExactArgs(1)}
	var locale, count string
	var input renderOptions
	cmd.Flags().StringVar(&locale, "locale", "", "fathomry.message_catalog.flag_render_locale")
	cmd.Flags().StringArrayVar(&input.arguments, "arg", nil, "fathomry.message_catalog.flag_argument")
	cmd.Flags().StringVar(&count, "count", "", "fathomry.message_catalog.flag_count")
	invocation.Bind(cmd, func(ctx context.Context, args []string) error {
		selected := invocation.Locale()
		if cmd.Flags().Changed("locale") {
			selected = locale
		}
		request := input
		if cmd.Flags().Changed("count") {
			request.count = &count
		}
		result, err := renderMessage(ctx, invocation.Catalogs().Messages, args[0], selected, request)
		if err != nil {
			return err
		}
		return invocation.Result("i18n.render", result, func(writer io.Writer) error {
			return writeRendered(writer, invocation, result)
		})
	})
	return cmd
}
