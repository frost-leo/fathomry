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
	"fmt"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/spf13/cobra"
)

type explanation struct {
	Definition      failure.Definition `json:"definition"`
	Message         string             `json:"message"`
	RequestedLocale string             `json:"requested_locale"`
	Locale          string             `json:"locale"`
	Fallback        string             `json:"fallback,omitempty"`
}

// New registers only commands; it opens no source or runtime.
func New(invocation *command.Invocation) *cobra.Command {
	root := invocation.Group("error", "fathomry.command_line.error_group")
	var module, component string
	list := &cobra.Command{Use: "list", Short: "fathomry.command_line.error_list", Args: cobra.NoArgs}
	list.Flags().StringVar(&module, "module", "", "fathomry.command_line.flag_module")
	list.Flags().StringVar(&component, "component", "", "fathomry.command_line.flag_component")
	invocation.Bind(list, func(ctx context.Context, _ []string) error {
		if (module == "") != (component == "") {
			return command.Fail(command.ErrUsage)
		}
		var definitions []failure.Definition
		var err error
		if module == "" {
			definitions, err = invocation.Catalogs().Errors.Inspect()
		} else {
			var found bool
			definitions, found, err = invocation.Catalogs().Errors.InComponent(module, component)
			if err != nil {
				return command.Fail(command.ErrUsage, err)
			}
			if !found {
				return command.Fail(command.ErrNotFound)
			}
		}
		if err != nil {
			return err
		}
		return invocation.Result("error.list", definitions, func(writer io.Writer) error {
			for _, definition := range definitions {
				if err := ctx.Err(); err != nil {
					return err
				}
				value, found, err := invocation.Catalogs().Messages.Explain(definition.Code, invocation.Locale())
				if err != nil {
					return err
				}
				if !found {
					return command.Fail(command.ErrNotFound)
				}
				if _, err := fmt.Fprintf(writer, "%s  %s  %s\n", definition.Code.String(), definition.Identifier, value.Message.Text); err != nil {
					return err
				}
			}
			return nil
		})
	})
	explain := &cobra.Command{Use: "explain CODE_OR_IDENTIFIER", Short: "fathomry.command_line.error_explain", Args: cobra.ExactArgs(1)}
	invocation.Bind(explain, func(_ context.Context, args []string) error {
		catalogs := invocation.Catalogs()
		var definition failure.Definition
		var found bool
		var err error
		if failure.Identifier(args[0]).Valid() {
			definition, found, err = catalogs.Errors.LookupIdentifier(failure.Identifier(args[0]))
		} else {
			code, problem := failure.ParseCode(args[0])
			if problem != nil {
				return command.Fail(command.ErrUsage, problem)
			}
			definition, found, err = catalogs.Errors.Lookup(code)
		}
		if err != nil {
			return err
		}
		if !found {
			return command.Fail(command.ErrNotFound)
		}
		value, found, err := catalogs.Messages.Explain(definition.Code, invocation.Locale())
		if err != nil {
			return err
		}
		if !found {
			return command.Fail(command.ErrNotFound)
		}
		result := explanation{Definition: value.Definition, Message: value.Message.Text,
			RequestedLocale: value.Selection.Requested, Locale: value.Message.Locale, Fallback: string(value.Selection.Fallback)}
		return invocation.Result("error.explain", result, func(writer io.Writer) error {
			_, err := fmt.Fprintf(writer, "%s  %s\n%s\n%s: %s\n", result.Definition.Code.String(),
				result.Definition.Identifier, result.Message, invocation.Text("fathomry.command_line.locale"), result.Locale)
			return err
		})
	})
	components := &cobra.Command{Use: "components", Short: "fathomry.command_line.error_components", Args: cobra.NoArgs}
	invocation.Bind(components, func(_ context.Context, _ []string) error {
		values, err := invocation.Catalogs().Errors.Components()
		if err != nil {
			return err
		}
		return invocation.Result("error.components", values, func(writer io.Writer) error {
			for _, value := range values {
				if _, err := fmt.Fprintf(writer, "%s.%s  %s  0x%03X  %d\n", value.Module, value.Name, value.Domain, uint16(value.Facility), len(value.Codes)); err != nil {
					return err
				}
			}
			return nil
		})
	})
	root.AddCommand(list, explain, components)
	return root
}
