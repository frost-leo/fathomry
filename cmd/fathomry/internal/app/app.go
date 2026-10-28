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

// Package app composes the official tool's commands over the shared invocation foundation.
package app

import (
	"context"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project"
	cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

// Run owns one finite CLI invocation; selected handlers own their explicit effects.
func Run(ctx context.Context, args []string, options command.Options) error {
	components := append(i18n.CoreComponents(), cli.Components()...)
	components = append(components, project.Components()...)
	var definitions []failure.Definition
	for _, component := range components {
		definitions = append(definitions, component.Definitions...)
	}
	errors, err := failure.Prepare(definitions...)
	if err != nil {
		return command.Unavailable(options.ErrorOutput, err)
	}
	messages, err := i18n.Prepare(components...)
	if err != nil {
		return command.Unavailable(options.ErrorOutput, err)
	}
	return command.Run(ctx, args, options, command.Catalogs{Errors: errors, Messages: messages}, func(invocation *command.Invocation) *cobra.Command {
		root := invocation.Group("fathomry", "fathomry.command_line.root")
		root.AddCommand(project.New(invocation))
		return root
	})
}
