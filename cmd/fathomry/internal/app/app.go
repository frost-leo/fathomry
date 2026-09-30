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

// Package app assembles the official executable's commands, not a project host.
package app

import (
	"context"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/errorcatalog"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/messages"
	"github.com/spf13/cobra"
)

// Run owns one finite CLI invocation; it does not read process globals.
func Run(ctx context.Context, args []string, options command.Options) error {
	catalogs, err := catalogs()
	if err != nil {
		return command.Unavailable(options.ErrorOutput, err)
	}
	return command.Run(ctx, args, options, catalogs, commands)
}

func commands(invocation *command.Invocation) *cobra.Command {
	root := invocation.Group("fathomry", "fathomry.command_line.root")
	root.AddCommand(errorcatalog.New(invocation), messages.New(invocation))
	return root
}
