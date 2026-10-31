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

package config

import (
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/config/check"
	"github.com/spf13/cobra"
)

// New composes the configuration command group without acquiring sources.
func New[T any](invocation *command.Invocation, declaration check.Declaration[T]) *cobra.Command {
	group := invocation.Group("config", "fathomry.configuration_command.group")
	group.AddCommand(check.New(invocation, declaration))
	return group
}
