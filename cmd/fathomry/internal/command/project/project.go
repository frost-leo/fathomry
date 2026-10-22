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

// Package project assembles project commands without adding a user-facing namespace.
package project

import (
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	buildcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project/build"
	newcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project/new"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

// Commands returns the top-level new and build commands.
func Commands(invocation *command.Invocation) []*cobra.Command {
	return []*cobra.Command{newcmd.New(invocation), buildcmd.New(invocation)}
}

// Components supplies only the project commands' metadata.
func Components() []i18n.Component {
	return []i18n.Component{newcmd.Component(), buildcmd.Component()}
}
