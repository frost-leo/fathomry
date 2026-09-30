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

package command

import "github.com/frost-leo/fathomry/failure/v1"

const (
	ErrOptions   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityCommandLine)<<16 | 0x0001
	ErrUsage     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityCommandLine)<<16 | 0x0002
	ErrNotFound  failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityCommandLine)<<16 | 0x0003
	ErrExecution failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityCommandLine)<<16 | 0x0004
	ErrOutput    failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityCommandLine)<<16 | 0x0005
	ErrCleanup   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityCommandLine)<<16 | 0x0006
	ErrCanceled  failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityCommandLine)<<16 | 0x0007
	ErrLimit     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityCommandLine)<<16 | 0x0008
)

// Definitions describes command invocation failures, not every domain command.
func Definitions() []failure.Definition {
	return []failure.Definition{
		{Code: ErrOptions, Identifier: "fathomry.command_line.invalid_options", Module: "fathomry", Component: "command_line", Revision: 1, Message: "The command invocation dependencies are invalid."},
		{Code: ErrUsage, Identifier: "fathomry.command_line.invalid_usage", Module: "fathomry", Component: "command_line", Revision: 1, Message: "The command or its arguments are invalid. Consult the command help."},
		{Code: ErrNotFound, Identifier: "fathomry.command_line.not_found", Module: "fathomry", Component: "command_line", Revision: 1, Message: "The requested catalog entry is not registered."},
		{Code: ErrExecution, Identifier: "fathomry.command_line.execution_failed", Module: "fathomry", Component: "command_line", Revision: 1, Message: "The command failed; native details are withheld."},
		{Code: ErrOutput, Identifier: "fathomry.command_line.output_failed", Module: "fathomry", Component: "command_line", Revision: 1, Message: "Writing command output failed; delivery may be partial."},
		{Code: ErrCleanup, Identifier: "fathomry.command_line.cleanup_failed", Module: "fathomry", Component: "command_line", Revision: 1, Message: "Command resource cleanup failed or did not finish within the waiting budget."},
		{Code: ErrCanceled, Identifier: "fathomry.command_line.canceled", Module: "fathomry", Component: "command_line", Revision: 1, Message: "The command was canceled; cancellation does not establish absence of effects."},
		{Code: ErrLimit, Identifier: "fathomry.command_line.limit_exceeded", Module: "fathomry", Component: "command_line", Revision: 1, Message: "A command invocation or output limit was exceeded."},
	}
}

// Fail retains original causes without making their formatting safe.
func Fail(code failure.Code, causes ...error) error {
	for _, definition := range Definitions() {
		if definition.Code == code {
			value, err := failure.New(definition, failure.Location{Operation: "command"}, causes...)
			if err != nil {
				return err
			}
			return value
		}
	}
	return failure.ErrDefinition
}
