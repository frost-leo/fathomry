/*
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

package chromedp

import (
	"context"

	"github.com/chromedp/cdproto/debugger"
)

// SetBlackboxContexts replaces this debugger's native context set. No grants
// clears the set. Unique IDs come only from current same-page execution grants.
// A native acknowledgment does not prove a later script was skipped.
func (control *Control) SetBlackboxContexts(ctx context.Context, executions ...*ExecutionContext) error {
	return SetBlackboxContextArguments(ctx, control, len(executions), func(index int) *ExecutionContext { return executions[index] })
}

// SetBlackboxContextArguments reads borrowed adapter grants only after command
// admission and the context-count bound. The accessor must return cooperatively.
func SetBlackboxContextArguments(ctx context.Context, control *Control, count int, execution func(int) *ExecutionContext) error {
	if control == nil || control.state == nil || count < 0 || execution == nil {
		return failure(ErrInput, "debugger-contexts")
	}
	state := control.state
	_, err := runDebuggerCommand(ctx, state, debugger.SetBlackboxExecutionContexts, debugger.SetBlackboxExecutionContextsParams{}, nil, func(copied *debugger.SetBlackboxExecutionContextsParams) error {
		if count > state.session.owner.settings.MaxEvents {
			return failure(ErrLimit, "debugger-contexts")
		}
		values := make([]*ExecutionContext, count)
		for index := range count {
			values[index] = execution(index)
		}
		copied.UniqueIDs = make([]string, count)
		for index, value := range values {
			if err := value.valid(state.session); err != nil {
				return err
			}
			copied.UniqueIDs[index] = value.origin.UniqueID
		}
		return nil
	})
	return err
}
