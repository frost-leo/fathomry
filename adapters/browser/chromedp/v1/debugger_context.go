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

	"github.com/chromedp/cdproto/cdp"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// SetBlackboxContexts replaces the native debugger's context list with observed
// same-page execution grants. No arguments clears it. Raw unique IDs, stale
// worlds and foreign-page grants cannot authorize this operation. Parameters are
// borrowed until return; later context destruction can make a rule ineffective.
func (control *Control) SetBlackboxContexts(ctx context.Context, executions ...*ExecutionContext) (*adapters.Receipt[Result], error) {
	if control == nil || control.state == nil {
		return nil, fail(ErrInput, "debugger-contexts")
	}
	_, receipt, err := runPageOperation(ctx, control.state.page, "debugger_contexts", func(work context.Context) (cdp.Empty, error) {
		return cdp.Empty{}, native.SetBlackboxContextArguments(work, control.state.native, len(executions), func(index int) *native.ExecutionContext {
			if executions[index] != nil {
				return executions[index].native
			}
			return nil
		})
	})
	return receipt, err
}
