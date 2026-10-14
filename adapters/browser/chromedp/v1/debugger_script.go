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
	"github.com/chromedp/cdproto/debugger"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// Script is an observed parsed/failed script, not a raw script ID or an
// execution-context grant. Metadata copies do not change its native authority.
type Script struct {
	private
	native  native.Script
	control *Control
}

func (script Script) DataCopy() []byte { return script.native.DataCopy() }

// Script selects the original script event. Reset execution contexts, a reused
// script ID and Control.Close revoke it without reviving old metadata copies.
func (pending Pending) Script() (Script, error) {
	if pending.control == nil {
		return Script{}, fail(ErrInput, "debugger-script-event")
	}
	value, err := pending.native.Script()
	return Script{native: value, control: pending.control}, translate(err, "debugger-script-event")
}

// RunScriptCommand accepts native GetScriptSource, SearchInContent,
// GetPossibleBreakpoints and SetBlackboxedRanges. IDs are filled on admitted
// copies; conflicting IDs and malformed nested locations are refused. It uses
// ordinary quotas but can run while another page operation is debugger-paused.
func RunScriptCommand[P, R any](ctx context.Context, script Script, command cdp.Command[P, R], params P) (result R, receipt *adapters.Receipt[Result], err error) {
	if script.control == nil || script.control.state == nil {
		return result, nil, fail(ErrInput, "debugger-script")
	}
	return runPageOperation(ctx, script.control.state.page, "debugger_script", func(work context.Context) (R, error) {
		return native.RunScriptCommand(work, script.native, command, params)
	})
}

// StepIntoDebugger borrows native stepping options and requires an observed
// Script for every skip-list range. One grant can fill absent range script IDs.
func StepIntoDebugger(params debugger.StepIntoParams, scripts ...Script) Reply {
	values := make([]native.Script, len(scripts))
	for index, script := range scripts {
		values[index] = script.native
	}
	return Reply{native: native.StepIntoDebugger(params, values...)}
}

// StepOverDebugger has the same skip-list and script ownership as StepIntoDebugger.
func StepOverDebugger(params debugger.StepOverParams, scripts ...Script) Reply {
	values := make([]native.Script, len(scripts))
	for index, script := range scripts {
		values[index] = script.native
	}
	return Reply{native: native.StepOverDebugger(params, values...)}
}

// ContinueDebuggerTo uses the pending pause's reserved reply lane to continue
// to an exact observed script location. Native TargetCallFrames is preserved.
func ContinueDebuggerTo(script Script, params debugger.ContinueToLocationParams) Reply {
	return Reply{native: native.ContinueDebuggerTo(script.native, params)}
}
