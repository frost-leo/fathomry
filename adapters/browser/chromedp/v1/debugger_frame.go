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

// CallFrame borrows an observed paused frame. Metadata includes sensitive scope
// data, but raw remote-object and execution-context IDs are not extra grants.
type CallFrame struct {
	private
	native  native.CallFrame
	control *Control
}

func (frame CallFrame) DataCopy() []byte { return frame.native.DataCopy() }

// CallFrame selects an original paused frame by index. Resume/new pause and
// submission of the pause's reply retire it, even when native IDs are reused.
func (pending Pending) CallFrame(index int) (CallFrame, error) {
	if pending.control == nil {
		return CallFrame{}, fail(ErrInput, "debugger-frame")
	}
	frame, err := pending.native.CallFrame(index)
	return CallFrame{native: frame, control: pending.control}, translate(err, "debugger-frame")
}

// EvaluateFrame preserves native evaluation options and puts result/exception
// references in this pre-created private ObjectGroup. Use a separate group from
// the operation that caused the pause: a busy group is refused, never waited on.
// Objects can outlive resume; any observed execution-context retirement revokes
// their use conservatively, without claiming their private group was released.
// Chrome 150 silently ignores nonzero ScopeNumber, so that combination is refused.
// Other browser versions' support for this optional field remains unqualified.
func (group *ObjectGroup) EvaluateFrame(ctx context.Context, frame CallFrame, params debugger.EvaluateOnCallFrameParams) (*Object, *adapters.Receipt[Result], error) {
	return group.run(ctx, "debugger_frame_evaluate", func(work context.Context) (*native.Object, error) {
		return group.state.native.EvaluateFrame(work, frame.native, params)
	})
}

// SetVariable keeps native scope/name fields. Leave NewValue absent and provide
// an explicit value, undefined/special value, or same-page managed Object instead.
func (frame CallFrame) SetVariable(ctx context.Context, params debugger.SetVariableValueParams, value Argument) (*adapters.Receipt[Result], error) {
	if frame.control == nil || frame.control.state == nil {
		return nil, fail(ErrInput, "debugger-variable")
	}
	_, receipt, err := runPageOperation(ctx, frame.control.state.page, "debugger_variable", func(work context.Context) (cdp.Empty, error) {
		return cdp.Empty{}, frame.native.SetVariable(work, params, value.native)
	})
	return receipt, err
}

// SetReturnValue requires the top frame at a native return pause. Other pause
// positions keep native rejection; values use the same managed argument boundary.
func (frame CallFrame) SetReturnValue(ctx context.Context, value Argument) (*adapters.Receipt[Result], error) {
	if frame.control == nil || frame.control.state == nil {
		return nil, fail(ErrInput, "debugger-return-value")
	}
	_, receipt, err := runPageOperation(ctx, frame.control.state.page, "debugger_return_value", func(work context.Context) (cdp.Empty, error) {
		return cdp.Empty{}, frame.native.SetReturnValue(work, value.native)
	})
	return receipt, err
}

// RestartDebuggerFrame restarts only the granted frame using its pause's reserved
// reply lane. Native Mode is preserved; StepInto is the selected required mode.
func RestartDebuggerFrame(frame CallFrame, params debugger.RestartFrameParams) Reply {
	return Reply{native: native.RestartDebuggerFrame(frame.native, params)}
}
