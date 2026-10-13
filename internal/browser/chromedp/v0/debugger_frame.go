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
	"strings"

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/debugger"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

// CallFrame borrows one original paused frame, including its pause generation.
// Script location/context metadata cannot create or replace this authority.
type CallFrame struct {
	private
	pending *pendingState
	index   int
}

type debuggerFrame struct {
	id   debugger.CallFrameID
	data []byte
}

func (pending *pendingState) decodeFrames(data jsonv2.Value) error {
	var event struct {
		CallFrames []jsonv2.Value `json:"callFrames"`
	}
	if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
		return err
	}
	if len(event.CallFrames) > pending.control.session.owner.settings.MaxEvents {
		return failure(ErrLimit, "debugger-call-frames")
	}
	for _, data := range event.CallFrames {
		var frame struct {
			ID debugger.CallFrameID `json:"callFrameId"`
		}
		if err := jsonv2.Unmarshal(data, &frame, sdk.DefaultUnmarshalOptions); err != nil {
			return err
		}
		if frame.ID == "" {
			return failure(ErrNative, "debugger-frame-identity")
		}
		pending.frames = append(pending.frames, debuggerFrame{id: frame.ID, data: data})
	}
	return nil
}

func (pending Pending) CallFrame(index int) (CallFrame, error) {
	if pending.state == nil {
		return CallFrame{}, failure(ErrInput, "debugger-frame")
	}
	frame := CallFrame{pending: pending.state, index: index}
	pending.state.control.mu.Lock()
	defer pending.state.control.mu.Unlock()
	if err := frame.checkLocked(); err != nil {
		return CallFrame{}, err
	}
	return frame, nil
}

func (frame CallFrame) DataCopy() []byte {
	if frame.pending == nil || frame.index < 0 || frame.index >= len(frame.pending.frames) {
		return nil
	}
	return append([]byte(nil), frame.pending.frames[frame.index].data...)
}

func (frame CallFrame) checkLocked() error {
	pending := frame.pending
	if pending == nil || pending.method != debugger.Paused.Method || frame.index < 0 || frame.index >= len(pending.frames) {
		return failure(ErrInput, "debugger-frame")
	}
	state := pending.control
	if state.closing || state.closed || state.life.Err() != nil || pending.attempted || pending.stale() {
		return failure(ErrState, "debugger-frame-retired")
	}
	return nil
}

func (frame CallFrame) fillID(id *debugger.CallFrameID) error {
	original := frame.pending.frames[frame.index].id
	if *id != "" && *id != original {
		return failure(ErrInput, "debugger-frame-parameters")
	}
	*id = original
	return nil
}

// EvaluateFrame evaluates in the original paused frame and retains every native
// reference/exception/unknown result in this private group. Pre-create a separate
// group: a busy group is refused rather than deadlocking behind its own producer.
// Returned objects outlive resume, but any observed execution-context retirement
// conservatively revokes them because the frame's execution origin is opaque.
func (group *ObjectGroup) EvaluateFrame(ctx context.Context, frame CallFrame, params debugger.EvaluateOnCallFrameParams) (*Object, error) {
	if group == nil || group.state == nil || frame.pending == nil || ctx == nil {
		return nil, failure(ErrInput, "debugger-frame-evaluate")
	}
	state, session := frame.pending.control, group.state.session
	if state.session.sessionState != session.sessionState || params.ObjectGroup != "" {
		return nil, failure(ErrInput, "debugger-frame-origin")
	}
	work, finish, err := state.enterCommand(ctx, frame.checkLocked)
	if err != nil {
		return nil, err
	}
	defer finish()
	product, version, _ := strings.Cut(session.owner.versionCopy().product, "/")
	if params.ScopeNumber != 0 && (product == "Chrome" || product == "Chromium" || product == "HeadlessChrome") && strings.HasPrefix(version, "150.") {
		return nil, failure(ErrUnsupported, "debugger-scope-number-peer")
	}
	check := func() error {
		state.mu.Lock()
		defer state.mu.Unlock()
		return frame.checkLocked()
	}
	origin := session.objectOrigin
	fence := &executionFence{origin: origin, revision: origin.retirement.Load()}
	return group.executeOrigin(work, func(name string) sdk.Action[*runtime.RemoteObject] {
		return func(ctx context.Context, target *sdk.Target) (*runtime.RemoteObject, error) {
			buffer := boundedBuffer{limit: session.owner.settings.MaxCommandBytes}
			if err := marshalBounded(&buffer, params); err != nil {
				return nil, err
			}
			var copied debugger.EvaluateOnCallFrameParams
			if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
				return nil, err
			}
			if err := frame.fillID(&copied.CallFrameID); err != nil {
				return nil, err
			}
			copied.ObjectGroup = name
			result, err := cdp.Call(ctx, target, debugger.EvaluateOnCallFrame, copied)
			if err != nil {
				return nil, err
			}
			if result.ExceptionDetails != nil {
				return nil, &sdk.ExceptionError{ExceptionDetails: result.ExceptionDetails}
			}
			return result.Result, nil
		}
	}, true, nil, fence, true, check)
}

// SetVariable preserves native scope/name parameters while converting an
// explicit value or managed object argument after ordinary command admission.
func (frame CallFrame) SetVariable(ctx context.Context, params debugger.SetVariableValueParams, value Argument) error {
	if frame.pending == nil || params.NewValue != nil {
		return failure(ErrInput, "debugger-variable")
	}
	state := frame.pending.control
	_, err := runDebuggerCommand(ctx, state, debugger.SetVariableValue, params, frame.checkLocked, func(copied *debugger.SetVariableValueParams) error {
		if err := frame.fillID(&copied.CallFrameID); err != nil {
			return err
		}
		arguments, err := objectArguments(state.session, 1, func(int) Argument { return value })
		if err == nil {
			copied.NewValue = arguments[0]
		}
		return err
	})
	return err
}

// SetReturnValue changes the top frame's return value at a native return pause.
// Native return-position validation is preserved; a pause alone is not enough.
func (frame CallFrame) SetReturnValue(ctx context.Context, value Argument) error {
	if frame.pending == nil || frame.index != 0 {
		return failure(ErrInput, "debugger-return-frame")
	}
	state := frame.pending.control
	_, err := runDebuggerCommand(ctx, state, debugger.SetReturnValue, debugger.SetReturnValueParams{}, frame.checkLocked, func(copied *debugger.SetReturnValueParams) error {
		arguments, err := objectArguments(state.session, 1, func(int) Argument { return value })
		if err == nil {
			copied.NewValue = arguments[0]
		}
		return err
	})
	return err
}

// RestartDebuggerFrame fills only the granted frame ID and keeps native Mode.
// It consumes the pause reply; the subsequent pause must grant its own frames.
func RestartDebuggerFrame(frame CallFrame, params debugger.RestartFrameParams) Reply {
	check := func(p *pendingState) error {
		if frame.pending != p {
			return failure(ErrInput, "debugger-frame-pause")
		}
		return frame.checkLocked()
	}
	return Reply{method: debugger.RestartFrame.Method, check: check, params: func(p *pendingState) (any, error) {
		if err := check(p); err != nil {
			return nil, err
		}
		copied := params
		return &copied, frame.fillID(&copied.CallFrameID)
	}}
}
