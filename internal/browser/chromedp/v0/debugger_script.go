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
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/debugger"
	sdk "github.com/chromedp/chromedp"
)

// Script borrows a native parsed/failed script from its original Debugger
// Control. Metadata is not an execution-context or call-frame grant.
type Script struct {
	private
	control *controlState
	entry   *debuggerScript
	data    []byte
}

type debuggerScript struct {
	id       cdp.ScriptID
	sequence uint64
	epoch    uint64
}

func (state *controlState) observeScript(data jsonv2.Value, sequence uint64) error {
	domain, settings := state.debugger, state.session.owner.settings
	if sequence == 0 || domain.scriptCount >= settings.MaxEvents || int64(len(data))+512 > settings.MaxResultBytes-domain.scriptBytes {
		return failure(ErrLimit, "debugger-script-origins")
	}
	domain.scriptCount++
	domain.scriptBytes += int64(len(data)) + 512
	var event struct {
		ScriptID cdp.ScriptID `json:"scriptId"`
	}
	if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
		return err
	}
	if event.ScriptID == "" {
		return failure(ErrNative, "debugger-script-identity")
	}
	entry := &debuggerScript{id: event.ScriptID, sequence: sequence, epoch: domain.scriptEpoch}
	domain.scripts[event.ScriptID], domain.scriptEvents[sequence] = entry, entry
	return nil
}

// Script derives authority only from an observed ScriptParsed/ScriptFailedToParse
// event. A later event reusing its ID or an execution-context reset retires it.
func (pending Pending) Script() (Script, error) {
	if pending.state == nil || pending.state.script == nil {
		return Script{}, failure(ErrInput, "debugger-script-event")
	}
	p := pending.state
	value := Script{control: p.control, entry: p.script, data: p.data}
	p.control.mu.Lock()
	defer p.control.mu.Unlock()
	if err := value.checkLocked(); err != nil {
		return Script{}, err
	}
	return value, nil
}

func (script Script) DataCopy() []byte { return append([]byte(nil), script.data...) }

func (script Script) checkLocked() error {
	state, entry := script.control, script.entry
	if state == nil || entry == nil || entry.id == "" || state.debugger == nil {
		return failure(ErrInput, "debugger-script")
	}
	if state.closing || state.closed || state.life.Err() != nil || state.debugger.scriptEpoch != entry.epoch || state.debugger.scripts[entry.id] != entry {
		return failure(ErrState, "debugger-script-retired")
	}
	return nil
}

func (script Script) fillID(id *cdp.ScriptID) error {
	if *id != "" && *id != script.entry.id {
		return failure(ErrInput, "debugger-script-parameters")
	}
	*id = script.entry.id
	return nil
}

func (script Script) fillLocation(location *debugger.Location) error {
	if location == nil || location.LineNumber < 0 || location.ColumnNumber != nil && *location.ColumnNumber < 0 {
		return failure(ErrInput, "debugger-location")
	}
	return script.fillID(&location.ScriptID)
}

// RunScriptCommand delegates finite source/search/breakable-location/blackbox
// operations. Every nested location belongs to the same observed Script. Native
// optional fields are preserved in an admitted copy; caller data is not changed.
func RunScriptCommand[P, R any](ctx context.Context, script Script, command cdp.Command[P, R], params P) (result R, err error) {
	switch command.Method {
	case debugger.GetScriptSource.Method, debugger.SearchInContent.Method, debugger.GetPossibleBreakpoints.Method, debugger.SetBlackboxedRanges.Method:
	default:
		return result, failure(ErrUnsupported, "debugger-script-command")
	}
	return runDebuggerCommand(ctx, script.control, command, params, script.checkLocked, func(copied *P) error {
		switch value := any(copied).(type) {
		case *debugger.GetScriptSourceParams:
			return script.fillID(&value.ScriptID)
		case *debugger.SearchInContentParams:
			return script.fillID(&value.ScriptID)
		case *debugger.GetPossibleBreakpointsParams:
			if err := script.fillLocation(value.Start); err != nil {
				return err
			}
			if value.End != nil {
				return script.fillLocation(value.End)
			}
			return nil
		case *debugger.SetBlackboxedRangesParams:
			for _, position := range value.Positions {
				if position == nil || position.LineNumber < 0 || position.ColumnNumber < 0 {
					return failure(ErrInput, "debugger-blackbox-position")
				}
			}
			return script.fillID(&value.ScriptID)
		default:
			return failure(ErrInput, "debugger-script-parameters")
		}
	})
}

func debuggerReplyScripts(pending *pendingState, scripts []Script) error {
	if pending.method != debugger.Paused.Method {
		return failure(ErrInput, "debugger-pause-grant")
	}
	if int64(len(scripts)) > pending.control.session.owner.settings.MaxCommandBytes/64 {
		return failure(ErrLimit, "debugger-reply-scripts")
	}
	for _, script := range scripts {
		if script.control != pending.control {
			return failure(ErrInput, "debugger-script-origin")
		}
		if err := script.checkLocked(); err != nil {
			return err
		}
	}
	return nil
}

func debuggerSkipList(pending *pendingState, ranges []*debugger.LocationRange, scripts []Script) ([]*debugger.LocationRange, error) {
	if err := debuggerReplyScripts(pending, scripts); err != nil {
		return nil, err
	}
	if int64(len(ranges)) > pending.control.session.owner.settings.MaxCommandBytes/64 {
		return nil, failure(ErrLimit, "debugger-skip-list")
	}
	if ranges == nil {
		return nil, nil
	}
	values := make([]*debugger.LocationRange, len(ranges))
	for index, location := range ranges {
		if location == nil || location.Start == nil || location.End == nil {
			return nil, failure(ErrInput, "debugger-skip-location")
		}
		copied := *location
		found := false
		for _, script := range scripts {
			if copied.ScriptID == script.entry.id || copied.ScriptID == "" && len(scripts) == 1 {
				copied.ScriptID, found = script.entry.id, true
				break
			}
		}
		if !found {
			return nil, failure(ErrInput, "debugger-skip-script")
		}
		values[index] = &copied
	}
	return values, nil
}

// StepIntoDebugger preserves native stepping options. Every skip-list range
// requires its observed Script; absent IDs can be filled for exactly one grant.
func StepIntoDebugger(params debugger.StepIntoParams, scripts ...Script) Reply {
	return Reply{method: debugger.StepInto.Method,
		check: func(p *pendingState) error { return debuggerReplyScripts(p, scripts) },
		params: func(p *pendingState) (any, error) {
			copied := params
			var err error
			copied.SkipList, err = debuggerSkipList(p, params.SkipList, scripts)
			return copied, err
		},
	}
}

// StepOverDebugger has the same script/skip-list ownership as StepIntoDebugger.
func StepOverDebugger(params debugger.StepOverParams, scripts ...Script) Reply {
	return Reply{method: debugger.StepOver.Method,
		check: func(p *pendingState) error { return debuggerReplyScripts(p, scripts) },
		params: func(p *pendingState) (any, error) {
			copied := params
			var err error
			copied.SkipList, err = debuggerSkipList(p, params.SkipList, scripts)
			return copied, err
		},
	}
}

// ContinueDebuggerTo continues to a location in exactly the granted Script.
func ContinueDebuggerTo(script Script, params debugger.ContinueToLocationParams) Reply {
	check := func(p *pendingState) error { return debuggerReplyScripts(p, []Script{script}) }
	return Reply{method: debugger.ContinueToLocation.Method, check: check, params: func(p *pendingState) (any, error) {
		if err := check(p); err != nil {
			return nil, err
		}
		if params.Location == nil {
			return nil, failure(ErrInput, "debugger-location")
		}
		copied, location := params, *params.Location
		if err := script.fillLocation(&location); err != nil {
			return nil, err
		}
		copied.Location = &location
		return copied, nil
	}}
}
