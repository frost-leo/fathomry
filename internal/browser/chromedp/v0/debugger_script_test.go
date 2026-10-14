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
	"errors"
	"testing"

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/debugger"
	"github.com/chromedp/cdproto/runtime"
)

func TestDebuggerScriptOriginReusedIDsResetAndCumulativeBounds(t *testing.T) {
	session := unitSession(t)
	session.owner.settings.MaxEvents = 2
	state := &controlState{session: session, life: context.Background()}
	if err := state.prepareDebugger(); err != nil {
		t.Fatal(err)
	}
	data := jsonv2.Value(`{"scriptId":"same-id","url":"private-script"}`)
	if err := state.observeDebugger(debugger.ScriptParsed.Method, data, 1); err != nil {
		t.Fatal(err)
	}
	pending := Pending{state: &pendingState{control: state, script: state.debugger.scriptEvents[1], data: data}}
	script, err := pending.Script()
	if err != nil {
		t.Fatal(err)
	}
	copy := script.DataCopy()
	copy[0] = 'x'
	if script.DataCopy()[0] != '{' {
		t.Fatal("script metadata aliases native evidence")
	}
	if err := state.observeDebugger(debugger.ScriptFailedToParse.Method, data, 2); err != nil || script.checkLocked() == nil {
		t.Fatal("reused script ID revived an old grant", err)
	}
	newScript := Script{control: state, entry: state.debugger.scriptEvents[2]}
	if err := newScript.checkLocked(); err != nil {
		t.Fatal("replacement script was not admitted", err)
	}
	if err := state.observeDebugger(runtime.ExecutionContextsCleared.Method, jsonv2.Value(`{}`), 3); err != nil || newScript.checkLocked() == nil {
		t.Fatal("execution reset retained script authority", err)
	}
	if err := state.observeDebugger(debugger.ScriptParsed.Method, data, 4); !errors.Is(err, ErrLimit) {
		t.Fatal("reset refunded cumulative retained script capacity", err)
	}
}

func TestDebuggerLocationRepliesRequireOriginalScriptGrants(t *testing.T) {
	state := &controlState{session: unitSession(t), life: context.Background()}
	if err := state.prepareDebugger(); err != nil {
		t.Fatal(err)
	}
	if err := state.observeDebugger(debugger.ScriptParsed.Method, jsonv2.Value(`{"scriptId":"known"}`), 1); err != nil {
		t.Fatal(err)
	}
	script := Script{control: state, entry: state.debugger.scriptEvents[1]}
	pending := &pendingState{control: state, method: debugger.Paused.Method}
	params := debugger.StepOverParams{SkipList: []*debugger.LocationRange{{Start: &debugger.ScriptPosition{}, End: &debugger.ScriptPosition{LineNumber: 1}}}}
	value, err := StepOverDebugger(params, script).params(pending)
	if err != nil || value.(debugger.StepOverParams).SkipList[0].ScriptID != "known" || params.SkipList[0].ScriptID != "" {
		t.Fatal("skip-list copy did not fill only its granted identity", err)
	}
	params.SkipList[0].ScriptID = "foreign"
	if _, err := StepOverDebugger(params, script).params(pending); !errors.Is(err, ErrInput) {
		t.Fatal("foreign nested script ID reached stepping", err)
	}
	if _, err := StepIntoDebugger(debugger.StepIntoParams{SkipList: []*debugger.LocationRange{nil}}, script).params(pending); !errors.Is(err, ErrInput) {
		t.Fatal("nil skip-list member reached stepping", err)
	}
	location := debugger.ContinueToLocationParams{Location: &debugger.Location{LineNumber: 1, ColumnNumber: new(int64(0))}}
	if _, err := ContinueDebuggerTo(script, location).params(pending); err != nil || location.Location.ScriptID != "" {
		t.Fatal("continue location mutated caller data", err)
	}
	foreign := script
	foreign.control = &controlState{}
	if _, err := ContinueDebuggerTo(foreign, location).params(pending); !errors.Is(err, ErrInput) {
		t.Fatal("foreign control script reached stepping", err)
	}
}
