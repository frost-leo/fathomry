/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package chromedp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/debugger"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
)

func TestLiveDebuggerSetReturnValuePreservesValuesAtReturnPause(t *testing.T) {
	endpoint, peer := cleanupFaultPeer(t, nil)
	prepared, err := Prepare(Settings{Name: "return-presence", RemoteURL: endpoint}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Page().Navigate(bounded(t), "data:text/html,<title>return_presence_fixture</title>"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Debugger: &debugger.EnableParams{}, SharedIsolateEffects: true})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := cdp.Call(bounded(t), sdk.FromContext(peer).Browser, target.GetTargets, target.GetTargetsParams{})
	if err != nil {
		t.Fatal(err)
	}
	var selected target.ID
	for _, info := range listed.TargetInfos {
		if strings.Contains(info.URL, "return_presence_fixture") {
			if selected != "" {
				t.Fatal("fixture target is ambiguous")
			}
			selected = info.TargetID
		}
	}
	if selected == "" {
		t.Fatal("test-owned target missing")
	}
	setup, detach := sdk.NewContext(peer, sdk.WithTargetID(selected), sdk.WithDetachOnCancel())
	defer detach()
	if err := sdk.Do(setup); err != nil {
		t.Fatal(err)
	}
	native := sdk.FromContext(setup).Target
	if _, err := cdp.Call(bounded(t), native, debugger.Enable, debugger.EnableParams{MaxScriptsCacheSize: 65536}); err != nil {
		t.Fatal(err)
	}
	scripts, unsubscribe := native.Subscribe(debugger.ScriptParsed.Method)
	defer unsubscribe()
	if _, err := sdk.Run(setup, sdk.Evaluate[sdk.Void]("globalThis.returnValueFixture = function returnValueFixture() {\n  return 42;\n}; void 0;\n//# sourceURL=managed-return-presence.js")); err != nil {
		t.Fatal(err)
	}
	var script cdp.ScriptID
	for script == "" {
		select {
		case raw := <-scripts:
			var parsed debugger.EventScriptParsed
			if err := json.Unmarshal(raw, &parsed); err != nil {
				t.Fatal(err)
			}
			if parsed.URL == "managed-return-presence.js" {
				script = parsed.ScriptID
			}
		case <-bounded(t).Done():
			t.Fatal("native fixture script was not observed")
		}
	}
	locations, err := cdp.Call(bounded(t), native, debugger.GetPossibleBreakpoints, debugger.GetPossibleBreakpointsParams{Start: &debugger.Location{ScriptID: script}})
	if err != nil {
		t.Fatal(err)
	}
	var location *debugger.Location
	for _, candidate := range locations.Locations {
		if candidate.Type == debugger.BreakLocationTypeReturn {
			column := candidate.ColumnNumber
			location = &debugger.Location{ScriptID: script, LineNumber: candidate.LineNumber, ColumnNumber: &column}
			break
		}
	}
	if location == nil {
		t.Fatal("native script has no return breakpoint")
	}
	if _, err := cdp.Call(bounded(t), native, debugger.SetBreakpoint, debugger.SetBreakpointParams{Location: location}); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		name     string
		argument Argument
		want     string
		skip     bool
	}{
		{"original-control", Argument{}, "number:42", true},
		{"string", ValueArgument(""), `string:""`, false},
		{"array", ValueArgument([]any{}), "object:[]", false},
		{"object", ValueArgument(map[string]any{}), "object:{}", false},
		{"null", ValueArgument(nil), "object:null", false},
		{"false", ValueArgument(false), "boolean:false", false},
		{"zero", ValueArgument(0), "number:0", false},
		{"undefined", UndefinedArgument(), "undefined:undefined", false},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			type returned struct {
				value string
				err   error
			}
			done := make(chan returned, 1)
			go func() {
				value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[string](`(function(){const result=returnValueFixture();return typeof result+':'+JSON.stringify(result);})()`))
				done <- returned{value, err}
			}()
			pending := debuggerPause(t, control)
			var observed debugger.EventPaused
			if err := json.Unmarshal(pending.DataCopy(), &observed); err != nil || len(observed.CallFrames) == 0 || observed.CallFrames[0].ReturnValue == nil || string(observed.CallFrames[0].ReturnValue.Value) != "42" {
				t.Fatal("fixture is not paused at a native return", err)
			}
			frame, err := pending.CallFrame(0)
			if err != nil {
				t.Fatal(err)
			}
			if !fixture.skip {
				if _, err := frame.SetReturnValue(bounded(t), fixture.argument); err != nil {
					t.Error("native return replacement failed", err)
				}
				drain(t, dependencies, 1)
			}
			if err := pending.Reply(bounded(t), ResumeDebugger(debugger.ResumeParams{})); err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-done:
				t.Logf("native return pause observed; returned=%q, expected=%q, error=%v", result.value, fixture.want, result.err)
				if result.err != nil || result.value != fixture.want {
					t.Error("return replacement lost its native value")
				}
			case <-bounded(t).Done():
				t.Fatal("return fixture did not resume")
			}
			drain(t, dependencies, 1)
		})
	}
	if _, err := cdp.Call(bounded(t), native, debugger.Disable, cdp.Empty{}); err != nil {
		t.Fatal(err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Error("return fixture cleanup incomplete")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
