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
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/chromedp/cdproto/debugger"
)

func debuggerScriptPause(t *testing.T, control *Control, name string) (Script, Pending) {
	t.Helper()
	var script Script
	for {
		pending, err := control.Next(bounded(t))
		if err != nil {
			t.Fatal("script/pause fixture observation failed", err)
		}
		switch pending.Method() {
		case debugger.ScriptParsed.Method:
			var event debugger.EventScriptParsed
			if err := json.Unmarshal(pending.DataCopy(), &event); err != nil {
				t.Fatal(err)
			}
			if event.URL == name {
				script, err = pending.Script()
				if err != nil {
					t.Fatal(err)
				}
			}
		case debugger.Paused.Method:
			if script.control == nil {
				t.Fatal("native pause did not follow its script observation")
			}
			return script, pending
		}
	}
}

func TestLiveDebuggerScriptCommandsAndLocationRepliesWhilePaused(t *testing.T) {
	for _, mode := range []string{"step-into", "step-over", "continue-to"} {
		t.Run(mode, func(t *testing.T) {
			owner, retained, dependencies := controlFixture(t, "data:text/html,<title>local script fixture</title>", 32)
			control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Debugger: &debugger.EnableParams{MaxScriptsCacheSize: 65536}, SharedIsolateEffects: true})
			if err != nil {
				t.Fatal(err)
			}
			const expression = "function scriptFixture(){\nlet count=1;debugger;\ncount+=2;\nreturn count;\n}\nscriptFixture();\n//# sourceURL=local-debugger-script.js"
			done := make(chan error, 1)
			go func() {
				value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](expression))
				if err == nil && value != 3 {
					err = errors.New("script stepping changed the result")
				}
				done <- err
			}()
			script, pending := debuggerScriptPause(t, control, "local-debugger-script.js")
			params := debugger.GetScriptSourceParams{}
			result, _, err := RunScriptCommand(bounded(t), script, debugger.GetScriptSource, params)
			if err != nil || !strings.Contains(result.ScriptSource, "count+=2") || params.ScriptID != "" {
				t.Fatal("observed script source/copy mismatch", err)
			}
			drain(t, dependencies, 1)
			search, _, err := RunScriptCommand(bounded(t), script, debugger.SearchInContent, debugger.SearchInContentParams{Query: "count", CaseSensitive: new(true), IsRegex: new(false)})
			if err != nil || len(search.Result) != 3 {
				t.Fatal("native script search mismatch", err)
			}
			drain(t, dependencies, 1)
			locations := debugger.GetPossibleBreakpointsParams{Start: &debugger.Location{LineNumber: 1, ColumnNumber: new(int64(0))}, RestrictToFunction: new(true)}
			possible, _, err := RunScriptCommand(bounded(t), script, debugger.GetPossibleBreakpoints, locations)
			if err != nil || len(possible.Locations) == 0 || locations.Start.ScriptID != "" {
				t.Fatal("breakpoint location source/copy mismatch", err)
			}
			drain(t, dependencies, 1)
			if _, _, err := RunScriptCommand(bounded(t), script, debugger.SetBlackboxedRanges, debugger.SetBlackboxedRangesParams{Positions: []*debugger.ScriptPosition{}}); err != nil {
				t.Fatal(err)
			}
			drain(t, dependencies, 1)
			var reply Reply
			switch mode {
			case "step-into":
				reply = StepIntoDebugger(debugger.StepIntoParams{BreakOnAsyncCall: new(false)})
			case "step-over":
				reply = StepOverDebugger(debugger.StepOverParams{})
			case "continue-to":
				reply = ContinueDebuggerTo(script, debugger.ContinueToLocationParams{Location: &debugger.Location{LineNumber: 3}, TargetCallFrames: debugger.ContinueToLocationTargetCallFramesCurrent})
			}
			if err := pending.Reply(bounded(t), reply); err != nil {
				t.Fatal("native stepping/continue failed", err)
			}
			next := debuggerPause(t, control)
			if err := next.Reply(bounded(t), ResumeDebugger(debugger.ResumeParams{})); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-bounded(t).Done():
				t.Fatal("script did not finish")
			}
			if err := control.Close(bounded(t)); err != nil {
				t.Fatal(err)
			}
			_ = owner.Close(bounded(t))
			if !owner.ShutdownComplete() {
				t.Fatal("script grant prevented cleanup")
			}
			status, _ := dependencies.Evidence.Inspect()
			drain(t, dependencies, status.Outstanding)
		})
	}
}
