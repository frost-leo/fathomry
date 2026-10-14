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
	"testing"

	"github.com/chromedp/cdproto/debugger"
)

func TestLiveDebuggerSetVariablePreservesEmptyJSONValues(t *testing.T) {
	for _, fixture := range []struct {
		name  string
		value any
		want  string
	}{
		{"string", "", `string:""`},
		{"array", []any{}, "object:[]"},
		{"object", map[string]any{}, "object:{}"},
		{"null", nil, "object:null"},
		{"false-control", false, "boolean:false"},
		{"zero-control", 0, "number:0"},
		{"undefined", UndefinedArgument(), "undefined:undefined"},
		{"special-nan", UnserializableArgument("NaN"), "number:null"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			owner, retained, dependencies := controlFixture(t, "data:text/html,<title>frame value fixture</title>", 32)
			control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Debugger: &debugger.EnableParams{}, SharedIsolateEffects: true})
			if err != nil {
				t.Fatal(err)
			}
			type returned struct {
				value string
				err   error
			}
			done := make(chan returned, 1)
			go func() {
				value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[string](`(function frameValueFixture(){let value=42;debugger;return typeof value+':'+JSON.stringify(value);})()`))
				done <- returned{value: value, err: err}
			}()
			pending := debuggerPause(t, control)
			frame, err := pending.CallFrame(0)
			if err != nil {
				t.Fatal(err)
			}
			argument, managed := fixture.value.(Argument)
			if !managed {
				argument = ValueArgument(fixture.value)
			}
			_, mutationErr := frame.SetVariable(bounded(t), debugger.SetVariableValueParams{ScopeNumber: 0, VariableName: "value"}, argument)
			if mutationErr != nil {
				t.Error("native mutation failed", mutationErr)
			}
			drain(t, dependencies, 1)
			if err := pending.Reply(bounded(t), ResumeDebugger(debugger.ResumeParams{})); err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-done:
				t.Logf("setVariable error=%v, observed=%q, expected=%q", mutationErr, result.value, fixture.want)
				if result.err != nil || result.value != fixture.want {
					t.Error("native frame value was changed or became undefined", result.err)
				}
			case <-bounded(t).Done():
				t.Fatal("frame did not resume")
			}
			drain(t, dependencies, 1)
			_ = owner.Close(bounded(t))
			if !owner.ShutdownComplete() {
				t.Error("fixture cleanup incomplete")
			}
			status, _ := dependencies.Evidence.Inspect()
			drain(t, dependencies, status.Outstanding)
		})
	}
}
