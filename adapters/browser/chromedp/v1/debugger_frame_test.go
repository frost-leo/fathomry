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
	"github.com/chromedp/cdproto/runtime"
)

func TestLiveDebuggerFrameEvaluationMutationAndExecutionRetirement(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, `data:text/html,<title>frame fixture</title><iframe src="about:blank"></iframe>`, 32)
	group, _, err := retained.Page().NewObjectGroup(retained.Context())
	if err != nil {
		t.Fatal(err)
	}
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Debugger: &debugger.EnableParams{}, SharedIsolateEffects: true})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`(function frameFixture(){let value={nested:{answer:7}};debugger;return value.nested.answer;})()`))
		if err == nil && value != 13 {
			err = errors.New("native variable mutation was not applied")
		}
		done <- err
	}()
	pending := debuggerPause(t, control)
	frame, err := pending.CallFrame(0)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := owner.Client().Profile(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(profile.ServiceVersion.Value, "Chrome-150.") || strings.HasPrefix(profile.ServiceVersion.Value, "Chromium-150.") || strings.HasPrefix(profile.ServiceVersion.Value, "HeadlessChrome-150.") {
		if _, _, err := group.EvaluateFrame(bounded(t), frame, debugger.EvaluateOnCallFrameParams{Expression: "value", ScopeNumber: 1}); !errors.Is(err, ErrUnsupported) {
			t.Fatal("Chrome 150 silently accepted unsupported scope-number semantics", err)
		}
		drain(t, dependencies, 1)
	}
	params := debugger.SetVariableValueParams{ScopeNumber: 0, VariableName: "value"}
	if _, err := frame.SetVariable(bounded(t), params, ValueArgument(map[string]any{"nested": map[string]int{"answer": 13}})); err != nil || params.CallFrameID != "" || params.NewValue != nil {
		t.Fatal("frame mutation/copy failed", err)
	}
	drain(t, dependencies, 1)
	evaluate := debugger.EvaluateOnCallFrameParams{Expression: "value", Silent: new(true), ReturnByValue: new(false), GeneratePreview: new(true), ThrowOnSideEffect: new(false), Timeout: runtime.TimeDelta(1000)}
	object, _, err := group.EvaluateFrame(bounded(t), frame, evaluate)
	if err != nil || object == nil || evaluate.CallFrameID != "" || evaluate.ObjectGroup != "" {
		t.Fatal("frame object/copy failed", err)
	}
	drain(t, dependencies, 1)
	if err := pending.Reply(bounded(t), ResumeDebugger(debugger.ResumeParams{})); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-bounded(t).Done():
		t.Fatal("frame mutation prevented resume")
	}
	drain(t, dependencies, 1)
	if _, _, err := group.EvaluateFrame(bounded(t), frame, evaluate); !errors.Is(err, ErrState) {
		t.Fatal("resumed frame regained authority", err)
	}
	drain(t, dependencies, 1)
	if err := control.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	properties, _, err := object.Properties(bounded(t), runtime.GetPropertiesParams{OwnProperties: new(true)})
	if err != nil {
		t.Fatal("separately grouped object did not outlive Debugger.Close", err)
	}
	drain(t, dependencies, 1)
	var nested *Object
	for _, property := range properties.Result {
		if property.Name == "nested" {
			nested, err = retained.Page().Object(property.Value)
		}
	}
	if err != nil || nested == nil {
		t.Fatal("property reference did not inherit managed custody", err)
	}
	derived, _, err := nested.Call(bounded(t), "function(){return this}")
	if err != nil || derived == nil {
		t.Fatal("derived call object failed", err)
	}
	drain(t, dependencies, 1)
	sub, _, err := Subscribe(retained.Context(), retained.Page(), runtime.ExecutionContextDestroyed)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool](`document.querySelector('iframe').remove();true`)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if event, _, err := sub.Next(bounded(t)); err != nil || event.ExecutionContextUniqueID == "" {
		t.Fatal("child-world retirement was not observed", err)
	}
	drain(t, dependencies, 1)
	for _, retired := range []*Object{object, nested, derived} {
		if _, _, err := retired.Call(bounded(t), "function(){return 1}"); !errors.Is(err, ErrState) {
			t.Fatal("unknown-origin fence was lost or restamped", err)
		}
		drain(t, dependencies, 1)
		if err := retired.Close(bounded(t)); !errors.Is(err, ErrCleanup) || retired.Closed() {
			t.Fatal("retired raw ID was released or custody refunded", err)
		}
	}
	if err := group.Close(bounded(t)); !group.Closed() || !object.Closed() || !nested.Closed() || !derived.Closed() {
		t.Fatal("safe private-group cleanup did not settle retired references", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("retired frame results prevented shutdown")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveDebuggerRestartFrameAndBusyGroupRefusal(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<title>restart fixture</title>", 16)
	group, _, err := retained.Page().NewObjectGroup(retained.Context())
	if err != nil {
		t.Fatal(err)
	}
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Debugger: &debugger.EnableParams{}, SharedIsolateEffects: true})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := group.Evaluate(bounded(t), `(function restartFixture(){let value=7;debugger;return {value};})()`)
		done <- err
	}()
	pending := debuggerPause(t, control)
	frame, err := pending.CallFrame(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := group.EvaluateFrame(bounded(t), frame, debugger.EvaluateOnCallFrameParams{Expression: "value"}); !errors.Is(err, ErrState) {
		t.Fatal("frame inspection waited on its suspended producer's group", err)
	}
	drain(t, dependencies, 1)
	params := debugger.RestartFrameParams{Mode: debugger.RestartFrameModeStepInto}
	if err := pending.Reply(bounded(t), RestartDebuggerFrame(frame, params)); err != nil || params.CallFrameID != "" {
		t.Fatal("native frame restart/copy failed", err)
	}
	next := debuggerPause(t, control)
	var event debugger.EventPaused
	if err := json.Unmarshal(next.DataCopy(), &event); err != nil || len(event.CallFrames) == 0 {
		t.Fatal("restart pause missing", err)
	}
	if _, _, err := RunDiagnosticCommand(bounded(t), control, debugger.SetSkipAllPauses, debugger.SetSkipAllPausesParams{Skip: true}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if err := next.Reply(bounded(t), ResumeDebugger(debugger.ResumeParams{})); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-bounded(t).Done():
		t.Fatal("restarted producer did not finish")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("restart fixture did not release")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
