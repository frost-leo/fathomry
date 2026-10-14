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
	"bytes"
	"errors"
	"testing"

	"github.com/chromedp/cdproto/debugger"
)

func debuggerPause(t *testing.T, control *Control) Pending {
	t.Helper()
	for {
		pending, err := control.Next(bounded(t))
		if err != nil {
			t.Fatal("debugger pause was not delivered", err)
		}
		if pending.Method() == debugger.Paused.Method {
			return pending
		}
	}
}

func TestLiveDebuggerReservedResumeAtOrdinaryAndEvidenceSaturation(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<title>local debugger fixture</title>", 2)
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Debugger: &debugger.EnableParams{MaxScriptsCacheSize: 65536}, SharedIsolateEffects: true})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`(function localFixture(){debugger; return 42;})()`))
		if err == nil && value != 42 {
			err = errors.New("debugger changed the fixture result")
		}
		done <- err
	}()
	pending := debuggerPause(t, control)
	fillControlInbox(t, retained, dependencies)
	params := debugger.ResumeParams{TerminateOnResume: new(false)}
	if err := pending.Reply(bounded(t), ResumeDebugger(params)); err != nil {
		t.Fatal("reserved resume blocked behind its paused evaluation", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-bounded(t).Done():
		t.Fatal("evaluation did not resume")
	}
	if *params.TerminateOnResume {
		t.Fatal("resume mutated native options")
	}
	if err := pending.Reply(bounded(t), ResumeDebugger(params)); !errors.Is(err, ErrState) {
		t.Fatal("submitted pause token was reused", err)
	}
	if err := control.Close(bounded(t)); err != nil || !control.Closed() {
		t.Fatal("debugger cleanup required new ordinary admission", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	value, _ := snapshot.ValueCopy()
	for _, field := range []string{`"acknowledged":true`, `"resumeCompleted":true`, `"disabled":true`, `"backtraceFreed":true`} {
		if !bytes.Contains(value.PayloadCopy(), []byte(field)) {
			t.Fatal("independent debugger evidence missing", field)
		}
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("debugger source did not release")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveDebuggerConfigurationAndCloseWhileEvaluationPaused(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<title>debugger close fixture</title>", 8)
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Debugger: &debugger.EnableParams{}, SharedIsolateEffects: true})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`(function localFixture(){debugger; return 37;})()`))
		if err == nil && value != 37 {
			err = errors.New("close changed the paused result")
		}
		done <- err
	}()
	_ = debuggerPause(t, control)
	if _, _, err := RunDiagnosticCommand(bounded(t), control, debugger.SetPauseOnExceptions, debugger.SetPauseOnExceptionsParams{State: debugger.SetPauseOnExceptionsStateNone}); err != nil {
		t.Fatal("finite configuration could not run during a paused operation", err)
	}
	drain(t, dependencies, 1)
	if err := control.Close(bounded(t)); err != nil || !control.Closed() {
		t.Fatal("ignored debugger pause could not be cleaned up", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("cleanup did not preserve the waiting page operation", err)
		}
	case <-bounded(t).Done():
		t.Fatal("close left the page paused")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("debugger cleanup retained source")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
