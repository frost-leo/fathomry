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
	"time"

	"github.com/chromedp/cdproto/debugger"
)

func TestLiveDebuggerBlackboxUsesCurrentSamePageContextGrants(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<title>blackbox fixture</title>", 32)
	executions, _, err := retained.Page().ExecutionContexts(bounded(t))
	if err != nil || len(executions) != 1 {
		t.Fatal("main world fixture identity missing", err)
	}
	drain(t, dependencies, 1)
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Debugger: &debugger.EnableParams{}, SharedIsolateEffects: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.SetBlackboxContexts(bounded(t), executions...); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	work, cancel := context.WithTimeout(retained.Context(), 3*time.Second)
	value, _, err := RunAction(work, retained.Page(), Evaluate[int](`(function blackboxFixture(){debugger;return 7;})()`))
	cancel()
	if err != nil || value != 7 {
		_ = control.Close(bounded(t))
		t.Fatal("native context blackboxing did not suppress the fixture pause", err)
	}
	drain(t, dependencies, 1)
	if _, err := control.SetBlackboxContexts(bounded(t)); err != nil {
		t.Fatal("empty list did not clear native blackboxing", err)
	}
	drain(t, dependencies, 1)
	done := make(chan error, 1)
	go func() {
		_, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`(function restoredFixture(){debugger;return 7;})()`))
		done <- err
	}()
	pending := debuggerPause(t, control)
	if err := pending.Reply(bounded(t), ResumeDebugger(debugger.ResumeParams{})); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-bounded(t).Done():
		t.Fatal("cleared blackboxing left a fixture pause unresolved")
	}
	drain(t, dependencies, 1)
	other, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	foreign, _, err := other.ExecutionContexts(bounded(t))
	if err != nil || len(foreign) == 0 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, err := control.SetBlackboxContexts(bounded(t), foreign[0]); !errors.Is(err, ErrInput) {
		t.Fatal("foreign execution context granted blackboxing", err)
	}
	drain(t, dependencies, 1)
	if _, err := retained.Page().Navigate(bounded(t), "data:text/html,<title>replacement world</title>"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, err := control.SetBlackboxContexts(bounded(t), executions[0]); !errors.Is(err, ErrState) {
		t.Fatal("retired world regained blackboxing authority", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("blackbox control did not release")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
