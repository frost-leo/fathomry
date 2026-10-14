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
)

func TestDebuggerConsentAndEvidenceBeforeNativeWork(t *testing.T) {
	session := unitSession(t)
	if _, err := session.Control(context.Background(), ControlOptions{Debugger: &debugger.EnableParams{}}); !errors.Is(err, ErrInput) || session.control != nil {
		t.Fatal("debugger admitted without shared-isolate consent", err)
	}
	session.owner.settings.MaxEvents = 1
	state := &controlState{session: session}
	if err := state.prepareDebugger(); err != nil || len(state.records) != 1 {
		t.Fatal(err)
	}
	if err := state.prepareDebugger(); !errors.Is(err, ErrLimit) || len(state.records) != 1 {
		t.Fatal("full ledger admitted another debugger", err)
	}
	if complete, err := state.closeDebugger(context.Background()); !complete || err != nil {
		t.Fatal("unsubmitted debugger could not release", err)
	}
}

func TestDebuggerWireOrderRetiresReusedPauseIdentity(t *testing.T) {
	state := &controlState{session: unitSession(t), debugger: &debuggerState{}}
	data := jsonv2.Value(`{"callFrames":[{"callFrameId":"reused-native-id"}],"reason":"other"}`)
	if err := state.observeDebugger(debugger.Paused.Method, data, 1); err != nil {
		t.Fatal(err)
	}
	old := &pendingState{control: state, method: debugger.Paused.Method, sequence: 1}
	if old.stale() {
		t.Fatal("current pause was already stale")
	}
	if err := state.observeDebugger(debugger.Resumed.Method, jsonv2.Value(`{}`), 2); err != nil || !old.stale() {
		t.Fatal("resume failed to retire an undelivered pause", err)
	}
	if err := state.observeDebugger(debugger.Paused.Method, data, 3); err != nil || !old.stale() {
		t.Fatal("reused native identity revived an old pause", err)
	}
	current := &pendingState{control: state, method: debugger.Paused.Method, sequence: 3}
	if current.stale() {
		t.Fatal("new pause did not receive new authority")
	}
	for _, invalid := range []string{`{"callFrames":[null]}`, `{"callFrames":[{}]}`} {
		if err := state.observeDebugger(debugger.Paused.Method, jsonv2.Value(invalid), 4); !errors.Is(err, ErrNative) || state.debugger.pause != 3 {
			t.Fatal("malformed pause received authority", err)
		}
	}
	for _, reply := range []Reply{ResumeDebugger(debugger.ResumeParams{}), StepOutDebugger()} {
		if _, err := reply.params(&pendingState{}); !errors.Is(err, ErrInput) {
			t.Fatal("non-pause granted debugger reply", err)
		}
	}
}
