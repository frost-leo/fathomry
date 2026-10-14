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

	"github.com/chromedp/cdproto/page"
)

func TestRecordingPreflightReservesStreamAndLedgerBeforeNativeWork(t *testing.T) {
	session := unitSession(t)
	session.owner.settings.MaxStreams = 1
	session.owner.settings.MaxEvents = 1
	session.owner.settings.MaxResultBytes = 16384
	state := &controlState{session: session}
	if err := state.prepareRecording(); err != nil || len(session.browserContext.streams) != 1 || len(state.records) != 1 {
		t.Fatal("recording did not reserve both resource and ledger", err)
	}
	if err := state.prepareRecording(); !errors.Is(err, ErrLimit) || len(session.browserContext.streams) != 1 || len(state.records) != 1 {
		t.Fatal("full recording ledger retained another producer", err)
	}
	if complete, err := state.closeRecording(context.Background()); !complete || err != nil || len(session.browserContext.streams) != 0 {
		t.Fatal("unsubmitted recording kept a stream slot", err)
	}
	state = &controlState{session: session}
	session.owner.settings.MaxResultBytes = 8191
	if err := state.prepareRecording(); !errors.Is(err, ErrLimit) || len(state.records) != 0 || len(session.browserContext.streams) != 0 {
		t.Fatal("insufficient evidence admitted recording", err)
	}
	if _, err := session.Control(context.Background(), ControlOptions{Recording: &page.StartScreenRecordingParams{}, Screencast: &page.StartScreencastParams{}}); !errors.Is(err, ErrInput) || session.control != nil {
		t.Fatal("mutually exclusive producers reached native setup", err)
	}
}

func TestRecordingDisposalKeepsHistoryWithoutLosingCompletion(t *testing.T) {
	session := unitSession(t)
	done := make(chan struct{})
	close(done)
	life, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	state := &controlState{session: session, life: life, cancel: cancel, setupDone: done, pumpDone: done, gate: make(chan struct{}, 1), stopCaller: func() bool { return false }}
	if err := state.prepareRecording(); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("original recording cleanup fixture")
	state.recording.stream.state.cleanup = cause
	state.recording.evidence.StartQueued = true
	state.recording.evidence.Started = true
	session.mu.Lock()
	session.targetGone = true
	session.mu.Unlock()
	control := &Control{state: state}
	if err := state.disposed(context.Background()); !errors.Is(err, cause) || !control.Closed() || !state.recording.stream.Closed() {
		t.Fatal("historical failure blocked confirmed disposal", err)
	}
	if state.recording.evidence.Stopped || !state.recording.evidence.ResourcesGone {
		t.Fatal("native resource destruction fabricated a stop acknowledgement")
	}
}

func TestRecordingZeroHandlesAndReadGate(t *testing.T) {
	var control *Control
	if _, err := control.StopRecording(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	if _, err := control.ReadRecording(context.Background(), 1); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	state := &controlState{session: unitSession(t), life: context.Background(), recording: &recordingState{}}
	if _, err := (&Control{state: state}).ReadRecording(context.Background(), 1); !errors.Is(err, ErrState) || state.commands != 0 {
		t.Fatal("unfinished recorder reached native IO", err)
	}
}
