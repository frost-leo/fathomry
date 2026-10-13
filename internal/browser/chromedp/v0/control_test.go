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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/page"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestScreencastControlRejectsMissingIdentityAndBoundsLedger(t *testing.T) {
	for _, fixture := range []struct {
		name   string
		params []string
		want   error
	}{
		{"missing-id", []string{`{"data":"eA==","metadata":{}}`}, ErrNative},
		{"null-id", []string{`{"data":"eA==","metadata":{},"sessionId":null}`}, ErrNative},
		{"missing-metadata", []string{`{"data":"eA==","sessionId":0}`}, ErrNative},
		{"overflow", []string{`{"data":"eA==","metadata":{},"sessionId":0}`, `{"data":"eA==","metadata":{},"sessionId":1}`}, ErrLimit},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			session := unitSession(t)
			session.owner.settings.MaxEvents = 1
			life, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			state := &controlState{session: session, life: life, cancel: cancel, pending: make(chan *pendingState, 1), pumpDone: make(chan struct{}), unsubscribe: func() {}}
			raw := make(chan jsonv2.Value, len(fixture.params))
			for _, params := range fixture.params {
				raw <- jsonv2.Value(`{"method":"Page.screencastFrame","sequence":1,"params":` + params + `}`)
			}
			go state.pump(raw)
			select {
			case <-state.pumpDone:
			case <-deadline(t).Done():
				t.Fatal("invalid or saturated frame producer did not stop")
			}
			if !errors.Is((&Control{state: state}).Err(), fixture.want) || len(state.records) > 1 {
				t.Fatal("frame identity or cumulative event bound was ignored")
			}
		})
	}
	params, err := ScreencastFrameAck().params(&pendingState{method: page.ScreencastFrame.Method, frame: 0})
	if err != nil || params.(page.ScreencastFrameAckParams).SessionID != 0 {
		t.Fatal("explicit native zero frame identity was lost", err)
	}
	if _, err := ScreencastFrameAck().params(&pendingState{method: fetch.RequestPaused.Method, request: "foreign"}); !errors.Is(err, ErrInput) {
		t.Fatal("non-frame token authorized a frame acknowledgement", err)
	}
}

func TestControlObservationReservesSetupAndInFlightEventsBeforeDecode(t *testing.T) {
	session := unitSession(t)
	session.owner.settings.MaxEvents = 2
	state := &controlState{session: session, eventReservations: make(map[uint64]struct{})}
	if err := state.prepareDebugger(); err != nil {
		t.Fatal(err)
	}
	state.eventCount = len(state.records)
	data := jsonv2.Value(`{"scriptId":"first"}`)
	if err := state.reserveControlEvent("Debugger.scriptParsed", data, 1); err != nil {
		t.Fatal(err)
	}
	before := state.bytes
	if err := state.reserveControlEvent("Debugger.scriptParsed", data, 2); !errors.Is(err, ErrLimit) || state.bytes != before || len(state.eventReservations) != 1 || state.debugger.scriptCount != 0 {
		t.Fatal("setup/in-flight event capacity was checked after observer decoding", err)
	}
}

func TestControlDisposalProofStillJoinsActiveReply(t *testing.T) {
	session := unitSession(t)
	life, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	setup, pump, idle := make(chan struct{}), make(chan struct{}), make(chan struct{})
	close(setup)
	close(pump)
	state := &controlState{session: session, life: life, cancel: cancel, setupDone: setup, pumpDone: pump, gate: make(chan struct{}, 1), stopCaller: func() bool { return true }, active: 1, idle: idle}
	session.control = state
	control := &Control{state: state}
	state.records = []*pendingState{{control: state, method: fetch.RequestPaused.Method, attempted: true}}
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond)
	defer done()
	if err := state.disposed(ctx); !errors.Is(err, context.DeadlineExceeded) || control.Closed() {
		t.Fatal("remote disposal was treated as local completion", err)
	}
	if _, err := control.EvidenceCopy(); !errors.Is(err, ErrState) {
		t.Fatal("evidence froze before actual reply exit", err)
	}
	state.mu.Lock()
	state.records[0].acknowledged = true
	state.active = 0
	close(idle)
	state.mu.Unlock()
	if err := state.disposed(deadline(t)); err != nil || !control.Closed() {
		t.Fatal(err)
	}
	data, err := control.EvidenceCopy()
	if err != nil || !strings.Contains(string(data), `"acknowledged":true`) {
		t.Fatal("late actual outcome lost", err)
	}
}

func TestControlNativeCauseRemainsPrivate(t *testing.T) {
	state := &controlState{}
	cause := &cdproto.Error{Code: -32001, Message: "private-control-canary"}
	state.fail(cause)
	control := &Control{state: state}
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		if strings.Contains(fmt.Sprintf(verb, control.Err()), cause.Message) {
			t.Fatal("ordinary control error leaked")
		}
	}
	var native *cdproto.Error
	if !errors.As(control.Err(), &native) || native != cause {
		t.Fatal("deliberate native inspection removed")
	}
}

func TestBrowserControlJoinsAfterOwnedProcessAlreadyExited(t *testing.T) {
	fixture := bindFixture(t, browserOptions(t), 1)
	ctx, err := fixture.client.Retain(deadline(t), fault.Correlation{Call: "control-process-exit"}, SessionOptionsV1{ManagedEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	control, err := ctx.Page().Control(ctx.Page().Context(), ControlOptions{Fetch: &fetch.EnableParams{}, Dialogs: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.owner.process.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-fixture.client.owner.processDone
	_ = ctx.Close(deadline(t))
	if !control.Closed() {
		t.Fatal("known process exit left a control lease forever open")
	}
	snapshot, err := ctx.Receipt().WaitReleased(deadline(t))
	if err != nil || !snapshot.Outcome.Value.ContextReleased() {
		t.Fatal("positive process termination did not reconcile contexts", err)
	}
	// A disconnected producer remains an error, independent of complete cleanup.
	if snapshot.Outcome.Primary == nil {
		t.Fatal("process failure disappeared")
	}
	if err := fixture.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
}
