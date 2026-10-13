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
	"math"
	"strings"
	"testing"

	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/page"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestResponseBodyAdmissionAndNativeReplyConstraints(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*controlState, *pendingState)
		limit  int64
		want   error
	}{
		{"zero-limit", nil, 0, ErrInput},
		{"negative-limit", nil, -1, ErrInput},
		{"overflow-limit", nil, math.MaxInt64, ErrLimit},
		{"request-stage", func(_ *controlState, pending *pendingState) { pending.response = false }, 64, ErrInput},
		{"foreign-kind", func(_ *controlState, pending *pendingState) { pending.method = page.JavascriptDialogOpening.Method }, 64, ErrInput},
		{"already-replied", func(_ *controlState, pending *pendingState) { pending.attempted = true }, 64, ErrState},
		{"already-read", func(_ *controlState, pending *pendingState) { pending.body = &responseBodyState{} }, 64, ErrState},
		{"reply-active", func(state *controlState, _ *pendingState) { state.active = 1 }, 64, ErrState},
		{"ledger-full", func(state *controlState, _ *pendingState) { state.bytes = state.session.owner.settings.MaxResultBytes }, 64, ErrLimit},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &controlState{session: unitSession(t), life: context.Background()}
			pending := &pendingState{control: state, method: fetch.RequestPaused.Method, request: "original", response: true}
			if test.change != nil {
				test.change(state, pending)
			}
			beforeBytes, beforeBody, beforeActive := state.bytes, pending.body, state.active
			_, finish, err := (Pending{state: pending}).beginResponseBody(context.Background(), false, test.limit)
			if !errors.Is(err, test.want) || finish != nil || state.bytes != beforeBytes || pending.body != beforeBody || state.active != beforeActive {
				t.Fatal("refusal mutated admission or reached native work", err)
			}
		})
	}
	pending := &pendingState{method: fetch.RequestPaused.Method, response: true, request: "original", body: &responseBodyState{streaming: true, settled: true, stream: &Stream{state: &streamState{}}}}
	if err := pending.responseReplyReady(); !errors.Is(err, ErrState) {
		t.Fatal("unfinished body permitted request mutation", err)
	}
	pending.body.stream.state.eof = true
	if err := pending.responseReplyReady(); err != nil {
		t.Fatal(err)
	}
	for _, reply := range []Reply{ContinueRequest(fetch.ContinueRequestParams{}), ContinueResponse(fetch.ContinueResponseParams{})} {
		if _, err := reply.params(pending); !errors.Is(err, ErrInput) {
			t.Fatal("taken stream allowed original continuation", err)
		}
	}
	if _, err := FulfillRequest(fetch.FulfillRequestParams{ResponseCode: 200}).params(pending); err != nil {
		t.Fatal("consumed stream could not be fulfilled", err)
	}
}

func TestResponseBodyUncertainWorkCannotReportCleanup(t *testing.T) {
	session := unitSession(t)
	state := &controlState{session: session}
	state.records = []*pendingState{{body: &responseBodyState{settled: false}}}
	if complete, err := state.closeResponseBodies(context.Background()); complete || !errors.Is(err, ErrCleanup) {
		t.Fatal("unknown body producer was reported complete", complete, err)
	}
	session.mu.Lock()
	session.targetGone = true
	session.mu.Unlock()
	if complete, err := state.closeResponseBodies(context.Background()); !complete || err != nil {
		t.Fatal("positive target destruction did not settle body custody", complete, err)
	}
}

func TestResponseControlSourceCustodyCountsOriginalContextOnce(t *testing.T) {
	session := unitSession(t)
	own := session.owner
	own.settings.MaxSessions = 2
	own.sessions = make(map[*browserContext]struct{})
	first := &controlState{session: session}
	second := &controlState{session: &Session{sessionState: &sessionState{browserContext: session.browserContext}}}
	own.cleanupControls = map[*controlState]struct{}{first: {}, second: {}}
	newPage := &Session{sessionState: &sessionState{browserContext: &browserContext{}}}
	if err := own.reserve(newPage, 2); err != nil {
		t.Fatal("page control aliases multiplied retained context capacity", err)
	}
	third := &Session{sessionState: &sessionState{browserContext: &browserContext{}}}
	if err := own.reserve(third, 2); !errors.Is(err, ErrLimit) {
		t.Fatal("retired control custody stopped consuming capacity", err)
	}
	delete(own.cleanupControls, first)
	if err := own.reserve(third, 2); !errors.Is(err, ErrLimit) {
		t.Fatal("one of two pending controls released the context slot", err)
	}
	delete(own.cleanupControls, second)
	if err := own.reserve(third, 2); err != nil {
		t.Fatal("actual control cleanup did not release capacity", err)
	}
}

func TestResponseControlPreparationReservesEachPageHandoff(t *testing.T) {
	options := inertOptions()
	options.MaxSessions, options.MaxPages = 2, 1
	one, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	options.MaxPages = 3
	three, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	settings := defaults(options)
	perPage := 2*settings.MaxEventBytes + 2*settings.MaxResultBytes + int64(settings.MaxEvents)*512 + 65536
	if three.budget.SourceBytes-one.budget.SourceBytes != 2*2*perPage {
		t.Fatal("source reservation did not cover per-page cleanup handoff")
	}
}

func TestBrowserResponseInputRefusalIsNotAnObservedNativeResult(t *testing.T) {
	fixture := bindFixture(t, browserOptions(t), 1)
	retained, err := fixture.client.Retain(deadline(t), fault.Correlation{Call: "response-input-refusal"}, SessionOptionsV1{ManagedEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close(context.Background())
	control, err := retained.Page().Control(retained.Page().Context(), ControlOptions{Fetch: &fetch.EnableParams{}})
	if err != nil {
		t.Fatal(err)
	}
	pending := &pendingState{control: control.state, method: fetch.RequestPaused.Method, response: true, request: fetch.RequestID(strings.Repeat("x", 8192))}
	if _, err := (Pending{state: pending}).ResponseBody(deadline(t), 4096); !errors.Is(err, ErrLimit) {
		t.Fatal("native input bound did not refuse malformed peer identity", err)
	}
	if pending.body == nil || pending.body.settled || pending.body.evidence.Acknowledged {
		t.Fatal("input refusal was mistaken for an observed native body result")
	}
}
