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

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/network"
)

func requestControl(t *testing.T) *controlState {
	t.Helper()
	life, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	return &controlState{session: unitSession(t), life: life, cancel: cancel,
		network: &requestLedger{requests: make(map[network.RequestID]*requestState), events: make(map[uint64]*requestState)}}
}

func TestRequestOriginUsesProducerOrderAndRejectsUnobservedStarts(t *testing.T) {
	state := requestControl(t)
	start := jsonv2.Value(`{"requestId":"same","loaderId":"loader","frameId":"frame","type":"XHR","request":{"url":"https://synthetic.invalid/first"}}`)
	response := jsonv2.Value(`{"requestId":"same","loaderId":"loader","frameId":"frame","response":{"status":418}}`)
	if err := state.observeRequest(network.ResponseReceived.Method, response, 1); err != nil || state.network.events[1] != nil {
		t.Fatal("unobserved request start acquired authority", err)
	}
	if _, err := (Pending{state: &pendingState{control: state}}).Request(); !errors.Is(err, ErrInput) {
		t.Fatal("unobserved response created a grant", err)
	}
	if err := state.observeRequest(network.RequestWillBeSent.Method, start, 2); err != nil {
		t.Fatal(err)
	}
	first := state.network.events[2]
	grant, err := (Pending{state: &pendingState{control: state, network: first}}).Request()
	if err != nil {
		t.Fatal(err)
	}
	if err := grant.check(network.GetResponseBody.Method); !errors.Is(err, ErrState) {
		t.Fatal("incomplete load authorized body access", err)
	}
	if err := state.observeRequest(network.ResponseReceived.Method, response, 3); err != nil {
		t.Fatal(err)
	}
	if err := state.observeRequest(network.LoadingFinished.Method, jsonv2.Value(`{"requestId":"same"}`), 4); err != nil {
		t.Fatal(err)
	}
	if err := grant.check(network.GetResponseBody.Method); err != nil {
		t.Fatal("HTTP status was mistaken for native load failure", err)
	}
	if err := state.observeRequest(network.RequestWillBeSent.Method, start, 5); err != nil {
		t.Fatal(err)
	}
	if state.network.events[3] != first || state.network.events[4] != first || state.network.events[5] == first {
		t.Fatal("queued observations adopted a replacement request")
	}
	if err := grant.check(network.GetRequestPostData.Method); !errors.Is(err, ErrState) {
		t.Fatal("old grant followed a reused native ID", err)
	}
}

func TestRequestOriginBoundsAndMalformedEvents(t *testing.T) {
	for _, data := range []string{`{`, `{}`, `{"requestId":"x"}`, `{"requestId":"x","request":null}`, `{"requestId":"x","request":"not-an-object"}`} {
		state := requestControl(t)
		if err := state.observeRequest(network.RequestWillBeSent.Method, jsonv2.Value(data), 1); !errors.Is(err, ErrNative) || len(state.network.requests) != 0 {
			t.Fatal("malformed event became authority", data, err)
		}
	}
	state := requestControl(t)
	state.session.owner.settings.MaxEvents = 1
	data := jsonv2.Value(`{"requestId":"first","request":{}}`)
	if err := state.observeRequest(network.RequestWillBeSent.Method, data, 1); err != nil {
		t.Fatal(err)
	}
	if err := state.observeRequest(network.RequestWillBeSent.Method, jsonv2.Value(`{"requestId":"second","request":{}}`), 2); !errors.Is(err, ErrLimit) || len(state.network.requests) != 1 {
		t.Fatal("producer-side request count is unbounded", err)
	}
	state = requestControl(t)
	state.session.owner.settings.MaxResultBytes = int64(len(data)) + 255
	if err := state.observeRequest(network.RequestWillBeSent.Method, data, 1); !errors.Is(err, ErrLimit) || len(state.network.requests) != 0 {
		t.Fatal("producer-side request bytes are unbounded", err)
	}
}

func TestRequestCloseWaitsForActualWorkWithoutTakingReplyLane(t *testing.T) {
	state := requestControl(t)
	if err := state.observeRequest(network.RequestWillBeSent.Method, jsonv2.Value(`{"requestId":"one","request":{}}`), 1); err != nil {
		t.Fatal(err)
	}
	request := Request{state: state.network.requests["one"]}
	work, finish, err := request.enter(context.Background(), network.GetRequestPostData.Method)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if finish != nil {
			finish()
		}
	}()
	if state.active != 0 {
		t.Fatal("ordinary observation took the reserved control reply lane")
	}
	state.mu.Lock()
	state.closing = true
	state.mu.Unlock()
	state.cancel(context.Canceled)
	if work.Err() == nil {
		t.Fatal("control close did not cancel request work")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := state.joinCommands(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("cancellation was mistaken for actual work exit", err)
	}
	if _, _, err := request.enter(context.Background(), network.GetRequestPostData.Method); !errors.Is(err, ErrState) {
		t.Fatal("closing control admitted another request", err)
	}
	finish()
	finish = nil
	if err := state.joinCommands(context.Background()); err != nil {
		t.Fatal("actual exit did not release request work", err)
	}
}

func FuzzRequestOrigin(f *testing.F) {
	f.Add(uint8(0), uint64(1), []byte(`{"requestId":"one","request":{}}`))
	f.Add(uint8(1), uint64(1), []byte(`{"requestId":"one","response":{}}`))
	f.Add(uint8(0), uint64(0), []byte(`{"requestId":"one","request":{}}`))
	f.Add(uint8(0), uint64(2), []byte(`{"requestId":"one","request":"invalid"}`))
	f.Add(uint8(3), uint64(1), []byte(`null`))
	f.Fuzz(func(t *testing.T, kind uint8, sequence uint64, data []byte) {
		state := requestControl(t)
		state.session.owner.settings.MaxEvents = 1
		state.session.owner.settings.MaxResultBytes = 4096
		methods := []string{network.RequestWillBeSent.Method, network.ResponseReceived.Method, network.LoadingFinished.Method, network.LoadingFailed.Method}
		err := state.observeRequest(methods[int(kind)%len(methods)], data, sequence)
		if len(state.network.requests) > 1 || len(state.network.events) > 1 || state.network.bytes > 4096 || state.network.count > 1 {
			t.Fatal("request observer exceeded its finite reservation")
		}
		if err != nil && len(state.network.requests) != 0 {
			t.Fatal("invalid input acquired request authority")
		}
		for id, request := range state.network.requests {
			if id == "" || request.id != id || request.control != state || sequence == 0 {
				t.Fatal("request provenance was lost")
			}
		}
	})
}
