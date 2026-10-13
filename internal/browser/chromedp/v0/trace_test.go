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
	"github.com/chromedp/cdproto/tracing"
)

func TestTracePreflightDoesNotStartUnownedOrInvalidProducer(t *testing.T) {
	own := &owner{settings: defaults(inertOptions())}
	if _, err := StartTrace(context.Background(), Source{owner: own}, tracing.StartParams{TransferMode: tracing.StartTransferModeReportEvents}); !errors.Is(err, ErrInput) || own.started {
		t.Fatal("unmanaged output mode started a browser", err)
	}
	own.settings.RemoteURL = "ws://not-connected.invalid/devtools/browser/peer"
	if _, err := StartTrace(context.Background(), Source{owner: own}, tracing.StartParams{}); !errors.Is(err, ErrState) || own.started {
		t.Fatal("borrowed trace acquired a native owner", err)
	}
}

func TestTraceRejectsMalformedCompletionBeforeCreatingStream(t *testing.T) {
	for _, data := range []string{`{}`, `{"stream":"untrusted"}`, `{"dataLossOccurred":false}`, `{"stream":"untrusted","dataLossOccurred":null}`, `{"dataLossOccurred":`} {
		state := &traceState{done: make(chan struct{}), unsubscribe: func() {}}
		raw := make(chan jsonv2.Value, 1)
		raw <- jsonv2.Value(data)
		state.receive(raw)
		if state.stream != nil || !errors.Is(state.primary, ErrNative) {
			t.Fatal("malformed trace identity became a stream", state.primary)
		}
	}
}

func TestTraceZeroHandles(t *testing.T) {
	var trace *Trace
	if _, err := trace.Stop(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	if _, err := trace.Read(context.Background(), 1); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	if err := trace.Close(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
}
