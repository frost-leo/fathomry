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

	"github.com/chromedp/cdproto/tracing"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestKeepOpenRefusesBorrowedOwnershipBeforeStarting(t *testing.T) {
	own := &owner{settings: defaults(OptionsV1{RemoteURL: "ws://not-connected.invalid/devtools/browser/peer"})}
	if kept, err := KeepOpen(context.Background(), Source{owner: own}); kept != nil || !errors.Is(err, ErrState) || own.started {
		t.Fatal("borrowed browser ownership inferred", err)
	}
	var kept *KeptBrowser
	if kept.ProcessExited() || kept.Closed() || kept.Endpoint() != "" || kept.ProfileDirectory() != "" {
		t.Fatal("zero transferred owner reported lifecycle facts")
	}
	if err := kept.Close(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
}

func TestBrowserKeepOpenRequiresActualContextAndProducerQuiescence(t *testing.T) {
	fixture := bindFixture(t, browserOptions(t), 1)
	id := fault.Correlation{Call: "transfer-preconditions"}
	retained, err := fixture.client.Retain(deadline(t), id)
	if err != nil {
		t.Fatal(err)
	}
	source := Source{owner: fixture.client.owner}
	if kept, err := KeepOpen(deadline(t), source); kept != nil || !errors.Is(err, ErrState) || source.owner.transferred {
		t.Fatal("live context was transferred", err)
	}
	if err := retained.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	settle(t, fixture, id, retained.Receipt(), nil, nil, func(testing.TB, Result) {})
	trace, err := StartTrace(deadline(t), source, tracing.StartParams{TraceConfig: &tracing.TraceConfig{TraceBufferSizeInKb: 1024, IncludedCategories: []string{"blink.user_timing"}}})
	if err != nil {
		t.Fatal(err)
	}
	if kept, err := KeepOpen(deadline(t), source); kept != nil || !errors.Is(err, ErrState) || source.owner.transferred {
		t.Fatal("live producer was transferred", err)
	}
	if err := trace.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
}

func TestKeepOpenRevokesBlockedContextReservation(t *testing.T) {
	own := &owner{settings: defaults(inertOptions()), sessions: make(map[*browserContext]struct{})}
	session := &Session{sessionState: &sessionState{browserContext: &browserContext{}}}
	own.mu.Lock()
	started, done := make(chan struct{}), make(chan error, 1)
	go func() { close(started); done <- own.reserve(session, 1) }()
	<-started
	own.transferred = true
	own.mu.Unlock()
	if err := <-done; !errors.Is(err, ErrState) {
		t.Fatal("context admission survived ownership transfer", err)
	}
	if len(own.sessions) != 0 {
		t.Fatal("transferred source retained a new context")
	}
}
