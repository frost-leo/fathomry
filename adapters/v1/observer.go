/**
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

package adapters

import (
	"context"
	"io"
	"math"
	"sync"
)

// Phase describes a local operation transition, not native dispatch or effects.
type Phase uint8

const (
	Admitted Phase = 1
	Resolved Phase = 2
	Released Phase = 3
)

// Event is payload-free optional diagnostics. Failed denotes reported errors,
// never a business terminal decision. It excludes correlation/source IDs, values,
// native errors and arbitrary attributes; it is not required evidence.
type Event struct {
	Operation string
	Phase     Phase
	Failed    bool
}

// ObservationStatus is a detached bounded-queue observation.
type ObservationStatus struct {
	Queued  int
	Dropped uint64
	Closed  bool
}

// Observer owns an optional bounded event queue. A full/closed observer drops and
// counts events, never blocking producers or replacing required evidence.
type Observer struct{ state *observerState }

type observerState struct {
	mu       sync.Mutex
	capacity int
	queue    []Event
	dropped  uint64
	closed   bool
	changed  chan struct{}
}

// NewObserver requires capacity 1..65536. No exporter/global registration starts.
func NewObserver(capacity int) (*Observer, error) {
	if capacity < 1 || capacity > 65536 {
		return nil, failureOf(ErrOptions, "observer", "", Details{})
	}
	return &Observer{state: &observerState{capacity: capacity, changed: make(chan struct{})}}, nil
}
func (observer *Observer) publish(event Event) {
	if observer == nil || observer.state == nil {
		return
	}
	observer.state.mu.Lock()
	defer observer.state.mu.Unlock()
	if observer.state.closed || len(observer.state.queue) == observer.state.capacity {
		if observer.state.dropped < math.MaxUint64 {
			observer.state.dropped++
		}
		return
	}
	observer.state.queue = append(observer.state.queue, event)
	close(observer.state.changed)
	observer.state.changed = make(chan struct{})
}

// Next consumes a local event. Cancellation affects this wait only. A sealed,
// drained observer reports EOF; no native retry or background drain is performed.
func (observer *Observer) Next(ctx context.Context) (Event, error) {
	if observer == nil || observer.state == nil {
		return Event{}, failureOf(ErrHandle, "observe", "", Details{})
	}
	if ctx == nil {
		return Event{}, failureOf(ErrOptions, "observe", "", Details{})
	}
	for {
		if ctx.Err() != nil {
			return Event{}, failureOf(ErrWait, "observe", "", Details{}, ctx.Err(), context.Cause(ctx))
		}
		observer.state.mu.Lock()
		if len(observer.state.queue) > 0 {
			event := observer.state.queue[0]
			observer.state.queue[0] = Event{}
			observer.state.queue = observer.state.queue[1:]
			observer.state.mu.Unlock()
			return event, nil
		}
		if observer.state.closed {
			observer.state.mu.Unlock()
			return Event{}, failureOf(ErrClosed, "observe", "", Details{}, io.EOF)
		}
		changed := observer.state.changed
		observer.state.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return Event{}, failureOf(ErrWait, "observe", "", Details{}, ctx.Err(), context.Cause(ctx))
		}
	}
}

// Seal affects optional observation only, not work or required evidence.
func (observer *Observer) Seal() error {
	if observer == nil || observer.state == nil {
		return failureOf(ErrHandle, "seal_observer", "", Details{})
	}
	observer.state.mu.Lock()
	defer observer.state.mu.Unlock()
	observer.state.closed = true
	close(observer.state.changed)
	observer.state.changed = make(chan struct{})
	return nil
}

// Inspect reports queue occupancy and cumulative loss, not operation outcomes.
func (observer *Observer) Inspect() (ObservationStatus, error) {
	if observer == nil || observer.state == nil {
		return ObservationStatus{}, failureOf(ErrHandle, "inspect_observer", "", Details{})
	}
	observer.state.mu.Lock()
	defer observer.state.mu.Unlock()
	return ObservationStatus{len(observer.state.queue), observer.state.dropped, observer.state.closed}, nil
}
