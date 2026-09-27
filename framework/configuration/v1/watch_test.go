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

package configuration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	local "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	adapters "github.com/frost-leo/fathomry/adapters/v1"
)

type controlledProject struct {
	Count int `json:"count"`
}
type controlledBatch struct {
	private
	raw string
}

func (batch *controlledBatch) Documents() []source.Document {
	return []source.Document{{Name: "document", Presence: source.Present, Bytes: len(batch.raw)}}
}
func (batch *controlledBatch) RawCopy(name string) ([]byte, source.Presence, error) {
	if name != "document" {
		return nil, 0, fail(ErrValue)
	}
	return []byte(batch.raw), source.Present, nil
}

type controlledCursor struct {
	private
	owner *controlledObserver
}
type controlledObserver struct {
	private
	mu            sync.Mutex
	state         source.State
	changed       chan struct{}
	cursor        *controlledCursor
	closeStarted  chan struct{}
	closeOnce     sync.Once
	release       <-chan struct{}
	cleanup       error
	closed        bool
	afterTerminal atomic.Int32
}

func newControlledObserver(count int) *controlledObserver {
	observer := &controlledObserver{changed: make(chan struct{}), closeStarted: make(chan struct{})}
	observer.set(count, source.Available, nil)
	return observer
}
func (observer *controlledObserver) set(count int, status source.Status, err error) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.closed {
		return
	}
	observer.cursor = &controlledCursor{owner: observer}
	observer.state = source.State{Batch: &controlledBatch{raw: fmt.Sprintf("format: 1\nproject: {count: %d}", count)}, Status: status, Failure: err, Cursor: observer.cursor}
	close(observer.changed)
	observer.changed = make(chan struct{})
}
func (observer *controlledObserver) Current() (source.State, error) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.state, nil
}
func (observer *controlledObserver) Next(ctx context.Context, after source.Cursor) (source.State, error) {
	for {
		observer.mu.Lock()
		if after != observer.cursor {
			state := observer.state
			observer.mu.Unlock()
			return state, nil
		}
		changed := observer.changed
		closed := observer.closed
		observer.mu.Unlock()
		if closed {
			observer.afterTerminal.Add(1)
			return source.State{}, fail(source.ErrClosed)
		}
		select {
		case <-ctx.Done():
			return source.State{}, ctx.Err()
		case <-changed:
		}
	}
}
func (observer *controlledObserver) Close(ctx context.Context) error {
	observer.closeOnce.Do(func() { close(observer.closeStarted) })
	if observer.release != nil {
		select {
		case <-observer.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if !observer.closed {
		observer.closed = true
		observer.state.Status = source.Closed
		observer.cursor = &controlledCursor{owner: observer}
		observer.state.Cursor = observer.cursor
		close(observer.changed)
	}
	return observer.cleanup
}

type controlledSource struct {
	private
	name                   string
	observer               *controlledObserver
	startup                error
	captures, observations atomic.Int32
}

func (selected *controlledSource) Description() (source.Description, error) {
	return source.Description{Name: selected.name, Module: local.ModuleID, Documents: []string{"document"}, Observable: true}, nil
}
func (selected *controlledSource) Capture(context.Context) (source.Batch, error) {
	selected.captures.Add(1)
	state, _ := selected.observer.Current()
	return state.Batch, nil
}
func (selected *controlledSource) Observe(context.Context) (source.Observer, error) {
	selected.observations.Add(1)
	return selected.observer, selected.startup
}
func controlledPlan(selected *controlledSource) Plan {
	return Plan{Modules: []adapters.Module{local.Module()}, Inputs: []Input{{Source: selected, Documents: []LayerDocument{{Document: "document", Layer: Base}}}}}
}
func controlledSchema() Schema[controlledProject] {
	return Schema[controlledProject]{FormatVersion: 1, Defaults: DefaultSettings(controlledProject{})}
}
func boundedWait(t testing.TB, condition func() bool) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(until) {
			t.Fatal("controlled state did not converge")
		}
		time.Sleep(time.Millisecond)
	}
}
func stateWhere(t testing.TB, live *Live[controlledProject], predicate func(State[controlledProject]) bool) State[controlledProject] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	state, err := live.Current()
	if err != nil {
		t.Fatal(err)
	}
	for !predicate(state) {
		state, err = live.Next(ctx, state.Cursor)
		if err != nil {
			t.Fatal("controlled wait", err)
		}
	}
	return state
}
func joined(t testing.TB, live *Live[controlledProject]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := live.Close(ctx); err != nil {
		t.Error(err)
	}
}
func TestWatchFencesAlreadyObservedVectorsAndEligibility(t *testing.T) {
	observer := newControlledObserver(1)
	selected := &controlledSource{name: "controlled", observer: observer}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	schema := controlledSchema()
	var calls atomic.Int32
	schema.Validate = func(value Settings[controlledProject]) error {
		calls.Add(1)
		if value.Project.Count == 1 {
			close(entered)
			<-release
		}
		return nil
	}
	live, err := Watch(context.Background(), schema, controlledPlan(selected))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { once.Do(func() { close(release) }); joined(t, live) })
	<-entered
	observer.set(2, source.Degraded, errors.New("source-unavailable"))
	stateWhere(t, live, func(state State[controlledProject]) bool { return state.Status == Degraded })
	observer.set(2, source.Available, nil)
	stateWhere(t, live, func(state State[controlledProject]) bool { return state.Status == Pending })
	once.Do(func() { close(release) })
	accepted := stateWhere(t, live, func(state State[controlledProject]) bool { return state.Status == Ready })
	value, _ := accepted.Snapshot.ValueCopy()
	if value.Project.Count != 2 || calls.Load() != 2 || selected.captures.Load() != 0 {
		t.Fatal("stale candidate published or Framework recaptured")
	}
}
func TestDiagnosticCursorDoesNotStarveStablePreparation(t *testing.T) {
	observer := newControlledObserver(1)
	selected := &controlledSource{name: "controlled", observer: observer}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	schema := controlledSchema()
	var calls atomic.Int32
	schema.Validate = func(value Settings[controlledProject]) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	}
	live, err := Watch(context.Background(), schema, controlledPlan(selected))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { once.Do(func() { close(release) }); joined(t, live) })
	<-entered
	for range 100 {
		observer.set(1, source.Available, nil)
	}
	once.Do(func() { close(release) })
	stateWhere(t, live, func(state State[controlledProject]) bool { return state.Status == Ready })
	if calls.Load() != 1 {
		t.Fatal("diagnostic cursors invalidated stable candidate", calls.Load())
	}
}
func TestCloseTimeoutRetainsBlockedPreparationAndDoesNotPoisonJoin(t *testing.T) {
	observer := newControlledObserver(1)
	selected := &controlledSource{name: "controlled", observer: observer}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	schema := controlledSchema()
	schema.Validate = func(Settings[controlledProject]) error { close(entered); <-release; return nil }
	live, err := Watch(context.Background(), schema, controlledPlan(selected))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { once.Do(func() { close(release) }); joined(t, live) })
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := live.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Close abandoned or hid pending validation", err)
	}
	state, _ := live.Current()
	if state.Status != Closing || state.Snapshot.state != nil {
		t.Fatal("publication escaped Closing")
	}
	observer.set(2, source.Available, nil)
	once.Do(func() { close(release) })
	joined(t, live)
	terminal, _ := live.Current()
	if terminal.Status != Closed || terminal.Snapshot.state != nil {
		t.Fatal("late validator published after Closing")
	}
	if _, err := live.Next(context.Background(), terminal.Cursor); !errors.Is(err, ErrClosed) {
		t.Fatal("terminal cursor did not end wait", err)
	}
}
func TestPartialStartupAndActualCleanupRemainOwned(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	startup, cleanup := errors.New("startup-canary"), errors.New("cleanup-canary")
	observer := newControlledObserver(1)
	observer.release = release
	observer.cleanup = cleanup
	selected := &controlledSource{name: "controlled", observer: observer, startup: startup}
	live, err := Watch(context.Background(), controlledSchema(), controlledPlan(selected))
	if err != nil || live == nil {
		t.Fatal("startup escaped unreachable owner", err)
	}
	<-observer.closeStarted
	state, _ := live.Current()
	if state.Status != Closing || !errors.Is(state.Failure, startup) {
		t.Fatal("partial startup evidence lost")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := live.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("pending cleanup hidden", err)
	}
	once.Do(func() { close(release) })
	joinedCtx, cancelJoin := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelJoin()
	if err := live.Close(joinedCtx); !errors.Is(err, cleanup) || errors.Is(err, context.Canceled) {
		t.Fatal("actual cleanup mixed with past wait failure", err)
	}
	state, _ = live.Current()
	if state.Status != Closed || !errors.Is(state.Failure, cleanup) || !errors.Is(state.Failure, startup) {
		t.Fatal("cleanup history missing")
	}
	assertPrivate(t, state.Failure)
}
func TestOneWaiterNoLostWakeAndRejectedCandidateCache(t *testing.T) {
	observer := newControlledObserver(1)
	selected := &controlledSource{name: "controlled", observer: observer}
	var calls atomic.Int32
	refusal := errors.New("validator-canary")
	schema := controlledSchema()
	schema.Validate = func(Settings[controlledProject]) error { calls.Add(1); return refusal }
	live, err := Watch(context.Background(), schema, controlledPlan(selected))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { joined(t, live) })
	rejected := stateWhere(t, live, func(state State[controlledProject]) bool { return state.Status == Degraded })
	for range 5 {
		observer.set(1, source.Available, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		next, err := live.Next(ctx, rejected.Cursor)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		rejected = next
	}
	if calls.Load() != 1 || !errors.Is(rejected.Failure, refusal) {
		t.Fatal("unchanged pure rejection revalidated")
	}
	wait, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := live.Next(wait, rejected.Cursor); result <- err }()
	boundedWait(t, func() bool { live.mu.Lock(); defer live.mu.Unlock(); return live.waiting })
	if _, err := live.Next(context.Background(), rejected.Cursor); !errors.Is(err, ErrBusy) {
		t.Fatal("second waiter admitted", err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if state, _ := live.Current(); state.Status != Degraded {
		t.Fatal("wait cancellation canceled owner")
	}
	observer.set(2, source.Available, nil)
	stateWhere(t, live, func(state State[controlledProject]) bool { return state.Status == Degraded && calls.Load() == 2 })
	if calls.Load() != 2 {
		t.Fatal("changed rejected vector ignored")
	}
}
func TestAllPreflightBeforeAnyAcquisition(t *testing.T) {
	observer := newControlledObserver(1)
	selected := &controlledSource{name: "controlled", observer: observer}
	for _, input := range []Plan{
		{Inputs: controlledPlan(selected).Inputs},
		{Modules: []adapters.Module{local.Module(), local.Module()}, Inputs: controlledPlan(selected).Inputs},
		{Modules: []adapters.Module{local.Module()}, Inputs: []Input{{Source: selected, Documents: []LayerDocument{{Document: "wrong", Layer: Base}}}}},
		{Modules: []adapters.Module{local.Module()}, Inputs: []Input{{Source: selected, Documents: []LayerDocument{{Document: "document", Layer: 0}}}}},
		{Modules: []adapters.Module{local.Module()}, Inputs: controlledPlan(selected).Inputs, Variables: []Variable{{Name: "X", Field: "/format"}}},
	} {
		if _, err := Load(context.Background(), controlledSchema(), input); err == nil {
			t.Fatal("invalid Load plan admitted")
		}
		if _, err := Watch(context.Background(), controlledSchema(), input); err == nil {
			t.Fatal("invalid Watch plan admitted")
		}
	}
	schema := controlledSchema()
	schema.FormatVersion = 0
	if _, err := Load(context.Background(), schema, controlledPlan(selected)); !errors.Is(err, ErrSchema) {
		t.Fatal(err)
	}
	if selected.captures.Load() != 0 || selected.observations.Load() != 0 {
		t.Fatal("preflight performed I/O")
	}
}

func TestLoadCancellationPreservesValidatorCause(t *testing.T) {
	observer := newControlledObserver(1)
	selected := &controlledSource{name: "controlled", observer: observer}
	schema := controlledSchema()
	entered, release := make(chan struct{}), make(chan struct{})
	validatorCause, cancelCause := errors.New("validator"), errors.New("caller cancellation")
	schema.Validate = func(Settings[controlledProject]) error { close(entered); <-release; return validatorCause }
	ctx, cancel := context.WithCancelCause(context.Background())
	result := make(chan error, 1)
	go func() { _, err := Load(ctx, schema, controlledPlan(selected)); result <- err }()
	<-entered
	cancel(cancelCause)
	close(release)
	err := <-result
	if !errors.Is(err, validatorCause) || !errors.Is(err, cancelCause) || !errors.Is(err, context.Canceled) {
		t.Fatal("compound cancellation lost deliberate causes")
	}
}

func TestLifetimeCancellationRetainsPreviousOperationFailure(t *testing.T) {
	refusal, cancellation := errors.New("operation-private-canary"), errors.New("cancel-private-canary")
	observer := newControlledObserver(1)
	selected := &controlledSource{name: "controlled", observer: observer}
	schema := controlledSchema()
	schema.Validate = func(Settings[controlledProject]) error { return refusal }
	ctx, cancel := context.WithCancelCause(context.Background())
	live, err := Watch(ctx, schema, controlledPlan(selected))
	if err != nil {
		t.Fatal(err)
	}
	stateWhere(t, live, func(state State[controlledProject]) bool { return state.Status == Degraded })
	cancel(cancellation)
	joined(t, live)
	terminal, _ := live.Current()
	if !errors.Is(terminal.Failure, refusal) || !errors.Is(terminal.Failure, cancellation) || !errors.Is(terminal.Failure, context.Canceled) {
		t.Fatal("closing discarded observed operation/cancellation facts")
	}
	assertPrivate(t, terminal.Failure)
}

func TestIndependentSourceTerminationRetainsItsFinalFailure(t *testing.T) {
	operation, cleanup := errors.New("source-operation"), errors.New("source-cleanup")
	observer := newControlledObserver(1)
	selected := &controlledSource{name: "controlled", observer: observer}
	live, err := Watch(context.Background(), controlledSchema(), controlledPlan(selected))
	if err != nil {
		t.Fatal(err)
	}
	stateWhere(t, live, func(state State[controlledProject]) bool { return state.Status == Ready })
	observer.mu.Lock()
	observer.closed = true
	observer.state.Status = source.Closed
	observer.state.Failure = fail(source.ErrCleanup, operation, cleanup)
	observer.cursor = &controlledCursor{owner: observer}
	observer.state.Cursor = observer.cursor
	close(observer.changed)
	observer.mu.Unlock()
	state := stateWhere(t, live, func(state State[controlledProject]) bool { return state.Status == Degraded })
	// Drain any erroneous terminal-sentinel event before joining the reader.
	wait, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	for {
		next, err := live.Next(wait, state.Cursor)
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			break
		}
		state = next
	}
	joined(t, live)
	final, _ := live.Current()
	if observer.afterTerminal.Load() != 0 || !errors.Is(state.Failure, operation) || !errors.Is(state.Failure, cleanup) || !errors.Is(final.Failure, operation) || !errors.Is(final.Failure, cleanup) {
		t.Fatal("Framework replaced the final source occurrence by waiting beyond its terminal cursor")
	}
}
