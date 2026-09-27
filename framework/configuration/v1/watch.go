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
	"bytes"
	"context"
	"sync"

	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
)

// Status associates the last accepted snapshot with current preparation status.
// Pending means initial/replacement preparation is outstanding; an earlier
// snapshot may remain readable. Ready is accepted, not service/client readiness.
type Status uint8

const (
	Pending Status = iota + 1
	Ready
	Degraded
	Closing
	Closed
)

type position struct{ marker byte }

// Cursor is opaque, owner-bound, ephemeral and not serializable. Zero requests
// current state immediately. Positions are not source generations or revisions.
type Cursor struct {
	private
	owner    any
	position *position
}

// State is one atomic value/status observation. Snapshot is invalid until the
// first acceptance. Failures and closure never mutate an already returned value.
type State[T any] struct {
	private
	Snapshot Snapshot[T]
	Status   Status
	Failure  error
	Cursor   Cursor
	_        typeIdentity[T]
}

// Live owns its source observers and one serial preparation worker. Do not copy.
// Current supports concurrent readers; at most one Next waiter is admitted.
// Canceling a wait never cancels ownership. A timed-out Close retains this owner.
type Live[T any] struct {
	private
	mu                       sync.Mutex
	state                    State[T]
	changed, done            chan struct{}
	parent, ctx              context.Context
	cancel                   context.CancelFunc
	stop                     func() bool
	closing, closed, waiting bool
	cleanup                  error
	_                        typeIdentity[T]
}

// Watch admits/copies schema, defaults, declarations and environment before
// creating ownership. Initial acquisition/preparation is asynchronous: success
// returns an owner, not a ready configuration. No Framework source poller exists.
func Watch[T any](ctx context.Context, schema Schema[T], plan Plan) (*Live[T], error) {
	if nilValue(ctx) {
		return nil, fail(ErrValue)
	}
	admitted, err := admit(schema, plan, true)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(ctx)
	live := &Live[T]{state: State[T]{Status: Pending}, changed: make(chan struct{}), done: make(chan struct{}), parent: ctx, ctx: lifetime, cancel: cancel}
	live.state.Cursor = Cursor{owner: live, position: &position{}}
	live.stop = context.AfterFunc(lifetime, live.beginClose)
	go live.coordinate(admitted)
	return live, nil
}
func (live *Live[T]) signal() {
	live.state.Cursor = Cursor{owner: live, position: &position{}}
	close(live.changed)
	live.changed = make(chan struct{})
}
func (live *Live[T]) publish(snapshot Snapshot[T], status Status, err error) {
	live.mu.Lock()
	defer live.mu.Unlock()
	if live.closing || live.ctx.Err() != nil {
		return
	}
	if snapshot.state == nil {
		snapshot = live.state.Snapshot
	}
	if err == nil && live.state.Failure == nil && live.state.Status == status && live.state.Snapshot.state == snapshot.state {
		return
	}
	live.state.Snapshot = snapshot
	live.state.Status = status
	live.state.Failure = err
	live.signal()
}
func (live *Live[T]) beginClose() {
	live.mu.Lock()
	if !live.closing {
		live.closing = true
		live.state.Status = Closing
		if live.parent.Err() != nil {
			live.state.Failure = fail(ErrClosed, live.state.Failure, live.parent.Err(), context.Cause(live.parent))
		}
		live.signal()
		live.cancel()
	}
	live.mu.Unlock()
}
func (live *Live[T]) Current() (State[T], error) {
	if live == nil || live.cancel == nil {
		return State[T]{}, fail(ErrValue)
	}
	live.mu.Lock()
	defer live.mu.Unlock()
	return live.state, nil
}
func (live *Live[T]) Next(ctx context.Context, after Cursor) (State[T], error) {
	if live == nil || live.cancel == nil || nilValue(ctx) {
		return State[T]{}, fail(ErrValue)
	}
	live.mu.Lock()
	if after.owner != nil && (after.owner != live || after.position == nil) || after.owner == nil && after.position != nil {
		live.mu.Unlock()
		return State[T]{}, fail(ErrCursor)
	}
	if live.waiting {
		live.mu.Unlock()
		return State[T]{}, fail(ErrBusy)
	}
	live.waiting = true
	defer func() { live.mu.Lock(); live.waiting = false; live.mu.Unlock() }()
	for {
		if ctx.Err() != nil {
			live.mu.Unlock()
			return State[T]{}, fail(ErrWait, ctx.Err(), context.Cause(ctx))
		}
		if live.state.Cursor.position != after.position {
			state := live.state
			live.mu.Unlock()
			return state, nil
		}
		if live.closed {
			live.mu.Unlock()
			return State[T]{}, fail(ErrClosed)
		}
		changed := live.changed
		live.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		live.mu.Lock()
	}
}
func (live *Live[T]) Close(ctx context.Context) error {
	if live == nil || live.cancel == nil || nilValue(ctx) {
		return fail(ErrValue)
	}
	live.beginClose()
	select {
	case <-live.done:
		return live.cleanup
	default:
	}
	select {
	case <-live.done:
		return live.cleanup
	case <-ctx.Done():
		return fail(ErrWait, ctx.Err(), context.Cause(ctx))
	}
}

type sourceEvent struct {
	index int
	state source.State
	err   error
}
type candidate struct {
	batches []source.Batch
	epoch   *position
}
type outcome[T any] struct {
	candidate
	snapshot Snapshot[T]
	err      error
}

func (live *Live[T]) coordinate(input *admitted[T]) {
	var observers []source.Observer
	var readers, worker sync.WaitGroup
	updates := make(chan sourceEvent, len(input.inputs))
	jobs := make(chan candidate)
	results := make(chan outcome[T], 1)
	worker.Go(func() {
		for task := range jobs {
			snapshot, err := input.prepare(task.batches)
			results <- outcome[T]{candidate: task, snapshot: snapshot, err: err}
		}
	})
	defer func() {
		live.beginClose()
		close(jobs)
		var cleanup []error
		for _, observer := range observers {
			if err := observer.Close(context.Background()); err != nil {
				cleanup = append(cleanup, err)
			}
		}
		readers.Wait()
		worker.Wait()
		var err error
		if len(cleanup) > 0 {
			err = fail(ErrCleanup, cleanup...)
		}
		live.mu.Lock()
		live.closed = true
		live.cleanup = err
		live.state.Status = Closed
		if err != nil {
			if live.state.Failure == nil {
				live.state.Failure = err
			} else {
				live.state.Failure = fail(ErrCleanup, live.state.Failure, err)
			}
		}
		live.signal()
		close(live.done)
		live.mu.Unlock()
		live.stop()
	}()
	for index, selected := range input.inputs {
		if live.ctx.Err() != nil {
			return
		}
		observer, err := selected.source.Observe(live.ctx)
		if !nilValue(observer) {
			observers = append(observers, observer)
		}
		if err != nil {
			live.publish(Snapshot[T]{}, Degraded, err)
			return
		}
		if nilValue(observer) {
			live.publish(Snapshot[T]{}, Degraded, fail(ErrValue))
			return
		}
		readers.Go(func() {
			state, err := observer.Current()
			for {
				select {
				case updates <- sourceEvent{index: index, state: state, err: err}:
				case <-live.ctx.Done():
					return
				}
				if err != nil || state.Status == source.Closed {
					return
				}
				state, err = observer.Next(live.ctx, state.Cursor)
				if live.ctx.Err() != nil {
					return
				}
			}
		})
	}
	latest := make([]source.State, len(input.inputs))
	epoch := &position{}
	var accepted, rejected []source.Batch
	var acceptedSnapshot Snapshot[T]
	var rejectedError error
	working := false
	for {
		batches := make([]source.Batch, len(latest))
		eligible := true
		var failures []error
		for index, state := range latest {
			batches[index] = state.Batch
			if !available(state) {
				eligible = false
			}
			if state.Failure != nil {
				failures = append(failures, state.Failure)
			} else if state.Status == source.Closed || state.Status == source.Closing {
				failures = append(failures, fail(ErrPreparation, source.ErrClosed))
			}
		}
		if !eligible {
			if len(failures) > 0 {
				err := failures[0]
				if len(failures) > 1 {
					err = fail(ErrPreparation, failures...)
				}
				live.publish(Snapshot[T]{}, Degraded, err)
			} else {
				live.publish(Snapshot[T]{}, Pending, nil)
			}
		} else if equalVector(accepted, batches) {
			live.publish(acceptedSnapshot, Ready, nil)
		} else if equalVector(rejected, batches) {
			live.publish(Snapshot[T]{}, Degraded, rejectedError)
		} else {
			live.publish(Snapshot[T]{}, Pending, nil)
			if !working {
				select {
				case jobs <- candidate{batches: batches, epoch: epoch}:
					working = true
				case <-live.ctx.Done():
					return
				}
			}
		}
		select {
		case <-live.ctx.Done():
			return
		case event := <-updates:
			next := event.state
			if event.err != nil {
				next = latest[event.index]
				next.Status = source.Degraded
				next.Failure = event.err
			}
			old := latest[event.index]
			if available(old) != available(next) || !equalBatch(old.Batch, next.Batch) {
				epoch = &position{}
			}
			latest[event.index] = next
		case result := <-results:
			working = false
			if result.epoch != epoch {
				continue
			}
			if result.err != nil {
				rejected = result.batches
				rejectedError = result.err
			} else {
				accepted = result.batches
				acceptedSnapshot = result.snapshot
				rejected = nil
				rejectedError = nil
			}
		}
	}
}
func available(state source.State) bool {
	return state.Status == source.Available && !nilValue(state.Batch)
}
func equalVector(left, right []source.Batch) bool {
	if left == nil || len(left) != len(right) {
		return false
	}
	for index := range left {
		if !equalBatch(left[index], right[index]) {
			return false
		}
	}
	return true
}
func equalBatch(left, right source.Batch) bool {
	if nilValue(left) || nilValue(right) {
		return nilValue(left) && nilValue(right)
	}
	leftInfo, rightInfo := left.Documents(), right.Documents()
	if len(leftInfo) != len(rightInfo) {
		return false
	}
	for index, entry := range leftInfo {
		if entry != rightInfo[index] {
			return false
		}
		raw, _, err := left.RawCopy(entry.Name)
		other, _, otherErr := right.RawCopy(entry.Name)
		if err != nil || otherErr != nil || !bytes.Equal(raw, other) {
			return false
		}
	}
	return true
}
