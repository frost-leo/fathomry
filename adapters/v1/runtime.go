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
	"errors"
	"math"
	"strings"
	"sync"
)

type typeIdentity[T any] [0]func() T

// Runtime owns admission and actual operation work, not native instances or
// durable evidence storage. Copies share state; do not overwrite shared handles.
type Runtime struct{ state *runtimeState }
type runtimeState struct {
	mu          sync.Mutex
	options     Options
	base        context.Context
	stopParent  func() bool
	closing     bool
	closed      bool
	stop        chan struct{}
	done        chan struct{}
	active      int
	workBytes   int64
	queuedBytes int64
	queue       []*waiter
	roots       map[*node]struct{}
	sequence    uint64
}
type waiter struct {
	ctx     context.Context
	bytes   int64
	ready   chan struct{}
	granted bool
	err     error
}
type node struct {
	owner         *runtimeState
	parent        *node
	root          *node
	request       Request
	sequence      uint64
	depth         int
	family        int
	children      int
	refs          int
	guards        int
	resolving     bool
	resolved      bool
	failed        bool
	finishing     bool
	released      bool
	ctx           context.Context
	cancel        context.CancelCauseFunc
	ready         chan struct{}
	done          chan struct{}
	source        Source
	releaseSource func() error
	unreported    func(error)
	addCleanup    func(error)
	observer      *Observer
	evidenceReady func()
}

// Statistics is a detached local observation. Active includes capacity granted
// to an entering caller; it is not a count of SDK requests or remote effects.
type Statistics struct {
	Active      int
	Queued      int
	WorkBytes   int64
	QueuedBytes int64
	Accepted    uint64
	Closing     bool
	Closed      bool
}

// New establishes ownership before work. Parent cancellation initiates Close,
// but callers still retain the Runtime until actual work is confirmed released.
func New(ctx context.Context, options Options) (*Runtime, error) {
	if ctx == nil {
		return nil, failureOf(ErrOptions, "new", "", Details{})
	}
	options, err := normalize(options)
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, failureOf(ErrClosed, "new", options.Name, Details{}, ctx.Err(), context.Cause(ctx))
	}
	state := &runtimeState{options: options, base: ctx, stop: make(chan struct{}), done: make(chan struct{}), roots: make(map[*node]struct{})}
	state.mu.Lock()
	state.stopParent = context.AfterFunc(ctx, func() { state.shutdown(errors.Join(ctx.Err(), context.Cause(ctx))) })
	state.mu.Unlock()
	return &Runtime{state: state}, nil
}
func (state *runtimeState) stopped() bool {
	select {
	case <-state.stop:
		return true
	default:
		return state.base.Err() != nil
	}
}
func (state *runtimeState) fits(bytes int64) bool {
	return state.active < state.options.MaxActive && bytes <= state.options.MaxWorkBytes-state.workBytes
}
func (state *runtimeState) acquire(ctx context.Context, bytes int64) error {
	state.mu.Lock()
	if state.closing || state.base.Err() != nil {
		state.mu.Unlock()
		return failureOf(ErrClosed, "admit", state.options.Name, Details{})
	}
	if ctx.Err() != nil {
		state.mu.Unlock()
		return failureOf(ErrWait, "admit", state.options.Name, Details{}, ctx.Err(), context.Cause(ctx))
	}
	if bytes > state.options.MaxWorkBytes {
		state.mu.Unlock()
		return failureOf(ErrLimit, "admit", state.options.Name, Details{})
	}
	if len(state.queue) == 0 && state.fits(bytes) {
		state.active++
		state.workBytes += bytes
		state.mu.Unlock()
		return nil
	}
	if len(state.queue) >= state.options.MaxQueued || bytes > state.options.MaxQueuedBytes-state.queuedBytes {
		state.mu.Unlock()
		return failureOf(ErrLimit, "admit", state.options.Name, Details{})
	}
	pending := &waiter{ctx: ctx, bytes: bytes, ready: make(chan struct{})}
	state.queue = append(state.queue, pending)
	state.queuedBytes += bytes
	state.mu.Unlock()
	select {
	case <-pending.ready:
	case <-ctx.Done():
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if pending.granted {
		if ctx.Err() == nil && !state.closing && state.base.Err() == nil {
			return nil
		}
		state.releasePermitLocked(bytes)
	} else if pending.err == nil {
		for index, entry := range state.queue {
			if entry == pending {
				copy(state.queue[index:], state.queue[index+1:])
				state.queue[len(state.queue)-1] = nil
				state.queue = state.queue[:len(state.queue)-1]
				state.queuedBytes -= bytes
				break
			}
		}
		state.promoteLocked()
	}
	if pending.err != nil {
		return pending.err
	}
	if state.closing || state.base.Err() != nil {
		return failureOf(ErrClosed, "admit", state.options.Name, Details{})
	}
	return failureOf(ErrWait, "admit", state.options.Name, Details{}, ctx.Err(), context.Cause(ctx))
}
func (state *runtimeState) promoteLocked() {
	if state.closing {
		return
	}
	for len(state.queue) > 0 {
		pending := state.queue[0]
		if pending.ctx.Err() == nil && !state.fits(pending.bytes) {
			return
		}
		state.queue[0] = nil
		state.queue = state.queue[1:]
		state.queuedBytes -= pending.bytes
		if pending.ctx.Err() != nil {
			pending.err = failureOf(ErrWait, "admit", state.options.Name, Details{}, pending.ctx.Err(), context.Cause(pending.ctx))
		} else {
			pending.granted = true
			state.active++
			state.workBytes += pending.bytes
		}
		close(pending.ready)
	}
}
func (state *runtimeState) releasePermitLocked(bytes int64) {
	state.active--
	state.workBytes -= bytes
	state.promoteLocked()
	state.closeDoneLocked()
}
func (state *runtimeState) closeDoneLocked() {
	if state.closing && state.active == 0 && !state.closed {
		state.closed = true
		close(state.done)
	}
}
func (state *runtimeState) shutdown(cause error) {
	state.mu.Lock()
	if state.closing {
		state.mu.Unlock()
		return
	}
	state.closing = true
	close(state.stop)
	for _, pending := range state.queue {
		pending.err = failureOf(ErrClosed, "admit", state.options.Name, Details{}, cause)
		close(pending.ready)
	}
	state.queue = nil
	state.queuedBytes = 0
	for root := range state.roots {
		root.cancel(cause)
	}
	stopParent := state.stopParent
	state.closeDoneLocked()
	state.mu.Unlock()
	if stopParent != nil {
		stopParent()
	}
}

// Close stops root/child admission, requests cancellation and joins actual work.
// It neither acknowledges nor deletes independent evidence. A timeout leaves this
// same owner reachable; no native work or borrowed instance is declared ended.
// Successful Close confirms the join only; operation failures remain in receipts.
func (runtime *Runtime) Close(ctx context.Context) error {
	if runtime == nil || runtime.state == nil {
		return failureOf(ErrHandle, "close", "", Details{})
	}
	if ctx == nil {
		return failureOf(ErrOptions, "close", "", Details{})
	}
	state := runtime.state
	state.shutdown(ErrClosed)
	select {
	case <-state.done:
		return nil
	default:
	}
	select {
	case <-state.done:
		return nil
	case <-ctx.Done():
		return failureOf(ErrWait, "close", state.options.Name, Details{Pending: true}, ctx.Err(), context.Cause(ctx))
	}
}

// Inspect reports coherent local admission/ownership counters.
func (runtime *Runtime) Inspect() (Statistics, error) {
	if runtime == nil || runtime.state == nil {
		return Statistics{}, failureOf(ErrHandle, "inspect", "", Details{})
	}
	state := runtime.state
	state.mu.Lock()
	defer state.mu.Unlock()
	return Statistics{state.active, len(state.queue), state.workBytes, state.queuedBytes, state.sequence, state.closing, state.closed}, nil
}
func (state *runtimeState) newNodeLocked(ctx context.Context, request Request, parent *node, observer *Observer) (*node, error) {
	if state.sequence == math.MaxUint64 {
		return nil, failureOf(ErrLimit, "sequence", state.options.Name, Details{})
	}
	if state.closing || state.base.Err() != nil {
		return nil, failureOf(ErrClosed, "begin", state.options.Name, Details{})
	}
	if ctx.Err() != nil {
		return nil, failureOf(ErrWait, "begin", state.options.Name, Details{}, ctx.Err(), context.Cause(ctx))
	}
	if parent != nil {
		cause := parent.cancellation()
		if parent.finishing || parent.released || cause != nil {
			return nil, failureOf(ErrClosed, "child", state.options.Name, Details{Pending: !parent.released}, cause)
		}
	}
	if parent != nil && (parent.root.family >= state.options.MaxTasks || parent.depth >= state.options.MaxDepth) {
		return nil, failureOf(ErrLimit, "child", state.options.Name, Details{Pending: !parent.released})
	}
	state.sequence++
	lifetime, cancel := context.WithCancelCause(ctx)
	request.Operation, request.ID = strings.Clone(request.Operation), strings.Clone(request.ID)
	value := &node{owner: state, parent: parent, request: request, sequence: state.sequence, depth: 1, refs: 1, ctx: lifetime, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{}), observer: observer}
	if parent == nil {
		value.root = value
		value.family = 1
		state.roots[value] = struct{}{}
	} else {
		value.root = parent.root
		value.depth = parent.depth + 1
		value.source = parent.source
		parent.children++
		value.root.family++
		stop := context.AfterFunc(parent.ctx, func() { cancel(context.Cause(parent.ctx)) })
		value.releaseSource = func() error { stop(); return nil }
	}
	return value, nil
}
func (value *node) cancellation() error {
	for current := value; current != nil; current = current.parent {
		if current.ctx.Err() != nil {
			return errors.Join(current.ctx.Err(), context.Cause(current.ctx))
		}
	}
	return nil
}
func (value *node) details() Details {
	parent := uint64(0)
	if value.parent != nil {
		parent = value.parent.sequence
	}
	return Details{Sequence: value.sequence, Parent: parent, Pending: !value.released}
}
func (value *node) canFinishLocked() bool {
	if value.refs != 0 || value.children != 0 || value.finishing {
		return false
	}
	value.finishing = true
	if !value.resolved {
		value.unreported(failureOf(ErrOutcome, value.request.Operation, value.owner.options.Name, value.details(), value.cancellation()))
		value.resolved = true
		value.failed = true
		value.observer.publish(Event{Operation: value.request.Operation, Phase: Resolved, Failed: true})
		close(value.ready)
	}
	return true
}
func finish(value *node) {
	for value != nil {
		var cleanup error
		if value.releaseSource != nil {
			cleanup = value.releaseSource()
			value.releaseSource = nil
		}
		state := value.owner
		state.mu.Lock()
		if cleanup != nil {
			value.addCleanup(cleanup)
			value.failed = true
		}
		value.released = true
		value.cancel(nil)
		value.observer.publish(Event{Operation: value.request.Operation, Phase: Released, Failed: value.failed})
		close(value.done)
		parent := value.parent
		var next *node
		if parent == nil {
			delete(state.roots, value)
			state.releasePermitLocked(value.request.WorkBytes)
		} else {
			parent.children--
			value.root.family--
			if parent.canFinishLocked() {
				next = parent
			}
		}
		state.mu.Unlock()
		if value.evidenceReady != nil {
			value.evidenceReady()
			value.evidenceReady = nil
		}
		value = next
	}
}
func (value *node) returned(guard bool) {
	state := value.owner
	state.mu.Lock()
	value.refs--
	if guard {
		value.guards--
	}
	finishNow := value.canFinishLocked()
	state.mu.Unlock()
	if finishNow {
		finish(value)
	}
}
