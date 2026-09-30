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

package resource

import (
	"context"
	"errors"
	"math"
	"sync"

	"github.com/frost-leo/fathomry/settings/v1"
)

// Scope owns explicitly bound component generations. New creates it before
// fallible assembly. Copies share the owner; do not overwrite shared handles.
// Cancellation stops admission and starts cleanup, but Close is still required
// to confirm that actual construction, use and cleanup have ended.
type Scope struct{ state *scopeState }

type scopeState struct {
	mu         sync.Mutex
	options    Options
	base       context.Context
	stopParent func() bool
	stopping   bool
	sealed     bool
	entries    []managed
	names      map[string]bool
	applyGate  chan struct{}
	preparing  chan struct{}
	stop       chan struct{}
	done       chan struct{}
	watch      *Watch
	sequence   uint64
	noticeMu   sync.Mutex
	changed    chan struct{}
}

type managed interface {
	name() string
	prepare(settings.View) (func() *target, error)
	stop(error)
	retryCleanup()
	closed() <-chan struct{}
	inspect() Status
	cleanupFailure() error
}

// New creates an empty scope. MaxBindings defaults to 64 (1..256),
// MaxPreparing to 4 (1..64), CleanupTimeout to 5s (1ms..1min).
// Parent cancellation initiates shutdown; borrowed generations are not declared
// released or forcibly closed merely because the parent or a waiter was canceled.
func New(ctx context.Context, options Options) (*Scope, error) {
	if ctx == nil {
		return nil, fail(ErrOptions, "new", "", Details{})
	}
	options, err := normalize(options)
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, fail(ErrClosed, "new", "", Details{}, ctx.Err(), context.Cause(ctx))
	}
	state := &scopeState{
		options: options, base: ctx, names: make(map[string]bool),
		applyGate: make(chan struct{}, 1), preparing: make(chan struct{}, options.MaxPreparing),
		stop: make(chan struct{}), done: make(chan struct{}), changed: make(chan struct{}),
	}
	state.mu.Lock()
	state.stopParent = context.AfterFunc(ctx, func() {
		state.shutdown(errors.Join(ctx.Err(), context.Cause(ctx)))
	})
	state.mu.Unlock()
	return &Scope{state: state}, nil
}

func (state *scopeState) stopped() bool {
	select {
	case <-state.stop:
		return true
	default:
		return state.base.Err() != nil
	}
}

func (state *scopeState) notify() {
	state.noticeMu.Lock()
	close(state.changed)
	state.changed = make(chan struct{})
	state.noticeMu.Unlock()
}

func (state *scopeState) changes() <-chan struct{} {
	state.noticeMu.Lock()
	defer state.noticeMu.Unlock()
	return state.changed
}

func (state *scopeState) enterApply(ctx context.Context) error {
	if ctx == nil {
		return fail(ErrOptions, "apply", "", Details{Scope: state.options.Name})
	}
	if ctx.Err() != nil {
		return fail(ErrWait, "apply", "", Details{Scope: state.options.Name}, ctx.Err(), context.Cause(ctx))
	}
	if state.stopped() {
		return fail(ErrClosed, "apply", "", Details{Scope: state.options.Name})
	}
	select {
	case state.applyGate <- struct{}{}:
		if state.stopped() {
			<-state.applyGate
			return fail(ErrClosed, "apply", "", Details{Scope: state.options.Name})
		}
		return nil
	case <-state.stop:
		return fail(ErrClosed, "apply", "", Details{Scope: state.options.Name})
	case <-ctx.Done():
		return fail(ErrWait, "apply", "", Details{Scope: state.options.Name}, ctx.Err(), context.Cause(ctx))
	}
}

// Apply admits one coherent accepted settings view, selecting/copying every
// relevant binding before committing any target. After view/context admission,
// declarations stay sealed even if selection fails. Fixed bindings ignore later views.
// No settings publication, parsing, global lookup or service readiness is implied.
//
// Selection callbacks cannot be forcibly interrupted. Concurrent Apply and Retry
// admission is serialized; callers own ordering of source observations. The
// returned receipt tracks this request's targets while construction runs in owned
// workers. Different bindings adopt independently, not as one resource transaction.
func (scope *Scope) Apply(ctx context.Context, view settings.View) (*Update, error) {
	if scope == nil || scope.state == nil {
		return nil, fail(ErrScope, "apply", "", Details{})
	}
	state := scope.state
	if err := state.enterApply(ctx); err != nil {
		return nil, err
	}
	defer func() { <-state.applyGate }()
	if view == (settings.View{}) {
		return nil, fail(ErrSelection, "apply", "", Details{Scope: state.options.Name})
	}
	state.mu.Lock()
	state.sealed = true
	entries := append([]managed(nil), state.entries...)
	state.mu.Unlock()
	commits := make([]func() *target, 0, len(entries))
	for _, entry := range entries {
		if ctx.Err() != nil {
			return nil, fail(ErrWait, "apply", entry.name(), Details{Scope: state.options.Name}, ctx.Err(), context.Cause(ctx))
		}
		commit, err := entry.prepare(view)
		if err != nil {
			return nil, err
		}
		commits = append(commits, commit)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.stopping || state.base.Err() != nil {
		return nil, fail(ErrClosed, "apply", "", Details{Scope: state.options.Name})
	}
	if ctx.Err() != nil {
		return nil, fail(ErrWait, "apply", "", Details{Scope: state.options.Name}, ctx.Err(), context.Cause(ctx))
	}
	if state.sequence == math.MaxUint64 {
		return nil, fail(ErrLimit, "apply", "", Details{Scope: state.options.Name})
	}
	state.sequence++
	result := &Update{sequence: state.sequence, scope: state.options.Name}
	for _, commit := range commits {
		result.targets = append(result.targets, commit())
	}
	return result, nil
}

func (state *scopeState) shutdown(cause error) {
	state.mu.Lock()
	if state.stopping {
		state.mu.Unlock()
		return
	}
	state.stopping = true
	close(state.stop)
	entries := append([]managed(nil), state.entries...)
	watch, stopParent := state.watch, state.stopParent
	state.mu.Unlock()
	if stopParent != nil {
		stopParent()
	}
	if watch != nil {
		watch.state.cancel(cause)
	}
	for _, entry := range entries {
		entry.stop(cause)
	}
	go func() {
		if watch != nil {
			<-watch.state.done
		}
		state.applyGate <- struct{}{}
		<-state.applyGate
		for _, entry := range entries {
			<-entry.closed()
		}
		close(state.done)
	}()
}

// Close stops new input/use and waits for all actual work and ownership to end.
// A timeout retains this scope and every outstanding owner. Call again to join or
// continue an incomplete cleanup attempt. Close does not release callers' leases.
// Completed cleanup errors remain reportable even after all resources are released.
func (scope *Scope) Close(ctx context.Context) error {
	if scope == nil || scope.state == nil {
		return fail(ErrScope, "close", "", Details{})
	}
	if ctx == nil {
		return fail(ErrOptions, "close", "", Details{})
	}
	state := scope.state
	state.shutdown(nil)
	state.mu.Lock()
	entries := append([]managed(nil), state.entries...)
	state.mu.Unlock()
	for _, entry := range entries {
		entry.retryCleanup()
	}
	for {
		changed := state.changes()
		select {
		case <-state.done:
			return state.closeResult(entries)
		default:
		}
		for _, entry := range entries {
			status := entry.inspect()
			if status.PendingCleanup != 0 {
				return fail(ErrCleanup, "close", entry.name(), Details{Scope: state.options.Name, Pending: true}, status.CleanupErr)
			}
		}
		select {
		case <-state.done:
			return state.closeResult(entries)
		case <-changed:
		case <-ctx.Done():
			return fail(ErrWait, "close", "", Details{Scope: state.options.Name, Pending: true}, ctx.Err(), context.Cause(ctx))
		}
	}
}

func (state *scopeState) closeResult(entries []managed) error {
	var causes []error
	for _, entry := range entries {
		if err := entry.cleanupFailure(); err != nil {
			causes = append(causes, err)
		}
	}
	if len(causes) != 0 {
		return fail(ErrCleanup, "close", "", Details{Scope: state.options.Name}, errors.Join(causes...))
	}
	return nil
}

// Inspect returns detached per-binding observations in declaration order. Each
// entry is coherent, but the collection is not an atomic cross-resource snapshot.
func (scope *Scope) Inspect() ([]Status, error) {
	if scope == nil || scope.state == nil {
		return nil, fail(ErrScope, "inspect", "", Details{})
	}
	scope.state.mu.Lock()
	entries := append([]managed(nil), scope.state.entries...)
	scope.state.mu.Unlock()
	result := make([]Status, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry.inspect())
	}
	return result, nil
}
