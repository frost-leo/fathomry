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
	"math"
	"strings"
	"sync"

	"github.com/frost-leo/fathomry/settings/v1"
)

// Status is a detached per-binding observation. Active means a locally adopted
// instance, not a healthy remote service. Target/Applied are binding-local tokens;
// Generation identifies construction, and may be reused after an equivalent update.
// Errors deliberately retain native causes and are not safe diagnostic payloads.
type Status struct {
	Name            string
	Policy          Policy
	Target          uint64
	Applied         uint64
	Generation      uint64
	Active          bool
	Preparing       bool
	Retiring        int
	Borrowers       int
	PendingCleanup  int
	Closing         bool
	Closed          bool
	CleanupFailures uint64
	Err             error
	CleanupErr      error
}

type selection[C any] struct {
	value  C
	target *target
}

type generation[C, T any] struct {
	number    uint64
	selection *selection[C]
	ctx       context.Context
	cancel    context.CancelCauseFunc
	value     T
	release   func(context.Context) ReleaseResult
	borrows   int
	cleaning  bool
	blocked   bool
}

type binding[C, T any] struct {
	mu              sync.Mutex
	scope           *scopeState
	options         Binding[C, T]
	wake            chan struct{}
	done            chan struct{}
	stopping        bool
	exited          bool
	desired         *selection[C]
	wanted          bool
	active          *generation[C, T]
	pending         *generation[C, T]
	retired         []*generation[C, T]
	version         uint64
	generation      uint64
	applied         uint64
	lastError       error
	cleanupError    error
	terminalCleanup error
	cleanupFailures uint64
}

// Bind registers one typed named component before declarations are sealed. It
// starts one owned coordinator, but does not select configuration or construct
// an instance until Apply/Watch receives a view. Ref has use/retry authority;
// only Scope closes the entire lifetime. Separate names never share implicitly.
func Bind[C, T any](scope *Scope, options Binding[C, T]) (Ref[T], error) {
	if scope == nil || scope.state == nil {
		return Ref[T]{}, fail(ErrScope, "bind", "", Details{})
	}
	state := scope.state
	if !validName(options.Name) || options.Policy != Fixed && options.Policy != Follow ||
		options.Select == nil || options.Clone == nil || options.Build == nil ||
		options.Policy == Follow && options.Equal == nil {
		return Ref[T]{}, fail(ErrOptions, "bind", "", Details{Scope: state.options.Name})
	}
	if options.MaxGenerations == 0 {
		options.MaxGenerations = 2
	}
	if options.MaxBorrowers == 0 {
		options.MaxBorrowers = 1024
	}
	if options.MaxGenerations < 2 || options.MaxGenerations > 16 ||
		options.MaxBorrowers < 1 || options.MaxBorrowers > 65536 {
		return Ref[T]{}, fail(ErrOptions, "bind", "", Details{Scope: state.options.Name})
	}
	options.Name = strings.Clone(options.Name)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.stopping || state.base.Err() != nil {
		return Ref[T]{}, fail(ErrClosed, "bind", options.Name, Details{Scope: state.options.Name})
	}
	if state.sealed {
		return Ref[T]{}, fail(ErrSealed, "bind", options.Name, Details{Scope: state.options.Name})
	}
	if state.names[options.Name] {
		return Ref[T]{}, fail(ErrOptions, "bind", options.Name, Details{Scope: state.options.Name})
	}
	if len(state.entries) == state.options.MaxBindings {
		return Ref[T]{}, fail(ErrLimit, "bind", options.Name, Details{Scope: state.options.Name})
	}
	entry := &binding[C, T]{scope: state, options: options, wake: make(chan struct{}, 1), done: make(chan struct{})}
	state.entries = append(state.entries, entry)
	state.names[options.Name] = true
	go entry.run()
	return Ref[T]{state: entry}, nil
}

func (entry *binding[C, T]) name() string { return entry.options.Name }

func (entry *binding[C, T]) details(target, generation uint64, pending bool) Details {
	return Details{Scope: entry.scope.options.Name, Target: target, Generation: generation, Pending: pending}
}

func (entry *binding[C, T]) signal() {
	select {
	case entry.wake <- struct{}{}:
	default:
	}
	entry.scope.notify()
}

func (entry *binding[C, T]) prepare(view settings.View) (func() *target, error) {
	entry.mu.Lock()
	previous, active := entry.desired, entry.active
	version := entry.version
	entry.mu.Unlock()
	if entry.options.Policy == Fixed && previous != nil {
		return func() *target { return previous.target }, nil
	}
	value, err := entry.options.Select(view)
	if err != nil {
		return nil, fail(ErrSelection, "select", entry.name(), entry.details(0, 0, false), err)
	}
	value = entry.options.Clone(value)
	equalDesired, equalActive := false, false
	if entry.options.Equal != nil {
		if previous != nil {
			equalDesired = entry.options.Equal(previous.value, value)
		}
		if active != nil {
			equalActive = entry.options.Equal(active.selection.value, value)
		}
	}
	if equalDesired {
		return func() *target { return previous.target }, nil
	}
	if version == math.MaxUint64 {
		return nil, fail(ErrLimit, "apply", entry.name(), entry.details(version, 0, false))
	}
	return func() *target {
		entry.mu.Lock()
		defer entry.mu.Unlock()
		if previous != nil {
			previous.target.settle(0, fail(ErrSuperseded, "apply", entry.name(), entry.details(previous.target.version, 0, false)))
		}
		entry.version++
		selected := &selection[C]{value: value, target: newTarget(entry.name(), entry.version)}
		entry.desired = selected
		entry.lastError = nil
		entry.wanted = true
		if entry.pending != nil {
			entry.pending.cancel(ErrSuperseded)
		}
		if equalActive && entry.active == active {
			entry.wanted = false
			entry.applied = entry.version
			selected.target.settle(active.number, nil)
		}
		entry.signal()
		return selected.target
	}, nil
}

func (entry *binding[C, T]) retry(ctx context.Context) (*Update, error) {
	if err := entry.scope.enterApply(ctx); err != nil {
		return nil, err
	}
	defer func() { <-entry.scope.applyGate }()
	entry.scope.mu.Lock()
	defer entry.scope.mu.Unlock()
	if entry.scope.stopping || entry.scope.base.Err() != nil {
		return nil, fail(ErrClosed, "retry", entry.name(), entry.details(0, 0, false))
	}
	if ctx.Err() != nil {
		return nil, fail(ErrWait, "retry", entry.name(), entry.details(0, 0, false), ctx.Err(), context.Cause(ctx))
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.desired == nil {
		return nil, fail(ErrUnavailable, "retry", entry.name(), entry.details(0, 0, false))
	}
	if entry.scope.sequence == math.MaxUint64 {
		return nil, fail(ErrLimit, "retry", entry.name(), entry.details(entry.version, 0, false))
	}
	settled, result := entry.desired.target.result()
	if settled && result != nil && entry.pending == nil {
		if entry.version == math.MaxUint64 {
			return nil, fail(ErrLimit, "retry", entry.name(), entry.details(entry.version, 0, false))
		}
		entry.version++
		entry.desired = &selection[C]{value: entry.desired.value, target: newTarget(entry.name(), entry.version)}
		entry.wanted = true
		entry.lastError = nil
	}
	for _, retired := range entry.retired {
		retired.blocked = false
	}
	entry.scope.sequence++
	entry.signal()
	return &Update{sequence: entry.scope.sequence, scope: entry.scope.options.Name, targets: []*target{entry.desired.target}}, nil
}

func (entry *binding[C, T]) stop(cause error) {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.stopping {
		return
	}
	entry.stopping = true
	if entry.desired != nil {
		entry.desired.target.settle(0, fail(ErrClosed, "stop", entry.name(), entry.details(entry.version, 0, true), cause))
	}
	if entry.pending != nil {
		entry.pending.cancel(cause)
	}
	if entry.active != nil {
		entry.retired = append(entry.retired, entry.active)
		entry.active = nil
	}
	entry.signal()
}

func (entry *binding[C, T]) retryCleanup() {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	for _, retired := range entry.retired {
		retired.blocked = false
	}
	entry.signal()
}

func (entry *binding[C, T]) closed() <-chan struct{} { return entry.done }

func (entry *binding[C, T]) cleanupFailure() error {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.terminalCleanup
}

func (entry *binding[C, T]) inspect() Status {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	value := Status{
		Name: entry.name(), Policy: entry.options.Policy, Target: entry.version, Applied: entry.applied,
		Active: entry.active != nil, Preparing: entry.pending != nil, Retiring: len(entry.retired),
		Closing: entry.stopping, Closed: entry.exited, CleanupFailures: entry.cleanupFailures,
		Err: entry.lastError, CleanupErr: entry.cleanupError,
	}
	if entry.active != nil {
		value.Generation = entry.active.number
		value.Borrowers = entry.active.borrows
	}
	for _, retired := range entry.retired {
		value.Borrowers += retired.borrows
		if retired.blocked {
			value.PendingCleanup++
		}
	}
	return value
}

func (entry *binding[C, T]) run() {
	defer close(entry.done)
	for {
		entry.mu.Lock()
		if entry.stopping && len(entry.retired) == 0 && entry.pending == nil {
			entry.exited = true
			entry.mu.Unlock()
			entry.scope.notify()
			return
		}
		var cleanup *generation[C, T]
		for _, retired := range entry.retired {
			if retired.borrows == 0 && !retired.blocked {
				cleanup = retired
				retired.cleaning = true
				break
			}
		}
		if cleanup != nil {
			entry.mu.Unlock()
			entry.clean(cleanup)
			continue
		}
		owned := len(entry.retired)
		if entry.active != nil {
			owned++
		}
		if !entry.stopping && entry.wanted && entry.pending == nil && owned < entry.options.MaxGenerations {
			if entry.generation == math.MaxUint64 {
				entry.wanted = false
				entry.lastError = fail(ErrLimit, "construct", entry.name(), entry.details(entry.version, 0, false))
				entry.desired.target.settle(0, entry.lastError)
				entry.signal()
			} else {
				entry.generation++
				lifetime, cancel := context.WithCancelCause(context.WithoutCancel(entry.scope.base))
				candidate := &generation[C, T]{number: entry.generation, selection: entry.desired, ctx: lifetime, cancel: cancel}
				entry.pending, entry.wanted = candidate, false
				entry.signal()
				entry.mu.Unlock()
				entry.construct(candidate)
				continue
			}
		}
		entry.mu.Unlock()
		<-entry.wake
	}
}

func (entry *binding[C, T]) construct(candidate *generation[C, T]) {
	var instance *Instance[T]
	var err error
	select {
	case entry.scope.preparing <- struct{}{}:
		if err = candidate.ctx.Err(); err == nil && !entry.scope.stopped() {
			input := entry.options.Clone(candidate.selection.value)
			if err = candidate.ctx.Err(); err == nil && !entry.scope.stopped() {
				instance, err = entry.options.Build(candidate.ctx, input)
			}
		}
		<-entry.scope.preparing
	case <-candidate.ctx.Done():
		err = candidate.ctx.Err()
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.pending = nil
	currentTarget := entry.desired == candidate.selection
	adopt := err == nil && instance != nil && currentTarget && !entry.stopping &&
		!entry.scope.stopped() && candidate.ctx.Err() == nil
	if instance != nil {
		candidate.value, candidate.release = instance.Value, instance.Release
	}
	if adopt {
		if entry.active != nil {
			entry.retired = append(entry.retired, entry.active)
		}
		entry.active, entry.applied = candidate, candidate.selection.target.version
		entry.lastError = nil
		candidate.selection.target.settle(candidate.number, nil)
	} else {
		contextErr, cause := candidate.ctx.Err(), context.Cause(candidate.ctx)
		candidate.cancel(ErrBuild)
		if instance != nil {
			entry.retired = append(entry.retired, candidate)
		}
		if currentTarget && !entry.stopping {
			if entry.scope.stopped() {
				entry.lastError = fail(ErrClosed, "construct", entry.name(),
					entry.details(candidate.selection.target.version, candidate.number, instance != nil),
					err, contextErr, cause, entry.scope.base.Err(), context.Cause(entry.scope.base))
			} else {
				entry.lastError = fail(ErrBuild, "construct", entry.name(),
					entry.details(candidate.selection.target.version, candidate.number, instance != nil), err, contextErr, cause)
			}
			candidate.selection.target.settle(0, entry.lastError)
		}
	}
	entry.signal()
}

func (entry *binding[C, T]) clean(generation *generation[C, T]) {
	generation.cancel(ErrClosed)
	result := ReleaseResult{Complete: true}
	if generation.release != nil {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(entry.scope.base), entry.scope.options.CleanupTimeout)
		result = generation.release(ctx)
		cancel()
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	generation.cleaning = false
	if result.Err != nil || !result.Complete {
		entry.cleanupError = fail(ErrCleanup, "release", entry.name(),
			entry.details(generation.selection.target.version, generation.number, !result.Complete), result.Err)
		if entry.cleanupFailures < math.MaxUint64 {
			entry.cleanupFailures++
		}
		if result.Complete {
			entry.terminalCleanup = entry.cleanupError
		}
	}
	if result.Complete {
		for index, retired := range entry.retired {
			if retired == generation {
				copy(entry.retired[index:], entry.retired[index+1:])
				entry.retired[len(entry.retired)-1] = nil
				entry.retired = entry.retired[:len(entry.retired)-1]
				break
			}
		}
		var zero T
		generation.value, generation.release = zero, nil
	} else {
		generation.blocked = true
	}
	entry.signal()
}
