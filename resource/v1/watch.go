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
	"io"
	"math"
	"sync"

	"github.com/frost-leo/fathomry/settings/v1"
)

// Observation describes one locally consumed settings input. Update is a receipt,
// not a completed construction. Err reports Apply admission failure. Skipped counts
// older reports coalesced before Next consumed them; this is not a complete log.
type Observation struct {
	Sequence uint64
	Skipped  uint64
	Update   *Update
	Err      error
}

// Watch owns the scope's local settings receiver. It does not own/close the input
// channel or upstream source. Next callers compete for one bounded latest report.
// Close stops and joins only this receiver, leaving adopted resources running.
type Watch struct{ state *watchState }

type watchState struct {
	cancel   context.CancelCauseFunc
	done     chan struct{}
	mu       sync.Mutex
	changed  chan struct{}
	pending  *Observation
	terminal error
}

// Watch receives explicitly supplied accepted settings views and applies them
// through the same admission path as Apply. It seals declarations immediately.
// Only one receiver may be active per scope. Context cancellation, channel EOF,
// Watch.Close or Scope.Close terminates it. Source parsing/recovery is not added.
func (scope *Scope) Watch(ctx context.Context, input <-chan settings.View) (*Watch, error) {
	if scope == nil || scope.state == nil {
		return nil, fail(ErrScope, "watch", "", Details{})
	}
	if ctx == nil || input == nil {
		return nil, fail(ErrOptions, "watch", "", Details{})
	}
	if ctx.Err() != nil {
		return nil, fail(ErrClosed, "watch", "", Details{}, ctx.Err(), context.Cause(ctx))
	}
	state := scope.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.stopping || state.base.Err() != nil {
		return nil, fail(ErrClosed, "watch", "", Details{Scope: state.options.Name})
	}
	if state.watch != nil {
		select {
		case <-state.watch.state.done:
		default:
			return nil, fail(ErrLimit, "watch", "", Details{Scope: state.options.Name})
		}
	}
	work, cancel := context.WithCancelCause(ctx)
	watch := &Watch{state: &watchState{cancel: cancel, done: make(chan struct{}), changed: make(chan struct{})}}
	state.watch, state.sealed = watch, true
	go func() {
		defer close(watch.state.done)
		defer cancel(nil)
		var terminal error
		var sequence uint64
		for {
			if work.Err() != nil {
				watch.state.mu.Lock()
				watch.state.terminal = fail(ErrClosed, "watch", "", Details{Scope: state.options.Name}, work.Err(), context.Cause(work))
				close(watch.state.changed)
				watch.state.changed = make(chan struct{})
				watch.state.mu.Unlock()
				return
			}
			select {
			case <-work.Done():
				terminal = fail(ErrClosed, "watch", "", Details{Scope: state.options.Name}, work.Err(), context.Cause(work))
			case view, open := <-input:
				if !open {
					terminal = fail(ErrClosed, "watch", "", Details{Scope: state.options.Name}, io.EOF)
				} else if sequence == math.MaxUint64 {
					terminal = fail(ErrLimit, "watch", "", Details{Scope: state.options.Name})
				} else {
					sequence++
					update, err := scope.Apply(work, view)
					if work.Err() == nil {
						watch.state.publish(Observation{Sequence: sequence, Update: update, Err: err})
					}
				}
			}
			if terminal != nil {
				watch.state.mu.Lock()
				watch.state.terminal = terminal
				close(watch.state.changed)
				watch.state.changed = make(chan struct{})
				watch.state.mu.Unlock()
				return
			}
		}
	}()
	return watch, nil
}

func (state *watchState) publish(value Observation) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.pending != nil {
		value.Skipped = state.pending.Skipped
		if value.Skipped < math.MaxUint64 {
			value.Skipped++
		}
	}
	state.pending = &value
	close(state.changed)
	state.changed = make(chan struct{})
}

// Next consumes the latest pending report, including one retained before EOF.
// A waiting context controls only this wait, not the watcher or any construction.
func (watch *Watch) Next(ctx context.Context) (Observation, error) {
	if watch == nil || watch.state == nil {
		return Observation{}, fail(ErrScope, "next", "", Details{})
	}
	if ctx == nil {
		return Observation{}, fail(ErrOptions, "next", "", Details{})
	}
	for {
		if ctx.Err() != nil {
			return Observation{}, fail(ErrWait, "next", "", Details{}, ctx.Err(), context.Cause(ctx))
		}
		watch.state.mu.Lock()
		if watch.state.pending != nil {
			value := *watch.state.pending
			watch.state.pending = nil
			watch.state.mu.Unlock()
			return value, nil
		}
		if watch.state.terminal != nil {
			err := watch.state.terminal
			watch.state.mu.Unlock()
			return Observation{}, err
		}
		changed := watch.state.changed
		watch.state.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return Observation{}, fail(ErrWait, "next", "", Details{}, ctx.Err(), context.Cause(ctx))
		}
	}
}

// Close cancels and joins the receiver. A timed-out wait retains this owner;
// call again to confirm completion. Scope.Close also joins its active watcher.
func (watch *Watch) Close(ctx context.Context) error {
	if watch == nil || watch.state == nil {
		return fail(ErrScope, "close_watch", "", Details{})
	}
	if ctx == nil {
		return fail(ErrOptions, "close_watch", "", Details{})
	}
	watch.state.cancel(ErrClosed)
	select {
	case <-watch.state.done:
		return nil
	default:
		select {
		case <-watch.state.done:
			return nil
		case <-ctx.Done():
			return fail(ErrWait, "close_watch", "", Details{Pending: true}, ctx.Err(), context.Cause(ctx))
		}
	}
}
