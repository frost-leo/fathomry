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

package owned

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"

	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
)

type cursor struct {
	Guard
	owner *observer
}
type observer struct {
	Guard
	mu                       sync.Mutex
	state                    source.State
	position                 *cursor
	changed                  chan struct{}
	done                     chan struct{}
	closing, closed, waiting bool
	cleanup                  error
	parent, ctx              context.Context
	cancel                   context.CancelFunc
	stop                     func() bool
}

// Observe returns before asynchronous startup. Run owns acquisition and joins
// native cleanup before returning; publish receives only complete captures.
func Observe(ctx context.Context, run func(context.Context, func(source.Batch, error)) error) source.Observer {
	lifetime, cancel := context.WithCancel(ctx)
	value := &observer{state: source.State{Status: source.Pending}, parent: ctx, ctx: lifetime, cancel: cancel, changed: make(chan struct{}), done: make(chan struct{})}
	value.position = &cursor{owner: value}
	value.stop = context.AfterFunc(lifetime, value.beginClose)
	go func() {
		err := run(lifetime, value.publish)
		value.beginClose()
		value.mu.Lock()
		value.closed = true
		value.cleanup = err
		value.state.Status = source.Closed
		if err != nil {
			if value.state.Failure == nil {
				value.state.Failure = err
			} else {
				value.state.Failure = Fail(source.ErrCleanup, value.state.Failure, err)
			}
		}
		value.signal()
		close(value.done)
		value.mu.Unlock()
		value.stop()
	}()
	return value
}
func (value *observer) signal() {
	// Each retained position is a different live allocation; there is no finite
	// public sequence to wrap or content fingerprint to leak.
	value.position = &cursor{owner: value}
	close(value.changed)
	value.changed = make(chan struct{})
}
func (value *observer) publish(batch source.Batch, err error) {
	if err == nil && Nil(batch) {
		err = Fail(source.ErrValue)
	}
	previous, _ := value.Current()
	equal := err == nil && Equal(previous.Batch, batch)
	value.mu.Lock()
	if value.closing || value.ctx.Err() != nil {
		value.mu.Unlock()
		return
	}
	exhausted := false
	if err != nil {
		value.state.Status = source.Degraded
		value.state.Failure = err
		value.signal()
	} else {
		if !equal {
			if value.state.Generation == math.MaxUint64 {
				value.state.Status = source.Degraded
				value.state.Failure = Fail(source.ErrLimit)
				exhausted = true
			} else {
				value.state.Batch = batch
				value.state.Generation++
			}
		}
		changed := !equal || value.state.Status != source.Available
		if !exhausted {
			value.state.Status = source.Available
			value.state.Failure = nil
		}
		if changed {
			value.signal()
		}
	}
	value.mu.Unlock()
	if exhausted {
		value.beginClose()
	}
}
func (value *observer) beginClose() {
	value.mu.Lock()
	if !value.closing {
		value.closing = true
		value.state.Status = source.Closing
		if value.parent.Err() != nil {
			value.state.Failure = Fail(source.ErrClosed, value.state.Failure, value.parent.Err(), context.Cause(value.parent))
		}
		value.signal()
		value.cancel()
	}
	value.mu.Unlock()
}
func (value *observer) Current() (source.State, error) {
	if value == nil || value.cancel == nil {
		return source.State{}, Fail(source.ErrValue)
	}
	value.mu.Lock()
	defer value.mu.Unlock()
	result := value.state
	result.Cursor = value.position
	return result, nil
}
func (value *observer) Next(ctx context.Context, after source.Cursor) (source.State, error) {
	if value == nil || value.cancel == nil || Nil(ctx) {
		return source.State{}, Fail(source.ErrValue)
	}
	value.mu.Lock()
	var position *cursor
	if after != nil {
		var ok bool
		position, ok = after.(*cursor)
		if !ok || position == nil || position.owner != value {
			value.mu.Unlock()
			return source.State{}, Fail(source.ErrCursor)
		}
	}
	if value.waiting {
		value.mu.Unlock()
		return source.State{}, Fail(source.ErrBusy)
	}
	value.waiting = true
	defer func() { value.mu.Lock(); value.waiting = false; value.mu.Unlock() }()
	for {
		if ctx.Err() != nil {
			value.mu.Unlock()
			return source.State{}, ContextError(source.ErrWait, ctx)
		}
		if value.position != position {
			result := value.state
			result.Cursor = value.position
			value.mu.Unlock()
			return result, nil
		}
		if value.closed {
			value.mu.Unlock()
			return source.State{}, Fail(source.ErrClosed)
		}
		changed := value.changed
		value.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		value.mu.Lock()
	}
}
func (value *observer) Close(ctx context.Context) error {
	if value == nil || value.cancel == nil || Nil(ctx) {
		return Fail(source.ErrValue)
	}
	value.beginClose()
	select {
	case <-value.done:
		return value.cleanup
	default:
	}
	select {
	case <-value.done:
		return value.cleanup
	case <-ctx.Done():
		return ContextError(source.ErrWait, ctx)
	}
}
func (*observer) Format(state fmt.State, verb rune) { Guard{}.Format(state, verb) }
func (*observer) LogValue() slog.Value              { return Guard{}.LogValue() }
func (*cursor) Format(state fmt.State, verb rune)   { Guard{}.Format(state, verb) }
func (*cursor) LogValue() slog.Value                { return Guard{}.LogValue() }
