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
	"sync/atomic"
)

type typeIdentity[T any] [0]func() T

type access[T any] interface {
	acquire(context.Context) (Lease[T], error)
	inspect() Status
	retry(context.Context) (*Update, error)
}

// Ref is a typed stable use/retry entry for one binding. It does not expose scope
// shutdown authority or global lookup. Copies share the same binding.
type Ref[T any] struct {
	state access[T]
	_     typeIdentity[T]
}

// Acquire atomically selects and retains the active generation. It does not wait
// for initial availability or migrate an existing lease. The context controls
// admission only; the operation must use its own context when calling the value.
func (ref Ref[T]) Acquire(ctx context.Context) (Lease[T], error) {
	if ref.state == nil {
		return Lease[T]{}, fail(ErrScope, "acquire", "", Details{})
	}
	return ref.state.acquire(ctx)
}

// Inspect returns a coherent per-binding observation without selecting its value.
func (ref Ref[T]) Inspect() (Status, error) {
	if ref.state == nil {
		return Status{}, fail(ErrScope, "inspect", "", Details{})
	}
	return ref.state.inspect(), nil
}

// Retry explicitly schedules another failed-construction or incomplete-cleanup
// attempt. It neither duplicates an in-progress constructor nor automatically
// retries external operations. Historical Update receipts remain unchanged.
func (ref Ref[T]) Retry(ctx context.Context) (*Update, error) {
	if ref.state == nil {
		return nil, fail(ErrScope, "retry", "", Details{})
	}
	return ref.state.retry(ctx)
}

// Lease borrows one generation until Release. Copies share a single release
// authority. Do not call Value concurrently with Release, and never retain/use
// a returned resource beyond the lease. Multiple leases follow the component's
// concurrent-use contract; the holder does not make arbitrary values thread-safe.
type Lease[T any] struct {
	state *leaseState[T]
	_     typeIdentity[T]
}

type leaseState[T any] struct {
	value      T
	generation uint64
	released   atomic.Bool
	release    func()
}

// Value returns the deliberately borrowed instance, not a clone or snapshot.
func (lease Lease[T]) Value() (T, error) {
	if lease.state == nil || lease.state.released.Load() {
		var zero T
		return zero, fail(ErrLease, "value", "", Details{})
	}
	return lease.state.value, nil
}

// Generation identifies this construction within its binding. Zero is invalid.
func (lease Lease[T]) Generation() uint64 {
	if lease.state == nil {
		return 0
	}
	return lease.state.generation
}

// Release relinquishes this borrow exactly once. It does not wait for native
// cleanup. Repeated calls return ErrLease without decrementing another borrow.
func (lease Lease[T]) Release() error {
	if lease.state == nil || lease.state.released.Swap(true) {
		return fail(ErrLease, "release_lease", "", Details{})
	}
	release := lease.state.release
	var zero T
	lease.state.value, lease.state.release = zero, nil
	release()
	return nil
}

func (entry *binding[C, T]) acquire(ctx context.Context) (Lease[T], error) {
	if ctx == nil {
		return Lease[T]{}, fail(ErrOptions, "acquire", entry.name(), entry.details(0, 0, false))
	}
	if ctx.Err() != nil {
		return Lease[T]{}, fail(ErrWait, "acquire", entry.name(), entry.details(0, 0, false), ctx.Err(), context.Cause(ctx))
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if ctx.Err() != nil {
		return Lease[T]{}, fail(ErrWait, "acquire", entry.name(), entry.details(0, 0, false), ctx.Err(), context.Cause(ctx))
	}
	if entry.stopping || entry.scope.stopped() {
		return Lease[T]{}, fail(ErrClosed, "acquire", entry.name(), entry.details(entry.version, 0, false))
	}
	generation := entry.active
	if generation == nil {
		return Lease[T]{}, fail(ErrUnavailable, "acquire", entry.name(), entry.details(entry.version, 0, entry.pending != nil))
	}
	count := generation.borrows
	for _, retired := range entry.retired {
		count += retired.borrows
	}
	if count == entry.options.MaxBorrowers {
		return Lease[T]{}, fail(ErrLimit, "acquire", entry.name(), entry.details(entry.version, generation.number, false))
	}
	generation.borrows++
	return Lease[T]{state: &leaseState[T]{
		value: generation.value, generation: generation.number,
		release: func() {
			entry.mu.Lock()
			generation.borrows--
			if generation != entry.active && generation.borrows == 0 {
				entry.signal()
			}
			entry.mu.Unlock()
		},
	}}, nil
}
