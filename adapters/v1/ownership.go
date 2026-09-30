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

	"github.com/frost-leo/fathomry/resource/v1"
)

// Using runs a root producer with one explicitly borrowed public resource.
// The provider's T must be a non-owning facade: the common runtime cannot revoke
// native/Close authority exposed by an unsafe component value. The lease remains
// until the submitting stack, registered guards and all descendants actually end.
func Using[R, T any](ctx context.Context, endpoint Endpoint[T], source resource.Ref[R], request Request, producer func(*Call[T], R)) (*Receipt[T], error) {
	return using(ctx, endpoint, source, request, producer, false)
}

// StartUsing owns the producer goroutine as well as the borrowed generation.
// Neither an early method return nor receipt wait timeout returns the lease.
func StartUsing[R, T any](ctx context.Context, endpoint Endpoint[T], source resource.Ref[R], request Request, producer func(*Call[T], R)) (*Receipt[T], error) {
	return using(ctx, endpoint, source, request, producer, true)
}
func using[R, T any](ctx context.Context, endpoint Endpoint[T], source resource.Ref[R], request Request, producer func(*Call[T], R), async bool) (*Receipt[T], error) {
	if producer == nil {
		return nil, failureOf(ErrOptions, "using", "", Details{})
	}
	call, err := endpoint.begin(ctx, request, Scope{})
	if err != nil {
		return nil, err
	}
	run := func(call *Call[T]) {
		lease, err := source.Acquire(call.Context())
		if err != nil {
			_ = call.Resolve(Outcome[T]{Primary: failureOf(ErrSource, request.Operation, call.state.node.owner.options.Name, Details{Sequence: call.state.node.sequence}, err)})
			return
		}
		control := call.state.node
		status, _ := source.Inspect()
		control.owner.mu.Lock()
		control.source = Source{Name: status.Name, Generation: lease.Generation()}
		control.releaseSource = lease.Release
		control.owner.mu.Unlock()
		value, err := lease.Value()
		if err != nil {
			_ = call.Resolve(Outcome[T]{Primary: failureOf(ErrSource, request.Operation, control.owner.options.Name, Details{Sequence: control.sequence}, err)})
			return
		}
		if control.owner.stopped() || call.Context().Err() != nil {
			_ = call.Resolve(Outcome[T]{Primary: failureOf(ErrClosed, request.Operation, control.owner.options.Name, Details{Sequence: control.sequence}, call.Context().Err(), context.Cause(call.Context()))})
			return
		}
		producer(call, value)
	}
	if async {
		go call.dispatch(run)
	} else {
		call.dispatch(run)
	}
	return call.Receipt(), nil
}
