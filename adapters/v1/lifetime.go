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
)

// RunWithLifetime separates admission from an explicitly owned retained lifetime.
// Both contexts can stop queued admission. After acceptance Context follows
// lifetime and runtime shutdown, not ctx. The producer must separately use ctx
// for bounded setup; a successful setup does not end a transaction or preparation.
// Guards still retain actual work after cancellation. This method starts no worker.
func (endpoint Endpoint[T]) RunWithLifetime(ctx, lifetime context.Context, request Request, producer func(*Call[T])) (*Receipt[T], error) {
	return endpoint.retained(ctx, lifetime, Scope{}, request, producer)
}

// ChildWithLifetime retains the existing root envelope and parent cancellation,
// while a separate ctx governs admission/setup. A fresh lifetime cannot revive
// a canceled or released parent. Children still require independent evidence.
func (endpoint Endpoint[T]) ChildWithLifetime(ctx, lifetime context.Context, parent Scope, request Request, producer func(*Call[T])) (*Receipt[T], error) {
	if parent.node == nil {
		return nil, failureOf(ErrHandle, "child", "", Details{})
	}
	return endpoint.retained(ctx, lifetime, parent, request, producer)
}

func (endpoint Endpoint[T]) retained(ctx, lifetime context.Context, parent Scope, request Request, producer func(*Call[T])) (*Receipt[T], error) {
	if producer == nil || ctx == nil || lifetime == nil {
		return nil, failureOf(ErrOptions, "retained", "", Details{})
	}
	admission, stop := admissionLifetime(ctx, lifetime)
	defer stop()
	call, err := endpoint.beginContexts(admission, lifetime, request, parent)
	if err != nil {
		return nil, err
	}
	call.dispatch(producer)
	return call.Receipt(), nil
}

func admissionLifetime(ctx, lifetime context.Context) (context.Context, func()) {
	admission, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	stop := context.AfterFunc(lifetime, func() {
		defer close(done)
		cancel(errors.Join(lifetime.Err(), context.Cause(lifetime)))
	})
	if lifetime.Err() != nil {
		cancel(errors.Join(lifetime.Err(), context.Cause(lifetime)))
	}
	return admission, func() {
		if !stop() {
			<-done
		}
		cancel(nil)
	}
}
