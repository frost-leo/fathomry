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

package minio

import (
	"context"
	"errors"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

func joinContexts(ctx, owner context.Context) (context.Context, func()) {
	work, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	stop := context.AfterFunc(owner, func() { defer close(done); cancel(errors.Join(owner.Err(), context.Cause(owner))) })
	if owner.Err() != nil {
		cancel(errors.Join(owner.Err(), context.Cause(owner)))
	}
	return work, sync.OnceFunc(func() {
		if !stop() {
			<-done
		}
		cancel(nil)
	})
}
func (client *Client) beginOwned(ctx, lifetime context.Context, id fault.Correlation, name string) (*invocation.Call[Result], context.Context, context.CancelFunc, error) {
	if err := client.valid(ctx); err != nil {
		return nil, nil, nil, err
	}
	if lifetime == nil || client.access.Limits().MaxLeases < 2 {
		return nil, nil, nil, failure(ErrInput, "session")
	}
	value := client.owner.settings
	live, cancel, err := (invocation.Budget{Limit: value.SessionTimeout}).Context(lifetime, invocation.Lifetime)
	if err != nil {
		return nil, nil, nil, err
	}
	admission, stop := joinContexts(ctx, live)
	defer stop()
	call, err := invocation.Begin(admission, client.access, invocation.Request{Name: name, Correlation: id, Shape: invocation.Session,
		Bytes: value.reservation(), EvidenceBytes: value.evidenceReservation(), Admission: invocation.Budget{Limit: value.Timeout},
		AttemptsKnown: true, MaxAttempts: uint64(value.MaxRequests) + 1}, client.inbox, client.observer)
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}
	return call, live, cancel, nil
}
func (client *Client) child(ctx context.Context, parent *invocation.Call[Result], id fault.Correlation, name string, exact bool) (*invocation.Call[Result], error) {
	value := client.owner.settings
	request := invocation.Request{Name: name, Correlation: id, Shape: invocation.Async, EvidenceBytes: value.evidenceReservation(), Admission: invocation.Budget{Limit: value.Timeout}, AttemptsKnown: exact}
	if exact {
		request.MaxAttempts = uint64(value.MaxRequests)
	}
	return invocation.BeginNested(ctx, parent.Scope(), request, client.inbox, client.observer)
}
func (client *Client) phase(ctx, lifetime context.Context) (context.Context, func(), error) {
	joined, stop := joinContexts(ctx, lifetime)
	work, cancel, err := (invocation.Budget{Limit: client.owner.settings.Timeout}).Context(joined, invocation.Execute)
	if err != nil {
		stop()
		return nil, nil, err
	}
	return work, func() { cancel(); stop() }, nil
}
