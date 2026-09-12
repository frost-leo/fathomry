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

package pgx

import (
	"context"
	"errors"
	"sync"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/jackc/puddle/v2"
)

// Source is an opaque assembly capability. Only Bind grants controlled database
// operations; no native pool, configuration, close or callback is exposed.
type Source struct {
	private
	owner *pool
}
type pool struct {
	settings    settings
	native      *puddle.Pool[*connection]
	acquire     chan struct{}
	closeOnce   sync.Once
	closed      chan struct{}
	mu          sync.Mutex
	closeErrors []error
}

// Select prepares version-1 settings and their optional raw overlays before any
// native pool construction. Overlays use resource's existing strict contract;
// zero in an overlay is explicit, not a request to reapply bootstrap defaults.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: 1, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	selected := resource.Select(prepared, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		config, err := nativeConfig(value)
		if err != nil {
			return resource.Resource[Source]{}, err
		}
		owner := &pool{settings: value, acquire: make(chan struct{}, 1), closed: make(chan struct{})}
		owner.native, err = puddle.NewPool(&puddle.Config[*connection]{MaxSize: int32(value.MaxConnections),
			Constructor: func(ctx context.Context) (*connection, error) { return connect(ctx, config, value.RootCAPEM) },
			Destructor: func(connection *connection) {
				cleanup, cancel := context.WithTimeout(context.Background(), value.CloseTimeout)
				defer cancel()
				if err := connection.close(cleanup); err != nil {
					owner.mu.Lock()
					owner.closeErrors = append(owner.closeErrors, err)
					owner.mu.Unlock()
				}
			}})
		if err != nil {
			return resource.Resource[Source]{}, failure(ErrConnect, "pool", err)
		}
		return resource.Resource[Source]{Acquired: true, Capability: Source{owner: owner}, Release: owner.close}, nil
	})
	return selected, nil
}

// LimitsV1 returns the defaulted options' recommended shared policy. When layers
// alter bounds, composition must use the corresponding resolved policy instead.
func LimitsV1(options OptionsV1) resource.Limits { return defaults(options).limits() }

// Database is a concurrent, non-owning facade bound to one assembly scope and
// an independently owned evidence inbox. Copies share the same resource allowance.
type Database struct {
	private
	owner    *pool
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Bind joins the exact selected source, admission and independent evidence. A
// borrowing alias uses the same native pool, original identity and allowance.
func Bind(assembly *resource.Assembly, selected resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Database, error) {
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	value, limits := source.owner.settings, access.Limits()
	if limits.Active > value.MaxConnections || limits.Bytes < value.reservation() ||
		limits.MaxLeases < 2 || limits.Queued > 64 ||
		limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Database{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}

func (owner *pool) close(ctx context.Context) resource.ReleaseResult {
	owner.closeOnce.Do(func() { go func() { owner.native.Close(); close(owner.closed) }() })
	select {
	case <-owner.closed:
		owner.mu.Lock()
		err := errors.Join(owner.closeErrors...)
		owner.mu.Unlock()
		return resource.ReleaseResult{Quiescent: true, Released: true, Err: err}
	case <-ctx.Done():
		return resource.ReleaseResult{Err: failure(ErrCleanup, "pool-close", ctx.Err(), context.Cause(ctx)), Continue: owner.close}
	}
}

// take uses native synchronous CreateResource instead of Acquire's detached
// constructor. The short gate bounds admission to that path, not query execution.
// All other idle resources are returned without changing their idle timestamps.
func (owner *pool) take(ctx context.Context) (*puddle.Resource[*connection], error) {
	select {
	case owner.acquire <- struct{}{}:
		defer func() { <-owner.acquire }()
	case <-ctx.Done():
		return nil, failure(ErrConnect, "acquire", ctx.Err(), context.Cause(ctx))
	}
	if ctx.Err() != nil {
		return nil, failure(ErrConnect, "acquire", ctx.Err(), context.Cause(ctx))
	}
	idle := owner.native.AcquireAllIdle()
	if len(idle) == 0 {
		if err := owner.native.CreateResource(ctx); err != nil {
			return nil, nativeFailure(ErrConnect, "acquire", ctx, err)
		}
		idle = owner.native.AcquireAllIdle()
	}
	if len(idle) == 0 {
		return nil, failure(ErrState, "acquire")
	}
	for _, extra := range idle[1:] {
		extra.ReleaseUnused()
	}
	return idle[0], nil
}

// give returns only protocol-idle connections. Unusable sessions are closed and
// joined while the call still owns its root allowance; Hijack merely removes the
// already-closed native resource synchronously, avoiding a second destructor.
func (owner *pool) give(handle *puddle.Resource[*connection]) error {
	connection := handle.Value()
	if !connection.native.IsClosed() && !connection.native.PgConn().IsBusy() && connection.native.PgConn().TxStatus() == 'I' {
		handle.Release()
		return nil
	}
	cleanup, cancel := context.WithTimeout(context.Background(), owner.settings.CloseTimeout)
	defer cancel()
	err := connection.close(cleanup)
	handle.Hijack()
	return err
}
