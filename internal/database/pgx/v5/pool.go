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
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
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
	sealed      bool
	stop        context.CancelFunc
	maintained  chan struct{}
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
		owner := &pool{settings: value, acquire: make(chan struct{}, 1), closed: make(chan struct{}), maintained: make(chan struct{})}
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
		if value.MaxIdleTime > 0 || value.MaxLifetime > 0 {
			var maintenance context.Context
			maintenance, owner.stop = context.WithCancel(context.Background())
			go owner.expire(maintenance)
		} else {
			close(owner.maintained)
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
	owner.closeOnce.Do(func() {
		owner.mu.Lock()
		owner.sealed = true
		owner.mu.Unlock()
		if owner.stop != nil {
			owner.stop()
		}
		go func() { <-owner.maintained; owner.native.Close(); close(owner.closed) }()
	})
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
	owner.mu.Lock()
	if owner.sealed {
		err := failure(ErrState, "pool", owner.closeErrors...)
		owner.mu.Unlock()
		return nil, err
	}
	owner.mu.Unlock()
	idle := owner.native.AcquireAllIdle()
	var usable []*puddle.Resource[*connection]
	var cleanup []error
	for _, handle := range idle {
		if owner.expired(handle, true) {
			cleanup = append(cleanup, owner.retire(handle))
		} else {
			usable = append(usable, handle)
		}
	}
	idle = usable
	if err := errors.Join(cleanup...); err != nil {
		for _, handle := range idle {
			handle.ReleaseUnused()
		}
		owner.backgroundFailure(err)
		return nil, err
	}
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
	owner.mu.Lock()
	sealed := owner.sealed
	owner.mu.Unlock()
	if !sealed && !owner.expired(handle, false) && !connection.native.IsClosed() && !connection.native.PgConn().IsBusy() && connection.native.PgConn().TxStatus() == 'I' {
		ctx, cancel := context.WithTimeout(context.Background(), owner.settings.CloseTimeout)
		_, primary, cleanup := consume(ctx, connection.native, owner.settings, "DISCARD ALL", nil, false)
		cancel()
		if primary == nil && cleanup == nil && !connection.native.IsClosed() && connection.native.PgConn().TxStatus() == 'I' {
			handle.Release()
			return nil
		}
		return failureOrNil(ErrCleanup, "session-reset", primary, cleanup, owner.retire(handle))
	}
	return owner.retire(handle)
}
func (owner *pool) retire(handle *puddle.Resource[*connection]) error {
	cleanup, cancel := context.WithTimeout(context.Background(), owner.settings.CloseTimeout)
	defer cancel()
	err := handle.Value().close(cleanup)
	handle.Hijack()
	return err
}
func (owner *pool) expired(handle *puddle.Resource[*connection], idle bool) bool {
	return owner.settings.MaxLifetime > 0 && time.Since(handle.CreationTime()) >= owner.settings.MaxLifetime ||
		idle && owner.settings.MaxIdleTime > 0 && handle.IdleDuration() >= owner.settings.MaxIdleTime
}
func (owner *pool) backgroundFailure(err error) {
	if err == nil {
		return
	}
	owner.mu.Lock()
	owner.sealed = true
	owner.closeErrors = append(owner.closeErrors, err)
	owner.mu.Unlock()
}
func (owner *pool) expire(ctx context.Context) {
	defer close(owner.maintained)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		select {
		case owner.acquire <- struct{}{}:
		case <-ctx.Done():
			return
		}
		if ctx.Err() != nil {
			<-owner.acquire
			return
		}
		var failures []error
		for _, handle := range owner.native.AcquireAllIdle() {
			if owner.expired(handle, true) {
				failures = append(failures, owner.retire(handle))
			} else {
				handle.ReleaseUnused()
			}
		}
		err := errors.Join(failures...)
		owner.backgroundFailure(err)
		<-owner.acquire
		if err != nil {
			return
		}
	}
}

// Stats copies native pool counters. Maintenance can acquire resources, and the
// synchronous AcquireAllIdle path does not increment native AcquireCount metrics.
func (database *Database) Stats() *puddle.Stat {
	if database == nil || database.owner == nil {
		return &puddle.Stat{}
	}
	return database.owner.native.Stat()
}

// Ping explicitly checks the native connection and retains independent evidence.
// Pool construction and expiration do not perform hidden readiness queries.
func (database *Database) Ping(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	call, err := database.beginCall(ctx, id, "ping", invocation.Finite, nil)
	if err != nil {
		return nil, err
	}
	receipt := call.Receipt()
	work, cancel, err := (invocation.Budget{Limit: database.owner.settings.Timeout}).Context(ctx, invocation.Execute)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return receipt, nil
	}
	defer cancel()
	handle, err := database.owner.take(work)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return receipt, nil
	}
	_, _ = call.Attempt()
	err = handle.Value().native.Ping(nativeContext{work})
	data := &resultData{complete: err == nil, serverVersion: handle.Value().native.PgConn().ParameterStatus("server_version")}
	call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: nativeFailure(ErrQuery, "ping", work, err), Cleanup: database.owner.give(handle)})
	return receipt, nil
}
