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

package redis

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/frost-leo/fathomry/adapters/cache/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/cache/redis/v9"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Owner retains native shutdown authority. Retain every non-nil Owner, even
// alongside an Open error, until ShutdownComplete confirms actual termination.
type Owner struct {
	private
	state  *sourceState
	client *Client
}
type sourceState struct {
	selection      source.Selection[native.Source]
	assembly       *source.Assembly
	metadata       native.Metadata
	policy         cache.Policy
	diagnostics    *native.Client
	call           *adapters.Call[Result]
	guard          adapters.Guard
	primary, final error
	serial         atomic.Uint64
	complete       atomic.Bool
	gate           chan struct{}
	mu             sync.Mutex
	uses           int
	sealed         bool
	idle           chan struct{}
	info           cache.Info
	custody        map[*operation]error
	custodyFailed  chan struct{}
}

// Handle is a non-owning source capability for resource.Instance. Copies, views,
// Fixed and Follow aliases share this source's admission and socket authority.
type Handle struct {
	private
	state *sourceState
}

func bind(deps Dependencies) (adapters.Endpoint[Result], error) {
	return adapters.Bind(deps.Runtime, adapters.Declaration[Result]{Evidence: deps.Evidence, Observer: deps.Observer, Copy: func(value Result) Result { return value }})
}

// Open prepares and constructs an owner under a caller-owned lifetime. It makes
// no readiness promise: topology/cache background work may start at construction.
func Open(ctx context.Context, value Settings, deps Dependencies) (*Owner, error) {
	prepared, err := Prepare(value)
	if err != nil {
		return nil, err
	}
	return prepared.Open(ctx, deps)
}
func OpenWithPassword(ctx context.Context, value Settings, password *Password, deps Dependencies) (*Owner, error) {
	prepared, err := PrepareWithPassword(value, password)
	if err != nil {
		return nil, err
	}
	return prepared.Open(ctx, deps)
}
func (prepared Prepared) Open(ctx context.Context, deps Dependencies) (*Owner, error) {
	if ctx == nil {
		return nil, fail(ErrInput, "open")
	}
	policy, err := prepared.Policy()
	if err != nil {
		return nil, err
	}
	endpoint, err := bind(deps)
	if err != nil {
		return nil, err
	}
	selected := prepared.native.Selection()
	var owner *Owner
	var primary error
	receipt, err := endpoint.Run(ctx, request(Cache, "open", "", prepared.metadata.SourceBytes, sourceEvidenceBytes), func(call *adapters.Call[Result]) {
		guard, err := call.Hold()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		assembly, err := source.Assemble(call.Context(), context.Background(), "redis", selected)
		primary = translate(err, "open", Cache)
		if assembly == nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		state := &sourceState{selection: selected, assembly: assembly, metadata: prepared.metadata, policy: policy, call: call, guard: guard,
			primary: primary, gate: make(chan struct{}, 1), idle: make(chan struct{}), custodyFailed: make(chan struct{})}
		if snapshot := assembly.Snapshot(); len(snapshot.Sources) == 1 {
			state.info = sourceInfo(snapshot.Sources[0].Info)
		}
		if primary == nil {
			inbox, inboxErr := invocation.NewInbox[native.Result](1, prepared.metadata.EvidenceBytes)
			if inboxErr == nil {
				state.diagnostics, inboxErr = native.Bind(assembly, selected, inbox, nil)
			}
			primary = translate(inboxErr, "open", Cache)
			state.primary = primary
		}
		owner = &Owner{state: state}
		owner.client = &Client{endpoint: endpoint, direct: Handle{state: state}, lifetime: call.Context(), budget: policy.Budget}
		go func() { <-call.Context().Done(); _ = state.close(context.Background()) }()
	})
	if err != nil {
		return nil, err
	}
	if owner == nil && primary == nil {
		snapshot, _ := receipt.Snapshot()
		primary = snapshot.Err()
	}
	return owner, primary
}
func (owner *Owner) Client() *Client {
	if owner == nil {
		return nil
	}
	return owner.client
}
func (owner *Owner) Handle() Handle {
	if owner == nil {
		return Handle{}
	}
	return Handle{state: owner.state}
}
func (owner *Owner) Info() cache.Info { return owner.Handle().Info() }
func (handle Handle) Info() cache.Info {
	if handle.state == nil {
		return cache.Info{}
	}
	return handle.state.info
}
func (owner *Owner) ShutdownComplete() bool {
	return owner != nil && owner.state != nil && owner.state.complete.Load()
}

// PendingTransfers reports quarantined bridge invariant failures, not ordinary
// pending calls. Such failures retain native records and public holds on this
// reachable Owner; they never authorize silent discard or successful shutdown.
func (owner *Owner) PendingTransfers() int {
	if owner == nil || owner.state == nil {
		return 0
	}
	owner.state.mu.Lock()
	defer owner.state.mu.Unlock()
	return len(owner.state.custody)
}

// Close seals and cancels before waiting. It needs no new admission/evidence;
// timed-out waiting retains the owner and its cleanup worker. A callback ignoring
// cancellation remains owned until it actually returns.
func (owner *Owner) Close(ctx context.Context) error {
	if owner == nil || owner.state == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	return owner.state.close(ctx)
}
func (owner *Owner) Release(ctx context.Context) resource.ReleaseResult {
	err := owner.Close(ctx)
	return resource.ReleaseResult{Complete: owner.ShutdownComplete(), Err: err}
}
func (state *sourceState) use() (func(), error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.sealed || state.call.Context().Err() != nil || state.primary != nil {
		return nil, fail(ErrState, "source", state.primary)
	}
	state.uses++
	return sync.OnceFunc(func() {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.uses--
		if state.sealed && state.uses == 0 {
			close(state.idle)
		}
	}), nil
}
func (state *sourceState) next() (uint64, bool) {
	for {
		old := state.serial.Load()
		if old == math.MaxUint64 {
			return 0, false
		}
		if state.serial.CompareAndSwap(old, old+1) {
			return old + 1, true
		}
	}
}
func (state *sourceState) close(ctx context.Context) error {
	state.mu.Lock()
	if !state.sealed {
		state.sealed = true
		if state.uses == 0 {
			close(state.idle)
		}
	}
	state.mu.Unlock()
	_ = state.call.Cancel(nil)
	if state.complete.Load() {
		return state.final
	}
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close", Cache)
	}
	if state.complete.Load() {
		return state.final
	}
	select {
	case <-state.idle:
	case <-state.custodyFailed:
		return fail(ErrCleanup, "evidence-custody")
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close", Cache)
	}
	state.mu.Lock()
	pending := len(state.custody) != 0
	state.mu.Unlock()
	if pending {
		return fail(ErrCleanup, "evidence-custody")
	}
	err := state.assembly.Close(ctx)
	complete := true
	for _, item := range state.assembly.Snapshot().Sources {
		if item.Pending || !item.Quiescent || !item.Released {
			complete = false
		}
	}
	if !complete {
		if err == nil {
			err = source.ErrIncomplete
		}
		return translate(err, "close", Cache)
	}
	state.final = translate(err, "close", Cache)
	snapshot, _ := state.call.Receipt().Snapshot()
	value := Result{kind: Lifecycle, capability: Cache, source: state.info, attribution: publicAttribution(snapshot.Info()),
		primary: state.primary, cleanup: state.final}
	_ = state.call.Resolve(adapters.Outcome[Result]{Present: true, Value: value, Primary: state.primary, Cleanup: state.final})
	state.complete.Store(true)
	_ = state.guard.Release()
	return state.final
}
