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

package kafka

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/frost-leo/fathomry/adapters/broker/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Owner retains source shutdown authority. Always retain and close a non-nil
// owner returned with an Open error. Cancellation continues cleanup independently.
type Owner struct {
	private
	state  *sourceState
	client *Client
}
type sourceState struct {
	selection       source.Selection[native.Source]
	assembly        *source.Assembly
	policy          broker.Policy
	maxRecords      int
	call            *adapters.Call[Result]
	guard           adapters.Guard
	releaseIdentity func()
	primary         error
	final           error
	serial          atomic.Uint64
	complete        atomic.Bool
	gate            chan struct{}
	mu              sync.Mutex
	uses            int
	sealed          bool
	idle            chan struct{}
	info            broker.Info
}

// Handle is an opaque non-owning value suitable for a public resource.Instance.
type Handle struct {
	private
	state *sourceState
}

func bind(deps Dependencies) (adapters.Endpoint[Result], error) {
	return adapters.Bind(deps.Runtime, adapters.Declaration[Result]{Evidence: deps.Evidence, Observer: deps.Observer, Copy: func(value Result) Result { return value }})
}

// Open performs real metadata readiness on owned native clients; it creates no
// topics and starts no group. ctx owns the full source lifetime, not just setup.
// Source admission precedes native construction. Failed cleanup remains reachable.
func Open(ctx context.Context, settings Settings, deps Dependencies) (*Owner, error) {
	if ctx == nil {
		return nil, fail(ErrInput, "open")
	}
	endpoint, err := bind(deps)
	if err != nil {
		return nil, err
	}
	policy, err := Recommend(settings)
	if err != nil {
		return nil, err
	}
	selected, err := native.Select(options(settings))
	if err != nil {
		return nil, translate(err, "open")
	}
	selected = source.WithLimits(selected, native.LimitsV1(options(settings)))
	var owner *Owner
	var primary error
	receipt, err := endpoint.Run(ctx, request("open", "", policy.SourceWorkBytes, sourceEvidenceBytes), func(call *adapters.Call[Result]) {
		releaseIdentity, err := deps.Transactions.acquire(settings.ClusterID, settings.TransactionalID)
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		guard, err := call.Hold()
		if err != nil {
			releaseIdentity()
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		assembly, err := source.Assemble(call.Context(), context.Background(), "kafka", selected)
		primary = translate(err, "open")
		if assembly == nil {
			releaseIdentity()
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		state := &sourceState{selection: selected, assembly: assembly, policy: policy, call: call, guard: guard, releaseIdentity: releaseIdentity,
			primary: primary, gate: make(chan struct{}, 1), idle: make(chan struct{})}
		state.maxRecords = native.BudgetV1(options(settings)).MaxRecords
		if snapshot := assembly.Snapshot(); len(snapshot.Sources) == 1 {
			state.info = sourceInfo(snapshot.Sources[0].Info)
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
func (owner *Owner) Info() broker.Info { return owner.Handle().Info() }
func (handle Handle) Info() broker.Info {
	if handle.state == nil {
		return broker.Info{}
	}
	return handle.state.info
}
func (owner *Owner) ShutdownComplete() bool {
	return owner != nil && owner.state != nil && owner.state.complete.Load()
}

// Close requests stop before waiting; it never acquires a new admission or evidence
// slot. A timeout affects the wait, not cleanup ownership or native fencing.
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
		previous := state.serial.Load()
		if previous == math.MaxUint64 {
			return 0, false
		}
		if state.serial.CompareAndSwap(previous, previous+1) {
			return previous + 1, true
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
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close")
	}
	if state.complete.Load() {
		return state.final
	}
	select {
	case <-state.idle:
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close")
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
		return translate(err, "close")
	}
	state.final = translate(err, "close")
	state.releaseIdentity()
	_ = state.call.Resolve(adapters.Outcome[Result]{Primary: state.primary, Cleanup: state.final})
	state.complete.Store(true)
	_ = state.guard.Release()
	return state.final
}
