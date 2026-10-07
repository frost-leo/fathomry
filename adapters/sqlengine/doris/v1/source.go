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

package doris

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/sqlengine/doris/v1"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Owner owns one local native assembly, not a shared MySQL pool. A non-nil owner must be closed
// even when Open fails. Cleanup timeouts leave this same owner reachable.
type Owner struct {
	private
	state  *sourceState
	client *Client
}

// Handle is an opaque non-owning capability for resource.Instance bindings.
type Handle struct {
	private
	state *sourceState
}
type sourceState struct {
	assembly  *source.Assembly
	selection source.Selection[native.Source]
	policy    sqlengine.Policy
	timeout   time.Duration
	call      *adapters.Call[Result]
	guard     adapters.Guard
	primary   error
	mu        sync.Mutex
	uses      int
	sealed    bool
	idle      chan struct{}
	gate      chan struct{}
	complete  atomic.Bool
	final     error
	serial    atomic.Uint64
}

func bind(dependencies Dependencies) (adapters.Endpoint[Result], error) {
	return adapters.Bind(dependencies.Runtime, adapters.Declaration[Result]{Evidence: dependencies.Evidence, Observer: dependencies.Observer, Copy: func(value Result) Result { return value }})
}

// Open freezes validated settings without service I/O. ctx owns the full source
// lifetime, including retained cursors. Construction is not readiness or permission.
func Open(ctx context.Context, value Settings, dependencies Dependencies) (*Owner, error) {
	if ctx == nil {
		return nil, fail(ErrInput, "open")
	}
	prepared, policy, err := prepare(value)
	if err != nil {
		return nil, err
	}
	endpoint, err := bind(dependencies)
	if err != nil {
		return nil, err
	}
	budget := prepared.Reservation()
	selected := source.WithLimits(prepared.Selection(), budget.Limits)
	var owner *Owner
	var primary error
	receipt, err := endpoint.Run(ctx, request("open", "", policy.SourceWorkBytes, policy.SourceEvidenceBytes), func(call *adapters.Call[Result]) {
		guard, err := call.Hold()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		assembly, err := source.Assemble(call.Context(), context.Background(), "database-doris", selected)
		primary = translate(err, "open")
		if assembly == nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		state := &sourceState{assembly: assembly, selection: selected, policy: policy, timeout: prepared.OptionsCopy().Timeout, call: call, guard: guard, primary: primary, idle: make(chan struct{}), gate: make(chan struct{}, 1)}
		owner = &Owner{state: state}
		if primary == nil {
			owner.client = &Client{endpoint: endpoint, direct: Handle{state: state}, lifetime: call.Context(), budget: policy.Budget}
		}
		go func() { <-call.Context().Done(); _ = owner.Close(context.Background()) }()
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
	if owner == nil || owner.client == nil {
		return Handle{}
	}
	return Handle{state: owner.state}
}

// ShutdownComplete reports confirmed local release, not successful remote cleanup.
func (owner *Owner) ShutdownComplete() bool {
	return owner != nil && owner.state != nil && owner.state.complete.Load()
}

// Release adapts repeatable cleanup observation to resource ownership.
func (owner *Owner) Release(ctx context.Context) resource.ReleaseResult {
	err := owner.Close(ctx)
	return resource.ReleaseResult{Complete: owner.ShutdownComplete(), Err: err}
}
func (state *sourceState) use() (func(), error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.sealed || state.call.Context().Err() != nil {
		return nil, fail(adapters.ErrClosed, "source-use")
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

// Close seals new work, cancels retained cursors and joins their native release.
// Waiting uses ctx; timeout leaves cleanup reachable through this same owner.
// Finalization needs no new admission or evidence slot.
func (owner *Owner) Close(ctx context.Context) error {
	if owner == nil || owner.state == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	state := owner.state
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
	for _, entry := range state.assembly.Snapshot().Sources {
		if entry.Pending || !entry.Quiescent || !entry.Released {
			return translate(errors.Join(err, source.ErrIncomplete), "close")
		}
	}
	state.final = translate(err, "close")
	_ = state.call.Resolve(adapters.Outcome[Result]{Primary: state.primary, Cleanup: state.final})
	state.complete.Store(true)
	_ = state.guard.Release()
	return state.final
}
