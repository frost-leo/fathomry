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

package otel

import (
	"context"
	"errors"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"math"
	"sync"
	"sync/atomic"
)

// Owner owns one native source and its optional explicit export loop. A non-nil
// Owner must be closed even when Open returns an error. Never copy an Owner.
type Owner struct {
	private
	state         *sourceState
	client        *Client
	loopMu        sync.Mutex
	loop          *ExportLoop
	exportClosing bool
}

// Handle is a non-owning source token suitable for typed resource bindings.
type Handle struct {
	private
	state *sourceState
}

type sourceState struct {
	assembly   *source.Assembly
	selection  source.Selection[native.Source]
	policy     Policy
	metadata   native.Metadata
	call       *adapters.Call[Result]
	guard      adapters.Guard
	primary    error
	mu         sync.Mutex
	uses       int
	sealed     bool
	idle       chan struct{}
	gate       chan struct{}
	complete   atomic.Bool
	final      error
	serial     atomic.Uint64
	inspection *native.Client
	info       source.Info
}

func bind(dependencies Dependencies) (adapters.Endpoint[Result], error) {
	return adapters.Bind(dependencies.Runtime, adapters.Declaration[Result]{Evidence: dependencies.Evidence, Observer: dependencies.Observer, Copy: func(value Result) Result { return value }})
}

// Open prepares and constructs a source without asserting backend readiness.
// ctx owns the source's whole lifetime, not merely construction waiting.
func Open(ctx context.Context, value Settings, dependencies Dependencies) (*Owner, error) {
	prepared, err := Prepare(value)
	if err != nil {
		return nil, err
	}
	return prepared.Open(ctx, dependencies)
}

// Open constructs exactly this frozen preparation after validating the supplied
// public capacity. Final cleanup is retained even after partial construction.
func (prepared Prepared) Open(ctx context.Context, dependencies Dependencies) (*Owner, error) {
	if ctx == nil {
		return nil, fail(ErrInput, "open")
	}
	policy, err := prepared.Policy()
	if err != nil {
		return nil, err
	}
	if err := checkPolicy(policy, dependencies); err != nil {
		return nil, err
	}
	endpoint, err := bind(dependencies)
	if err != nil {
		return nil, err
	}
	selected := source.WithLimits(prepared.native.Select(), prepared.metadata.Limits)
	var owner *Owner
	var primary error
	receipt, err := endpoint.Run(ctx, request("open", "", policy.SourceWorkBytes, policy.SourceEvidenceBytes), func(call *adapters.Call[Result]) {
		guard, err := call.Hold()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		assembly, err := source.Assemble(call.Context(), context.Background(), "telemetry-otel", selected)
		primary = translate(err, "open")
		if assembly == nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		state := &sourceState{assembly: assembly, selection: selected, policy: policy, metadata: prepared.metadata, call: call, guard: guard, primary: primary,
			idle: make(chan struct{}), gate: make(chan struct{}, 1)}
		if entries := assembly.Snapshot().Sources; len(entries) == 1 {
			state.info = entries[0].Info
		}
		if primary == nil {
			inbox, bindErr := invocation.NewInbox[native.Result](1, prepared.metadata.EvidenceBytes)
			if bindErr == nil {
				state.inspection, bindErr = native.Bind(assembly, selected, inbox, nil)
			}
			if bindErr != nil {
				primary = translate(bindErr, "open")
				state.primary = primary
			}
		}
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

// Client returns a non-owning direct facade, or nil after unsuccessful construction.
func (owner *Owner) Client() *Client {
	if owner == nil {
		return nil
	}
	return owner.client
}

// Handle grants resource bindings a non-owning source token. Failed construction
// returns a zero token while the Owner still retains cleanup responsibility.
func (owner *Owner) Handle() Handle {
	if owner == nil || owner.client == nil {
		return Handle{}
	}
	return Handle{state: owner.state}
}

// ShutdownComplete reports confirmed native release, independently of errors or
// evidence acknowledgement. A timeout or canceled caller does not imply release.
func (owner *Owner) ShutdownComplete() bool {
	return owner != nil && owner.state != nil && owner.state.complete.Load()
}

// Release preserves completion independently from retained cleanup errors.
func (owner *Owner) Release(ctx context.Context) resource.ReleaseResult {
	err := owner.Close(ctx)
	return resource.ReleaseResult{Complete: owner.ShutdownComplete(), Err: err}
}

func (state *sourceState) use() (func(), error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.sealed || state.call.Context().Err() != nil {
		return nil, adapters.ErrClosed
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

// Close stops and joins scheduling, refuses new work, then joins retained spans
// and actual native work before final bounded export and source release. Every
// live Span still requires End. An interrupted wait retains this same Owner.
func (owner *Owner) Close(ctx context.Context) error {
	if owner == nil || owner.state == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	if err := owner.stopExport(ctx); err != nil {
		return err
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
	observation, _ := state.call.Receipt().Snapshot()
	_ = state.call.Resolve(adapters.Outcome[Result]{Present: true,
		Value:   Result{source: info(state.info), attribution: publicAttribution(observation.Info())},
		Primary: state.primary, Cleanup: state.final})
	state.complete.Store(true)
	_ = state.guard.Release()
	return state.final
}

func checkPolicy(policy Policy, dependencies Dependencies) error {
	options, err := dependencies.Runtime.Options()
	if err != nil {
		return err
	}
	evidence, err := dependencies.Evidence.Options()
	if err != nil {
		return err
	}
	want := policy.Runtime
	if options.MaxActive < want.MaxActive || options.MaxQueued < want.MaxQueued ||
		options.MaxWorkBytes < want.MaxWorkBytes || options.MaxQueuedBytes < want.MaxQueuedBytes ||
		options.MaxTasks < want.MaxTasks || options.MaxDepth < want.MaxDepth || options.MaxHolds < want.MaxHolds ||
		evidence.Capacity < policy.Evidence.Capacity || evidence.MaxBytes < policy.Evidence.MaxBytes {
		return fail(ErrLimit, "policy")
	}
	return nil
}
