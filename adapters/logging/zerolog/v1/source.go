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

package zerolog

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Owner owns one immutable logical policy. Level-only adopted owners share the
// original physical source. Every non-nil Owner needs cleanup, including failures.
type Owner struct {
	private
	state  *sourceState
	client *Client
}

// Handle is a non-owning immutable policy/source token.
type Handle struct {
	private
	state *sourceState
}

type physicalSource struct {
	prepared           Prepared
	endpoint           adapters.Endpoint[Result]
	runtime            adapters.Runtime
	checks             []RuntimeCheck
	assembly           *source.Assembly
	selection          source.Selection[native.Source]
	call               *adapters.Call[Result]
	guard              adapters.Guard
	info               source.Info
	primary            error
	mu                 sync.Mutex
	policies, families int
	sealed             bool
	gate               chan struct{}
	complete           atomic.Bool
	final              error
	serial             atomic.Uint64
}
type sourceState struct {
	physical               *physicalSource
	prepared               Prepared
	policy                 Policy
	call                   *adapters.Call[Result]
	guard                  adapters.Guard
	primary                error
	inspection             *native.Logger
	outputs                []logging.Output
	mu                     sync.Mutex
	uses                   int
	sealed, detached, last bool
	idle                   chan struct{}
	gate                   chan struct{}
	complete               atomic.Bool
	final                  error
}

func bind(deps Dependencies) (adapters.Endpoint[Result], error) {
	return adapters.Bind(deps.Runtime, adapters.Declaration[Result]{Evidence: deps.Evidence, Observer: deps.Observer, Copy: func(value Result) Result { return value }})
}

// Open freezes settings, then constructs only explicitly enabled destinations.
// ctx owns the logical policy lifetime, not only setup waiting.
func Open(ctx context.Context, value Settings, deps Dependencies) (*Owner, error) {
	prepared, err := Prepare(value)
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
	if err = checkPolicy(policy, deps); err != nil {
		return nil, err
	}
	bindings, checks, err := prepared.bindings(deps)
	if err != nil {
		return nil, err
	}
	for _, check := range checks {
		if err := check.CheckRuntime(deps.Runtime); err != nil {
			return nil, translate(err, "composition")
		}
	}
	endpoint, err := bind(deps)
	if err != nil {
		return nil, err
	}
	selected, err := prepared.native.Select(bindings)
	if err != nil {
		return nil, translate(err, "select")
	}
	selected = source.WithLimits(selected, prepared.metadata.Limits)
	var owner *Owner
	var primary error
	receipt, err := endpoint.RunWithLifetime(ctx, context.WithoutCancel(ctx), request("physical", "", policy.SourceWorkBytes, policy.SourceEvidenceBytes), func(call *adapters.Call[Result]) {
		guard, holdErr := call.Hold()
		if holdErr != nil {
			primary = holdErr
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			return
		}
		physical := &physicalSource{prepared: prepared, endpoint: endpoint, runtime: *deps.Runtime, checks: checks, selection: selected, call: call, guard: guard, gate: make(chan struct{}, 1)}
		owner, primary = physical.newPolicy(ctx, prepared, true)
		if owner == nil {
			physical.primary = primary
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
		}
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

// Adopt creates only a frozen threshold view, preserving every original physical
// destination and dependency. It rejects every non-level change. Use a distinct
// Open for a different directory; a same-path ownership change requires quiescence.
func (prepared Prepared) Adopt(ctx context.Context, previous Handle) (*Owner, error) {
	if ctx == nil || previous.state == nil {
		return nil, fail(ErrInput, "adopt")
	}
	state := previous.state
	release, err := state.use()
	if err != nil {
		return nil, err
	}
	defer release()
	if !prepared.PhysicalEquivalent(state.prepared) {
		return nil, fail(ErrUnsupported, "adopt")
	}
	return state.physical.newPolicy(ctx, prepared, false)
}

func (physical *physicalSource) newPolicy(ctx context.Context, prepared Prepared, construct bool) (*Owner, error) {
	policy, err := prepared.Policy()
	if err != nil {
		return nil, err
	}
	physical.mu.Lock()
	if physical.sealed || physical.policies >= maxPolicies {
		physical.mu.Unlock()
		return nil, fail(ErrLimit, "policy-view")
	}
	physical.policies++
	physical.mu.Unlock()
	var owner *Owner
	var primary error
	receipt, err := physical.endpoint.ChildWithLifetime(ctx, ctx, physical.call.Scope(), request("policy", "", 0, publicMetadataBytes), func(call *adapters.Call[Result]) {
		guard, holdErr := call.Hold()
		if holdErr != nil {
			primary = holdErr
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			return
		}
		state := &sourceState{physical: physical, prepared: prepared, policy: policy, call: call, guard: guard, idle: make(chan struct{}), gate: make(chan struct{}, 1)}
		for _, output := range prepared.native.Options().Sinks {
			state.outputs = append(state.outputs, logging.Output{Name: output.Name, Kind: output.Kind})
		}
		owner = &Owner{state: state}
		if construct {
			physical.assembly, err = source.Assemble(call.Context(), context.Background(), "logging-zerolog", physical.selection)
			primary = translate(err, "open")
			physical.primary = primary
			if physical.assembly != nil {
				if entries := physical.assembly.Snapshot().Sources; len(entries) == 1 {
					physical.info = entries[0].Info
				}
			}
		}
		if primary == nil {
			inbox, bindErr := invocation.NewInbox[native.Result](1, prepared.metadata.EvidenceBytes)
			if bindErr == nil {
				state.inspection, bindErr = native.Bind(physical.assembly, physical.selection, inbox, nil)
			}
			if bindErr == nil {
				state.inspection, bindErr = state.inspection.WithPolicy(prepared.native)
			}
			primary = translate(bindErr, "bind")
		}
		state.primary = primary
		if primary == nil {
			owner.client = &Client{endpoint: physical.endpoint, runtime: physical.runtime, direct: Handle{state: state}, lifetime: call.Context(), budget: policy.Budget}
		}
		go func() { <-call.Context().Done(); _ = owner.Close(context.Background()) }()
	})
	if owner == nil {
		physical.mu.Lock()
		physical.policies--
		physical.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if primary == nil {
			snapshot, _ := receipt.Snapshot()
			primary = snapshot.Err()
		}
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
func (owner *Owner) ShutdownComplete() bool {
	return owner != nil && owner.state != nil && owner.state.complete.Load()
}
func (owner *Owner) Release(ctx context.Context) resource.ReleaseResult {
	err := owner.Close(ctx)
	return resource.ReleaseResult{Complete: owner.ShutdownComplete(), Err: err}
}

func (state *sourceState) use() (func(), error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.sealed || state.call.Context().Err() != nil {
		return nil, fail(ErrState, "use")
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
func (physical *physicalSource) next() (uint64, bool) {
	for {
		previous := physical.serial.Load()
		if previous == math.MaxUint64 {
			return 0, false
		}
		if physical.serial.CompareAndSwap(previous, previous+1) {
			return previous + 1, true
		}
	}
}

// Close refuses new work, joins existing work and releases this policy. Only
// the last policy closes files/sink maintenance and the original physical root.
// A canceled wait retains this Owner; repeat with a live cleanup context.
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
	physical := state.physical
	if !state.detached {
		physical.mu.Lock()
		physical.policies--
		state.last = physical.policies == 0
		if state.last {
			physical.sealed = true
		}
		physical.mu.Unlock()
		state.detached = true
	}
	if state.last {
		err := physical.close(ctx)
		if !physical.complete.Load() {
			return err
		}
		state.final = err
	}
	observation, _ := state.call.Receipt().Snapshot()
	_ = state.call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{source: info(physical.info), policyRevision: state.prepared.native.Description().Revision, attribution: publicAttribution(observation.Info())}, Primary: state.primary, Cleanup: state.final})
	state.complete.Store(true)
	_ = state.guard.Release()
	return state.final
}
func (physical *physicalSource) close(ctx context.Context) error {
	_ = physical.call.Cancel(nil)
	if physical.complete.Load() {
		return physical.final
	}
	select {
	case physical.gate <- struct{}{}:
		defer func() { <-physical.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close")
	}
	if physical.complete.Load() {
		return physical.final
	}
	var err error
	if physical.assembly != nil {
		err = physical.assembly.Close(ctx)
		for _, entry := range physical.assembly.Snapshot().Sources {
			if entry.Pending || !entry.Quiescent || !entry.Released {
				return translate(errors.Join(err, source.ErrIncomplete), "close")
			}
		}
	}
	physical.final = translate(err, "close")
	observation, _ := physical.call.Receipt().Snapshot()
	_ = physical.call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{source: info(physical.info), attribution: publicAttribution(observation.Info())}, Primary: physical.primary, Cleanup: physical.final})
	physical.complete.Store(true)
	_ = physical.guard.Release()
	return physical.final
}
