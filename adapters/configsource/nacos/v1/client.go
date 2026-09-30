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

package nacos

import (
	"context"
	"slices"
	"sync/atomic"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Evidence preserves observed facts independently of the caller's result.
// MutationPresent separates a mutation result from unrelated read/lifetime facts.
// FailedIndex is -1 when no failed batch slot was identified. No payload is logged.
type Evidence struct {
	Documents       int
	FailedIndex     int
	MutationPresent bool
	Mutation        MutationResult
}

// Dependencies binds caller-owned operation admission and required evidence.
// The caller owns runtime shutdown and evidence delivery/acknowledgement.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Evidence]
	Observer *adapters.Observer
}

// Handle is opaque non-owning instance access for resource bindings. It grants
// neither native client access nor shutdown/operation authority by itself.
type Handle struct {
	private
	state *ownerState
}

// Client is a concurrent-safe operation facade, with no Close authority.
// Direct facades use their Owner; Using facades borrow a generation per operation.
type Client struct {
	private
	endpoint adapters.Endpoint[Evidence]
	direct   Handle
	source   *resource.Ref[Handle]
}

// Owner retains native ownership through cancellation, failures and cleanup.
// Do not discard it after a timed-out Close. Its common runtime remains borrowed.
type Owner struct {
	private
	state  *ownerState
	client *Client
}
type ownerState struct {
	native   *native.Client
	call     *adapters.Call[Evidence]
	guard    adapters.Guard
	gate     chan struct{}
	complete atomic.Bool
	final    error
	primary  error
	info     Info
}

// LayerInfo copies native preparation provenance, not application settings.
type LayerInfo struct {
	Kind   uint8
	Fields []string
}

// Info is detached native preparation metadata, not readiness or a public
// resource generation. Provider, schema Format and opaque Revision are separate.
type Info struct {
	Scope      string
	Provider   string
	Name       string
	Format     uint32
	Revision   string
	Provenance []LayerInfo
}

func bind(dependencies Dependencies) (adapters.Endpoint[Evidence], error) {
	return adapters.Bind(dependencies.Runtime, adapters.Declaration[Evidence]{Evidence: dependencies.Evidence, Observer: dependencies.Observer, Copy: func(value Evidence) Evidence { return value }})
}

// Open validates and freezes technical settings, acquiring local ownership only.
// ctx owns the entire lifetime. A non-nil Owner must be closed even with an error.
func Open(ctx context.Context, settings Settings, dependencies Dependencies) (*Owner, error) {
	endpoint, err := bind(dependencies)
	if err != nil {
		return nil, err
	}
	var owner *Owner
	var primary error
	receipt, err := endpoint.Run(ctx, request("open", 1<<20), func(call *adapters.Call[Evidence]) {
		selected, err := options(settings)
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: err})
			return
		}
		guard, err := call.Hold()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: err})
			return
		}
		client, err := native.Open(call.Context(), selected)
		primary = translate(err, "open")
		if client == nil {
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: primary})
			_ = guard.Release()
			return
		}
		info := client.Info()
		projection := Info{Scope: info.Scope, Provider: info.Configuration.Identity.Provider, Name: info.Configuration.Identity.Name, Format: info.Configuration.Format, Revision: info.Configuration.Revision}
		for _, layer := range info.Configuration.Provenance {
			projection.Provenance = append(projection.Provenance, LayerInfo{uint8(layer.Kind), slices.Clone(layer.Fields)})
		}
		state := &ownerState{native: client, call: call, guard: guard, gate: make(chan struct{}, 1), primary: primary, info: projection}
		owner = &Owner{state: state, client: &Client{endpoint: endpoint, direct: Handle{state: state}}}
		go func() { <-call.Context().Done(); _, _ = state.close(context.Background()) }()
	})
	if err != nil {
		return nil, err
	}
	if owner == nil {
		if primary != nil {
			return nil, primary
		}
		snapshot, _ := receipt.Snapshot()
		return nil, snapshot.Err()
	}
	return owner, primary
}

// Client returns direct non-owning operation access, not another lifetime owner.
func (owner *Owner) Client() *Client {
	if owner == nil {
		return nil
	}
	return owner.client
}

// Handle returns the opaque value used by a public resource.Instance declaration.
func (owner *Owner) Handle() Handle {
	if owner == nil {
		return Handle{}
	}
	return Handle{state: owner.state}
}

// Info returns detached native preparation metadata, without probing a service.
func (owner *Owner) Info() Info { return owner.Handle().Info() }
func (handle Handle) Info() Info {
	if handle.state == nil {
		return Info{}
	}
	value := handle.state.info
	value.Provenance = slices.Clone(value.Provenance)
	for index := range value.Provenance {
		value.Provenance[index].Fields = slices.Clone(value.Provenance[index].Fields)
	}
	return value
}

// Using constructs a resource-backed facade without borrowing yet. Each operation
// borrows once; a subscription guard retains that lease until native cleanup.
func Using(source resource.Ref[Handle], dependencies Dependencies) (*Client, error) {
	if _, err := source.Inspect(); err != nil {
		return nil, err
	}
	endpoint, err := bind(dependencies)
	if err != nil {
		return nil, err
	}
	return &Client{endpoint: endpoint, source: &source}, nil
}

// Close requests stop, joins native work, and reports retained cleanup errors.
// An error can coexist with ShutdownComplete; neither result proves remote effects.
func (owner *Owner) Close(ctx context.Context) error {
	if owner == nil || owner.state == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	_ = owner.state.call.Cancel(nil)
	_, err := owner.state.close(ctx)
	return err
}

// ShutdownComplete reports confirmed local cleanup, not an error-free history.
func (owner *Owner) ShutdownComplete() bool {
	return owner != nil && owner.state != nil && owner.state.complete.Load()
}

// Release adapts the explicit owner to public resource cleanup continuation.
func (owner *Owner) Release(ctx context.Context) resource.ReleaseResult {
	err := owner.Close(ctx)
	return resource.ReleaseResult{Complete: owner.ShutdownComplete(), Err: err}
}
func (state *ownerState) close(ctx context.Context) (bool, error) {
	if state.complete.Load() {
		return true, state.final
	}
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return state.complete.Load(), fail(ErrState, "close", ctx.Err(), context.Cause(ctx))
	}
	if state.complete.Load() {
		return true, state.final
	}
	err := translate(state.native.Close(ctx), "close")
	if !state.native.ShutdownComplete() {
		if err == nil {
			err = fail(ErrState, "close")
		}
		return false, err
	}
	state.final = err
	_ = state.call.Resolve(adapters.Outcome[Evidence]{Value: Evidence{FailedIndex: -1}, Present: true, Primary: state.primary, Cleanup: err})
	state.complete.Store(true)
	_ = state.guard.Release()
	return true, err
}
func request(operation string, workBytes int64) adapters.Request {
	return adapters.Request{Operation: "config.nacos." + operation, WorkBytes: workBytes, EvidenceBytes: 64 << 10}
}
func (client *Client) dispatch(ctx context.Context, operation string, workBytes int64, work func(*adapters.Call[Evidence], *ownerState)) (*adapters.Receipt[Evidence], error) {
	if client == nil {
		return nil, fail(ErrInput, operation)
	}
	run := func(call *adapters.Call[Evidence], handle Handle) {
		if handle.state == nil || handle.state.native == nil {
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: fail(ErrInput, operation)})
			return
		}
		if handle.state.call.Context().Err() != nil {
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: fail(ErrClosed, operation, handle.state.call.Context().Err(), context.Cause(handle.state.call.Context()))})
			return
		}
		work(call, handle.state)
	}
	if client.source != nil {
		return adapters.Using(ctx, client.endpoint, *client.source, request(operation, workBytes), run)
	}
	return client.endpoint.Run(ctx, request(operation, workBytes), func(call *adapters.Call[Evidence]) { run(call, client.direct) })
}
func (client *Client) run(ctx context.Context, operation string, work func(context.Context, *native.Client) (Evidence, error)) error {
	receipt, err := client.dispatch(ctx, operation, 2*MaxWireBytes, func(call *adapters.Call[Evidence], state *ownerState) {
		evidence, err := work(call.Context(), state.native)
		_ = call.Resolve(adapters.Outcome[Evidence]{Value: evidence, Present: true, Primary: translate(err, operation)})
	})
	if err != nil {
		return err
	}
	value, _ := receipt.Snapshot()
	return value.Err()
}
