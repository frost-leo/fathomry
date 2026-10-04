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

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/frost-leo/fathomry/adapters/database/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/database/mysql/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Owner owns one native pool. A non-nil owner must be closed even with an Open
// error. Timed-out cleanup remains reachable through this same owner.
type Owner struct {
	private
	state  *sourceState
	client *Client
}

type sourceState struct {
	owner      *sourceOwner
	selection  source.Selection[native.Source]
	policy     database.Policy
	inspection *native.Database
}

// Handle grants opaque non-owning access for public resource bindings.
type Handle struct {
	private
	state *sourceState
}

func bind(dependencies Dependencies) (adapters.Endpoint[Result], error) {
	return adapters.Bind(dependencies.Runtime, adapters.Declaration[Result]{Evidence: dependencies.Evidence, Observer: dependencies.Observer, Copy: func(value Result) Result { return value }})
}

// Open acquires local ownership, not service readiness. ctx owns its full
// lifetime, including retained operations. Ping explicitly checks readiness.
func Open(ctx context.Context, settings Settings, dependencies Dependencies) (*Owner, error) {
	endpoint, err := bind(dependencies)
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
	var inspection *native.Database
	owned, err := openSource(ctx, endpoint, request("open", "", sourceWorkBytes, sourceEvidenceBytes),
		func(work context.Context) (*source.Assembly, error) {
			assembly, err := source.Assemble(work, context.Background(), "database-mysql", selected)
			if err != nil {
				return assembly, err
			}
			inbox, err := invocation.NewInbox[native.Result](1, policy.Budget.EvidenceBytes)
			if err == nil {
				inspection, err = native.Bind(assembly, selected, inbox, nil)
			}
			return assembly, err
		})
	if owned == nil {
		return nil, err
	}
	state := &sourceState{owner: owned, selection: selected, policy: policy, inspection: inspection}
	owner := &Owner{state: state}
	owner.client = &Client{endpoint: endpoint, direct: Handle{state: state}, lifetime: owned.context(), budget: policy.Budget}
	return owner, translate(err, "open")
}

// Client returns the direct, non-owning facade.
func (owner *Owner) Client() *Client {
	if owner == nil {
		return nil
	}
	return owner.client
}

// Handle returns an opaque value for resource.Instance declarations.
func (owner *Owner) Handle() Handle {
	if owner == nil {
		return Handle{}
	}
	return Handle{state: owner.state}
}

// Info returns detached preparation metadata, not readiness.
func (owner *Owner) Info() database.Info { return owner.Handle().Info() }

// Info returns detached preparation metadata, not a resource generation.
func (handle Handle) Info() database.Info {
	if handle.state == nil {
		return database.Info{}
	}
	return info(handle.state.owner.info)
}

// Close cancels operations, joins actual native release and retains cleanup errors.
// Finalization uses existing reservations even when admission/evidence is full.
func (owner *Owner) Close(ctx context.Context) error {
	if owner == nil || owner.state == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	return owner.state.owner.close(ctx)
}

// ShutdownComplete is confirmed local release, independent of cleanup errors.
func (owner *Owner) ShutdownComplete() bool {
	return owner != nil && owner.state != nil && owner.state.owner.isComplete()
}

// Release adapts cleanup continuation to public resource ownership.
func (owner *Owner) Release(ctx context.Context) resource.ReleaseResult {
	err := owner.Close(ctx)
	return resource.ReleaseResult{Complete: owner.ShutdownComplete(), Err: err}
}

// Stats is the standard-library detached connection-pool snapshot.
type Stats = sql.DBStats

// Profile copies effective source settings without service I/O or SQL admission.
func (client *Client) Profile(ctx context.Context) (database.Profile, error) {
	var result database.Profile
	err := client.inspect(ctx, func(state *sourceState) {
		value := state.inspection.Profile()
		result = database.Profile{ImplementationModule: value.ImplementationModule, SDKMode: value.SDKMode,
			ServiceMode:    database.Fact{Kind: string(value.ServiceMode.Kind), Value: value.ServiceMode.Value},
			ServiceVersion: database.Fact{Kind: string(value.ServiceVersion.Kind), Value: value.ServiceVersion.Value},
			Protocol:       database.Fact{Kind: string(value.Protocol.Kind), Value: value.Protocol.Value},
			Native:         database.Fact{Kind: string(value.Native.Kind), Value: value.Native.Value}}
		for _, option := range value.Options {
			result.Options = append(result.Options, database.Option{Name: option.Name, Value: option.Value})
		}
	})
	return result, err
}

// Stats borrows the current generation only for the snapshot. It does not Ping,
// dispatch SQL or acquire a public operation slot. Separate reads may see different
// Follow generations; retained SQL handles never retarget.
func (client *Client) Stats(ctx context.Context) (Stats, error) {
	var result Stats
	err := client.inspect(ctx, func(state *sourceState) {
		value := state.inspection.Stats()
		result = value
	})
	return result, err
}

func (client *Client) inspect(ctx context.Context, read func(*sourceState)) error {
	if client == nil || client.lifetime == nil || ctx == nil {
		return fail(ErrInput, "inspect")
	}
	if ctx.Err() != nil {
		return fail(ErrState, "inspect", ctx.Err(), context.Cause(ctx))
	}
	if client.lifetime.Err() != nil {
		return fail(ErrState, "inspect", client.lifetime.Err(), context.Cause(client.lifetime))
	}
	handle := client.direct
	if client.source != nil {
		lease, err := client.source.Acquire(ctx)
		if err != nil {
			return err
		}
		defer lease.Release()
		handle, err = lease.Value()
		if err != nil {
			return err
		}
	}
	if handle.state == nil || handle.state.inspection == nil {
		return fail(ErrState, "inspect")
	}
	if handle.state.owner.context().Err() != nil {
		return fail(ErrState, "inspect", handle.state.owner.context().Err(), context.Cause(handle.state.owner.context()))
	}
	read(handle.state)
	return nil
}

const sourceWorkBytes int64 = 1 << 20

const sourceEvidenceBytes int64 = 64 << 10

// sourceOwner keeps the native assembly and the public source-lifetime reservation.
// Incomplete native cleanup remains reachable through this same object.
type sourceOwner struct {
	assembly *source.Assembly
	info     source.Info
	call     *adapters.Call[Result]
	guard    adapters.Guard
	gate     chan struct{}
	complete atomic.Bool
	serial   atomic.Uint64
	final    error
	primary  error
	useMu    sync.Mutex
	uses     int
	sealed   bool
	idle     chan struct{}
}

func openSource(ctx context.Context, endpoint adapters.Endpoint[Result], request adapters.Request, construct func(context.Context) (*source.Assembly, error)) (*sourceOwner, error) {
	var owner *sourceOwner
	var primary error
	receipt, err := endpoint.Run(ctx, request, func(call *adapters.Call[Result]) {
		guard, err := call.Hold()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		assembly, err := construct(call.Context())
		primary = translate(err, "open")
		if assembly == nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		owner = &sourceOwner{assembly: assembly, call: call, guard: guard, gate: make(chan struct{}, 1), primary: primary, idle: make(chan struct{})}
		if report := assembly.Snapshot(); len(report.Sources) == 1 {
			owner.info = report.Sources[0].Info
		}
		go func() { <-call.Context().Done(); _ = owner.close(context.Background()) }()
	})
	if err != nil {
		return nil, err
	}
	if owner == nil {
		if primary != nil {
			return nil, primary
		}
		value, _ := receipt.Snapshot()
		return nil, outcomeError(value)
	}
	return owner, primary
}

func (owner *sourceOwner) context() context.Context {
	if owner == nil || owner.call == nil {
		return nil
	}
	return owner.call.Context()
}

func (owner *sourceOwner) isComplete() bool { return owner != nil && owner.complete.Load() }

// use joins root families before Assembly.Close, which deliberately reports
// live leases as incomplete instead of waiting for them. Admission remains owned
// by the public runtime and native pool; this fence adds no permit or queue.
func (owner *sourceOwner) use() (func(), error) {
	owner.useMu.Lock()
	defer owner.useMu.Unlock()
	if owner.sealed || owner.context().Err() != nil {
		return nil, adapters.ErrClosed
	}
	owner.uses++
	return sync.OnceFunc(func() {
		owner.useMu.Lock()
		defer owner.useMu.Unlock()
		owner.uses--
		if owner.sealed && owner.uses == 0 {
			close(owner.idle)
		}
	}), nil
}

func (owner *sourceOwner) next() (uint64, bool) {
	for {
		previous := owner.serial.Load()
		if previous == math.MaxUint64 {
			return 0, false
		}
		if owner.serial.CompareAndSwap(previous, previous+1) {
			return previous + 1, true
		}
	}
}

func (owner *sourceOwner) close(ctx context.Context) error {
	if owner == nil || ctx == nil {
		return adapters.ErrHandle
	}
	owner.useMu.Lock()
	if !owner.sealed {
		owner.sealed = true
		if owner.uses == 0 {
			close(owner.idle)
		}
	}
	owner.useMu.Unlock()
	_ = owner.call.Cancel(nil)
	if owner.complete.Load() {
		return owner.final
	}
	select {
	case owner.gate <- struct{}{}:
		defer func() { <-owner.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close")
	}
	if owner.complete.Load() {
		return owner.final
	}
	select {
	case <-owner.idle:
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close")
	}
	err := owner.assembly.Close(ctx)
	report := owner.assembly.Snapshot()
	complete := true
	for _, source := range report.Sources {
		if source.Pending || !source.Quiescent || !source.Released {
			complete = false
		}
	}
	if !complete {
		if err == nil {
			err = source.ErrIncomplete
		}
		return translate(err, "close")
	}
	owner.final = translate(err, "close")
	_ = owner.call.Resolve(adapters.Outcome[Result]{Primary: owner.primary, Cleanup: owner.final})
	owner.complete.Store(true)
	_ = owner.guard.Release()
	return owner.final
}
