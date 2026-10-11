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

package iceberg

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/frost-leo/fathomry/adapters/tableformat/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/google/uuid"
)

// Owner is the sole physical cleanup authority. A non-nil Owner must be closed,
// including one returned together with an error. Close never drops remote data.
type Owner struct {
	private
	state  *sourceState
	client *Client
}
type Handle struct {
	private
	state *sourceState
}
type sourceState struct {
	prepared       Prepared
	policy         tableformat.Policy
	selection      source.Selection[native.ManagedSource]
	assembly       *source.Assembly
	call           *adapters.Call[Result]
	guard          adapters.Guard
	identity       string
	mu             sync.Mutex
	active         int
	closing        bool
	idle           chan struct{}
	cleanupGate    chan struct{}
	complete       atomic.Bool
	primary, final error
}

func bind(d Dependencies) (adapters.Endpoint[Result], error) {
	return adapters.Bind(d.Runtime, adapters.Declaration[Result]{Evidence: d.Evidence, Observer: d.Observer, Copy: func(v Result) Result { return v }})
}
func Open(ctx context.Context, settings Settings, d Dependencies) (*Owner, error) {
	p, err := Prepare(settings, d.Native)
	if err != nil {
		return nil, err
	}
	return p.Open(ctx, d)
}
func (p Prepared) Open(ctx context.Context, d Dependencies) (*Owner, error) {
	if ctx == nil {
		return nil, fail(ErrInput, "open")
	}
	selected, err := p.native.Select()
	if err != nil {
		return nil, translate(err, "open")
	}
	endpoint, err := bind(d)
	if err != nil {
		return nil, err
	}
	policy := p.Policy()
	if err = checkPolicy(policy, d); err != nil {
		return nil, err
	}
	selected = source.WithLimits(selected, p.native.Limits())
	var owner *Owner
	var primary error
	receipt, err := endpoint.Run(ctx, request("open", "", policy.Budget.SourceWorkBytes, policy.Budget.SourceEvidenceBytes), func(call *adapters.Call[Result]) {
		guard, err := call.Hold()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		assembly, err := source.Assemble(call.Context(), context.Background(), "iceberg", selected)
		primary = translate(err, "open")
		if assembly == nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		state := &sourceState{prepared: p, policy: policy, selection: selected, assembly: assembly, call: call, guard: guard, identity: uuid.NewString(), idle: make(chan struct{}), cleanupGate: make(chan struct{}, 1), primary: primary}
		owner = &Owner{state: state}
		owner.client = &Client{endpoint: endpoint, direct: Handle{state: state}, lifetime: call.Context(), budget: policy.Budget}
		context.AfterFunc(call.Context(), func() { _ = state.close(context.Background()) })
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
func checkPolicy(p tableformat.Policy, d Dependencies) error {
	if d.Runtime == nil || d.Evidence == nil {
		return fail(ErrInput, "dependencies")
	}
	r, err := d.Runtime.Options()
	if err != nil {
		return err
	}
	e, err := d.Evidence.Options()
	if err != nil {
		return err
	}
	if r.MaxActive < p.Runtime.MaxActive || r.MaxWorkBytes < p.Runtime.MaxWorkBytes || r.MaxHolds < p.Runtime.MaxHolds || e.Capacity < p.Evidence.Capacity || e.MaxBytes < p.Evidence.MaxBytes {
		return fail(ErrLimit, "policy")
	}
	return nil
}
func (o *Owner) Client() *Client {
	if o == nil {
		return nil
	}
	return o.client
}
func (o *Owner) Handle() Handle {
	if o == nil {
		return Handle{}
	}
	return Handle{state: o.state}
}
func (o *Owner) Close(ctx context.Context) error {
	if o == nil || o.state == nil {
		return fail(ErrInput, "close")
	}
	return o.state.close(ctx)
}
func (o *Owner) ShutdownComplete() bool { return o != nil && o.state != nil && o.state.complete.Load() }
func (o *Owner) Release(ctx context.Context) resource.ReleaseResult {
	err := o.Close(ctx)
	return resource.ReleaseResult{Complete: o.ShutdownComplete(), Err: err}
}
func (s *sourceState) use() (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.call.Context().Err() != nil {
		return nil, fail(ErrState, "source")
	}
	s.active++
	return sync.OnceFunc(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.active--
		if s.closing && s.active == 0 {
			close(s.idle)
		}
	}), nil
}
func (s *sourceState) close(ctx context.Context) error {
	if ctx == nil {
		return fail(ErrInput, "close")
	}
	s.mu.Lock()
	if !s.closing {
		s.closing = true
		if s.active == 0 {
			close(s.idle)
		}
	}
	s.mu.Unlock()
	_ = s.call.Cancel(nil)
	select {
	case s.cleanupGate <- struct{}{}:
		defer func() { <-s.cleanupGate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close")
	}
	if s.complete.Load() {
		return s.final
	}
	select {
	case <-s.idle:
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close")
	}
	err := s.assembly.Close(ctx)
	for _, item := range s.assembly.Snapshot().Sources {
		if item.Pending || !item.Quiescent || !item.Released {
			return translate(errors.Join(err, source.ErrIncomplete), "close")
		}
	}
	s.final = translate(err, "cleanup")
	_ = s.call.Resolve(adapters.Outcome[Result]{Primary: s.primary, Cleanup: s.final})
	s.complete.Store(true)
	_ = s.guard.Release()
	return s.final
}
func (h Handle) Info() tableformat.Info {
	if h.state == nil {
		return tableformat.Info{}
	}
	report := h.state.assembly.Snapshot()
	if len(report.Sources) != 1 {
		return tableformat.Info{}
	}
	value := report.Sources[0].Info
	return tableformat.Info{Scope: value.Scope, Provider: value.Configuration.Identity.Provider, Name: value.Configuration.Identity.Name, Revision: value.Configuration.Revision, FormatVersion: value.Configuration.Format}
}
func (o *Owner) Info() tableformat.Info { return o.Handle().Info() }
