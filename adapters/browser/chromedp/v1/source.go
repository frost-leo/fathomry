/*
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

package chromedp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
	source "github.com/frost-leo/fathomry/internal/resource"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Owner owns the physical source. A non-nil owner always needs Close.
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
	selected       source.Selection[native.Source]
	assembly       *source.Assembly
	call           *adapters.Call[Result]
	guard          adapters.Guard
	stopNative     context.CancelCauseFunc
	mu             sync.Mutex
	uses           int
	contexts       map[*contextState]struct{}
	traces         map[*traceState]struct{}
	closing        bool
	transferred    bool
	idle           chan struct{}
	gate           chan struct{}
	complete       atomic.Bool
	primary, final error
}

func bind(d Dependencies) (adapters.Endpoint[Result], error) {
	return adapters.Bind(d.Runtime, adapters.Declaration[Result]{Evidence: d.Evidence, Observer: d.Observer, Copy: func(r Result) Result { return r }})
}

func Open(ctx context.Context, settings Settings, d Dependencies) (*Owner, error) {
	p, err := Prepare(settings, d.Native)
	if err != nil {
		return nil, err
	}
	d.Native = NativeOptions{}
	return p.Open(ctx, d)
}
func request(name, id string, work, evidence int64) adapters.Request {
	return adapters.Request{Operation: "browser.chromedp." + name, ID: id, WorkBytes: work, EvidenceBytes: evidence}
}
func (p Prepared) Open(ctx context.Context, d Dependencies) (*Owner, error) {
	if ctx == nil || p.policy.Budget.WorkBytes == 0 || d.Native.TLS != nil {
		return nil, fail(ErrInput, "open")
	}
	r, err := d.Runtime.Options()
	if err != nil {
		return nil, err
	}
	e, err := d.Evidence.Options()
	if err != nil {
		return nil, err
	}
	want := p.policy
	if r.MaxActive < want.Runtime.MaxActive || r.MaxWorkBytes < want.Runtime.MaxWorkBytes || r.MaxQueued < want.Runtime.MaxQueued || r.MaxQueuedBytes < want.Runtime.MaxQueuedBytes || r.MaxTasks < want.Runtime.MaxTasks || r.MaxDepth < want.Runtime.MaxDepth || r.MaxHolds < want.Runtime.MaxHolds || e.Capacity < want.Evidence.Capacity || e.MaxBytes < want.Evidence.MaxBytes {
		return nil, fail(ErrLimit, "policy")
	}
	endpoint, err := bind(d)
	if err != nil {
		return nil, err
	}
	var owner *Owner
	var primary error
	receipt, err := endpoint.Run(ctx, request("open", "", p.policy.Budget.SourceWorkBytes, p.policy.Budget.SourceEvidenceBytes), func(call *adapters.Call[Result]) {
		guard, err := call.Hold()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		// Work cancellation stops admission, not the connection needed to clean
		// up resources in a borrowed browser. The source owns this final cancel.
		lifetime, stopNative := context.WithCancelCause(context.WithoutCancel(call.Context()))
		selected, err := p.native.Select(lifetime)
		if err != nil {
			stopNative(err)
			primary = translate(err, "open")
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		selected = source.WithLimits(selected, p.native.Limits())
		assembly, err := source.Assemble(call.Context(), context.Background(), "browser-chromedp", selected)
		primary = translate(err, "open")
		if assembly == nil {
			stopNative(err)
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		state := &sourceState{prepared: p, selected: selected, assembly: assembly, call: call, guard: guard, stopNative: stopNative, primary: primary, idle: make(chan struct{}), gate: make(chan struct{}, 1)}
		owner = &Owner{state: state}
		if primary == nil {
			owner.client = &Client{endpoint: endpoint, direct: Handle{state: state}, lifetime: call.Context(), budget: p.policy.Budget}
		}
		context.AfterFunc(call.Context(), func() { _ = owner.Close(context.Background()) })
	})
	if err != nil {
		return nil, err
	}
	if owner == nil && primary == nil {
		value, _ := receipt.Snapshot()
		primary = value.Err()
	}
	return owner, primary
}
func (o *Owner) Client() *Client {
	if o == nil {
		return nil
	}
	return o.client
}
func (o *Owner) Handle() Handle {
	if o == nil || o.client == nil {
		return Handle{}
	}
	return Handle{state: o.state}
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
	s.uses++
	return sync.OnceFunc(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.uses--
		if s.closing && s.uses == 0 {
			close(s.idle)
		}
	}), nil
}
func (o *Owner) Close(ctx context.Context) error {
	if o == nil || o.state == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	s := o.state
	s.mu.Lock()
	if !s.closing {
		s.closing = true
		if s.uses == 0 {
			close(s.idle)
		}
	}
	s.mu.Unlock()
	_ = s.call.Cancel(nil)
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close")
	}
	if s.complete.Load() {
		return s.final
	}
	s.mu.Lock()
	contexts := make([]*contextState, 0, len(s.contexts))
	for retained := range s.contexts {
		contexts = append(contexts, retained)
	}
	s.mu.Unlock()
	var pending error
	for _, retained := range contexts {
		err := (&Context{contextState: retained}).Close(ctx)
		retained.mu.Lock()
		complete := retained.complete
		retained.mu.Unlock()
		if !complete {
			pending = errors.Join(pending, err, source.ErrIncomplete)
		}
	}
	if pending != nil {
		if s.prepared.ownedProcess {
			// An unfinished borrowed-page lease can prevent assembly release.
			// Native cancellation can stop our process without releasing that
			// lease or killing a process whose ownership was already transferred.
			s.stopNative(context.Canceled)
		}
		// Native cleanup can retain uncertain remote custody after its local
		// invocation exits. Let the assembly reconcile it (or terminate its own
		// process) without declaring the still-held public children released.
		return translate(errors.Join(pending, s.assembly.Close(ctx)), "close")
	}
	select {
	case <-s.idle:
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close")
	}
	s.mu.Lock()
	traces := make([]*traceState, 0, len(s.traces))
	for trace := range s.traces {
		traces = append(traces, trace)
	}
	s.mu.Unlock()
	for _, state := range traces {
		trace := &Trace{state: state}
		err := trace.Close(ctx)
		if !trace.Closed() {
			return translate(errors.Join(err, source.ErrIncomplete, s.assembly.Close(ctx)), "trace-close")
		}
	}
	err := s.assembly.Close(ctx)
	for _, entry := range s.assembly.Snapshot().Sources {
		if entry.Pending || !entry.Quiescent || !entry.Released {
			return translate(errors.Join(err, source.ErrIncomplete), "close")
		}
	}
	s.final = translate(err, "cleanup")
	s.stopNative(context.Canceled)
	s.mu.Lock()
	transferred := s.transferred
	s.mu.Unlock()
	outcome := adapters.Outcome[Result]{Primary: s.primary, Cleanup: s.final}
	if transferred {
		outcome.Present = true
		outcome.Value = Result{observation: browser.Observation{Returned: true, Transferred: true}}
	}
	_ = s.call.Resolve(outcome)
	s.complete.Store(true)
	_ = s.guard.Release()
	return s.final
}
