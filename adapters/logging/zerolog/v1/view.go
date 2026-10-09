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
	"runtime"
	"sync"
	"sync/atomic"

	ingress "github.com/frost-leo/fathomry/adapters/logging/slog/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"log/slog"
)

// View is an immutable attribute facade over a retained generation family.
// All derivations share its Close authority. No derived view creates a new native
// allowance or changes destinations. Retain/With families must be closed.
type View struct {
	private
	family     *viewFamily
	attributes []slog.Attr
}
type viewFamily struct {
	call     *adapters.Call[Result]
	guard    adapters.Guard
	client   *Client
	state    *sourceState
	release  func()
	stop     func()
	mu       sync.Mutex
	views    int
	bytes    int64
	closing  bool
	gateway  bool
	handler  *ingress.Handler
	gate     chan struct{}
	complete atomic.Bool
	final    error
}

// Retain captures the actual generation and reserves bounded storage/child work.
// ctx owns this family's lifetime. Four simultaneous families per physical
// source are admitted; each has 128 cumulative derivations and 8 MiB storage.
func (client *Client) Retain(ctx context.Context) (*View, error) {
	if client == nil || client.lifetime == nil || ctx == nil {
		return nil, fail(ErrInput, "retain")
	}
	live, stop := joinContexts(ctx, client.lifetime)
	retained := false
	defer func() {
		if !retained {
			stop()
		}
	}()
	var view *View
	var primary error
	run := func(call *adapters.Call[Result], handle Handle) {
		reject := func(err error) {
			primary = translate(err, "retain")
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
		}
		state := handle.state
		if state == nil {
			reject(fail(ErrState, "retain"))
			return
		}
		if state.policy.Budget.WorkBytes > client.budget.WorkBytes || state.policy.Budget.EvidenceBytes > client.budget.EvidenceBytes {
			reject(fail(ErrLimit, "retain"))
			return
		}
		release, err := state.use()
		if err != nil {
			reject(err)
			return
		}
		if err := client.checkComposition(state); err != nil {
			release()
			reject(err)
			return
		}
		physical := state.physical
		physical.mu.Lock()
		if physical.families >= maxFamilies {
			physical.mu.Unlock()
			release()
			reject(fail(ErrLimit, "retained-families"))
			return
		}
		physical.families++
		physical.mu.Unlock()
		releaseFamily := sync.OnceFunc(func() { physical.mu.Lock(); physical.families--; physical.mu.Unlock(); release() })
		guard, err := call.Hold()
		if err != nil {
			releaseFamily()
			reject(err)
			return
		}
		family := &viewFamily{call: call, guard: guard, state: state, release: releaseFamily, stop: stop, views: 1, bytes: 256, gate: make(chan struct{}, 1)}
		family.client = &Client{endpoint: client.endpoint, runtime: client.runtime, direct: handle, lifetime: call.Context(), budget: client.budget, id: client.id, family: family}
		view = &View{family: family}
		retained = true
		go func() { <-call.Context().Done(); _ = view.Close(context.Background()) }()
	}
	req := request("retain", client.id, 2*derivedBytes+maxChildren*client.budget.WorkBytes, publicMetadataBytes)
	var receipt *adapters.Receipt[Result]
	var err error
	if client.source != nil {
		receipt, err = adapters.UsingWithLifetime(ctx, live, client.endpoint, *client.source, req, run)
	} else {
		receipt, err = client.endpoint.RunWithLifetime(ctx, live, req, func(call *adapters.Call[Result]) { run(call, client.direct) })
	}
	if err != nil {
		return nil, err
	}
	if view == nil && primary == nil {
		snapshot, _ := receipt.Snapshot()
		primary = snapshot.Err()
	}
	return view, primary
}

// With captures one actual generation and freezes derived attributes.
func (client *Client) With(ctx context.Context, attributes ...slog.Attr) (*View, error) {
	view, err := client.Retain(ctx)
	if err != nil {
		return nil, err
	}
	derived, err := view.With(attributes...)
	if err != nil {
		_ = view.Close(context.Background())
	}
	return derived, err
}

// With keeps immutable attributes without changing source or native allowance.
func (view *View) With(attributes ...slog.Attr) (*View, error) {
	if view == nil || view.family == nil {
		return nil, fail(ErrInput, "with")
	}
	if len(attributes) > native.MaxAttributes-len(view.attributes) {
		return nil, fail(ErrLimit, "attributes")
	}
	all := append(append([]slog.Attr(nil), view.attributes...), attributes...)
	limit := view.family.state.prepared.metadata.MaxRecordBytes
	frozen, err := freezeAttributes(all, limit)
	if err != nil {
		return nil, err
	}
	family := view.family
	family.mu.Lock()
	defer family.mu.Unlock()
	if family.closing || family.call.Context().Err() != nil {
		return nil, fail(ErrState, "with")
	}
	charge := family.state.prepared.metadata.ViewBytes
	if family.views >= maxDerivedViews || charge > derivedBytes-family.bytes {
		return nil, fail(ErrLimit, "with")
	}
	family.views++
	family.bytes += charge
	return &View{family: family, attributes: frozen}, nil
}
func (view *View) Log(ctx context.Context, level Level, message string, attributes ...slog.Attr) (*adapters.Receipt[Result], error) {
	if view == nil || view.family == nil {
		return nil, fail(ErrInput, "log")
	}
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:])
	return view.LogEntry(ctx, Entry{Time: now(), PC: pcs[0], Level: level, Message: message}, attributes...)
}
func (view *View) LogEntry(ctx context.Context, entry Entry, attributes ...slog.Attr) (*adapters.Receipt[Result], error) {
	if view == nil || view.family == nil {
		return nil, fail(ErrInput, "log")
	}
	if len(attributes) > native.MaxAttributes-len(view.attributes) {
		return nil, fail(ErrLimit, "attributes")
	}
	all := append(append([]slog.Attr(nil), view.attributes...), attributes...)
	return view.family.client.LogEntry(ctx, entry, all...)
}
func (view *View) Sync(ctx context.Context) (*adapters.Receipt[Result], error) {
	if view == nil || view.family == nil {
		return nil, fail(ErrInput, "sync")
	}
	return view.family.client.Sync(ctx)
}
func (view *View) Rotate(ctx context.Context) (*adapters.Receipt[Result], error) {
	if view == nil || view.family == nil {
		return nil, fail(ErrInput, "rotate")
	}
	return view.family.client.Rotate(ctx)
}
func (view *View) Enabled(ctx context.Context, level Level) (bool, error) {
	if view == nil || view.family == nil {
		return false, fail(ErrInput, "enabled")
	}
	return view.family.client.Enabled(ctx, level)
}

// Close stops the entire family and joins actual children before releasing its
// generation. Timeout/cancellation of this waiter never substitutes for release.
func (view *View) Close(ctx context.Context) error {
	if view == nil || view.family == nil || ctx == nil {
		return fail(ErrInput, "close-view")
	}
	view.family.mu.Lock()
	handler := view.family.handler
	if handler == nil {
		view.family.beginCloseLocked()
	}
	view.family.mu.Unlock()
	if handler != nil {
		return handler.Close(ctx)
	}
	return view.finishClose(ctx)
}

func (view *View) closeFamily(ctx context.Context) error {
	family := view.family
	family.mu.Lock()
	family.beginCloseLocked()
	family.mu.Unlock()
	return view.finishClose(ctx)
}

func (family *viewFamily) beginCloseLocked() {
	if !family.closing {
		family.closing = true
		_ = family.call.Cancel(nil)
		snapshot, _ := family.call.Receipt().Snapshot()
		_ = family.call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{source: info(family.state.physical.info), policyRevision: family.state.prepared.native.Description().Revision, attribution: publicAttribution(snapshot.Info())}})
		_ = family.guard.Release()
	}
}

func (view *View) finishClose(ctx context.Context) error {
	family := view.family
	if family.complete.Load() {
		return family.final
	}
	select {
	case family.gate <- struct{}{}:
		defer func() { <-family.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close-view")
	}
	if family.complete.Load() {
		return family.final
	}
	_, err := family.call.Receipt().WaitReleased(ctx)
	if err != nil {
		return err
	}
	family.stop()
	family.release()
	family.complete.Store(true)
	return family.final
}
func (view *View) Release(ctx context.Context) resource.ReleaseResult {
	err := view.Close(ctx)
	return resource.ReleaseResult{Complete: view != nil && view.family != nil && view.family.complete.Load(), Err: err}
}

func (view *View) releaseFamily(ctx context.Context) resource.ReleaseResult {
	err := view.closeFamily(ctx)
	return resource.ReleaseResult{Complete: view.family.complete.Load(), Err: err}
}
