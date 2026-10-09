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

package zap

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	ingress "github.com/frost-leo/fathomry/adapters/logging/slog/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/logging/zap/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"go.uber.org/zap/zapcore"
)

// View is an immutable field/name facade over a retained generation family.
// All derivations share its Close authority. No derived view creates a new native
// allowance or changes destinations. Retain/With/Named families must be closed.
type View struct {
	private
	family *viewFamily
	name   string
	fields []zapcore.Field
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
		family.client = &Client{endpoint: client.endpoint, direct: handle, lifetime: call.Context(), budget: client.budget, id: client.id, family: family}
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

// With captures one generation and creates an immutable field derivation.
func (client *Client) With(ctx context.Context, fields ...zapcore.Field) (*View, error) {
	view, err := client.Retain(ctx)
	if err != nil {
		return nil, err
	}
	derived, err := view.With(fields...)
	if err != nil {
		_ = view.Close(context.Background())
	}
	return derived, err
}

// Named captures one generation and creates a bounded hierarchical name.
func (client *Client) Named(ctx context.Context, name string) (*View, error) {
	view, err := client.Retain(ctx)
	if err != nil {
		return nil, err
	}
	derived, err := view.Named(name)
	if err != nil {
		_ = view.Close(context.Background())
	}
	return derived, err
}
func (view *View) derive(name string, fields []zapcore.Field, charge int64) (*View, error) {
	if view == nil || view.family == nil {
		return nil, fail(ErrInput, "derive")
	}
	family := view.family
	family.mu.Lock()
	defer family.mu.Unlock()
	if family.closing || family.call.Context().Err() != nil {
		return nil, fail(ErrState, "derive")
	}
	if family.views >= maxDerivedViews || charge > derivedBytes-family.bytes {
		return nil, fail(ErrLimit, "derive")
	}
	family.views++
	family.bytes += charge
	return &View{family: family, name: name, fields: fields}, nil
}

// With copies inputs and preserves duplicate/type/size refusal for the whole view.
func (view *View) With(fields ...zapcore.Field) (*View, error) {
	if view == nil || view.family == nil {
		return nil, fail(ErrInput, "with")
	}
	if len(fields) > native.MaxFields-len(view.fields) {
		return nil, fail(ErrLimit, "fields")
	}
	combined := append(append([]zapcore.Field(nil), view.fields...), fields...)
	frozen, err := freezeFields(combined)
	if err != nil {
		return nil, err
	}
	// The admitted field ceiling plus facade/name is a conservative retained charge.
	return view.derive(view.name, frozen, int64(64<<10+len(view.name)+256))
}

// Named keeps existing fields at their original destination and name prefix.
func (view *View) Named(name string) (*View, error) {
	if view == nil || view.family == nil || name == "" || len(name) > 128 || !utf8.ValidString(name) {
		return nil, fail(ErrInput, "named")
	}
	if _, err := native.FreezeFields([]zapcore.Field{{Key: name, Type: zapcore.StringType}}); err != nil {
		return nil, translate(err, "named")
	}
	for _, char := range name {
		if char < 32 || char == 127 {
			return nil, fail(ErrInput, "named")
		}
	}
	if view.name != "" {
		name = view.name + "." + name
	}
	if len(name) > 128 {
		return nil, fail(ErrLimit, "named")
	}
	return view.derive(strings.Clone(name), view.fields, int64(len(name)+256))
}
func (view *View) Log(ctx context.Context, level zapcore.Level, message string, fields ...zapcore.Field) (*adapters.Receipt[Result], error) {
	if view == nil || view.family == nil {
		return nil, fail(ErrInput, "log")
	}
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:])
	return view.LogEntry(ctx, Entry{Time: now(), PC: pcs[0], Level: level, Message: message}, fields...)
}

// LogEntry preserves an original record's time and PC. The immutable view name
// prefixes an explicitly supplied entry name.
func (view *View) LogEntry(ctx context.Context, entry Entry, fields ...zapcore.Field) (*adapters.Receipt[Result], error) {
	if view == nil || view.family == nil {
		return nil, fail(ErrInput, "log")
	}
	if len(fields) > native.MaxFields-len(view.fields) || len(entry.Name) > 128 {
		return nil, fail(ErrLimit, "log")
	}
	if entry.Name == "" {
		entry.Name = view.name
	} else if view.name != "" {
		entry.Name = view.name + "." + entry.Name
	}
	combined := append(append([]zapcore.Field(nil), view.fields...), fields...)
	return view.family.client.LogEntry(ctx, entry, combined...)
}
func (view *View) Sync(ctx context.Context) (*adapters.Receipt[Result], error) {
	if view == nil || view.family == nil {
		return nil, fail(ErrInput, "sync")
	}
	return view.family.client.Sync(ctx)
}
func (view *View) Enabled(ctx context.Context, level zapcore.Level) (bool, error) {
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
