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

package tlsclient

import (
	"context"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Source is a non-owning composition token. Bind exposes controlled HTTP calls.
type Source struct {
	private
	owner *owner
}

// Client is concurrent and non-owning. Aliases share original identity, native
// resource ceilings and independent evidence admission.
type Client struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Select copies native containers and prepares explicit configuration without
// network I/O. Opaque native dependencies remain borrowed through assembly release.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	native, err := copyNative(options.Native)
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	version := options.Version
	if version == 0 {
		version = 1
	}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: func(value settings) error {
		if err := validate(value); err != nil {
			return err
		}
		return validateNative(value, native)
	}}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		if err := ctx.Err(); err != nil {
			return resource.Resource[Source]{}, failure(ErrState, "construct", err)
		}
		copied, err := copyNative(native)
		if err != nil {
			return resource.Resource[Source]{}, err
		}
		own := &owner{settings: value, native: copied, callbacks: newActivity(), bindings: make(map[bindingKey]*binding), held: make(map[*binding]struct{})}
		own.callbacks.limit = 4 * (value.MaxTCPConnections + value.MaxHTTP3Transports)
		return resource.Resource[Source]{Acquired: true, Capability: Source{owner: own}, Release: own.release}, nil
	}), nil
}

// LimitsV1 returns the policy for unoverridden Go options. When layers override
// settings, composition must supply matching limits; Bind validates them.
func LimitsV1(options OptionsV1) (resource.Limits, error) {
	if options.Version != 0 && options.Version != 1 {
		return resource.Limits{}, failure(ErrInput, "version")
	}
	value := defaults(options)
	if err := validate(value); err != nil {
		return resource.Limits{}, err
	}
	native, err := copyNative(options.Native)
	if err != nil {
		return resource.Limits{}, err
	}
	if err := validateNative(value, native); err != nil {
		return resource.Limits{}, err
	}
	return value.limits(), nil
}

// Bind joins authoritative resource admission and the independent required Inbox.
func Bind(assembly *resource.Assembly, selected resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Client, error) {
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	value, limits := source.owner.settings, access.Limits()
	if limits.Active > value.MaxActive || limits.Queued > value.QueuedCalls || limits.Bytes < value.reservation() || limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}

// EvidenceBytes is a per-call retained-result reservation, not native heap/RSS.
func (client *Client) EvidenceBytes() int64 {
	if client == nil || client.owner == nil {
		return 0
	}
	return client.owner.settings.evidenceBytes()
}
func (client *Client) valid(ctx context.Context) error {
	if client == nil || client.owner == nil || client.access == nil || ctx == nil {
		return failure(ErrInput, "call")
	}
	if err := ctx.Err(); err != nil {
		return failure(ErrState, "call", err, context.Cause(ctx))
	}
	return nil
}
func (client *Client) begin(ctx context.Context, id fault.Correlation, shape invocation.Shape, route routeChoice) (*operation, error) {
	value := client.owner.settings
	call, err := invocation.Begin(ctx, client.access, invocation.Request{Name: "request", Correlation: id, Shape: shape, Bytes: value.reservation(), EvidenceBytes: value.evidenceBytes(), Admission: invocation.Budget{Limit: value.AdmissionTimeout}}, client.inbox, client.observer)
	if err != nil {
		return nil, err
	}
	op := &operation{client: client, call: call, callbacks: newActivity(), done: make(chan struct{}), route: route, data: &resultData{proxyMode: route.mode()}}
	work, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(ctx, invocation.Lifetime)
	if err != nil {
		op.ctx = ctx
		op.cancel = func() {}
		op.primary = err
		op.finish()
		return op, err
	}
	op.ctx = context.WithValue(work, callKey{}, op.callbacks)
	op.cancel = cancel
	return op, nil
}

type callKey struct{}
type activity struct {
	mu            sync.Mutex
	closed        bool
	active, limit int
	done          chan struct{}
}

func newActivity() *activity { return &activity{done: make(chan struct{})} }
func (work *activity) enter() bool {
	work.mu.Lock()
	defer work.mu.Unlock()
	if work.closed || work.limit > 0 && work.active >= work.limit {
		return false
	}
	work.active++
	return true
}
func (work *activity) leave() {
	work.mu.Lock()
	defer work.mu.Unlock()
	work.active--
	if work.closed && work.active == 0 {
		close(work.done)
	}
}
func (work *activity) stop() <-chan struct{} {
	work.mu.Lock()
	defer work.mu.Unlock()
	if !work.closed {
		work.closed = true
		if work.active == 0 {
			close(work.done)
		}
	}
	return work.done
}
func (own *owner) enterCallback(ctx context.Context) (func(), error) {
	if !own.callbacks.enter() {
		return nil, failure(ErrState, "native-callback")
	}
	call, _ := ctx.Value(callKey{}).(*activity)
	if call != nil && !call.enter() {
		own.callbacks.leave()
		return nil, failure(ErrState, "native-call-ended")
	}
	return func() {
		if call != nil {
			call.leave()
		}
		own.callbacks.leave()
	}, nil
}
