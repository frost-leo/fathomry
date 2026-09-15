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

package nethttp

import (
	"context"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Source is an opaque, non-owning composition token. Bind grants controlled calls.
type Source struct {
	private
	owner *owner
}

// Client is concurrent and non-owning. All aliases share the original resource,
// native connection capacity and configuration. It has no native-client escape.
type Client struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Select validates configuration without network I/O. Native containers and TLS
// data are copied; callbacks, Jar, keys, randomness and session caches are borrowed
// until assembly release. Explicitly supplying the same object permits sharing it.
// Apply resource.WithLimits before assembly; LimitsV1 is the no-layer convenience.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	native, err := copyNative(options.Native)
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	version := options.Version
	if version == 0 {
		version = 1
	}
	schema := resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: func(value settings) error {
		if err := validate(value); err != nil {
			return err
		}
		if native.TLS != nil && (value.RootCAPEM != "" || value.ServerName != "" || value.ClientCertPEM != "" || value.ClientKeyPEM != "") ||
			native.Proxy != nil && value.ProxyURL != "" {
			return failure(ErrInput, "native-options")
		}
		return nil
	}}
	prepared, err := resource.Prepare(schema, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		if err := ctx.Err(); err != nil {
			return resource.Resource[Source]{}, failure(ErrState, "construct", err, context.Cause(ctx))
		}
		instance, err := newOwner(value, native)
		if err != nil {
			return resource.Resource[Source]{}, err
		}
		return resource.Resource[Source]{Acquired: true, Capability: Source{owner: instance}, Release: instance.release}, nil
	}), nil
}

// LimitsV1 validates the unoverridden Go options and returns a recommended local
// admission policy. Layered composition must supply limits matching effective
// settings; Bind checks them. This does not create a quota, client or connection.
func LimitsV1(options OptionsV1) (resource.Limits, error) {
	if options.Version != 0 && options.Version != 1 {
		return resource.Limits{}, failure(ErrInput, "version")
	}
	value := defaults(options)
	if err := validate(value); err != nil {
		return resource.Limits{}, err
	}
	return value.limits(), nil
}

// Bind joins the exact source selection, authoritative admission and independent
// evidence receiver. It performs no I/O and grants no shutdown authority.
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
	if limits.Active > value.MaxActive || limits.Queued > value.QueuedCalls || limits.Bytes < value.reservation() ||
		limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}

// EvidenceBytes is the declared retained-result envelope for one accepted call,
// not an RSS measurement or a bound on arbitrary borrowed native error graphs.
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

func (client *Client) begin(ctx context.Context, id fault.Correlation, shape invocation.Shape, parent *operation, proxy proxyChoice) (*operation, error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	value := client.owner.settings
	request := invocation.Request{Name: "request", Correlation: id, Shape: shape,
		Bytes: value.reservation(), EvidenceBytes: value.evidenceBytes(),
		Admission: invocation.Budget{Limit: value.AdmissionTimeout}}
	if shape == invocation.Session {
		request.Name = "connect"
	}
	var call *invocation.Call[Result]
	var err error
	if parent == nil {
		call, err = invocation.Begin(ctx, client.access, request, client.inbox, client.observer)
	} else {
		request.Bytes = 0
		call, err = invocation.BeginNested(ctx, parent.call.Scope(), request, client.inbox, client.observer)
	}
	if err != nil {
		return nil, err
	}
	work, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(ctx, invocation.Lifetime)
	op := &operation{client: client, call: call, callbacks: newActivity(), done: make(chan struct{}), proxy: proxy, data: &resultData{proxyMode: proxy.mode()}}
	if err != nil {
		op.ctx, op.cancel = ctx, func() {}
		op.primary = err
		op.finish()
		return op, err
	}
	op.ctx = context.WithValue(work, operationKey{}, op)
	op.cancel = cancel
	return op, nil
}

// activity fences native callbacks before releasing their captured dependencies.
// It is a single lifetime, not another admission policy or a reusable worker pool.
type activity struct {
	mu     sync.Mutex
	closed bool
	active int
	limit  int
	done   chan struct{}
}

func newActivity() *activity { return &activity{done: make(chan struct{})} }
func (work *activity) enter() bool {
	return work.enterBounded(true)
}

func (work *activity) enterPrompt() bool {
	return work.enterBounded(false)
}

func (work *activity) enterBounded(bounded bool) bool {
	work.mu.Lock()
	defer work.mu.Unlock()
	if work.closed || bounded && work.limit > 0 && work.active >= work.limit {
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
