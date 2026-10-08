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

package surf

import (
	"context"
	"sync"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Source is a non-owning composition token; it exposes no native client.
type Source struct {
	private
	owner *owner
}

// Client is a concurrent non-owning facade over authoritative resource admission.
type Client struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Select freezes layered settings and native containers without network I/O.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	prepared, err := PrepareV1(options, layers...)
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return prepared.Select(), nil
}

// PrepareV1 resolves strict data/native inputs and accounting without native effects.
func PrepareV1(options OptionsV1, layers ...resource.Layer) (Prepared, error) {
	native, err := copyNative(options.Native)
	if err != nil {
		return Prepared{}, err
	}
	version := options.Version
	if version == 0 {
		version = 1
	}
	var metadata Budget
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: func(value settings) error {
		if err := validate(value); err != nil {
			return err
		}
		if err := validateNative(value, native); err != nil {
			return err
		}
		metadata = value.budget(native)
		return nil
	}},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	if err != nil {
		return Prepared{}, err
	}
	return Prepared{configuration: prepared, native: native, metadata: metadata}, nil
}

func (prepared Prepared) Select() resource.Selection[Source] {
	return resource.Select(prepared.configuration, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		if err := ctx.Err(); err != nil {
			return resource.Resource[Source]{}, failure(ErrState, "construct", err, context.Cause(ctx))
		}
		native, err := copyNative(prepared.native)
		if err != nil {
			return resource.Resource[Source]{}, err
		}
		own := &owner{settings: value, native: native, budget: prepared.metadata, routes: make(map[routeKey]*routeBinding), work: newActivity()}
		return resource.Resource[Source]{Acquired: true, Capability: Source{owner: own}, Release: own.release}, nil
	})
}

// LimitsV1 returns bounds for unoverridden Go options; layered overrides need
// matching explicit limits. Bind rejects admission above the instance ceiling.
func LimitsV1(options OptionsV1) (resource.Limits, error) {
	if options.Version != 0 && options.Version != 1 {
		return resource.Limits{}, failure(ErrInput, "version")
	}
	options.Name = "limits"
	prepared, err := PrepareV1(options)
	if err != nil {
		return resource.Limits{}, err
	}
	return prepared.Metadata().Limits, nil
}

// Bind attaches this named resource to an independent required evidence inbox.
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
	if limits.Active > value.MaxActive || limits.Queued > value.QueuedCalls || limits.Bytes < source.owner.budget.WorkBytes ||
		limits.Queued > 0 && limits.QueuedBytes < source.owner.budget.WorkBytes {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}

// EvidenceBytes is the retained-result reservation, not a process RSS estimate.
func (client *Client) EvidenceBytes() int64 {
	if client == nil || client.owner == nil {
		return 0
	}
	return client.owner.settings.evidenceBytes()
}

type activity struct {
	mu     sync.Mutex
	closed bool
	active int
	done   chan struct{}
}

func newActivity() *activity { return &activity{done: make(chan struct{})} }
func (work *activity) enter() bool {
	work.mu.Lock()
	defer work.mu.Unlock()
	if work.closed {
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

type operationKey struct{}
