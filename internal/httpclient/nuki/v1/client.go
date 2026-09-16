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

package nuki

import (
	"context"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Source is an opaque composition token. It conveys no native access or Close.
type Source struct {
	private
	owner *owner
}

// Client is a concurrent, non-owning capability. Every alias and runtime route
// shares the original source, authoritative admission and native socket ceiling.
type Client struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Select prepares and freezes explicit settings without network I/O or executing
// native profile factories. Native object borrowing is declared by NativeOptionsV1.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	native, err := copyNative(options.Native)
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	version := options.Version
	if version == 0 {
		version = 1
	}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options),
		Validate: func(value settings) error {
			if err := validate(value); err != nil {
				return err
			}
			return validateNative(value, native)
		}},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
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

// LimitsV1 returns bounds for Go options without layers. Layered composition must
// supply limits for the actual effective settings; Bind refuses looser allowances.
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

// Bind grants controlled operations over exactly the selected authoritative
// resource and a separately bounded required-evidence inbox. It performs no I/O.
func Bind(assembly *resource.Assembly, selection resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Client, error) {
	source, _, err := resource.Bind(assembly, selection)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selection)
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

// EvidenceBytes is the retained per-call envelope, not an RSS or native-error-graph measurement.
func (client *Client) EvidenceBytes() int64 {
	if client == nil || client.owner == nil {
		return 0
	}
	return client.owner.settings.evidenceBytes()
}
