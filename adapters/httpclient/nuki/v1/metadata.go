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
	"slices"

	"github.com/frost-leo/fathomry/adapters/httpclient/v1"
	"github.com/frost-leo/fathomry/internal/compatibility"
)

// Fact distinguishes unknown, declared, observed and not-applicable facts.
type Fact struct {
	private
	Kind, Value string
}

// Option is an effective non-secret choice, not qualification evidence.
type Option struct{ Name, Value string }

// Profile is detached local selection, not readiness or deployment certification.
type Profile struct {
	private
	ImplementationModule, SDKMode                 string
	ServiceMode, ServiceVersion, Protocol, Native Fact
	Options                                       []Option
}

func (value Profile) Clone() Profile     { value.Options = slices.Clone(value.Options); return value }
func fact(value compatibility.Fact) Fact { return Fact{Kind: string(value.Kind), Value: value.Value} }

// Info returns preparation identity, not readiness or a resource generation.
func (owner *Owner) Info() httpclient.Info { return owner.Handle().Info() }
func (handle Handle) Info() httpclient.Info {
	if handle.state == nil {
		return httpclient.Info{}
	}
	return info(handle.state.info)
}

// Profile inspects the actual current source without native dispatch.
func (client *Client) Profile(ctx context.Context) (Profile, error) {
	if client == nil || client.lifetime == nil || ctx == nil {
		return Profile{}, fail(ErrInput, "profile")
	}
	if ctx.Err() != nil {
		return Profile{}, fail(ErrState, "profile", ctx.Err(), context.Cause(ctx))
	}
	if client.lifetime.Err() != nil {
		return Profile{}, fail(ErrState, "profile", client.lifetime.Err(), context.Cause(client.lifetime))
	}
	handle := client.direct
	if client.source != nil {
		lease, err := client.source.Acquire(ctx)
		if err != nil {
			return Profile{}, err
		}
		defer lease.Release()
		handle, err = lease.Value()
		if err != nil {
			return Profile{}, err
		}
	}
	if handle.state == nil || handle.state.inspection == nil || handle.state.call.Context().Err() != nil {
		return Profile{}, fail(ErrState, "profile")
	}
	value := handle.state.inspection.Profile()
	result := Profile{ImplementationModule: value.ImplementationModule, SDKMode: value.SDKMode,
		ServiceMode: fact(value.ServiceMode), ServiceVersion: fact(value.ServiceVersion), Protocol: fact(value.Protocol), Native: fact(value.Native)}
	for _, option := range value.Options {
		result.Options = append(result.Options, Option{Name: option.Name, Value: option.Value})
	}
	return result, nil
}
