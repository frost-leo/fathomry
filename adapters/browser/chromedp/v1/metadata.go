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
	"slices"

	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
	"github.com/frost-leo/fathomry/internal/compatibility"
	resource "github.com/frost-leo/fathomry/internal/resource"
)

// Module records selected and replacement facts from the consuming executable.
type Module struct {
	private
	Path, Version, Sum                                  string
	ReplacementPath, ReplacementVersion, ReplacementSum string
	ReplacementKind                                     string
}
type BuildInfo struct {
	private
	Go      string
	Modules []Module
}

// Fact distinguishes an unknown fact from a declaration or actual observation.
type Fact struct {
	private
	Kind, Value string
}

// Profile reports detached source facts, not browser HTTP behavior or readiness.
type Profile struct {
	private
	ImplementationModule, SDKMode                 string
	ServiceMode, ServiceVersion, Protocol, Native Fact
	Options                                       []Option
}

// Option is a non-secret effective choice, not a qualification assertion.
type Option struct{ Name, Value string }

func (value Profile) Clone() Profile {
	value.Options = slices.Clone(value.Options)
	return value
}

// Profile reads the current source without launching or dispatching native work.
// With Follow it inspects the current generation, not an older retained context.
func (client *Client) Profile(ctx context.Context) (Profile, error) {
	if client == nil || client.lifetime == nil || ctx == nil {
		return Profile{}, fail(ErrInput, "profile")
	}
	if ctx.Err() != nil || client.lifetime.Err() != nil {
		return Profile{}, fail(ErrState, "profile", context.Cause(ctx), context.Cause(client.lifetime))
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
	if handle.state == nil {
		return Profile{}, fail(ErrState, "profile")
	}
	state := handle.state
	finish, err := state.use()
	if err != nil {
		return Profile{}, err
	}
	defer finish()
	capability, _, err := resource.Bind(state.assembly, state.selected)
	if err != nil {
		return Profile{}, translate(err, "profile")
	}
	value := capability.Profile()
	fact := func(value compatibility.Fact) Fact { return Fact{Kind: string(value.Kind), Value: value.Value} }
	result := Profile{ImplementationModule: value.ImplementationModule, SDKMode: value.SDKMode, ServiceMode: fact(value.ServiceMode), ServiceVersion: fact(value.ServiceVersion), Protocol: fact(value.Protocol), Native: fact(value.Native)}
	for _, option := range value.Options {
		result.Options = append(result.Options, Option{Name: option.Name, Value: option.Value})
	}
	return result, nil
}

// Build inspects the consumer. Local replacement paths do not certify distribution.
func Build() (BuildInfo, error) {
	facts, err := native.Build()
	if err != nil {
		return BuildInfo{}, translate(err, "build")
	}
	result := BuildInfo{Go: facts.Go.Value}
	for _, module := range facts.SDKs {
		item := Module{Path: module.Path.Value, Version: module.Version.Value, Sum: module.Sum.Value, ReplacementKind: string(module.Replacement.Kind)}
		if module.Replacement.Path.Value != "" {
			item.ReplacementPath = module.Replacement.Path.Value
			item.ReplacementVersion = module.Replacement.Version.Value
			item.ReplacementSum = module.Replacement.Sum.Value
		}
		result.Modules = append(result.Modules, item)
	}
	return result, nil
}
