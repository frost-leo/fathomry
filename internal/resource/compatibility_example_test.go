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

package resource_test

import (
	"context"
	"fmt"
	"strconv"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/resource"
)

func Example_compatibility() {
	type settings struct {
		Buffer int `json:"buffer"`
	}
	type instance struct {
		buffer  int
		profile compatibility.Profile
	}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: settings{Buffer: 256}},
		resource.Input{Identity: resource.Identity{Provider: "example.local", Name: "output"}, Format: 1})
	if err != nil {
		panic(err)
	}

	// The same resolved factory settings drive construction AND its safe profile.
	// This is a local example, not a selected service Provider.
	// This assembly path belongs to the framework, not a business module.
	selected := resource.WithLimits(resource.Select(prepared, func(_ context.Context, effective settings) (resource.Resource[instance], error) {
		absent := compatibility.Fact{Kind: compatibility.NotApplicable}
		profile := compatibility.Profile{ImplementationModule: compatibility.FrameworkModule,
			SDKMode: "local", ServiceMode: absent, ServiceVersion: absent, Protocol: absent, Native: absent,
			Options: []compatibility.Option{{Name: "buffer-bytes", Value: strconv.Itoa(effective.Buffer)}}}
		return resource.Resource[instance]{Acquired: true, Capability: instance{buffer: effective.Buffer, profile: profile}, Release: func(context.Context) resource.ReleaseResult {
			return resource.ReleaseResult{Quiescent: true, Released: true}
		}}, nil
	}), resource.Limits{Active: 1, Bytes: 256, MaxLeases: 1})
	ctx := context.Background()
	assembly, err := resource.Assemble(ctx, ctx, "application", selected)
	if err != nil {
		panic(err)
	}
	defer assembly.Close(ctx)
	bound, _, err := resource.Bind(assembly, selected)
	if err != nil {
		panic(err)
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		panic(err)
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"go.yaml.in/yaml/v3"}})
	if err != nil {
		panic(err)
	}
	report, err := compatibility.Assess(build, access, bound.profile,
		[]compatibility.Requirement{{Guarantee: "bounded-local-output", Layers: []compatibility.Layer{compatibility.Mechanism, compatibility.Capability}}}, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("source:", report.Source.Configuration.Identity.Name)
	fmt.Println("buffer bytes:", bound.buffer)
	fmt.Println("allowed without evidence:", report.Require(compatibility.Policy{}) == nil)
	// Output:
	// source: output
	// buffer bytes: 256
	// allowed without evidence: false
}
