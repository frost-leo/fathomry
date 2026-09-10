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

	"github.com/frost-leo/fathomry/internal/resource"
)

func ExampleAssemble() {
	type config struct {
		Prefix string `json:"prefix"`
	}
	prepared, err := resource.Prepare(resource.Schema[config]{
		Format: 1, Defaults: config{Prefix: "default"},
	}, resource.Input{
		Identity: resource.Identity{Provider: "example.local", Name: "greetings"},
		Format:   1,
		Layers:   []resource.Layer{{Kind: resource.Local, Content: []byte("prefix: hello")}},
	})
	if err != nil {
		panic(err)
	}

	// This local fixture demonstrates binding, not an SDK or service integration.
	selected := resource.Select(prepared, func(_ context.Context, settings config) (resource.Resource[func(string) string], error) {
		return resource.Resource[func(string) string]{
			Acquired:   true,
			Capability: func(name string) string { return settings.Prefix + " " + name },
			Release: func(context.Context) resource.ReleaseResult {
				return resource.ReleaseResult{Quiescent: true, Released: true}
			},
		}, nil
	})
	ctx := context.Background()
	assembly, err := resource.Assemble(ctx, ctx, "application", selected)
	if err != nil {
		if assembly != nil {
			fmt.Println("cleanup responsibility:", assembly.Snapshot())
		}
		panic(err)
	}
	greet, info, err := resource.Bind(assembly, selected)
	if err != nil {
		panic(err)
	}
	fmt.Println(info.Configuration.Identity.Name, greet("world"))
	if err := assembly.Close(ctx); err != nil {
		fmt.Println("shutdown evidence:", assembly.Snapshot())
		panic(err)
	}
	// Output: greetings hello world
}
