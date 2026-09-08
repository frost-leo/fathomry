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

package source_test

import (
	"context"
	"fmt"

	"github.com/frost-leo/fathomry/source"
)

func ExampleAssemble() {
	type config struct {
		Prefix string `json:"prefix"`
	}
	prepared, err := source.Prepare(source.Schema[config]{
		Format: 1, Defaults: config{Prefix: "default"},
	}, source.Input{
		Identity: source.Identity{Provider: "example.local", Name: "greetings"},
		Format:   1,
		Layers:   []source.Layer{{Kind: source.Local, Content: []byte("prefix: hello")}},
	})
	if err != nil {
		panic(err)
	}

	// This local fixture demonstrates binding, not an SDK or service integration.
	selected := source.Select(prepared, func(_ context.Context, settings config) (source.Resource[func(string) string], error) {
		return source.Resource[func(string) string]{
			Acquired:   true,
			Capability: func(name string) string { return settings.Prefix + " " + name },
			Release: func(context.Context) source.ReleaseResult {
				return source.ReleaseResult{Quiescent: true, Released: true}
			},
		}, nil
	})
	ctx := context.Background()
	assembly, err := source.Assemble(ctx, ctx, "application", selected)
	if err != nil {
		if assembly != nil {
			fmt.Println("cleanup responsibility:", assembly.Snapshot())
		}
		panic(err)
	}
	greet, info, err := source.Bind(assembly, selected)
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
