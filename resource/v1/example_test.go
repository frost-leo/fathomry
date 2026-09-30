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

	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func ExampleScope_Apply() {
	type configuration struct{ Value int }
	ctx := context.Background()
	scope, err := resource.New(ctx, resource.Options{})
	if err != nil {
		panic(err)
	}
	defer scope.Close(ctx)
	ref, err := resource.Bind(scope, resource.Binding[int, int]{
		Name: "example", Policy: resource.Follow,
		Select: func(view settings.View) (int, error) {
			snapshot, err := settings.As[configuration](view)
			if err != nil {
				return 0, err
			}
			value, err := snapshot.ValueCopy()
			return value.Value, err
		},
		Clone: func(value int) int { return value },
		Equal: func(left, right int) bool { return left == right },
		Build: func(_ context.Context, value int) (*resource.Instance[int], error) {
			return &resource.Instance[int]{Value: value}, nil
		},
	})
	if err != nil {
		panic(err)
	}
	apply := func(value int) {
		snapshot, err := settings.New(configuration{Value: value}, func(value configuration) configuration { return value })
		if err != nil {
			panic(err)
		}
		update, err := scope.Apply(ctx, snapshot.View())
		if err != nil {
			panic(err)
		}
		if err := update.Wait(ctx); err != nil {
			panic(err)
		}
	}
	apply(3)
	old, err := ref.Acquire(ctx)
	if err != nil {
		panic(err)
	}
	defer old.Release()
	apply(9)
	current, err := ref.Acquire(ctx)
	if err != nil {
		panic(err)
	}
	defer current.Release()
	before, _ := old.Value()
	after, _ := current.Value()
	fmt.Println(before, after)
	// Output: 3 9
}
