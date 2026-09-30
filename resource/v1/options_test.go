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

package resource

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/settings/v1"
)

func TestOptions(t *testing.T) {
	t.Run("binding_admission", func(t *testing.T) {
		scope := testScope(t, Options{})
		for _, change := range []func(*Binding[int, int]){
			func(value *Binding[int, int]) { value.Name = "" },
			func(value *Binding[int, int]) { value.Name = "secret/path" },
			func(value *Binding[int, int]) { value.Policy = 7 },
			func(value *Binding[int, int]) { value.Select = nil },
			func(value *Binding[int, int]) { value.Clone = nil },
			func(value *Binding[int, int]) { value.Build = nil },
			func(value *Binding[int, int]) { value.Equal = nil },
			func(value *Binding[int, int]) { value.MaxBorrowers = -1 },
			func(value *Binding[int, int]) { value.MaxGenerations = 1 },
		} {
			options := testBinding("instance", Follow)
			change(&options)
			if _, err := Bind(scope, options); !errors.Is(err, ErrOptions) {
				t.Fatal("invalid binding admitted", err)
			}
		}
		options := testBinding("fixed", Fixed)
		options.Equal = nil
		if _, err := Bind(scope, options); err != nil {
			t.Fatal("fixed binding unnecessarily required equality", err)
		}
	})

	t.Run("global_construction_bound", func(t *testing.T) {
		scope := testScope(t, Options{MaxPreparing: 2})
		var running, maximum atomic.Int32
		started, finish := make(chan struct{}, 6), make(chan struct{})
		for index := range 6 {
			options := testBinding(fmt.Sprintf("instance-%d", index), Fixed)
			options.Build = func(context.Context, int) (*Instance[int], error) {
				active := running.Add(1)
				for current := maximum.Load(); active > current && !maximum.CompareAndSwap(current, active); current = maximum.Load() {
				}
				started <- struct{}{}
				<-finish
				running.Add(-1)
				return &Instance[int]{}, nil
			}
			if _, err := Bind(scope, options); err != nil {
				t.Fatal(err)
			}
		}
		update := applyTest(t, scope, 1)
		receiveTest(t, started)
		receiveTest(t, started)
		if running.Load() != 2 {
			t.Fatal("constructors did not share the bound")
		}
		close(finish)
		waitTest(t, update)
		if maximum.Load() > 2 {
			t.Fatal("constructor bound exceeded")
		}
	})

	t.Run("mutable_config_and_factory_copy_are_isolated", func(t *testing.T) {
		type config struct{ Values []int }
		input := config{Values: []int{4}}
		scope := testScope(t, Options{})
		gate := make(chan struct{})
		var constructions atomic.Int32
		ref, err := Bind(scope, Binding[config, int]{
			Name: "instance", Policy: Follow,
			Select: func(settings.View) (config, error) { return input, nil },
			Clone:  func(value config) config { return config{Values: append([]int(nil), value.Values...)} },
			Equal:  func(left, right config) bool { return left.Values[0] == right.Values[0] },
			Build: func(_ context.Context, value config) (*Instance[int], error) {
				constructions.Add(1)
				<-gate
				selected := value.Values[0]
				value.Values[0] = 999
				return &Instance[int]{Value: selected}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		update := applyTest(t, scope, 1)
		input.Values[0] = 8
		close(gate)
		waitTest(t, update)
		if readTest(t, ref) != 4 {
			t.Fatal("selected storage was borrowed")
		}
		input.Values[0] = 4
		waitTest(t, applyTest(t, scope, 2))
		if constructions.Load() != 1 {
			t.Fatal("factory mutated retained configuration")
		}
	})

	t.Run("borrow_bound_and_nil_construction_result", func(t *testing.T) {
		scope := testScope(t, Options{})
		options := testBinding("bounded", Fixed)
		options.MaxBorrowers = 1
		ref, _ := Bind(scope, options)
		waitTest(t, applyTest(t, scope, 1))
		lease, _ := ref.Acquire(context.Background())
		if _, err := ref.Acquire(context.Background()); !errors.Is(err, ErrLimit) {
			t.Fatal("borrow bound ignored")
		}
		lease.Release()

		second := testScope(t, Options{CleanupTimeout: time.Millisecond})
		options = testBinding("nil", Fixed)
		options.Build = func(context.Context, int) (*Instance[int], error) { return nil, nil }
		unavailable, _ := Bind(second, options)
		if !errors.Is(applyTest(t, second, 1).Wait(context.Background()), ErrBuild) {
			t.Fatal("nil success created a ready instance")
		}
		if _, err := unavailable.Acquire(context.Background()); !errors.Is(err, ErrUnavailable) {
			t.Fatal("nil construction published")
		}
	})
}
