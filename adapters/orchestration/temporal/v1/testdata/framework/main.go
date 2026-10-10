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

package main

import (
	"context"
	"errors"
	"fmt"
	temporal "github.com/frost-leo/fathomry/adapters/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	"os"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("temporal framework consumer passed")
}
func run() error {
	ctx, cancel := bounded()
	defer cancel()
	first, err := newPeer()
	if err != nil {
		return err
	}
	defer first.stop()
	second, err := newPeer()
	if err != nil {
		return err
	}
	defer second.stop()
	one, err := prepare("first", first)
	if err != nil {
		return err
	}
	two, err := prepare("second", second)
	if err != nil {
		return err
	}
	policy, err := one.Policy()
	if err != nil {
		return err
	}
	// Reserve both overlapping source generations without resizing a live runtime.
	policy.Runtime.MaxActive *= 2
	policy.Runtime.MaxWorkBytes *= 2
	policy.Evidence.Capacity *= 2
	policy.Evidence.MaxBytes *= 2
	runtime, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	deps, err := dependencies(runtime.Operations(), policy)
	if err != nil {
		return err
	}
	ref, err := resource.Bind(runtime.Resources(), resource.Binding[int, temporal.Handle]{Name: "temporal", Policy: resource.Follow,
		Select: func(view settings.View) (int, error) {
			snapshot, err := settings.As[int](view)
			if err != nil {
				return 0, err
			}
			return snapshot.ValueCopy()
		},
		Clone: func(value int) int { return value }, Equal: func(a, b int) bool { return a == b },
		Build: func(lifetime context.Context, generation int) (*resource.Instance[temporal.Handle], error) {
			selected := one
			if generation == 2 {
				selected = two
			}
			owner, err := selected.Open(lifetime, deps)
			if owner == nil {
				return nil, err
			}
			return &resource.Instance[temporal.Handle]{Value: owner.Handle(), Release: owner.Release}, err
		}})
	if err != nil {
		return err
	}
	apply := func(value int) error {
		snapshot, err := settings.New(value, func(value int) int { return value })
		if err != nil {
			return err
		}
		update, err := runtime.Resources().Apply(ctx, snapshot.View())
		if err != nil {
			return err
		}
		return update.Wait(ctx)
	}
	if err := apply(1); err != nil {
		return err
	}
	binding, err := temporal.Using(ctx, ref, policy.Budget, deps)
	if err != nil {
		return err
	}
	old, err := binding.Retain(ctx)
	if err != nil {
		return err
	}
	defer old.Close(context.Background())
	peer, err := old.Borrow(ctx)
	if err != nil {
		return err
	}
	defer peer.Close(context.Background())
	if err := apply(2); err != nil {
		return err
	}
	current, err := binding.Retain(ctx)
	if err != nil {
		return err
	}
	defer current.Close(context.Background())
	if err := old.Close(ctx); err != nil {
		return err
	}
	if err := peer.SignalWorkflow(ctx, "workflow", "run", "old", nil); err != nil {
		return err
	}
	if err := current.SignalWorkflow(ctx, "workflow", "run", "new", nil); err != nil {
		return err
	}
	if first.signals.Load() != 1 || second.signals.Load() != 1 || peer.Attribution().Generation != 1 || current.Attribution().Generation != 2 {
		return errors.New("generation retargeted")
	}
	if err := peer.Close(ctx); err != nil {
		return err
	}
	if err := current.Close(ctx); err != nil {
		return err
	}
	if err := runtime.Resources().Close(ctx); err != nil {
		return err
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	for {
		status, err := deps.Evidence.Inspect()
		if err != nil {
			return err
		}
		if status.Outstanding == 0 {
			break
		}
		record, err := deps.Evidence.NextReleased(ctx)
		if err != nil {
			return err
		}
		if err := record.Ack(); err != nil {
			return err
		}
	}
	return nil
}
