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

package adapters

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestOwnership(t *testing.T) {
	t.Run("unavailable_source_is_accepted_failure_with_evidence", func(t *testing.T) {
		owner := testRuntime(t, Options{})
		endpoint, inbox := testEndpoint(t, owner, func(value int) int { return value }, 1)
		receipt, err := Using(context.Background(), endpoint, resource.Ref[int]{}, request("unavailable"), func(*Call[int], int) { t.Error("unavailable instance dispatched") })
		if err != nil {
			t.Fatal("accepted source failure disappeared", err)
		}
		result := waitReleased(t, receipt)
		if !errors.Is(result.Err(), ErrSource) || !errors.Is(result.Primary(), resource.ErrScope) || result.Info().Source.Generation != 0 {
			t.Fatal("source failure lost native public evidence")
		}
		if value, present := result.ValueCopy(); present || value != 0 {
			t.Fatal("missing source fabricated data")
		}
		if err := inbox.DeliverOne(context.Background(), func(_ context.Context, fact Snapshot[int]) error {
			if !errors.Is(fact.Err(), ErrSource) {
				t.Error("independent source evidence lost")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	runtime := testRuntime(t, Options{})
	endpoint, _ := testEndpoint(t, runtime, func(value int) int { return value }, 4)
	owner, err := resource.New(context.Background(), resource.Options{Name: "instances"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	var closed atomic.Int32
	ref, err := resource.Bind(owner, resource.Binding[int, int]{
		Name: "numbers", Policy: resource.Follow,
		Select: func(view settings.View) (int, error) {
			snapshot, err := settings.As[int](view)
			if err != nil {
				return 0, err
			}
			return snapshot.ValueCopy()
		},
		Clone: func(value int) int { return value }, Equal: func(left, right int) bool { return left == right },
		Build: func(_ context.Context, value int) (*resource.Instance[int], error) {
			return &resource.Instance[int]{Value: value, Release: func(context.Context) resource.ReleaseResult {
				closed.Add(1)
				return resource.ReleaseResult{Complete: true}
			}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	apply := func(number int) {
		t.Helper()
		snapshot, _ := settings.New(number, func(value int) int { return value })
		update, err := owner.Apply(context.Background(), snapshot.View())
		if err != nil || update.Wait(context.Background()) != nil {
			t.Fatal("instance adoption failed")
		}
	}
	apply(1)
	var call *Call[int]
	var guard Guard
	receipt, err := Using(context.Background(), endpoint, ref, request("borrow"), func(value *Call[int], number int) {
		call = value
		guard, _ = value.Hold()
		if number != 1 {
			t.Error("wrong borrowed generation")
		}
		_ = value.Resolve(Outcome[int]{Value: number, Present: true})
	})
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Release()
	before, _ := receipt.Snapshot()
	apply(2)
	if closed.Load() != 0 {
		t.Fatal("early producer return released the old instance")
	}
	if before.Info().Source.Name != "numbers" || before.Info().Source.Generation == 0 {
		t.Fatal("actual source binding absent")
	}
	wait, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := receipt.WaitReleased(wait); !errors.Is(err, ErrWait) || call.Context().Err() != nil {
		t.Fatal("wait changed actual borrowed work")
	}
	if guard.Release() != nil {
		t.Fatal("guard release failed")
	}
	after := waitReleased(t, receipt)
	if after.Info().Source != before.Info().Source {
		t.Fatal("source attribution retargeted")
	}
	next, err := StartUsing(context.Background(), endpoint, ref, request("new"), func(call *Call[int], number int) { _ = call.Resolve(Outcome[int]{Value: number, Present: true}) })
	if err != nil {
		t.Fatal(err)
	}
	if value, present := waitReleased(t, next).ValueCopy(); !present || value != 2 {
		t.Fatal("new work did not use new generation")
	}
	if owner.Close(context.Background()) != nil || closed.Load() != 2 {
		t.Fatal("borrowed native generations were not cleaned")
	}
}
