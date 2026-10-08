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
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestUsingRejectsLargerGenerationBeforeNativeDispatch(t *testing.T) {
	var dials atomic.Int64
	native := testNative()
	native.DialContext = func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("unexpected borrowed-generation dial")
	}
	small, err := Prepare(testSettings("small"), native)
	if err != nil {
		t.Fatal(err)
	}
	larger := testSettings("large")
	larger.MaxResponseBytes = pointer[int64](128 << 10)
	large, err := Prepare(larger, native)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := testMechanisms(t, small, large)
	policy, _ := small.Policy()
	scope, err := resource.New(testContext(t), resource.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scope.Close(testContext(t)); err != nil {
			t.Error(err)
		}
	})
	ref, err := resource.Bind(scope, resource.Binding[int, Handle]{
		Name: "larger", Policy: resource.Fixed,
		Select: func(settings.View) (int, error) { return 1, nil },
		Clone:  func(value int) int { return value },
		Build: func(ctx context.Context, _ int) (*resource.Instance[Handle], error) {
			owner, err := large.Open(ctx, dependencies)
			if owner == nil {
				return nil, err
			}
			return &resource.Instance[Handle]{Value: owner.Handle(), Release: owner.Release}, err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := Using(testContext(t), ref, policy.Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := settings.New(1, func(value int) int { return value })
	if err != nil {
		t.Fatal(err)
	}
	update, err := scope.Apply(testContext(t), snapshot.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(testContext(t)); err != nil {
		t.Fatal(err)
	}
	body := new(untouchedBody)
	receipt, err := client.Do(testContext(t), nativeRequest(t, "POST", "http://127.0.0.1:1", body))
	value, err := publicResult(t, dependencies, receipt, err)
	if !errors.Is(err, ErrLimit) || value.HasData() || dials.Load() != 0 || body.reads.Load() != 0 || body.closes.Load() != 0 {
		t.Fatal("oversized adopted source escaped frozen borrower budget", err)
	}
	if err := scope.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	for {
		status, _ := dependencies.Evidence.Inspect()
		if status.Outstanding == 0 {
			break
		}
		delivery, err := dependencies.Evidence.NextReleased(testContext(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
