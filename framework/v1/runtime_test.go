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

package framework

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestRuntime(t *testing.T) {
	ctx := context.Background()
	runtime, err := New(ctx, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(ctx)
	released := make(chan struct{})
	ref, err := resource.Bind(runtime.Resources(), resource.Binding[int, int]{
		Name: "instance", Select: func(settings.View) (int, error) { return 1, nil }, Clone: func(value int) int { return value },
		Build: func(context.Context, int) (*resource.Instance[int], error) {
			return &resource.Instance[int]{Value: 1, Release: func(context.Context) resource.ReleaseResult {
				close(released)
				return resource.ReleaseResult{Complete: true}
			}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := settings.New(1, func(value int) int { return value })
	update, err := runtime.Resources().Apply(ctx, snapshot.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	inbox, _ := adapters.NewInbox[int](adapters.EvidenceOptions{})
	endpoint, err := adapters.Bind(runtime.Operations(), adapters.Declaration[int]{Evidence: inbox, Copy: func(value int) int { return value }})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	finish := make(chan struct{})
	receipt, err := adapters.StartUsing(ctx, endpoint, ref, adapters.Request{Operation: "fixture.work", WorkBytes: 64, EvidenceBytes: 64}, func(call *adapters.Call[int], value int) {
		close(entered)
		<-call.Context().Done()
		<-finish
		_ = call.Resolve(adapters.Outcome[int]{Value: value, Present: true})
	})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if err := runtime.Close(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	select {
	case <-released:
		t.Fatal("resource released before actual operation")
	default:
	}
	close(finish)
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := receipt.WaitReleased(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-released:
	default:
		t.Fatal("resource not released")
	}
}
func TestRuntimeAdmissionAndCleanup(t *testing.T) {
	if _, err := New(nil, Options{}); !errors.Is(err, ErrOptions) {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), Options{Resources: resource.Options{MaxBindings: -1}}); err == nil {
		t.Fatal("invalid resource declaration")
	}
	runtime, err := New(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(nil); !errors.Is(err, ErrOptions) {
		t.Fatal(err)
	}
	marker := errors.New("native-cleanup")
	_, err = resource.Bind(runtime.Resources(), resource.Binding[int, int]{
		Name: "cleanup", Select: func(settings.View) (int, error) { return 1, nil }, Clone: func(value int) int { return value },
		Build: func(context.Context, int) (*resource.Instance[int], error) {
			return &resource.Instance[int]{Release: func(context.Context) resource.ReleaseResult {
				return resource.ReleaseResult{Complete: true, Err: marker}
			}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := settings.New(1, func(value int) int { return value })
	update, err := runtime.Resources().Apply(context.Background(), snapshot.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(context.Background()); !errors.Is(err, ErrClose) || !errors.Is(err, marker) {
		t.Fatal("cleanup evidence lost", err)
	}
	if err := new(Runtime).Close(context.Background()); !errors.Is(err, ErrHandle) {
		t.Fatal(err)
	}
}
