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
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/settings/v1"
)

type testConfig struct {
	Value int    `json:"value"`
	Noise string `json:"noise"`
}

func testView(value int) settings.View {
	snapshot, err := settings.New(testConfig{Value: value}, func(value testConfig) testConfig { return value })
	if err != nil {
		panic(err)
	}
	return snapshot.View()
}

func testScope(t testing.TB, options Options) *Scope {
	t.Helper()
	scope, err := New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := scope.Close(ctx); err != nil {
			t.Errorf("scope cleanup: %v", err)
		}
	})
	return scope
}

func testBinding(name string, policy Policy) Binding[int, int] {
	return Binding[int, int]{
		Name: name, Policy: policy,
		Select: func(view settings.View) (int, error) {
			snapshot, err := settings.As[testConfig](view)
			if err != nil {
				return 0, err
			}
			value, err := snapshot.ValueCopy()
			return value.Value, err
		},
		Clone: func(value int) int { return value },
		Equal: func(left, right int) bool { return left == right },
		Build: func(_ context.Context, value int) (*Instance[int], error) { return &Instance[int]{Value: value}, nil },
	}
}

func applyTest(t testing.TB, scope *Scope, value int) *Update {
	t.Helper()
	update, err := scope.Apply(context.Background(), testView(value))
	if err != nil {
		t.Fatal(err)
	}
	return update
}

func waitTest(t testing.TB, update *Update) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := update.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func readTest[T any](t testing.TB, ref Ref[T]) T {
	t.Helper()
	lease, err := ref.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	value, err := lease.Value()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func receiveTest[T any](t testing.TB, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("bounded test wait expired")
	}
	var zero T
	return zero
}

func statusTest[T any](t testing.TB, scope *Scope, ref Ref[T], ready func(Status) bool) Status {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		changed := scope.state.changes()
		status, err := ref.Inspect()
		if err != nil {
			t.Fatal(err)
		}
		if ready(status) {
			return status
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal("status did not reach expected state")
		}
	}
}

func TestBinding(t *testing.T) {
	t.Run("fixed_follow_and_unchanged_selection", func(t *testing.T) {
		scope := testScope(t, Options{})
		var fixedBuilds, followBuilds atomic.Int32
		fixedOptions := testBinding("fixed", Fixed)
		fixedOptions.Build = func(_ context.Context, value int) (*Instance[int], error) {
			fixedBuilds.Add(1)
			return &Instance[int]{Value: value}, nil
		}
		followOptions := testBinding("follow", Follow)
		followOptions.Build = func(_ context.Context, value int) (*Instance[int], error) {
			followBuilds.Add(1)
			return &Instance[int]{Value: value}, nil
		}
		fixed, err := Bind(scope, fixedOptions)
		if err != nil {
			t.Fatal(err)
		}
		follow, err := Bind(scope, followOptions)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixed.Acquire(context.Background()); !errors.Is(err, ErrUnavailable) {
			t.Fatal("initial value fabricated")
		}
		first := applyTest(t, scope, 1)
		waitTest(t, first)
		waitTest(t, applyTest(t, scope, 2))
		waitTest(t, applyTest(t, scope, 2))
		if readTest(t, fixed) != 1 || readTest(t, follow) != 2 || fixedBuilds.Load() != 1 || followBuilds.Load() != 2 {
			t.Fatal("binding policies or equality changed")
		}
		waitTest(t, first)
		statuses, err := scope.Inspect()
		if err != nil || len(statuses) != 2 || statuses[0].Name != "fixed" || statuses[1].Name != "follow" || first.Sequence() == 0 {
			t.Fatal("scope inspection or receipt identity changed")
		}
		statuses[0].Name = "mutated"
		again, _ := scope.Inspect()
		if again[0].Name != "fixed" {
			t.Fatal("inspection storage alias")
		}
	})

	t.Run("selection_failure_is_not_partial_admission", func(t *testing.T) {
		scope := testScope(t, Options{})
		first, _ := Bind(scope, testBinding("first", Follow))
		refusal := errors.New("private-selection-canary")
		options := testBinding("second", Follow)
		selectValue := options.Select
		options.Select = func(view settings.View) (int, error) {
			value, err := selectValue(view)
			if value == 2 {
				return 0, refusal
			}
			return value, err
		}
		_, err := Bind(scope, options)
		if err != nil {
			t.Fatal(err)
		}
		waitTest(t, applyTest(t, scope, 1))
		if update, err := scope.Apply(context.Background(), testView(2)); update != nil || !errors.Is(err, refusal) || !errors.Is(err, ErrSelection) {
			t.Fatal("selection refusal lost")
		}
		if readTest(t, first) != 1 {
			t.Fatal("partial target admission")
		}
	})

	t.Run("build_failure_retains_original_cause_and_last_good", func(t *testing.T) {
		scope := testScope(t, Options{})
		refusal := errors.New("private-build-canary")
		options := testBinding("instance", Follow)
		var failBuild atomic.Bool
		options.Build = func(_ context.Context, value int) (*Instance[int], error) {
			if value == 2 && failBuild.Load() {
				return nil, refusal
			}
			return &Instance[int]{Value: value}, nil
		}
		ref, _ := Bind(scope, options)
		waitTest(t, applyTest(t, scope, 1))
		failBuild.Store(true)
		failed := applyTest(t, scope, 2)
		err := failed.Wait(context.Background())
		if !errors.Is(err, refusal) || !errors.Is(err, ErrBuild) {
			t.Fatal("construction cause lost")
		}
		if errors.Is(err, ErrSuperseded) {
			t.Fatal("ordinary construction failure fabricated supersession")
		}
		if readTest(t, ref) != 1 {
			t.Fatal("last-good instance lost")
		}
		failBuild.Store(false)
		retry, err := ref.Retry(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		waitTest(t, retry)
		if readTest(t, ref) != 2 || !errors.Is(failed.Wait(context.Background()), refusal) {
			t.Fatal("retry rewrote history or failed")
		}
	})

	t.Run("superseded_constructor_and_aba", func(t *testing.T) {
		scope := testScope(t, Options{})
		started, finish := make(chan context.Context, 1), make(chan struct{})
		cleaned := make(chan int, 4)
		options := testBinding("instance", Follow)
		options.Build = func(ctx context.Context, value int) (*Instance[int], error) {
			if value == 2 {
				started <- ctx
				<-finish
			}
			return &Instance[int]{Value: value, Release: func(context.Context) ReleaseResult {
				cleaned <- value
				return ReleaseResult{Complete: true}
			}}, nil
		}
		ref, _ := Bind(scope, options)
		waitTest(t, applyTest(t, scope, 1))
		before, _ := ref.Inspect()
		second := applyTest(t, scope, 2)
		buildContext := receiveTest(t, started)
		reverted := applyTest(t, scope, 1)
		waitTest(t, reverted)
		if !errors.Is(second.Wait(context.Background()), ErrSuperseded) || buildContext.Err() == nil {
			t.Fatal("pending B not canceled")
		}
		close(finish)
		if receiveTest(t, cleaned) != 2 {
			t.Fatal("obsolete candidate was not cleaned")
		}
		after, _ := ref.Inspect()
		if after.Generation != before.Generation || readTest(t, ref) != 1 {
			t.Fatal("ABA changed the active value")
		}
	})

	t.Run("active_comparison_is_fenced", func(t *testing.T) {
		scope := testScope(t, Options{})
		started, finish := make(chan struct{}), make(chan struct{})
		comparison, resume := make(chan struct{}), make(chan struct{})
		var compareBlock atomic.Bool
		options := testBinding("instance", Follow)
		options.Equal = func(left, right int) bool {
			if left == 1 && right == 1 && compareBlock.CompareAndSwap(true, false) {
				close(comparison)
				<-resume
			}
			return left == right
		}
		options.Build = func(_ context.Context, value int) (*Instance[int], error) {
			if value == 2 {
				close(started)
				<-finish
			}
			return &Instance[int]{Value: value}, nil
		}
		ref, _ := Bind(scope, options)
		waitTest(t, applyTest(t, scope, 1))
		second := applyTest(t, scope, 2)
		receiveTest(t, started)
		compareBlock.Store(true)
		finished := make(chan *Update, 1)
		go func() {
			update, err := scope.Apply(context.Background(), testView(1))
			if err != nil {
				t.Error(err)
			}
			finished <- update
		}()
		receiveTest(t, comparison)
		close(finish)
		waitTest(t, second)
		close(resume)
		waitTest(t, receiveTest(t, finished))
		if readTest(t, ref) != 1 {
			t.Fatal("stale equality comparison reused a different active instance")
		}
	})
}
