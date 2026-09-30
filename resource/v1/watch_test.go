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
	"io"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/settings/v1"
)

func TestWatch(t *testing.T) {
	t.Run("coherent_updates_fixed_follow_and_eof", func(t *testing.T) {
		scope := testScope(t, Options{})
		fixed, _ := Bind(scope, testBinding("fixed", Fixed))
		follow, _ := Bind(scope, testBinding("follow", Follow))
		input := make(chan settings.View, 3)
		watch, err := scope.Watch(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := scope.Watch(context.Background(), input); !errors.Is(err, ErrLimit) {
			t.Fatal("duplicate receiver admitted")
		}
		if _, err := Bind(scope, testBinding("late", Fixed)); !errors.Is(err, ErrSealed) {
			t.Fatal("Watch did not seal declarations")
		}
		for _, value := range []int{1, 2, 3} {
			input <- testView(value)
		}
		close(input)
		receiveTest(t, watch.state.done)
		observation, err := watch.Next(context.Background())
		if err != nil || observation.Err != nil || observation.Sequence != 3 || observation.Skipped != 2 {
			t.Fatal("coalescing evidence lost", err)
		}
		waitTest(t, observation.Update)
		if readTest(t, fixed) != 1 || readTest(t, follow) != 3 {
			t.Fatal("Watch changed binding policy")
		}
		if _, err := watch.Next(context.Background()); !errors.Is(err, io.EOF) || !errors.Is(err, ErrClosed) {
			t.Fatal("EOF evidence lost")
		}
		if err := watch.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		next := make(chan settings.View)
		replacement, err := scope.Watch(context.Background(), next)
		if err != nil {
			t.Fatal("completed watcher retained active slot", err)
		}
		if err := replacement.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("selection_failure_and_wait_cancel_do_not_close_scope", func(t *testing.T) {
		scope := testScope(t, Options{})
		ref, _ := Bind(scope, testBinding("instance", Follow))
		input := make(chan settings.View)
		watch, err := scope.Watch(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		wait, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if _, err := watch.Next(wait); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("Next cancellation lost")
		}
		input <- settings.View{}
		observation, err := watch.Next(context.Background())
		if err != nil || !errors.Is(observation.Err, ErrSelection) {
			t.Fatal("selection failure lost")
		}
		input <- testView(2)
		observation, err = watch.Next(context.Background())
		if err != nil || observation.Err != nil {
			t.Fatal(err)
		}
		waitTest(t, observation.Update)
		if err := watch.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if readTest(t, ref) != 2 {
			t.Fatal("closing receiver destroyed instances")
		}
	})

	t.Run("scope_shutdown_joins_watcher_and_selector", func(t *testing.T) {
		scope, _ := New(context.Background(), Options{})
		entered, release := make(chan struct{}), make(chan struct{})
		options := testBinding("instance", Follow)
		options.Select = func(settings.View) (int, error) { close(entered); <-release; return 1, nil }
		_, _ = Bind(scope, options)
		input := make(chan settings.View, 1)
		input <- testView(1)
		watch, err := scope.Watch(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		receiveTest(t, entered)
		wait, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if !errors.Is(scope.Close(wait), ErrWait) {
			t.Fatal("blocked watcher was abandoned")
		}
		close(release)
		if err := scope.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := watch.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := watch.Next(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatal("watch remained active")
		}
	})
}
