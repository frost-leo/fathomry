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
	"testing"
	"testing/synctest"
)

func TestRetainedLifetime(t *testing.T) {
	t.Run("setup_cancel_does_not_revoke_children", func(t *testing.T) {
		runtime := testRuntime(t, Options{MaxActive: 1})
		endpoint, _ := testEndpoint(t, runtime, func(v int) int { return v }, 4)
		setup, cancel := context.WithCancel(context.Background())
		var parent *Call[int]
		var hold Guard
		receipt, err := endpoint.RunWithLifetime(setup, context.Background(), request("prepare"), func(call *Call[int]) {
			parent = call
			hold, _ = call.Hold()
		})
		if err != nil {
			t.Fatal(err)
		}
		defer hold.Release()
		cancel()
		if parent.Context().Err() != nil {
			t.Fatal("setup became retained lifetime")
		}
		childRequest := request("execute")
		childRequest.WorkBytes = 0
		child, err := endpoint.ChildWithLifetime(context.Background(), parent.Context(), parent.Scope(), childRequest, func(call *Call[int]) {
			_ = call.Resolve(Outcome[int]{Value: 2, Present: true})
		})
		if err != nil || waitReleased(t, child).Err() != nil {
			t.Fatal("retained child refused", err)
		}
		cause := errors.New("late cleanup")
		_ = parent.Resolve(Outcome[int]{Value: 1, Present: true, Cleanup: cause})
		_ = hold.Release()
		if !errors.Is(waitReleased(t, receipt).Cleanup(), cause) {
			t.Fatal("late cleanup lost")
		}
	})
	for _, cancelLifetime := range []bool{false, true} {
		name := "queued_setup_cancel"
		if cancelLifetime {
			name = "queued_lifetime_cancel"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				runtime := testRuntime(t, Options{MaxActive: 1, MaxQueued: 1})
				endpoint, inbox := testEndpoint(t, runtime, func(v int) int { return v }, 3)
				var hold Guard
				_, err := endpoint.Run(context.Background(), request("held"), func(call *Call[int]) { hold, _ = call.Hold(); _ = call.Resolve(Outcome[int]{}) })
				if err != nil {
					t.Fatal(err)
				}
				defer hold.Release()
				setup, cancelSetup := context.WithCancelCause(context.Background())
				defer cancelSetup(nil)
				lifetime, cancelOwner := context.WithCancelCause(context.Background())
				defer cancelOwner(nil)
				result := make(chan error, 1)
				entered := false
				go func() {
					_, err := endpoint.RunWithLifetime(setup, lifetime, request("queued"), func(call *Call[int]) { entered = true; _ = call.Resolve(Outcome[int]{}) })
					result <- err
				}()
				synctest.Wait()
				stats, _ := runtime.Inspect()
				if stats.Queued != 1 {
					t.Fatal("queue control missing")
				}
				cause := errors.New("explicit admission cancellation")
				if cancelLifetime {
					cancelOwner(cause)
				} else {
					cancelSetup(cause)
				}
				err = <-result
				if entered || !errors.Is(err, cause) || !errors.Is(err, ErrWait) {
					t.Fatal("canceled queue entered producer or lost cause", err)
				}
				stats, _ = runtime.Inspect()
				facts, _ := inbox.Inspect()
				if stats.Queued != 0 || stats.Active != 1 || facts.Outstanding != 1 {
					t.Fatal("queued reservation leaked", stats, facts)
				}
				_ = hold.Release()
			})
		})
	}
	t.Run("expired_parent_cannot_be_revived", func(t *testing.T) {
		runtime := testRuntime(t, Options{})
		endpoint, _ := testEndpoint(t, runtime, func(v int) int { return v }, 3)
		lifetime, cancel := context.WithCancel(context.Background())
		var parent *Call[int]
		var hold Guard
		_, err := endpoint.RunWithLifetime(context.Background(), lifetime, request("parent"), func(call *Call[int]) { parent = call; hold, _ = call.Hold() })
		if err != nil {
			t.Fatal(err)
		}
		defer hold.Release()
		cancel()
		child := request("child")
		child.WorkBytes = 0
		if _, err := endpoint.ChildWithLifetime(context.Background(), context.Background(), parent.Scope(), child, func(*Call[int]) { t.Error("revived parent") }); !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
		_ = parent.Resolve(Outcome[int]{})
		_ = hold.Release()
	})
}
