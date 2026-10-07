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

package tls_client

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestFathomryCallRetentionScopeAndRefusal(t *testing.T) {
	type marker struct{}
	for _, selection := range []string{"accepted", "refused", "partial", "missing"} {
		t.Run(selection, func(t *testing.T) {
			state := newCompatibilityState()
			var held, calls atomic.Int64
			cause := errors.New("synthetic retention refusal")
			state.control.RetainWork = func(ctx context.Context) (func(), error) {
				calls.Add(1)
				if ctx.Value(marker{}) != "caller" {
					t.Error("lost originating context")
				}
				switch selection {
				case "refused":
					return nil, cause
				case "missing":
					return nil, nil
				}
				held.Add(1)
				release := func() { held.Add(-1) }
				if selection == "partial" {
					return release, cause
				}
				return release, nil
			}
			source, finish, err := state.begin(context.Background())
			if err != nil || calls.Load() != 0 {
				t.Fatal("source-resident work retained an operation", err)
			}
			finish()
			if source.Err() == nil {
				t.Fatal("source work failed to cancel")
			}
			work, done, err := state.beginCall(context.WithValue(context.Background(), marker{}, "caller"))
			if selection == "accepted" {
				if err != nil || held.Load() != 1 || work.Value(marker{}) != "caller" {
					t.Fatal("call not retained", err)
				}
				state.cancel()
				<-work.Done()
				if held.Load() != 1 {
					t.Fatal("cancellation prematurely released actual work")
				}
				done()
			} else {
				if err == nil || done != nil || work != nil {
					t.Fatal("bad retention admitted work", err)
				}
				if selection != "missing" && !errors.Is(err, cause) {
					t.Fatal("retention cause lost", err)
				}
			}
			state.mu.Lock()
			active := state.active
			state.mu.Unlock()
			if held.Load() != 0 || active != 0 || calls.Load() != 1 {
				t.Fatal("refused/completed work leaked", held.Load(), active, calls.Load())
			}
		})
	}
}
