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
	"io"
	"sync"
	"testing"
)

func TestObserver(t *testing.T) {
	t.Run("phases_preserve_reported_and_missing_outcomes", func(t *testing.T) {
		for _, mode := range []string{"success", "primary", "cleanup", "missing"} {
			t.Run(mode, func(t *testing.T) {
				runtime := testRuntime(t, Options{})
				inbox, _ := NewInbox[int](EvidenceOptions{})
				observer, _ := NewObserver(3)
				endpoint, err := Bind(runtime, Declaration[int]{Copy: func(value int) int { return value }, Evidence: inbox, Observer: observer})
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := endpoint.Run(context.Background(), request(mode), func(call *Call[int]) {
					outcome := Outcome[int]{}
					switch mode {
					case "primary":
						outcome.Primary = errors.New("primary")
					case "cleanup":
						outcome.Cleanup = errors.New("cleanup")
					case "missing":
						return
					}
					_ = call.Resolve(outcome)
				})
				if err != nil {
					t.Fatal(err)
				}
				_ = waitReleased(t, receipt)
				status, _ := observer.Inspect()
				if status.Queued != 3 || status.Dropped != 0 {
					t.Fatal("operation transition omitted")
				}
				for _, phase := range []Phase{Admitted, Resolved, Released} {
					event, err := observer.Next(context.Background())
					if err != nil || event.Phase != phase || event.Failed != (phase != Admitted && mode != "success") {
						t.Fatalf("phase changed outcome evidence: %+v", event)
					}
				}
			})
		}
	})
	t.Run("loss_does_not_change_required_facts", func(t *testing.T) {
		runtime := testRuntime(t, Options{})
		inbox, _ := NewInbox[int](EvidenceOptions{})
		observer, _ := NewObserver(1)
		endpoint, err := Bind(runtime, Declaration[int]{Copy: func(value int) int { return value }, Evidence: inbox, Observer: observer})
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := endpoint.Run(context.Background(), request("observed"), func(call *Call[int]) { _ = call.Resolve(Outcome[int]{Value: 7, Present: true}) })
		if err != nil {
			t.Fatal(err)
		}
		_ = waitReleased(t, receipt)
		stats, _ := observer.Inspect()
		evidence, _ := inbox.Inspect()
		if stats.Queued != 1 || stats.Dropped != 2 || evidence.Outstanding != 1 {
			t.Fatal("diagnostic loss altered facts")
		}
		event, err := observer.Next(context.Background())
		if err != nil || event.Phase != Admitted {
			t.Fatal("bounded event lost")
		}
		_ = observer.Seal()
		if _, err := observer.Next(context.Background()); !errors.Is(err, io.EOF) {
			t.Fatal("observer EOF absent")
		}
	})
	t.Run("concurrent_publish_observe_and_seal", func(t *testing.T) {
		observer, _ := NewObserver(2)
		var workers sync.WaitGroup
		for range 4 {
			workers.Go(func() {
				for range 100 {
					observer.publish(Event{Operation: "event", Phase: Resolved})
					_, _ = observer.Inspect()
				}
			})
		}
		workers.Go(func() {
			for range 10 {
				_ = observer.Seal()
			}
		})
		workers.Wait()
		_ = observer.Seal()
		for {
			if _, err := observer.Next(context.Background()); err != nil {
				if !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
				break
			}
		}
	})
}
