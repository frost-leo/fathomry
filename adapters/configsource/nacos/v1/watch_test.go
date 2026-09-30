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

package nacos

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestObservation(t *testing.T) {
	if !errors.Is((Batch{}).Err(), ErrInput) || (Batch{}).FailedIndex() != -1 {
		t.Fatal("zero batch appeared usable")
	}
	fixture := newService(t)
	owner, _, _ := openService(t, fixture, fixture.settings())
	client := owner.Client()
	t.Run("raw acquired batch no refetch detached handles and gap", func(t *testing.T) {
		observation, err := client.ObserveRawKeys(context.Background(), []Key{{DataID: "main"}, {DataID: "empty"}, {DataID: "missing"}}, ObserveOptions{QueueCapacity: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer observation.Close(context.Background())
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		batch, err := observation.Next(ctx)
		if err != nil || batch.Err() != nil || batch.FailedIndex() != -1 || batch.Sequence() != 1 || batch.Gap() {
			t.Fatal("initial raw batch failed", err, batch.Err())
		}
		if fixture.queries.Load() != 3 {
			t.Fatal("public bridge refetched raw batch", fixture.queries.Load())
		}
		documents := batch.DocumentsCopy()
		if len(documents) != 3 || documents[0].Missing() || documents[1].Missing() || len(documents[1].RawCopy()) != 0 || !documents[2].Missing() {
			t.Fatal("raw presence lost")
		}
		*documents[0] = Document{}
		if string(batch.DocumentsCopy()[0].RawCopy()) != "value: initial\n" {
			t.Fatal("mutable wrapper alias")
		}
		stopped, stop := context.WithCancel(context.Background())
		stop()
		if _, err := observation.Next(stopped); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		fixture.mu.Lock()
		fixture.denied = true
		fixture.mu.Unlock()
		for {
			observation.state.mu.Lock()
			sequence, changed := observation.state.sequence, observation.state.changed
			observation.state.mu.Unlock()
			if sequence >= 4 {
				break
			}
			select {
			case <-changed:
			case <-ctx.Done():
				t.Fatal("native failures not observed")
			}
		}
		failed, err := observation.Next(ctx)
		if err != nil || failed.Err() == nil || !failed.Gap() || len(failed.DocumentsCopy()) != 0 {
			t.Fatal("failed/gapped batch looked healthy", err)
		}
		fixture.mu.Lock()
		fixture.denied = false
		fixture.values[Key{Group: "DEFAULT_GROUP", DataID: "main"}] = "value: recovered\n"
		fixture.mu.Unlock()
		for {
			recovered, err := observation.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if recovered.Err() != nil {
				continue
			}
			if string(recovered.DocumentsCopy()[0].RawCopy()) != "value: recovered\n" {
				t.Fatal("stale recovery")
			}
			break
		}
		if err := observation.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := observation.Next(ctx); !errors.Is(err, ErrClosed) {
			t.Fatal("post-close delivery", err)
		}
	})
	t.Run("default raw and invalidation forms", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		raw, err := client.ObserveRaw(ctx, ObserveOptions{})
		if err != nil {
			t.Fatal(err)
		}
		first, err := raw.Next(ctx)
		if err != nil || first.Err() != nil || len(first.DocumentsCopy()) != 1 {
			t.Fatal(err)
		}
		if err := raw.Close(ctx); err != nil {
			t.Fatal(err)
		}
		for _, explicit := range []bool{false, true} {
			var subscription *Subscription
			var err error
			if explicit {
				subscription, err = client.WatchKeys(ctx, []Key{{DataID: "empty"}})
			} else {
				subscription, err = client.Watch(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
			change, err := subscription.Next(ctx)
			if err != nil || !change.Resync() || change.Key() != (Key{}) {
				t.Fatal("initial invalidation lost", err)
			}
			if err := subscription.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := subscription.Next(ctx); !errors.Is(err, ErrClosed) {
				t.Fatal(err)
			}
		}
	})
	t.Run("invalid selection and subscription limit", func(t *testing.T) {
		if _, err := client.WatchKeys(context.Background(), nil); !errors.Is(err, ErrInput) {
			t.Fatal(err)
		}
		if _, err := client.ObserveRaw(context.Background(), ObserveOptions{QueueCapacity: 17}); !errors.Is(err, ErrInput) {
			t.Fatal(err)
		}
		if _, err := client.WatchKeys(context.Background(), []Key{{DataID: "main"}, {DataID: "main"}}); !errors.Is(err, ErrInput) {
			t.Fatal(err)
		}
		watch, err := client.Watch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Watch(context.Background()); !errors.Is(err, ErrLimit) {
			t.Fatal("native subscription limit lost", err)
		}
		if err := watch.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
func TestObservationRuntimeClose(t *testing.T) {
	fixture := newService(t)
	owner, runtime, inbox := openService(t, fixture, fixture.settings())
	observation, err := owner.Client().ObserveRaw(context.Background(), ObserveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := observation.Next(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !owner.ShutdownComplete() {
		t.Fatal("native owner survived runtime completion")
	}
	if err := observation.Close(ctx); err != nil {
		t.Fatal(err)
	}
	status, _ := inbox.Inspect()
	for range status.Queued {
		if err := inbox.DeliverOne(ctx, func(_ context.Context, value adapters.Snapshot[Evidence]) error {
			if !value.Info().Released {
				return errors.New("unreleased evidence")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}
