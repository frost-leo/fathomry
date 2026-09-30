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

package viper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
)

func TestSource(t *testing.T) {
	client, _, _ := testClient(t, 0)
	path := filepath.Join(t.TempDir(), "base.yaml")
	source, err := client.Source(WatchSettings{Paths: []string{path}, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var selected configsource.Source = source
	batch, index, err := selected.Capture(context.Background())
	if err != nil || index != -1 || !batch.Valid() {
		t.Fatal(err)
	}
	raw, _ := batch.DocumentsCopy()
	if !raw[0].Missing {
		t.Fatal("missing became empty")
	}
	observer, err := selected.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := observer.Next(ctx)
	if err != nil || first.Err != nil || !first.Gap || !first.Batch.Valid() {
		t.Fatal("initial capture missing", err)
	}
	if err := os.WriteFile(path, []byte("value: accepted\n"), 0600); err != nil {
		t.Fatal(err)
	}
	next, err := observer.Next(ctx)
	if err != nil || next.Err != nil {
		t.Fatal(err, next.Err)
	}
	documents, _ := next.Batch.DocumentsCopy()
	if string(documents[0].Content) != "value: accepted\n" {
		t.Fatal("invalidated file not acquired")
	}
	guard, err := observer.(*fileObserver).call.Hold()
	if err != nil {
		t.Fatal(err)
	}
	stopped, stop := context.WithCancel(context.Background())
	stop()
	if err := observer.Close(stopped); !errors.Is(err, context.Canceled) {
		t.Fatal("released retained acquisition", err)
	}
	if err := guard.Release(); err != nil {
		t.Fatal(err)
	}
	if err := observer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := observer.Next(ctx); err == nil {
		t.Fatal("observation after close")
	}
}

func TestSourceObservationRetainsItsReservation(t *testing.T) {
	client, _, inbox := testClient(t, 1)
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("value: initial"), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := client.Source(WatchSettings{Paths: []string{path}, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	observer, err := source.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close(context.Background())
	if err := inbox.Seal(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	observation, err := observer.Next(ctx)
	if err != nil || observation.Err != nil || !observation.Batch.Valid() {
		t.Fatal("accepted observation tried to reserve another operation", err, observation.Err)
	}
}
