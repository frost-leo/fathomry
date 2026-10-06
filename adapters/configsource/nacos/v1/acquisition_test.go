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
	"testing"
	"time"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
)

func TestSource(t *testing.T) {
	fixture := newService(t)
	options := fixture.settings()
	options.ReconcileInterval = 5 * time.Minute
	owner, _, _ := openService(t, fixture, options)
	for _, explicit := range []bool{false, true} {
		var keys []Key
		if explicit {
			keys = []Key{{DataID: "main"}, {DataID: "empty"}, {DataID: "missing"}}
		}
		source, err := owner.Client().Source(ObserveOptions{}, keys...)
		if err != nil {
			t.Fatal(err)
		}
		var selected configsource.Source = source
		batch, failed, err := selected.Capture(context.Background())
		if err != nil || failed != -1 || !batch.Valid() {
			t.Fatal(err)
		}
		expected := 1
		if explicit {
			expected = 3
		}
		if batch.Len() != expected {
			t.Fatal("wrong selected count")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		before := fixture.queries.Load()
		observer, err := selected.Observe(ctx)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		observed, err := observer.Next(ctx)
		if err != nil || observed.Err != nil || observed.Batch.Len() != expected {
			cancel()
			t.Fatal(err, observed.Err)
		}
		if fixture.queries.Load() != before+int32(expected) {
			cancel()
			t.Fatal("already acquired source was refetched")
		}
		if err := observer.Close(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
	}
}
