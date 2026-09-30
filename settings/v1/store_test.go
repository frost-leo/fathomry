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

package settings

import (
	"errors"
	"sync"
	"testing"
)

func TestStore(t *testing.T) {
	t.Run("publication_and_old_views", func(t *testing.T) {
		store := NewStore[int]()
		reader := store.Reader()
		if _, err := reader.Capture(); !errors.Is(err, ErrUnconfigured) {
			t.Fatal("empty store captured")
		}
		first := mustSnapshot(t, 1, identity[int])
		if err := store.Publish(first); err != nil {
			t.Fatal(err)
		}
		old, _ := reader.Capture()
		copyOfStore := store
		if err := copyOfStore.Publish(mustSnapshot(t, 2, identity[int])); err != nil {
			t.Fatal(err)
		}
		if err := store.Publish(Snapshot[int]{}); !errors.Is(err, ErrSnapshot) {
			t.Fatal("invalid publication accepted")
		}
		current, _ := reader.Capture()
		before, _, _ := Read(old, "", identity[int])
		after, _, _ := Read(current, "", identity[int])
		if before != 1 || after != 2 {
			t.Fatal("old view changed or invalid write replaced last value")
		}
		other := NewStore[string]()
		if err := other.Publish(mustSnapshot(t, "business", identity[string])); err != nil {
			t.Fatal(err)
		}
		current, _ = reader.Capture()
		after, _, _ = Read(current, "", identity[int])
		if after != 2 {
			t.Fatal("independent domains mixed")
		}
	})
	t.Run("zero_handles", func(t *testing.T) {
		var store Store[int]
		if err := store.Publish(mustSnapshot(t, 1, identity[int])); !errors.Is(err, ErrStore) {
			t.Fatal("zero write handle accepted")
		}
		if _, err := store.Reader().Capture(); !errors.Is(err, ErrUnconfigured) {
			t.Fatal("zero reader captured")
		}
	})
	t.Run("concurrent_complete_captures", func(t *testing.T) {
		type pair struct {
			Left  int `json:"left"`
			Right int `json:"right"`
		}
		store := NewStore[pair]()
		if err := store.Publish(mustSnapshot(t, pair{}, identity[pair])); err != nil {
			t.Fatal(err)
		}
		reader := store.Reader()
		old, _ := reader.Capture()
		var group sync.WaitGroup
		for worker := 0; worker < 8; worker++ {
			group.Go(func() {
				for number := 1; number <= 1000; number++ {
					snapshot, err := New(pair{number, number}, identity[pair])
					if err != nil {
						t.Error(err)
						return
					}
					if err := store.Publish(snapshot); err != nil {
						t.Error(err)
						return
					}
					view, err := reader.Capture()
					if err != nil {
						t.Error(err)
						return
					}
					left, _, leftErr := Read(view, "/left", identity[int])
					right, _, rightErr := Read(view, "/right", identity[int])
					if leftErr != nil || rightErr != nil || left != right {
						t.Error("torn capture")
						return
					}
				}
			})
		}
		group.Wait()
		value, _, _ := Read(old, "", identity[pair])
		if value != (pair{}) {
			t.Fatal("old publication changed")
		}
	})
}
