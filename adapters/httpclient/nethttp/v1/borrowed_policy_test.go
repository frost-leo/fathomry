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

package nethttp

import (
	"context"
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestUsingRejectsInsufficientBorrowedFamilyBeforeConstruction(t *testing.T) {
	prepared, err := Prepare(Settings{Name: "borrowed"}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	policy, _ := prepared.Policy()
	scope, err := resource.New(testContext(t), resource.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close(context.Background())
	builds := 0
	ref, err := resource.Bind(scope, resource.Binding[int, Handle]{Name: "borrowed", Policy: resource.Fixed,
		Select: func(settings.View) (int, error) { return 1, nil }, Clone: func(v int) int { return v },
		Build: func(context.Context, int) (*resource.Instance[Handle], error) {
			builds++
			return nil, errors.New("not expected")
		}})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"work", "tasks", "depth", "holds", "records", "bytes"} {
		t.Run(field, func(t *testing.T) {
			options := policy.Runtime
			evidence := policy.Evidence
			options.MaxActive = 1
			options.MaxWorkBytes = policy.Budget.WorkBytes
			evidence.Capacity = 2
			evidence.MaxBytes = 2 * policy.Budget.EvidenceBytes
			switch field {
			case "work":
				options.MaxWorkBytes--
			case "tasks":
				options.MaxTasks = 1
			case "depth":
				options.MaxDepth = 1
			case "holds":
				options.MaxHolds = 1
			case "records":
				evidence.Capacity = 1
			case "bytes":
				evidence.MaxBytes--
			}
			runtime, err := adapters.New(testContext(t), options)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			inbox, err := adapters.NewInbox[Result](evidence)
			if err != nil {
				t.Fatal(err)
			}
			if client, err := Using(testContext(t), ref, policy.Budget, Dependencies{Runtime: runtime, Evidence: inbox}); client != nil || !errors.Is(err, ErrLimit) {
				t.Fatal("unusable borrowed family accepted", err)
			}
			if builds != 0 {
				t.Fatal("borrow validation constructed a source")
			}
		})
	}
}
