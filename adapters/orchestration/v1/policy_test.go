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

package orchestration_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/frost-leo/fathomry/adapters/orchestration/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestPolicyPreservesSeparateValueOnlyEnvelopes(t *testing.T) {
	value := orchestration.Policy{
		Budget:   orchestration.Budget{WorkBytes: 11, WorkerBytes: 13, EvidenceBytes: 17},
		Runtime:  adapters.Options{Name: "runtime", MaxActive: 2, MaxQueued: 3, MaxWorkBytes: 19, MaxQueuedBytes: 23, MaxTasks: 5, MaxDepth: 7, MaxHolds: 11},
		Evidence: adapters.EvidenceOptions{Capacity: 5, MaxBytes: 29}, Workers: adapters.EvidenceOptions{Capacity: 7, MaxBytes: 31},
		Tasks: adapters.EvidenceOptions{Capacity: 11, MaxBytes: 37}, SourceWorkBytes: 41, SourceEvidenceBytes: 43,
		NativeWorkBytes: 47, NativeWorkerBytes: 53,
	}
	copy := value
	copy.Budget.WorkerBytes = 101
	copy.Runtime.MaxActive = 103
	copy.Workers.Capacity = 107
	copy.Tasks.MaxBytes = 109
	copy.SourceWorkBytes, copy.NativeWorkerBytes = 113, 127
	if value.Budget.WorkerBytes != 13 || value.Runtime.MaxActive != 2 || value.Workers.Capacity != 7 || value.Tasks.MaxBytes != 37 || value.SourceWorkBytes != 41 || value.NativeWorkerBytes != 53 {
		t.Fatal("copying category accounting retained mutable state")
	}
	encoded, err := json.Marshal(value.Budget)
	if err != nil || string(encoded) != `{"work_bytes":11,"worker_bytes":13,"evidence_bytes":17}` {
		t.Fatal("category budget changed its data-only wire names", err)
	}
	var decoded orchestration.Budget
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != value.Budget {
		t.Fatal("data-only budget did not round trip", err)
	}
	if (orchestration.Policy{}).Budget != (orchestration.Budget{}) {
		t.Fatal("zero policy acquired implicit accounting")
	}
	typeOf := reflect.TypeFor[orchestration.Policy]()
	want := []string{"Budget", "Runtime", "Evidence", "Workers", "Tasks", "SourceWorkBytes", "SourceEvidenceBytes", "NativeWorkBytes", "NativeWorkerBytes"}
	if typeOf.PkgPath() != "github.com/frost-leo/fathomry/adapters/orchestration/v1" || typeOf.NumField() != len(want) {
		t.Fatal("policy is not owned by the category or contains hidden runtime state")
	}
	for index, name := range want {
		if typeOf.Field(index).Name != name || !typeOf.Field(index).IsExported() {
			t.Fatal("category policy changed its exported accounting contract")
		}
	}
}
