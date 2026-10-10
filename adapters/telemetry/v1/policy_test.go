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

package telemetry_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/frost-leo/fathomry/adapters/telemetry/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestPolicyIsDetachedAccountingWithoutImplicitRuntime(t *testing.T) {
	value := telemetry.Policy{Budget: telemetry.Budget{WorkBytes: 11, EvidenceBytes: 13},
		Runtime:  adapters.Options{Name: "runtime", MaxActive: 2, MaxQueued: 3, MaxWorkBytes: 17, MaxQueuedBytes: 19, MaxTasks: 5, MaxDepth: 7, MaxHolds: 11},
		Evidence: adapters.EvidenceOptions{Capacity: 23, MaxBytes: 29}, SourceWorkBytes: 31, SourceEvidenceBytes: 37}
	copy := value
	copy.Budget.WorkBytes, copy.Runtime.MaxWorkBytes, copy.Evidence.MaxBytes, copy.SourceEvidenceBytes = 41, 43, 47, 53
	if value.Budget.WorkBytes != 11 || value.Runtime.MaxWorkBytes != 17 || value.Evidence.MaxBytes != 29 || value.SourceEvidenceBytes != 37 {
		t.Fatal("policy copy retained shared mutable accounting")
	}
	encoded, err := json.Marshal(value.Budget)
	if err != nil || string(encoded) != `{"work_bytes":11,"evidence_bytes":13}` {
		t.Fatal("budget changed its data-only wire names", err)
	}
	var decoded telemetry.Budget
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != value.Budget {
		t.Fatal("budget did not round trip", err)
	}
	if (telemetry.Policy{}).Budget != (telemetry.Budget{}) {
		t.Fatal("zero policy invented reservations")
	}
	typeOf := reflect.TypeFor[telemetry.Policy]()
	want := []string{"Budget", "Runtime", "Evidence", "SourceWorkBytes", "SourceEvidenceBytes"}
	if typeOf.PkgPath() != "github.com/frost-leo/fathomry/adapters/telemetry/v1" || typeOf.NumField() != len(want) {
		t.Fatal("policy is not category-owned data")
	}
	for index, name := range want {
		if typeOf.Field(index).Name != name || !typeOf.Field(index).IsExported() {
			t.Fatal("category policy acquired a hidden or changed field")
		}
	}
}
