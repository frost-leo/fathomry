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
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/adapters/orchestration/v1"
)

func TestAttributionKeepsSourceUseAndGenerationDistinct(t *testing.T) {
	owner := orchestration.Attribution{SourceID: "physical-one", Name: "same-name", Namespace: "namespace", Generation: 3}
	borrow := owner
	borrow.UseID = 7
	replacement := owner
	replacement.SourceID, replacement.Generation = "physical-two", 5
	if owner.UseID != 0 || borrow.SourceID != owner.SourceID || borrow.UseID == owner.UseID || replacement.Name != owner.Name || replacement.SourceID == owner.SourceID || replacement.Generation == owner.Generation {
		t.Fatal("category attribution collapsed independently meaningful identities")
	}
	zero := orchestration.Attribution{}
	if zero.SourceID != "" || zero.UseID != 0 || zero.Generation != 0 {
		t.Fatal("zero attribution invented source identity")
	}
	if reflect.TypeFor[orchestration.Attribution]().PkgPath() != "github.com/frost-leo/fathomry/adapters/orchestration/v1" {
		t.Fatal("attribution is an implementation alias")
	}
}

func TestAttributionDiagnosticsRefuseDisclosureAndReconstruction(t *testing.T) {
	const canary = "orchestration-private-canary"
	value := orchestration.Attribution{SourceID: canary, Name: canary, Namespace: canary, UseID: 7, Generation: 11}
	for _, candidate := range []any{value, &value} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
			if got := fmt.Sprintf(format, candidate); got != "orchestration[restricted]" {
				t.Fatal("ordinary attribution formatting disclosed or reclassified metadata", got)
			}
		}
		if got := slog.AnyValue(candidate).Resolve().String(); got != "orchestration[restricted]" {
			t.Fatal("structured attribution formatting changed", got)
		}
		if data, err := json.Marshal(candidate); err == nil || len(data) != 0 || strings.Contains(err.Error(), canary) {
			t.Fatal("runtime attribution serialized or leaked on refusal")
		}
	}
	if got := slog.AnyValue((*orchestration.Attribution)(nil)).Resolve().String(); got != "orchestration[restricted]" {
		t.Fatal("nil attribution structured logging is not safe", got)
	}
	before := value
	if err := json.Unmarshal([]byte(`{"Name":"replacement"}`), &value); err == nil || value != before {
		t.Fatal("runtime attribution reconstructed or mutated on refusal")
	}
	if err := json.Unmarshal([]byte(`null`), &value); err == nil || value != before {
		t.Fatal("null reconstructed runtime attribution")
	}
}
