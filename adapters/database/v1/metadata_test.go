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

package database_test

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/adapters/database/v1"
)

func TestMetadataOwnership(t *testing.T) {
	original := database.Info{Scope: "scope-canary", Name: "source-canary", Provenance: []database.LayerInfo{{Kind: 1, Fields: []string{"password"}}}}
	snapshot := original.Clone()
	snapshot.Provenance[0].Fields[0] = "changed"
	snapshot.Provenance = append(snapshot.Provenance, database.LayerInfo{})
	if original.Provenance[0].Fields[0] != "password" || len(original.Provenance) != 1 {
		t.Fatal("provenance clone aliases its input")
	}
	for _, value := range []any{original, database.Attribution{ID: "source-canary"}, database.Profile{Options: []database.Option{{Name: "source-canary"}}}, database.Fact{Value: "source-canary"}} {
		if strings.Contains(fmt.Sprintf("%v %+v %#v", value, value, value), "canary") {
			t.Fatal("implicit metadata disclosure")
		}
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("process-local observation became a durable DTO")
		}
	}
	for _, value := range []any{(*database.Info)(nil), (*database.Attribution)(nil), (*database.Fact)(nil), (*database.Profile)(nil)} {
		if logged := slog.AnyValue(value).Resolve(); logged.Kind() != slog.KindString || logged.String() != "database[restricted]" {
			t.Fatal("nil metadata emitted a panic diagnostic")
		}
	}
}
