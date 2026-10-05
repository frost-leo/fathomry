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

package objectstore_test

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/adapters/objectstore/v1"
)

func TestSharedEvidenceOwnership(t *testing.T) {
	original := objectstore.Info{Name: "private-canary", Provenance: []objectstore.LayerInfo{{Kind: 1, Fields: []string{"field"}}}}
	copy := original.Clone()
	copy.Provenance[0].Fields[0] = "changed"
	if original.Provenance[0].Fields[0] != "field" {
		t.Fatal("shared metadata aliases")
	}
	for _, value := range []any{original, objectstore.Attribution{ID: "private-canary"}} {
		if strings.Contains(fmt.Sprintf("%v %+v %#v", value, value, value), "private-canary") {
			t.Fatal("metadata disclosure")
		}
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime metadata serialized")
		}
	}
	for _, value := range []any{(*objectstore.Info)(nil), (*objectstore.Attribution)(nil)} {
		if slog.AnyValue(value).Resolve().String() != "objectstore[restricted]" {
			t.Fatal("nil metadata logging")
		}
	}
	if objectstore.NotSubmitted == objectstore.Unknown || objectstore.Unknown == objectstore.Acknowledged {
		t.Fatal("effect states collapsed")
	}
}
