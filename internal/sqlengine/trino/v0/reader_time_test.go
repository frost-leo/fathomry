/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package trino

import (
	"strings"
	"testing"
	"time"

	sdk "github.com/trinodb/trino-go-client/trino"
)

func TestReviewTimeLocationBackingIsDetached(t *testing.T) {
	location := time.FixedZone(strings.Repeat("private_zone_", 1<<16), 8*3600)
	original := time.Date(2026, 10, 6, 1, 2, 3, 123456789, location)
	statement := Statement{SQL: "SELECT ?", Args: []any{original}}
	if _, err := prepare(statement, defaults(OptionsV1{MaxSQLBytes: 512}), true); err != nil {
		t.Fatal("native supported time rejected", err)
	}
	copied := cloneStatement(statement).Args[0].(time.Time)
	oldSQL, err := sdk.Serial(original)
	if err != nil {
		t.Fatal(err)
	}
	newSQL, err := sdk.Serial(copied)
	if err != nil || oldSQL != newSQL {
		t.Fatal("exact temporal serialization changed")
	}
	name, _ := copied.Zone()
	if copied.Location() == original.Location() || len(name) > 128 {
		t.Fatalf("bounded asynchronous copy retained caller time.Location: zone bytes=%d", len(name))
	}
}
