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

package trino

import (
	"errors"
	"strings"
	"testing"

	native "github.com/frost-leo/fathomry/internal/sqlengine/trino/v0"
	sdk "github.com/trinodb/trino-go-client/trino"
)

func TestInwardNumericStatementBudget(t *testing.T) {
	config := native.OptionsV1{MaxSQLBytes: 128, MaxParameters: 64}
	for _, count := range []int{11, 12, 13} {
		statement := Statement{SQL: "SELECT " + strings.TrimSuffix(strings.Repeat("?,", count), ","), Args: make([]any, count)}
		for index := range statement.Args {
			statement.Args[index] = Numeric("1")
		}
		converted, err := inwardStatement(statement, config)
		// Native prepare charges two SQL copies, 32 framing bytes, two
		// separators per argument, then each Numeric's original text length.
		nativeCharge := 2*len(statement.SQL) + 32 + 3*count
		if nativeCharge > config.MaxSQLBytes {
			if !errors.Is(err, ErrLimit) {
				t.Fatal("oversized native statement admitted")
			}
			continue
		}
		if err != nil || converted.SQL != statement.SQL || len(converted.Args) != count {
			t.Fatal("public conversion narrowed the native Numeric envelope", count, err)
		}
		for _, argument := range converted.Args {
			if value, ok := argument.(sdk.Numeric); !ok || string(value) != "1" {
				t.Fatal("public Numeric was converted through another representation")
			}
		}
	}
}
