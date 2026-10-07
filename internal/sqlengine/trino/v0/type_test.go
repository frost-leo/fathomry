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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestTypeMetadataSemanticConsistency(t *testing.T) {
	scalar := func(name string, parameters ...any) map[string]any {
		return map[string]any{"rawType": name, "arguments": parameters}
	}
	typed := func(value any) map[string]any { return map[string]any{"kind": "TYPE", "value": value} }
	named := func(name *string, value any) map[string]any {
		var field any
		if name != nil {
			field = map[string]any{"name": *name}
		}
		return map[string]any{"kind": "NAMED_TYPE", "value": map[string]any{"fieldName": field, "typeSignature": value}}
	}
	name := "Case \"field"
	folded := "lower"
	row := scalar("row", named(&name, scalar("bigint")), named(&folded, scalar("array", typed(scalar("decimal", long(8), long(3))))), named(nil, scalar("varchar", long(2147483647))))
	tests := []struct {
		name, text string
		sig        any
		valid      bool
	}{
		{"decimal", "decimal(38,9)", scalar("decimal", long(5), long(2)), false},
		{"scale", "decimal(5,3)", scalar("decimal", long(5), long(2)), false},
		{"timestamp", "timestamp(3)", scalar("timestamp", long(9)), false},
		{"zone", "timestamp(3) with time zone", scalar("timestamp", long(3)), false},
		{"nested", "array(bigint)", scalar("array", typed(scalar("varchar", long(10)))), false},
		{"map-key", "map(bigint,bigint)", scalar("map", typed(scalar("varchar", long(4))), typed(scalar("bigint"))), false},
		{"map-value", "map(bigint,bigint)", scalar("map", typed(scalar("bigint")), typed(scalar("integer"))), false},
		{"integer-alias", " Int ", scalar("integer"), true},
		{"double-alias", " DOUBLE precision ", scalar("double"), true},
		{"varchar-unbounded", "VARCHAR", scalar("varchar", long(2147483647)), true},
		{"varchar-bounded", "varchar", scalar("varchar", long(10)), false},
		{"decimal-default", "DECIMAL", scalar("decimal", long(38), long(0)), true},
		{"decimal-scale-default", "decimal(5)", scalar("decimal", long(5), long(0)), true},
		{"decimal-default-mismatch", "decimal", scalar("decimal", long(38), long(1)), false},
		{"char-default", "CHAR", scalar("char", long(1)), true},
		{"char-alias", "character ( 2 )", scalar("char", long(2)), true},
		{"varchar-alias", "character varying(2)", scalar("varchar", long(2)), true},
		{"temporal-default", "TiMeStamp", scalar("timestamp", long(3)), true},
		{"temporal-default-zone", "TIME with TIME zone", scalar("time with time zone", long(3)), true},
		{"temporal-without-zone", "timestamp(6) without time zone", scalar("timestamp", long(6)), true},
		{"interval", " INTERVAL day TO second ", scalar("interval day to second"), true},
		{"quoted-row", " ROW(\"Case \"\"field\" BIGINT,LOWER ARRAY(DECIMAL(8,3)), VARCHAR) ", row, true},
		{"row-name-case", " ROW(\"case \"\"field\" BIGINT,LOWER ARRAY(DECIMAL(8,3)), VARCHAR) ", row, false},
		{"row-nested-type", " ROW(\"Case \"\"field\" BIGINT,LOWER ARRAY(DECIMAL(8,2)), VARCHAR) ", row, false},
		{"row-missing-name", " ROW(BIGINT,LOWER ARRAY(DECIMAL(8,3)), VARCHAR) ", row, false},
		{"row-order", " ROW(LOWER ARRAY(DECIMAL(8,3)),\"Case \"\"field\" BIGINT, VARCHAR) ", row, false},
		{"trailing", "bigint garbage", scalar("bigint"), false},
		{"overflow", "varchar(9223372036854775808)", scalar("varchar", long(2147483647)), false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			signature, _ := json.Marshal(test.sig)
			_, options := peer(t, func(w http.ResponseWriter, r *http.Request) {
				reply(t, w, map[string]any{"id": "semantics", "columns": []any{map[string]any{"name": "value", "type": test.text, "typeSignature": json.RawMessage(signature)}}, "data": [][]any{{nil}}})
			})
			fixture := bindFixture(t, options, 1)
			receipt, err := fixture.client.Query(deadline(t), deadline(t), correlation("semantics"), Statement{SQL: "SELECT NULL"})
			result := settle(t, receipt, err)
			if test.valid {
				if result.Err() != nil || !result.Outcome.Value.Complete() {
					t.Fatalf("legitimate metadata rejected: %v", result.Err())
				}
			} else if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Complete() {
				t.Fatalf("contradiction not rejected: %v", result.Err())
			}
			columns := result.Outcome.Value.ColumnsCopy()
			if len(columns) != 1 || columns[0].Type != test.text || string(columns[0].Signature) != string(signature) {
				t.Fatal("exact received metadata erased")
			}
		})
	}
}

func TestTypeParserBounds(t *testing.T) {
	for _, depth := range []int{8, 9, 1000} {
		text := "bigint"
		raw := `{"rawType":"bigint","arguments":[]}`
		for range depth {
			text = "array(" + text + ")"
			raw = fmt.Sprintf(`{"rawType":"array","arguments":[{"kind":"TYPE","value":%s}]}`, raw)
		}
		var sig signature
		if err := json.Unmarshal([]byte(raw), &sig); err != nil {
			t.Fatal(err)
		}
		if matchingType(text, sig) != (depth == 8) {
			t.Fatalf("depth bound changed at %d", depth)
		}
	}
}

func FuzzTypeMetadata(f *testing.F) {
	f.Add("decimal(38,9)", `{"rawType":"decimal","arguments":[{"kind":"LONG","value":5},{"kind":"LONG","value":2}]}`)
	f.Add("row(\"UP\" int)", `{"rawType":"row","arguments":[{"kind":"NAMED_TYPE","value":{"fieldName":{"name":"UP"},"typeSignature":{"rawType":"integer","arguments":[]}}}]}`)
	f.Fuzz(func(t *testing.T, text, raw string) {
		if len(text) > 8192 || len(raw) > 8192 {
			t.Skip()
		}
		var sig signature
		if json.Unmarshal([]byte(raw), &sig) != nil {
			return
		}
		_ = matchingType(text, sig)
	})
}
