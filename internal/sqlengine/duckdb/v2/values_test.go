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

package duckdb

import (
	"errors"
	"math"
	"math/big"
	"reflect"
	"testing"
	"time"

	sdk "github.com/duckdb/duckdb-go/v2"
)

func TestScalarRoundtripAndSnapshots(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	fixture.exec(t, "CREATE TABLE gh40_values (a BIGINT,b UBIGINT,c HUGEINT,d UHUGEINT,e DECIMAL(38,9),f DOUBLE,g FLOAT,h VARCHAR,i BLOB,j UUID,k TIMESTAMP,l TIMESTAMPTZ,m DATE,n TIME,o INTERVAL,p BOOLEAN)")
	huge := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
	unsignedHuge := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))
	decimalInt, _ := new(big.Int).SetString("-12345678901234567890123456789", 10)
	decimal := sdk.Decimal{Width: 38, Scale: 9, Value: decimalInt}
	instant := time.Date(2026, 9, 14, 3, 4, 5, 678000, time.UTC)
	date := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	clock := time.Date(1, 1, 1, 3, 4, 5, 678000, time.UTC)
	id := sdk.UUID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255}
	interval := sdk.Interval{Months: 2, Days: -3, Micros: 4000}
	input := [][]any{{int64(math.MinInt64), uint64(math.MaxUint64), huge, unsignedHuge, decimal, float64(1.5), float32(2.5),
		"hello 世界", []byte{0, 255, 7}, id, instant, instant, date, clock, interval, true}, make([]any, 16)}
	requireOK(t, fixture.run(t, Request{Mode: Append, Table: "gh40_values", Rows: input}))
	result := fixture.run(t, Request{Mode: Query, SQL: "SELECT a,b,c,d,e,f,g,h,i,j,k,l,m,n::VARCHAR AS n,o,p FROM gh40_values ORDER BY a NULLS LAST"})
	progress := requireOK(t, result)
	expected := append([]any(nil), input[0]...)
	expected[9] = append([]byte(nil), id[:]...)
	expected[13] = "03:04:05.000678"
	if !reflect.DeepEqual(progress.Steps[0].Rows, [][]any{expected, make([]any, 16)}) {
		t.Fatal("scalar exact/null roundtrip changed")
	}
	progress.Steps[0].Rows[0][2].(*big.Int).SetInt64(7)
	progress.Steps[0].Rows[0][4].(sdk.Decimal).Value.SetInt64(7)
	progress.Steps[0].Rows[0][8].([]byte)[0] = 7
	progress.Steps[0].Columns[0].Name = "changed"
	input[0][8].([]byte)[0] = 8
	again := result.Outcome.Value.Snapshot()
	if again.Steps[0].Rows[0][2].(*big.Int).Cmp(huge) != 0 ||
		again.Steps[0].Rows[0][4].(sdk.Decimal).Value.Cmp(decimalInt) != 0 ||
		again.Steps[0].Rows[0][8].([]byte)[0] != 0 || again.Steps[0].Columns[0].Name != "a" {
		t.Fatal("result/evidence or input alias escaped")
	}
	fixture.exec(t, "CREATE TABLE gh40_empty (id BIGINT, s VARCHAR,b BLOB)")
	requireOK(t, fixture.run(t, Request{Mode: Append, Table: "gh40_empty", Rows: [][]any{{1, "", []byte{}}, {2, nil, nil}}}))
	empty := fixture.rows(t, "SELECT s,b FROM gh40_empty ORDER BY id")
	if empty[0][0] != "" || len(empty[0][1].([]byte)) != 0 || empty[1][0] != nil || empty[1][1] != nil {
		t.Fatal("empty/null values conflated")
	}
}

func TestScalarRejectionAndTimestampPrecision(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	fixture.exec(t, "CREATE TABLE gh40_time (value TIMESTAMP)")
	for _, value := range []any{
		time.Date(2026, 9, 14, 0, 0, 0, 1, time.UTC), "2026-09-14", int64(0),
	} {
		result := fixture.run(t, Request{Mode: Append, Table: "gh40_time", Rows: [][]any{{value}}})
		if !errors.Is(result.Err(), ErrUnsupported) {
			t.Fatal("lossy timestamp accepted")
		}
	}
	for _, value := range []any{map[string]any{"x": 1}, []any{1}, new(int), (*big.Int)(nil), sdk.Decimal{}, new(big.Int).Lsh(big.NewInt(1), 129)} {
		if _, err := scalarSize(value); err == nil {
			t.Fatal("unqualified/nil/oversize input accepted")
		}
	}
	for _, value := range []time.Time{
		time.Date(1, 1, 1, 0, 0, 0, 0, time.FixedZone("test-positive", 3600)),
		time.Date(9999, 12, 31, 23, 59, 59, 0, time.FixedZone("test-negative", -3600)),
	} {
		if _, err := scalarSize(value); !errors.Is(err, ErrUnsupported) {
			t.Fatal("UTC conversion escaped supported time range")
		}
	}
	fixture.exec(t, "CREATE TABLE gh40_decimal (value DECIMAL(4,2))")
	for _, decimal := range []sdk.Decimal{
		{Width: 4, Scale: 2, Value: big.NewInt(10000)}, {Width: 4, Scale: 1, Value: big.NewInt(1)},
	} {
		result := fixture.run(t, Request{Mode: Append, Table: "gh40_decimal", Rows: [][]any{{decimal}}})
		if !errors.Is(result.Err(), ErrUnsupported) {
			t.Fatal("decimal overflow/scale coercion accepted")
		}
	}
	requireOK(t, fixture.run(t, Request{Mode: Append, Table: "gh40_decimal", Rows: [][]any{{sdk.Decimal{Width: 4, Scale: 2, Value: big.NewInt(-9999)}}}}))
	if got := fixture.rows(t, "SELECT value FROM gh40_decimal")[0][0].(sdk.Decimal); got.Value.Int64() != -9999 {
		t.Fatal("negative decimal changed")
	}
}

func FuzzScalarInputBounds(f *testing.F) {
	f.Add("hello", int64(1))
	f.Add("", int64(-128))
	f.Fuzz(func(t *testing.T, value string, number int64) {
		config := defaults(OptionsV1{InputBytes: 1024})
		request := Request{Mode: ExecuteMany, SQL: "INSERT INTO example VALUES (?,?)", Rows: [][]any{{value, number}}}
		err := validateRequests(config, []Request{request})
		if len(value) > 1024 && err == nil {
			t.Fatal("oversized input accepted")
		}
	})
}

func TestTimestampNSBoundsAcrossModes(t *testing.T) {
	for _, mode := range []Mode{Execute, ExecuteMany, Query, Append} {
		for _, instant := range []time.Time{time.Date(2262, 12, 31, 0, 0, 0, 0, time.UTC), time.Unix(0, math.MaxInt64), time.Unix(0, -math.MaxInt64)} {
			fixture := openFixture(t, OptionsV1{})
			fixture.exec(t, "CREATE TABLE gh40_ns (value TIMESTAMP_NS)")
			request := Request{Mode: mode, SQL: "INSERT INTO gh40_ns VALUES (?)", Args: []any{instant}}
			if mode == Query {
				request.SQL += " RETURNING value"
			}
			if mode == ExecuteMany || mode == Append {
				request.Args = nil
				request.Rows = [][]any{{instant}}
			}
			if mode == Append {
				request.SQL = ""
				request.Table = "gh40_ns"
			}
			result := fixture.run(t, request)
			if !errors.Is(result.Err(), ErrUnsupported) {
				t.Fatal("invalid nanosecond time accepted", result.Err())
			}
			if len(fixture.rows(t, "SELECT value FROM gh40_ns")) != 0 {
				t.Fatal("rejected timestamp mutated table")
			}
		}
	}
	fixture := openFixture(t, OptionsV1{})
	for _, instant := range []time.Time{time.Date(2026, 9, 14, 1, 2, 3, 456789123, time.UTC), time.Unix(0, math.MaxInt64-1)} {
		result := requireOK(t, fixture.run(t, Request{Mode: Query, SQL: "SELECT ?::TIMESTAMP_NS", Args: []any{instant}}))
		if !result.Steps[0].Rows[0][0].(time.Time).Equal(instant) {
			t.Fatal("finite timestamp control changed")
		}
	}
	for _, query := range []string{"SELECT TIMESTAMP_NS 'infinity'", "SELECT TIMESTAMP_NS '-infinity'"} {
		if result := fixture.run(t, Request{Mode: Query, SQL: query}); !errors.Is(result.Err(), ErrUnsupported) {
			t.Fatal("native timestamp sentinel was presented as finite")
		}
	}
}

func TestTimeRequiresExactQueryRepresentation(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	for _, literal := range []string{"00:00:00", "23:59:59.999999", "24:00:00"} {
		if result := fixture.run(t, Request{Mode: Query, SQL: "SELECT TIME '" + literal + "'"}); !errors.Is(result.Err(), ErrUnsupported) {
			t.Fatal("unsafe TIME decoder admitted")
		}
		if got := fixture.rows(t, "SELECT CAST(TIME '"+literal+"' AS VARCHAR)")[0][0]; got != literal {
			t.Fatal("exact TIME text control changed")
		}
	}
}

func TestKnownScalarParameterAdaptation(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	id := sdk.UUID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	decimal := sdk.Decimal{Width: 4, Scale: 2, Value: big.NewInt(-427)}
	fixture.exec(t, "CREATE TABLE gh40_parameters (u UBIGINT,id UUID,amount DECIMAL(4,2))")
	for _, mode := range []Mode{Execute, ExecuteMany, Append} {
		request := Request{Mode: mode, SQL: "INSERT INTO gh40_parameters VALUES (?,?,?)", Args: []any{uint(7), id, decimal}}
		if mode == ExecuteMany || mode == Append {
			request.Args = nil
			request.Rows = [][]any{{uint(7), id, decimal}}
		}
		if mode == Append {
			request.SQL = ""
			request.Table = "gh40_parameters"
		}
		requireOK(t, fixture.run(t, request))
	}
	rows := fixture.rows(t, "SELECT u,id::VARCHAR,amount::VARCHAR FROM gh40_parameters")
	if len(rows) != 3 {
		t.Fatal("scalar parameter writes missing")
	}
	for _, row := range rows {
		if !reflect.DeepEqual(row, []any{uint64(7), id.String(), "-4.27"}) {
			t.Fatal("bound scalar changed")
		}
	}
	result := requireOK(t, fixture.run(t, Request{Mode: Query, SQL: "SELECT ?::UBIGINT,?::UUID,?::DECIMAL(4,2)", Args: []any{uint(7), id, decimal}}))
	row := result.Steps[0].Rows[0]
	if row[0] != uint64(7) || !reflect.DeepEqual(row[1], id[:]) || row[2].(sdk.Decimal).Value.Cmp(decimal.Value) != 0 {
		t.Fatal("typed scalar parameter query changed")
	}
}
