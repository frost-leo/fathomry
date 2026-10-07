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
	"context"
	"errors"
	"math"
	"math/big"
	"reflect"
	"sync"
	"testing"
	"time"

	sdk "github.com/duckdb/duckdb-go/v2"
	native "github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2"
)

func TestExactPublicScalarsAndDetachedSnapshots(t *testing.T) {
	owner, inbox, _ := testOwner(t, testSettings(), 0)
	execute(t, owner, inbox, "CREATE TABLE exact_values (a BIGINT,b UBIGINT,c HUGEINT,d UHUGEINT,e DECIMAL(38,9),f DOUBLE,g FLOAT,h VARCHAR,i BLOB,j UUID,k TIMESTAMP,l TIMESTAMPTZ,m DATE,n TIME,o INTERVAL,p BOOLEAN)")
	huge := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
	unsignedHuge := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))
	decimalInteger, _ := new(big.Int).SetString("-12345678901234567890123456789", 10)
	decimal := Decimal{Width: 38, Scale: 9, Value: decimalInteger}
	instant := time.Date(2026, 10, 6, 3, 4, 5, 678000, time.UTC)
	date := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	clock := time.Date(1, 1, 1, 3, 4, 5, 678000, time.UTC)
	id := UUID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255}
	interval := Interval{Months: 2, Days: -3, Micros: 4000}
	input := [][]any{{int64(math.MinInt64), uint64(math.MaxUint64), huge, unsignedHuge, decimal,
		float64(1.5), float32(2.5), "value-canary 世界", []byte{0, 255, 7}, id, instant, instant, date, clock, interval, true}, make([]any, 16)}
	if _, err := owner.Client().Append(context.Background(), context.Background(), "", "exact_values", nil, input); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	value := query(t, owner, inbox, "SELECT a,b,c,d,e,f,g,h,i,j,k,l,m,n::VARCHAR AS n,o,p FROM exact_values ORDER BY a NULLS LAST")
	expected := append([]any(nil), input[0]...)
	expected[13] = "03:04:05.000678"
	want := [][]any{expected, make([]any, 16)}
	if !reflect.DeepEqual(value.Snapshot().Steps[0].Rows, want) {
		t.Fatal("public scalar width, precision, units, or NULL changed")
	}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 20 {
				copy := value.Snapshot()
				copy.Steps[0].Rows[0][2].(*big.Int).SetInt64(7)
				copy.Steps[0].Rows[0][4].(Decimal).Value.SetInt64(7)
				copy.Steps[0].Rows[0][8].([]byte)[0] = 7
				copy.Steps[0].Columns[0].Name = "changed"
			}
		})
	}
	workers.Wait()
	if !reflect.DeepEqual(value.Snapshot().Steps[0].Rows, want) || value.Snapshot().Steps[0].Columns[0].Name != "a" {
		t.Fatal("concurrent snapshots shared mutable storage")
	}
	execute(t, owner, inbox, "CREATE TABLE nulls (id BIGINT, s VARCHAR, b BLOB)")
	if _, err := owner.Client().Append(context.Background(), context.Background(), "", "nulls", nil, [][]any{{int64(1), "", []byte{}}, {int64(2), nil, nil}}); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	nulls := query(t, owner, inbox, "SELECT s AS duplicate,b AS duplicate FROM nulls ORDER BY id").Snapshot().Steps[0]
	if nulls.Columns[0].Name != "duplicate" || nulls.Columns[1].Name != "duplicate" || nulls.Rows[0][0] != "" || nulls.Rows[0][1] == nil || len(nulls.Rows[0][1].([]byte)) != 0 || nulls.Rows[1][0] != nil || nulls.Rows[1][1] != nil {
		t.Fatal("duplicate columns, empty values, and SQL NULL conflated")
	}
}

func TestExactParametersAndSafetyProjection(t *testing.T) {
	owner, inbox, _ := testOwner(t, testSettings(), 0)
	ctx := context.Background()
	id := UUID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	decimal := Decimal{Width: 4, Scale: 2, Value: big.NewInt(-427)}
	value := query(t, owner, inbox, "SELECT ?::UBIGINT, ?::UUID, ?::DECIMAL(4,2)", uint(7), id, decimal)
	row := value.Snapshot().Steps[0].Rows[0]
	if row[0] != uint64(7) || row[1] != id || !reflect.DeepEqual(row[2], decimal) {
		t.Fatal("exact public parameters changed")
	}
	for _, sql := range []string{"SELECT TIME '24:00:00'", `SELECT JSON '{"integer":9007199254740993}'`, "SELECT [1,2,NULL]", "SELECT {'nested': 1}", "SELECT TIMESTAMP_NS 'infinity'"} {
		value, err := owner.Client().Query(ctx, ctx, sql)
		if !errors.Is(err, ErrUnsupported) || value.Snapshot().Steps[0].Complete {
			t.Fatal("unsafe result decoder admitted", err)
		}
		ack(t, inbox)
	}
	projected := query(t, owner, inbox, `SELECT CAST(TIME '24:00:00' AS VARCHAR), CAST(JSON '{"integer":9007199254740993}' AS VARCHAR), CAST([1,2,NULL] AS VARCHAR)`).Snapshot().Steps[0]
	if !reflect.DeepEqual(projected.Rows[0], []any{"24:00:00", `{"integer":9007199254740993}`, "[1, 2, NULL]"}) {
		t.Fatal("explicit exact scalar projection changed")
	}
	for _, column := range projected.Columns {
		if column.Type != "VARCHAR" {
			t.Fatal("projection hid changed native schema")
		}
	}
	execute(t, owner, inbox, "CREATE TABLE narrowing (value TINYINT)")
	value, err := owner.Client().Append(ctx, ctx, "", "narrowing", nil, [][]any{{int64(128)}})
	if !errors.Is(err, ErrUnsupported) || value.Snapshot().Steps[0].AcceptedRows != 0 {
		t.Fatal("overflow Appender input was buffered")
	}
	ack(t, inbox)
	if len(query(t, owner, inbox, "SELECT * FROM narrowing").Snapshot().Steps[0].Rows) != 0 {
		t.Fatal("rejected overflow changed table")
	}
}

func TestMalformedNativeResultProjectionRejected(t *testing.T) {
	for _, invalid := range []native.Step{
		{Mode: native.Query, Complete: true, Columns: []native.Column{{Name: "a", Type: "BIGINT"}}, Rows: [][]any{{}}},
		{Mode: native.Query, Complete: true, Columns: []native.Column{{Name: "a", Type: "BIGINT"}}, Rows: [][]any{{int64(1), int64(2)}}},
		{Mode: native.Query, Complete: true, Columns: []native.Column{{Name: "a", Type: "UUID"}}, Rows: [][]any{{[]byte{1}}}},
		{Mode: native.Query, Complete: true, Columns: []native.Column{{Name: "a", Type: "DECIMAL(4,2)"}}, Rows: [][]any{{sdk.Decimal{Width: 0, Scale: 0, Value: big.NewInt(1)}}}},
		{Mode: native.Query, Complete: true, Columns: []native.Column{{Name: "a", Type: "DECIMAL(1,0)"}}, Rows: [][]any{{sdk.Decimal{Width: 1, Value: big.NewInt(100)}}}},
		{Mode: native.Query, Complete: true, Columns: []native.Column{{Name: "a", Type: "VARCHAR"}}, Rows: [][]any{{map[string]any{"unexpected": 1}}}},
	} {
		if _, err := projectProgress(native.Progress{Steps: []native.Step{invalid}, ConnectionClosed: true}); !errors.Is(err, ErrUnsupported) {
			t.Fatal("malformed or unknown native result representation accepted", err)
		}
	}
}

func TestDecimalPrecisionRejectedBeforeNativeSubmission(t *testing.T) {
	owner, inbox, _ := testOwner(t, testSettings(), 0)
	for _, number := range []int64{100, -100} {
		value, err := owner.Client().Query(context.Background(), context.Background(), "SELECT ?::DECIMAL(4,0)", Decimal{Width: 1, Value: big.NewInt(number)})
		if err == nil || value.HasData() || len(value.Snapshot().Steps) != 0 {
			t.Fatal("invalid public decimal precision reached native work")
		}
		if evidence := ack(t, inbox); evidence.HasData() || len(evidence.Snapshot().Steps) != 0 {
			t.Fatal("invalid decimal invented submitted progress")
		}
	}
	value := query(t, owner, inbox, "SELECT ?::DECIMAL(2,0)", Decimal{Width: 2, Value: big.NewInt(-99)})
	if actual := value.Snapshot().Steps[0].Rows[0][0].(Decimal); actual.Value.Int64() != -99 || actual.Width != 2 || actual.Scale != 0 {
		t.Fatal("representable precision boundary refused or changed")
	}
}

func FuzzScalarProjectionIsolation(f *testing.F) {
	f.Add([]byte("data"), int64(-427), uint8(4), uint8(2))
	f.Add([]byte{}, int64(0), uint8(38), uint8(9))
	f.Fuzz(func(t *testing.T, blob []byte, number int64, width, scale uint8) {
		if len(blob) > 64<<10 {
			return
		}
		projected, err := outward(blob, "BLOB")
		if err != nil || !reflect.DeepEqual(projected, blob) {
			t.Fatal("blob projection changed")
		}
		if len(blob) != 0 {
			projected.([]byte)[0] ^= 0xff
			if projected.([]byte)[0] == blob[0] {
				t.Fatal("blob projection aliases input")
			}
		}
		if width < 1 || width > 38 || scale > width {
			return
		}
		nativeValue := sdk.Decimal{Width: width, Scale: scale, Value: big.NewInt(number)}
		converted, err := outward(nativeValue, "DECIMAL")
		limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(width)), nil)
		if new(big.Int).Abs(nativeValue.Value).Cmp(limit) >= 0 {
			if !errors.Is(err, ErrUnsupported) {
				t.Fatal("out-of-precision decimal accepted")
			}
			return
		}
		if err != nil {
			t.Fatal("representable decimal rejected", err)
		}
		decimal := converted.(Decimal)
		if decimal.Width != width || decimal.Scale != scale || decimal.Value.Cmp(nativeValue.Value) != 0 {
			t.Fatal("decimal projection changed")
		}
		decimal.Value.Add(decimal.Value, big.NewInt(1))
		if nativeValue.Value.Int64() != number {
			t.Fatal("decimal projection aliases native integer")
		}
	})
}
