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

package iceberg

import (
	"bytes"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

func TestFlatScalarRoundTrip(t *testing.T) {
	_, options := newPeer(t)
	f := bindFixture(t, options, 6)
	schema := native.NewSchema(0,
		native.NestedField{ID: 1, Name: "flag", Type: native.PrimitiveTypes.Bool},
		native.NestedField{ID: 2, Name: "small", Type: native.PrimitiveTypes.Int32},
		native.NestedField{ID: 3, Name: "large", Type: native.PrimitiveTypes.Int64},
		native.NestedField{ID: 4, Name: "fraction32", Type: native.PrimitiveTypes.Float32},
		native.NestedField{ID: 5, Name: "fraction64", Type: native.PrimitiveTypes.Float64},
		native.NestedField{ID: 6, Name: "text", Type: native.PrimitiveTypes.String},
		native.NestedField{ID: 7, Name: "data", Type: native.PrimitiveTypes.Binary})
	receipt, err := f.client.CreateTable(deadline(t), correlation("create"), "scalars", schema)
	requireOK(t, settle(t, receipt, err))
	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, false, false)
	if err != nil {
		t.Fatal(err)
	}
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	builder := array.NewRecordBuilder(allocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.BooleanBuilder).AppendValues([]bool{false, true, false}, []bool{true, true, false})
	builder.Field(1).(*array.Int32Builder).AppendValues([]int32{-7, 0, 23}, []bool{true, true, false})
	builder.Field(2).(*array.Int64Builder).AppendValues([]int64{1<<60 + 1, -1 << 60, 0}, []bool{true, true, false})
	builder.Field(3).(*array.Float32Builder).AppendValues([]float32{1.25, -2.5, 0}, []bool{true, true, false})
	builder.Field(4).(*array.Float64Builder).AppendValues([]float64{1.0 / 3, -7.5, 0}, []bool{true, true, false})
	builder.Field(5).(*array.StringBuilder).AppendValues([]string{"", "utf8-\u03bb", ""}, []bool{true, true, false})
	builder.Field(6).(*array.BinaryBuilder).AppendValues([][]byte{{}, {0, 1, 255}, nil}, []bool{true, true, false})
	record := builder.NewRecordBatch()
	defer record.Release()
	receipt, err = f.client.Write(deadline(t), correlation("write"), "scalars", BatchWrite{Mode: Append, Batches: []arrow.RecordBatch{record}})
	requireOK(t, settle(t, receipt, err))
	receipt, err = f.client.Read(deadline(t), correlation("read"), "scalars", BatchRead{})
	result := requireOK(t, settle(t, receipt, err))
	reader, err := ipc.NewReader(bytes.NewReader(result.IPCCopy()), ipc.WithAllocator(allocator))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("missing scalar rows")
	}
	got := reader.RecordBatch()
	for column := 0; column < int(record.NumCols()); column++ {
		if !array.Equal(record.Column(column), got.Column(column)) {
			t.Fatalf("scalar column %d changed", column)
		}
	}
	if reader.Next() || reader.Err() != nil || !result.Complete() {
		t.Fatal("unexpected scalar result completion")
	}
}
func BenchmarkFreezeBatch(b *testing.B) {
	record := makeBatch(b, 0, 4097, "batch")
	settings := defaults(OptionsV1{})
	raw, _, err := freezeBatches(settings, []arrow.RecordBatch{record})
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		data, rows, err := freezeBatches(settings, []arrow.RecordBatch{record})
		if err != nil || rows != 4097 || len(data) != len(raw) {
			b.Fatal("batch preparation changed")
		}
	}
}

func TestDuplicateOptionalBatchColumnsAreRejected(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "value", Type: arrow.BinaryTypes.String, Nullable: true}, {Name: "value", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(1)
	builder.Field(1).(*array.StringBuilder).Append("first")
	builder.Field(2).(*array.StringBuilder).Append("second")
	record := builder.NewRecordBatch()
	defer record.Release()
	if _, _, err := freezeBatches(defaults(OptionsV1{}), []arrow.RecordBatch{record}); !errors.Is(err, ErrInput) {
		t.Fatal("duplicate optional columns admitted")
	}
}
