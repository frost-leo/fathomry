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
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func TestManagedAdvancedTypeFormatRoundTrips(t *testing.T) {
	common := []sdk.NestedField{
		{ID: 1, Name: "id", Type: sdk.PrimitiveTypes.Int64, Required: true},
		{ID: 2, Name: "flag", Type: sdk.PrimitiveTypes.Bool},
		{ID: 3, Name: "small", Type: sdk.PrimitiveTypes.Int32},
		{ID: 4, Name: "single", Type: sdk.PrimitiveTypes.Float32},
		{ID: 5, Name: "double", Type: sdk.PrimitiveTypes.Float64},
		{ID: 6, Name: "day", Type: sdk.PrimitiveTypes.Date},
		{ID: 7, Name: "clock", Type: sdk.PrimitiveTypes.Time},
		{ID: 8, Name: "instant", Type: sdk.PrimitiveTypes.TimestampTz},
		{ID: 9, Name: "uuid", Type: sdk.PrimitiveTypes.UUID},
		{ID: 10, Name: "fixed", Type: sdk.FixedTypeOf(3)},
		{ID: 11, Name: "binary", Type: sdk.PrimitiveTypes.Binary},
		{ID: 12, Name: "decimal", Type: sdk.DecimalTypeOf(38, 9)},
		{ID: 13, Name: "list", Type: &sdk.ListType{ElementID: 14, Element: sdk.PrimitiveTypes.String}},
		{ID: 15, Name: "map", Type: &sdk.MapType{KeyID: 16, KeyType: sdk.PrimitiveTypes.String, ValueID: 17, ValueType: sdk.PrimitiveTypes.Int64}},
	}
	const rows = "[{\"id\":1,\"flag\":true,\"small\":-2147483648,\"single\":1.25,\"double\":-123456789.125,\"day\":\"1969-12-31\",\"clock\":\"23:59:59.123456\",\"instant\":\"1969-12-31T23:59:59.123456Z\",\"uuid\":\"12345678-1234-5678-9abc-123456789abc\",\"fixed\":\"AQID\",\"binary\":\"AAEC/w==\",\"decimal\":\"12345678901234567890123456789.123456789\",\"list\":[\"x\",null,\"\"],\"map\":[{\"key\":\"one\",\"value\":1},{\"key\":\"null\",\"value\":null}]},{\"id\":2},{\"id\":3,\"flag\":false,\"small\":2147483647,\"single\":-1.5,\"double\":0,\"day\":\"2040-02-29\",\"clock\":\"00:00:00.000001\",\"instant\":\"2026-10-11T00:00:00.123456Z\",\"uuid\":\"00000000-0000-0000-0000-000000000001\",\"fixed\":\"/wAB\",\"binary\":\"\",\"decimal\":\"-0.000000001\",\"list\":[],\"map\":[]}]"
	for _, version := range []int{1, 2, 3} {
		t.Run("common-v"+strconv.Itoa(version), func(t *testing.T) {
			managedTypeRoundTrip(t, version, sdk.NewSchema(0, common...), func(allocator memory.Allocator, schema *arrow.Schema) (arrow.RecordBatch, error) {
				record, _, err := array.RecordFromJSON(allocator, schema, strings.NewReader(rows))
				return record, err
			})
		})
	}
	t.Run("v3-nanoseconds-and-geo", func(t *testing.T) {
		geography, err := sdk.GeographyTypeOf("OGC:CRS84", "spherical")
		if err != nil {
			t.Fatal(err)
		}
		schema := sdk.NewSchema(0,
			sdk.NestedField{ID: 1, Name: "id", Type: sdk.PrimitiveTypes.Int64, Required: true},
			sdk.NestedField{ID: 2, Name: "nanos", Type: sdk.PrimitiveTypes.TimestampNs},
			sdk.NestedField{ID: 3, Name: "zoned", Type: sdk.PrimitiveTypes.TimestampTzNs},
			sdk.NestedField{ID: 4, Name: "geom", Type: sdk.GeometryType{}},
			sdk.NestedField{ID: 5, Name: "geog", Type: geography})
		managedTypeRoundTrip(t, 3, schema, func(allocator memory.Allocator, schema *arrow.Schema) (arrow.RecordBatch, error) {
			record, _, err := array.RecordFromJSON(allocator, schema, strings.NewReader("[{\"id\":1,\"nanos\":-1234567890123456789,\"zoned\":1234567890123456789,\"geom\":\"0101000000000000000000f03f0000000000000040\",\"geog\":\"0101000000000000000000f03f0000000000000040\"},{\"id\":2},{\"id\":3,\"nanos\":1,\"zoned\":-1}]"))
			return record, err
		})
	})
	t.Run("v3-variant", func(t *testing.T) {
		schema := sdk.NewSchema(0, sdk.NestedField{ID: 1, Name: "id", Type: sdk.PrimitiveTypes.Int64, Required: true}, sdk.NestedField{ID: 2, Name: "payload", Type: sdk.VariantType{}})
		managedTypeRoundTrip(t, 3, schema, func(allocator memory.Allocator, schema *arrow.Schema) (arrow.RecordBatch, error) {
			ids := array.NewInt64Builder(allocator)
			defer ids.Release()
			values := extensions.NewVariantBuilder(allocator, schema.Field(1).Type.(*extensions.VariantType))
			defer values.Release()
			for index, value := range []any{map[string]any{"A": int64(5_000_000_001), "a": []any{true, "x", nil}}, nil, map[string]any{}} {
				ids.Append(int64(index + 1))
				var builder variant.Builder
				if err := builder.Append(value); err != nil {
					return nil, err
				}
				encoded, err := builder.Build()
				if err != nil {
					return nil, err
				}
				values.Append(encoded)
			}
			ids.Append(4)
			values.AppendNull()
			idArray, valueArray := ids.NewArray(), values.NewArray()
			defer idArray.Release()
			defer valueArray.Release()
			return array.NewRecordBatch(schema, []arrow.Array{idArray, valueArray}, 4), nil
		})
	})
}

func managedTypeRoundTrip(t *testing.T, version int, schema *sdk.Schema, build func(memory.Allocator, *arrow.Schema) (arrow.RecordBatch, error)) {
	t.Helper()
	_, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	f.run(t, "advanced-types", func(s *ManagedSession) error {
		tbl, err := s.CreateTable([]string{"isolated", "data"}, schema, catalog.WithLocation(options.Location+"data"), catalog.WithProperties(sdk.Properties{"format-version": strconv.Itoa(version)}))
		if err != nil {
			return err
		}
		meta, err := tbl.Metadata()
		if err != nil {
			return err
		}
		arrowSchema, err := table.SchemaToArrowSchema(meta.CurrentSchema(), nil, true, false)
		if err != nil {
			return err
		}
		record, err := build(allocator, arrowSchema)
		if err != nil {
			return err
		}
		defer record.Release()
		reader, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{record})
		if err != nil {
			return err
		}
		tx, err := tbl.NewTransaction("")
		if err == nil {
			err = tx.Append(reader, nil)
		}
		reader.Release()
		if err != nil {
			return err
		}
		tbl, err = tx.Commit()
		if err != nil {
			return err
		}
		after, err := tbl.Metadata()
		if err != nil {
			return err
		}
		if after.Version() != version || !reflect.DeepEqual(meta.CurrentSchema().Fields(), after.CurrentSchema().Fields()) {
			return fmt.Errorf("format/schema identity changed")
		}
		scan, err := tbl.Scan(ManagedScanOptions{BatchSize: 1})
		if err != nil {
			return err
		}
		output, summary, err := scan.ToArrowTable()
		if output != nil {
			defer output.Close()
		}
		if err != nil {
			return err
		}
		expected := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{record})
		defer expected.Release()
		return output.Visit(func(actual arrow.Table) error {
			if !summary.Complete || actual.NumRows() != expected.NumRows() || actual.NumCols() != expected.NumCols() {
				return fmt.Errorf("advanced result shape")
			}
			for index := range int(actual.NumCols()) {
				if !array.ChunkedEqual(actual.Column(index).Data(), expected.Column(index).Data()) {
					return fmt.Errorf("advanced column %s differs", expected.Column(index).Name())
				}
			}
			return nil
		})
	})
}
