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

package iceberg_test

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	iceberg "github.com/frost-leo/fathomry/adapters/tableformat/iceberg/v1"
)

func serviceTypedRows(s *iceberg.Session, namespace, root string, allocator memory.Allocator) error {
	schema := sdk.NewSchema(0,
		sdk.NestedField{ID: 1, Name: "id", Type: sdk.PrimitiveTypes.Int64, Required: true},
		sdk.NestedField{ID: 2, Name: "payload", Type: &sdk.StructType{FieldList: []sdk.NestedField{{ID: 3, Name: "score", Type: sdk.PrimitiveTypes.Int64}, {ID: 4, Name: "tag", Type: sdk.PrimitiveTypes.String}}}},
		sdk.NestedField{ID: 5, Name: "amount", Type: sdk.DecimalTypeOf(12, 2)},
		sdk.NestedField{ID: 6, Name: "clock", Type: sdk.PrimitiveTypes.Time},
		sdk.NestedField{ID: 7, Name: "at", Type: sdk.PrimitiveTypes.Timestamp},
		sdk.NestedField{ID: 8, Name: "uuid", Type: sdk.PrimitiveTypes.UUID})
	tbl, err := s.CreateTable([]string{namespace, "typed"}, schema, catalog.WithLocation(root+"typed"), catalog.WithProperties(sdk.Properties{"format-version": "2", "write.parquet.row-group-limit": "1024"}))
	if err != nil {
		return err
	}
	metadata, err := tbl.Metadata()
	if err != nil {
		return err
	}
	arrowSchema, err := table.SchemaToArrowSchema(metadata.CurrentSchema(), nil, true, false)
	if err != nil {
		return err
	}
	var data strings.Builder
	data.WriteByte('[')
	for id := range 4097 {
		if id != 0 {
			data.WriteByte(',')
		}
		if id%7 == 0 {
			fmt.Fprintf(&data, "{\"id\":%d}", id)
		} else {
			fmt.Fprintf(&data, "{\"id\":%d,\"payload\":{\"score\":%d,\"tag\":\"nested-%d\"},\"amount\":\"-%d.17\",\"clock\":\"23:59:59.123456\",\"at\":%d,\"uuid\":\"12345678-1234-5678-9abc-123456789abc\"}", id, int64(id)*1234567, id, id, int64(-1234567890123456)+int64(id))
		}
	}
	data.WriteByte(']')
	record, _, err := array.RecordFromJSON(allocator, arrowSchema, strings.NewReader(data.String()))
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
	current, err := tbl.Metadata()
	if err != nil {
		return err
	}
	if current.Version() != 2 || !reflect.DeepEqual(metadata.CurrentSchema().Fields(), current.CurrentSchema().Fields()) {
		return fmt.Errorf("typed live schema/field identity changed")
	}
	scan, err := tbl.Scan(iceberg.ScanOptions{BatchSize: 127})
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
	err = output.Visit(func(actual arrow.Table) error {
		if !summary.Complete || actual.NumRows() != 4097 || actual.NumCols() != expected.NumCols() {
			return fmt.Errorf("typed live output shape")
		}
		for column := range int(expected.NumCols()) {
			if !array.ChunkedEqual(expected.Column(column).Data(), actual.Column(column).Data()) {
				return fmt.Errorf("typed live exact column mismatch: %s", expected.Column(column).Name())
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	millis := current.CurrentSnapshot().TimestampMs
	scan, err = tbl.Scan(iceberg.ScanOptions{AsOfMillis: &millis, Fields: []string{"id", "amount"}, Filter: sdk.NewAnd(sdk.GreaterThanEqual(sdk.Reference("id"), int64(4090)), sdk.LessThan(sdk.Reference("id"), int64(4097))), BatchSize: 2})
	if err != nil {
		return err
	}
	projection, err := scan.Projection()
	if err != nil {
		return err
	}
	expectedAmount, _ := current.CurrentSchema().FindFieldByName("amount")
	actualAmount, found := projection.FindFieldByName("amount")
	if !found || actualAmount.ID != expectedAmount.ID || projection.NumFields() != 2 {
		return fmt.Errorf("historical projection changed field IDs")
	}
	seen := map[int64]bool{}
	summary, err = scan.Records(func(record arrow.RecordBatch) (bool, error) {
		ids, amounts := record.Column(0).(*array.Int64), record.Column(1).(*array.Decimal128)
		for row := range ids.Len() {
			id := ids.Value(row)
			if id < 4090 || id >= 4097 || seen[id] || amounts.IsNull(row) != (id%7 == 0) || !amounts.IsNull(row) && amounts.Value(row) != decimal128.FromI64(-id*100-17) {
				return false, fmt.Errorf("as-of filtered projection changed values")
			}
			seen[id] = true
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	if !summary.Complete || len(seen) != 7 {
		return fmt.Errorf("as-of filtered projection incomplete")
	}
	return nil
}
