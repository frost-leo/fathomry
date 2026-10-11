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

package table_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestNullableCopyOnWriteRetainsEveryNonmatch(t *testing.T) {
	value := iceberg.Reference("value")
	for _, version := range []string{"2", "3"} {
		for _, overwrite := range []bool{false, true} {
			for _, predicate := range []struct {
				name    string
				filter  iceberg.BooleanExpression
				removed map[int64]bool
			}{
				{"required", iceberg.EqualTo(iceberg.Reference("id"), int64(1)), map[int64]bool{1: true}},
				{"nullable", iceberg.EqualTo(value, int64(1)), map[int64]bool{1: true}},
				{"compound-and", iceberg.NewAnd(iceberg.EqualTo(value, int64(1)), iceberg.GreaterThan(iceberg.Reference("id"), int64(0))), map[int64]bool{1: true}},
				{"compound-or", iceberg.NewOr(iceberg.EqualTo(value, int64(1)), iceberg.EqualTo(iceberg.Reference("id"), int64(2))), map[int64]bool{1: true, 2: true}},
				{"null-test", iceberg.IsNull(value), map[int64]bool{0: true, 3: true}},
			} {
				t.Run(fmt.Sprintf("v%s/overwrite=%t/%s", version, overwrite, predicate.name), func(t *testing.T) {
					alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
					defer alloc.AssertSize(t, 0)
					ctx := compute.WithAllocator(t.Context(), alloc)
					location := filepath.ToSlash(t.TempDir())
					schema := iceberg.NewSchema(0,
						iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
						iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Int64})
					metadata, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location, iceberg.Properties{table.PropertyFormatVersion: version})
					require.NoError(t, err)
					fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
					catalog := &concurrentTestCatalog{metadata: metadata, location: location + "/metadata/v1.metadata.json", fsF: fsF}
					tbl := table.New(table.Identifier{"db", "nullable"}, metadata, catalog.location, fsF, catalog)
					arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
					require.NoError(t, err)
					path := location + "/data/initial.parquet"
					writeParquetFile(t, path, arrowSchema, `[{"id":0,"value":null},{"id":1,"value":1},{"id":2,"value":2},{"id":3,"value":null}]`)
					tx := tbl.NewTransaction()
					require.NoError(t, tx.AddFiles(ctx, []string{path}, nil, false))
					tbl, err = tx.Commit(ctx)
					require.NoError(t, err)
					tx = tbl.NewTransaction()
					if overwrite {
						builder := array.NewRecordBuilder(alloc, arrowSchema)
						builder.Field(0).(*array.Int64Builder).Append(9)
						builder.Field(1).(*array.Int64Builder).Append(9)
						record := builder.NewRecordBatch()
						builder.Release()
						reader, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{record})
						record.Release()
						require.NoError(t, err)
						err = tx.Overwrite(ctx, reader, nil, table.WithOverwriteFilter(predicate.filter), table.WithOverwriteConcurrency(1))
						reader.Release()
						require.NoError(t, err)
					} else {
						require.NoError(t, tx.Delete(ctx, predicate.filter, nil, table.WithDeleteConcurrency(1)))
					}
					tbl, err = tx.Commit(ctx)
					require.NoError(t, err)
					_, records, err := tbl.Scan(table.WithMaxConcurrency(1)).ToArrowRecords(ctx)
					require.NoError(t, err)
					actual := map[int64]string{}
					for record, err := range records {
						require.NoError(t, err)
						ids, values := record.Column(0).(*array.Int64), record.Column(1).(*array.Int64)
						for index := range ids.Len() {
							id := ids.Value(index)
							_, duplicate := actual[id]
							require.False(t, duplicate)
							actual[id] = "null"
							if !values.IsNull(index) {
								actual[id] = fmt.Sprint(values.Value(index))
							}
						}
						record.Release()
					}
					expected := map[int64]string{0: "null", 1: "1", 2: "2", 3: "null"}
					for id := range predicate.removed {
						delete(expected, id)
					}
					if overwrite {
						expected[9] = "9"
					}
					require.Equal(t, expected, actual)
				})
			}
		}
	}
}
