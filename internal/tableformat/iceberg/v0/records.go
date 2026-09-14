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
	"context"
	"errors"
	"iter"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/substrait"
)

// prepareRecords compiles before native ReadTasks starts workers. Its iterator
// owns each input reference and transfers each nonempty output to its consumer.
func prepareRecords(ctx context.Context, schema, projection *native.Schema, expression native.BooleanExpression, keepNonmatches bool) (func(iter.Seq2[arrow.RecordBatch, error]) iter.Seq2[arrow.RecordBatch, error], error) {
	bound, err := native.BindExpr(schema, expression, true)
	if err != nil {
		return nil, err
	}
	constant, constantValue := false, false
	switch bound.(type) {
	case native.AlwaysTrue:
		constant, constantValue = true, true
	case native.AlwaysFalse:
		constant = true
	}
	var evaluate func(arrow.RecordBatch) (compute.Datum, error)
	if !constant {
		registry, compiled, err := substrait.ConvertExpr(schema, bound, true)
		if err != nil {
			return nil, err
		}
		ctx = exprs.WithExtensionIDSet(ctx, exprs.NewExtensionSetDefault(*registry))
		evaluate = func(record arrow.RecordBatch) (compute.Datum, error) {
			return exprs.ExecuteScalarExpression(ctx, record.Schema(), compiled, compute.NewDatumWithoutOwning(record))
		}
	}
	transform := func(record arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer record.Release()
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, context.Cause(ctx))
		}
		if constant {
			selected := constantValue
			if keepNonmatches {
				selected = !selected
			}
			if !selected {
				return record.NewSlice(0, 0), nil
			}
			return table.ToRequestedSchema(ctx, projection, schema, record, table.SchemaOptions{})
		}
		mask, err := evaluate(record)
		if err != nil {
			return nil, err
		}
		defer mask.Release()
		effective := mask
		if keepNonmatches {
			// A null predicate does not match a DELETE/OVERWRITE. Inverting a nullable
			// mask directly would instead drop these unrelated rows.
			inverted, err := nonmatchMask(ctx, mask, int(record.NumRows()))
			if err != nil {
				return nil, err
			}
			defer inverted.Release()
			effective = compute.NewDatum(inverted)
			defer effective.Release()
		}
		filtered, err := compute.Filter(ctx, compute.NewDatumWithoutOwning(record), effective, *compute.DefaultFilterOptions())
		if err != nil {
			return nil, err
		}
		defer filtered.Release()
		result, ok := filtered.(*compute.RecordDatum)
		if !ok {
			return nil, failure(ErrProtocol, "filter-record")
		}
		return table.ToRequestedSchema(ctx, projection, schema, result.Value, table.SchemaOptions{})
	}
	return func(records iter.Seq2[arrow.RecordBatch, error]) iter.Seq2[arrow.RecordBatch, error] {
		return func(yield func(arrow.RecordBatch, error) bool) {
			for record, err := range records {
				if err != nil {
					if record != nil {
						record.Release()
					}
					yield(nil, err)
					return
				}
				if record == nil {
					yield(nil, failure(ErrProtocol, "nil-record"))
					return
				}
				output, err := transform(record)
				if err != nil {
					yield(nil, err)
					return
				}
				if output.NumRows() == 0 {
					output.Release()
					continue
				}
				if !yield(output, nil) {
					return
				}
			}
		}
	}, nil
}
func nonmatchMask(ctx context.Context, mask compute.Datum, rows int) (*array.Boolean, error) {
	builder := array.NewBooleanBuilder(compute.GetAllocator(ctx))
	defer builder.Release()
	builder.Reserve(rows)
	switch value := mask.(type) {
	case *compute.ArrayDatum:
		values := value.MakeArray()
		defer values.Release()
		booleans, ok := values.(*array.Boolean)
		if !ok || booleans.Len() != rows {
			return nil, failure(ErrProtocol, "filter-mask")
		}
		for index := 0; index < rows; index++ {
			builder.Append(booleans.IsNull(index) || !booleans.Value(index))
		}
	case *compute.ScalarDatum:
		boolean, ok := value.Value.(*scalar.Boolean)
		if !ok {
			return nil, failure(ErrProtocol, "filter-mask")
		}
		keep := !boolean.IsValid() || !boolean.Value
		for range rows {
			builder.Append(keep)
		}
	default:
		return nil, failure(ErrProtocol, "filter-mask")
	}
	return builder.NewBooleanArray(), nil
}

// joinBatches borrows its inputs and returns one owned record. One record per
// native write avoids v0.6.0's unreleased queued records on cancellation/error.
func joinBatches(ctx context.Context, schema *arrow.Schema, batches []arrow.RecordBatch) (arrow.RecordBatch, error) {
	if len(batches) == 0 {
		return nil, nil
	}
	var rows int64
	for _, batch := range batches {
		rows += batch.NumRows()
	}
	columns := make([]arrow.Array, schema.NumFields())
	defer func() {
		for _, column := range columns {
			if column != nil {
				column.Release()
			}
		}
	}()
	for column := range columns {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		chunks := make([]arrow.Array, len(batches))
		for index, batch := range batches {
			chunks[index] = batch.Column(column)
		}
		joined, err := array.Concatenate(chunks, compute.GetAllocator(ctx))
		if err != nil {
			return nil, err
		}
		columns[column] = joined
	}
	return array.NewRecordBatch(schema, columns, rows), nil
}
func coalesceRecords(ctx context.Context, s settings, schema *arrow.Schema, records iter.Seq2[arrow.RecordBatch, error]) (arrow.RecordBatch, error) {
	var batches []arrow.RecordBatch
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()
	var rows, bytesUsed int64
	for record, err := range records {
		if err != nil {
			if record != nil {
				record.Release()
			}
			return nil, err
		}
		if record == nil {
			return nil, failure(ErrProtocol, "nil-record")
		}
		if record.NumRows() > int64(s.MaxRows)-rows {
			record.Release()
			return nil, failure(ErrLimit, "rewrite-rows")
		}
		rows += record.NumRows()
		for _, column := range record.Columns() {
			for _, buffer := range column.Data().Buffers() {
				if buffer != nil {
					bytesUsed += int64(buffer.Len())
				}
			}
		}
		if bytesUsed > int64(s.MaxBatchBytes) {
			record.Release()
			return nil, failure(ErrLimit, "rewrite-buffers")
		}
		batches = append(batches, record)
	}
	return joinBatches(ctx, schema, batches)
}
func writeRecord(ctx context.Context, s settings, tbl *table.Table, record arrow.RecordBatch) ([]native.DataFile, error) {
	if record == nil || record.NumRows() == 0 {
		return nil, nil
	}
	if err := validateWriteRecord(tbl, record); err != nil {
		return nil, err
	}
	records := func(yield func(arrow.RecordBatch, error) bool) { record.Retain(); yield(record, nil) }
	var files []native.DataFile
	for file, err := range table.WriteRecords(ctx, tbl, record.Schema(), records, table.WithClusteredWrite(),
		table.WithTargetFileSize(int64(s.MaxObjectBytes/2))) {
		if err != nil {
			return nil, err
		}
		if len(files) == s.MaxFileOps {
			return nil, failure(ErrLimit, "written-files")
		}
		files = append(files, file)
	}
	return files, nil
}
func validateWriteRecord(tbl *table.Table, record arrow.RecordBatch) error {
	expected, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	if err != nil {
		return err
	}
	for _, field := range record.Schema().Fields() {
		indices := expected.FieldIndices(field.Name)
		if len(indices) != 1 || !arrow.TypeEqual(field.Type, expected.Field(indices[0]).Type) {
			return failure(ErrUnsupported, "write-schema")
		}
		if !expected.Field(indices[0]).Nullable && field.Nullable {
			return failure(ErrInput, "required-column")
		}
		if value, exists := field.Metadata.GetValue(table.ArrowParquetFieldIDKey); exists {
			id, err := strconv.Atoi(value)
			column, found := tbl.Schema().FindFieldByName(field.Name)
			if err != nil || !found || id != column.ID {
				return failure(ErrInput, "write-field-id")
			}
		}
	}
	for _, field := range tbl.Schema().Fields() {
		if field.Required {
			indices := record.Schema().FieldIndices(field.Name)
			if len(indices) != 1 || record.Column(indices[0]).NullN() != 0 {
				return failure(ErrInput, "required-column")
			}
		}
	}
	return nil
}
func rewrittenRows(ctx context.Context, s settings, tbl *table.Table, tasks []table.FileScanTask, expression native.BooleanExpression) (arrow.RecordBatch, error) {
	if err := validateRewriteRows(s, tasks); err != nil {
		return nil, err
	}
	scan := tbl.Scan(table.WitMaxConcurrency(1))
	schema, err := scan.Projection()
	if err != nil {
		return nil, err
	}
	wrap, err := prepareRecords(ctx, schema, schema, expression, true)
	if err != nil {
		return nil, err
	}
	arrowSchema, records, err := scan.ReadTasks(ctx, tasks)
	if err != nil {
		return nil, err
	}
	return coalesceRecords(ctx, s, arrowSchema, wrap(records))
}
