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
	"context"
	"errors"
	"iter"
	"math"
	"reflect"
	"slices"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Predicate is one scalar comparison. A list means AND; an empty list means all
// rows. Values use decimal text or literal strings, not SQL. Supported operators
// are eq, ne, lt, le, gt, ge, is-null and not-null.
type Predicate struct {
	private
	Column   string
	Operator string
	Value    string
}

func predicates(schema *native.Schema, input []Predicate) (native.BooleanExpression, error) {
	var expression native.BooleanExpression = native.AlwaysTrue{}
	for _, predicate := range input {
		field, exists := schema.FindFieldByName(predicate.Column)
		if !exists {
			return nil, failure(ErrInput, "predicate-column")
		}
		ref := native.Reference(predicate.Column)
		var next native.BooleanExpression
		switch predicate.Operator {
		case "is-null":
			next = native.IsNull(ref)
		case "not-null":
			next = native.NotNull(ref)
		default:
			var literal native.Literal
			var err error
			switch field.Type {
			case native.PrimitiveTypes.Int32, native.PrimitiveTypes.Int64:
				var value int64
				value, err = strconv.ParseInt(predicate.Value, 10, 64)
				literal = native.NewLiteral(value)
			case native.PrimitiveTypes.Float32, native.PrimitiveTypes.Float64:
				var value float64
				value, err = strconv.ParseFloat(predicate.Value, 64)
				if math.IsNaN(value) || math.IsInf(value, 0) {
					return nil, failure(ErrInput, "predicate-number")
				}
				literal = native.NewLiteral(value)
			case native.PrimitiveTypes.String:
				literal = native.NewLiteral(predicate.Value)
			case native.PrimitiveTypes.Bool:
				var value bool
				value, err = strconv.ParseBool(predicate.Value)
				literal = native.NewLiteral(value)
			default:
				return nil, failure(ErrUnsupported, "predicate-type")
			}
			if err != nil {
				return nil, failure(ErrInput, "predicate-value")
			}
			var operation native.Operation
			switch predicate.Operator {
			case "eq":
				operation = native.OpEQ
			case "ne":
				operation = native.OpNEQ
			case "lt":
				operation = native.OpLT
			case "le":
				operation = native.OpLTEQ
			case "gt":
				operation = native.OpGT
			case "ge":
				operation = native.OpGTEQ
			default:
				return nil, failure(ErrUnsupported, "predicate-operator")
			}
			next = native.LiteralPredicate(operation, ref, literal)
		}
		expression = native.NewAnd(expression, next)
	}
	return expression, nil
}
func validPredicates(input []Predicate) bool {
	if len(input) > 16 {
		return false
	}
	for _, predicate := range input {
		if !nameOK(predicate.Column) || len(predicate.Value) > 4096 {
			return false
		}
		switch predicate.Operator {
		case "eq", "ne", "lt", "le", "gt", "ge":
		case "is-null", "not-null":
			if predicate.Value != "" {
				return false
			}
		default:
			return false
		}
	}
	return true
}

type limitedBuffer struct {
	bytes.Buffer
	maximum int
}

func (buffer *limitedBuffer) Write(data []byte) (int, error) {
	if len(data) > buffer.maximum-buffer.Len() {
		return 0, failure(ErrLimit, "batch-bytes")
	}
	return buffer.Buffer.Write(data)
}
func freezeBatches(s settings, batches []arrow.RecordBatch) ([]byte, int64, error) {
	if len(batches) == 0 || len(batches) > 64 {
		return nil, 0, failure(ErrInput, "batch-count")
	}
	var rows, bytesUsed int64
	for _, batch := range batches {
		if batch == nil || reflect.ValueOf(batch).Kind() == reflect.Pointer && reflect.ValueOf(batch).IsNil() ||
			batch.Schema() == nil || batch.NumCols() < 1 || batch.NumCols() > 128 || batch.NumRows() < 0 {
			return nil, 0, failure(ErrInput, "batch")
		}
		if rows > int64(s.MaxRows)-batch.NumRows() {
			return nil, 0, failure(ErrLimit, "batch-rows")
		}
		rows += batch.NumRows()
		names := make(map[string]bool, batch.NumCols())
		for index, column := range batch.Columns() {
			if column == nil || int64(column.Len()) != batch.NumRows() || !nameOK(batch.Schema().Field(index).Name) {
				return nil, 0, failure(ErrInput, "batch-column")
			}
			name := batch.Schema().Field(index).Name
			if names[name] {
				return nil, 0, failure(ErrInput, "duplicate-batch-column")
			}
			names[name] = true
			if !batch.Schema().Field(index).Nullable && column.NullN() != 0 {
				return nil, 0, failure(ErrInput, "nonnullable-batch-column")
			}
			switch column.DataType().ID() {
			case arrow.BOOL, arrow.INT32, arrow.INT64, arrow.FLOAT32, arrow.FLOAT64, arrow.STRING, arrow.BINARY:
			default:
				return nil, 0, failure(ErrUnsupported, "batch-type")
			}
			for _, buffer := range column.Data().Buffers() {
				if buffer != nil {
					bytesUsed += int64(buffer.Len())
					if bytesUsed > int64(s.MaxBatchBytes) {
						return nil, 0, failure(ErrLimit, "batch-buffers")
					}
				}
			}
		}
		if !batch.Schema().Equal(batches[0].Schema()) {
			return nil, 0, failure(ErrInput, "batch-schema")
		}
	}
	if rows == 0 {
		return nil, 0, failure(ErrInput, "empty-write")
	}
	buffer := &limitedBuffer{maximum: s.MaxBatchBytes}
	writer := ipc.NewWriter(buffer, ipc.WithSchema(batches[0].Schema()))
	joined, err := joinBatches(context.Background(), batches[0].Schema(), batches)
	if err != nil {
		_ = writer.Close()
		return nil, 0, err
	}
	defer joined.Release()
	if err := writer.Write(joined); err != nil {
		_ = writer.Close()
		return nil, 0, err
	}
	if err := writer.Close(); err != nil {
		return nil, 0, err
	}
	return buffer.Bytes(), rows, nil
}

// WriteMode selects one finite, single-table transaction.
type WriteMode string

const (
	Append    WriteMode = "append"
	Overwrite WriteMode = "overwrite"
	Delete    WriteMode = "delete"
)

// BatchWrite borrows Batches during Write only; the method serializes them into
// bounded private storage before returning. Callers keep and release their Arrow
// references, and must not mutate them concurrently. Delete has no Batches.
// Destructive all-row predicates require AllRows; no SQL or business retries run.
type BatchWrite struct {
	private
	Mode       WriteMode
	Batches    []arrow.RecordBatch
	Predicates []Predicate
	AllRows    bool
}

// Write freezes bounded input before returning, then stages one table transaction,
// submits it once and reloads. Errors do not compensate acknowledged file writes.
func (client *Client) Write(ctx context.Context, id fault.Correlation, name string, request BatchWrite) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx, name, true); err != nil {
		return nil, err
	}
	if !validPredicates(request.Predicates) {
		return nil, failure(ErrInput, "predicates")
	}
	switch request.Mode {
	case Append:
		if len(request.Predicates) != 0 || request.AllRows {
			return nil, failure(ErrInput, "append-filter")
		}
	case Overwrite, Delete:
		if request.AllRows == (len(request.Predicates) > 0) {
			return nil, failure(ErrInput, "mutation-filter")
		}
	default:
		return nil, failure(ErrUnsupported, "write-mode")
	}
	if request.Mode == Delete && len(request.Batches) != 0 {
		return nil, failure(ErrInput, "delete-batches")
	}
	var input []byte
	var rows int64
	return client.start(ctx, id, "write", func() error {
		request.Predicates = slices.Clone(request.Predicates)
		if request.Mode == Delete {
			return nil
		}
		var err error
		input, rows, err = freezeBatches(client.owner.settings, request.Batches)
		request.Batches = nil
		return err
	}, func(ctx context.Context, state *exchange, data *resultData) error {
		tbl, err := client.load(ctx, state, name)
		if err != nil {
			return err
		}
		expression, err := predicates(tbl.Schema(), request.Predicates)
		if err != nil {
			return err
		}
		tx := tbl.NewTransaction()
		var inputRecord arrow.RecordBatch
		if request.Mode != Delete {
			reader, err := ipc.NewReader(bytes.NewReader(input))
			if err != nil {
				return err
			}
			defer reader.Release()
			if !reader.Next() {
				return failure(ErrInput, "input-record", reader.Err())
			}
			inputRecord = reader.RecordBatch()
			if err := validateWriteRecord(tbl, inputRecord); err != nil {
				return err
			}
		}
		var tasks []table.FileScanTask
		if request.Mode != Append {
			if !tbl.Spec().IsUnpartitioned() {
				return failure(ErrUnsupported, "partitioned-row-mutation")
			}
			tasks, err = tbl.Scan(table.WitMaxConcurrency(1)).PlanFiles(ctx)
			if err != nil {
				return err
			}
			if err = validateTasks(client.owner.settings, tasks); err != nil {
				return err
			}
		}
		err = func() error {
			if request.Mode == Append {
				files, err := writeRecord(ctx, client.owner.settings, tbl, inputRecord)
				if err != nil {
					return err
				}
				return tx.AddDataFiles(ctx, files, nil, table.WithoutDuplicateCheck())
			} else {
				kept, err := rewrittenRows(ctx, client.owner.settings, tbl, tasks, expression)
				if err != nil {
					return err
				}
				if kept != nil {
					defer kept.Release()
				}
				files, err := writeRecord(ctx, client.owner.settings, tbl, kept)
				if err != nil {
					return err
				}
				if inputRecord != nil {
					added, err := writeRecord(ctx, client.owner.settings, tbl, inputRecord)
					if err != nil {
						return err
					}
					files = append(files, added...)
				}
				removed := make([]native.DataFile, len(tasks))
				for index, task := range tasks {
					removed[index] = task.File
				}
				return tx.ReplaceDataFilesWithDataFiles(ctx, removed, files, nil)
			}
		}()
		if err != nil {
			return err
		}
		data.rows = rows
		return client.commit(ctx, state, data, name, tbl, tx)
	})
}
func validateTasks(s settings, tasks []table.FileScanTask) error {
	if len(tasks) > s.MaxFileOps {
		return failure(ErrLimit, "planned-files")
	}
	var total int64
	for _, task := range tasks {
		if len(task.DeleteFiles)+len(task.EqualityDeleteFiles)+len(task.DeletionVectorFiles) > 0 {
			return failure(ErrUnsupported, "delete-files")
		}
		if task.File == nil || task.File.FileFormat() != native.ParquetFile || task.File.Count() < 0 ||
			task.File.Count() > int64(s.MaxRows) || task.File.FileSizeBytes() < 0 || task.File.FileSizeBytes() > int64(s.MaxObjectBytes) {
			return failure(ErrLimit, "planned-file")
		}
		total += task.File.FileSizeBytes()
		if total > s.MaxIOBytes {
			return failure(ErrLimit, "planned-bytes")
		}
	}
	return nil
}

// BatchRead selects a snapshot and bounded projected/filtered rows. SnapshotID
// zero means the reloaded current snapshot. Limit zero selects MaxRows.
// Complete describes exhaustion of this scan, not publication or downstream reads.
type BatchRead struct {
	private
	SnapshotID int64
	Fields     []string
	Predicates []Predicate
	Limit      int
}

// Read consumes native records into a bounded copied IPC result. Predicate and
// projection names belong to the explicitly selected snapshot schema.
func (client *Client) Read(ctx context.Context, id fault.Correlation, name string, request BatchRead) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx, name, false); err != nil {
		return nil, err
	}
	s := client.owner.settings
	if request.SnapshotID < 0 || request.Limit < 0 || request.Limit > s.MaxRows || len(request.Fields) > 128 ||
		!validPredicates(request.Predicates) {
		return nil, failure(ErrInput, "read")
	}
	for _, field := range request.Fields {
		if !nameOK(field) {
			return nil, failure(ErrInput, "projection")
		}
	}
	if request.Limit == 0 {
		request.Limit = s.MaxRows
	}
	return client.start(ctx, id, "read", func() error {
		request.Fields = slices.Clone(request.Fields)
		request.Predicates = slices.Clone(request.Predicates)
		return nil
	}, func(ctx context.Context, state *exchange, data *resultData) error {
		tbl, err := client.load(ctx, state, name)
		if err != nil {
			return err
		}
		if err = capture(data, tbl); err != nil {
			return err
		}
		if request.SnapshotID > 0 {
			snapshot := tbl.SnapshotByID(request.SnapshotID)
			if snapshot == nil {
				return failure(ErrInput, "snapshot")
			}
			if snapshot.SchemaID == nil && len(tbl.Metadata().Schemas()) > 1 {
				return failure(ErrUnsupported, "historical-schema-unknown")
			}
			data.snapshotID = request.SnapshotID
		}
		scan := tbl.Scan(table.WithSnapshotID(request.SnapshotID), table.WitMaxConcurrency(1))
		readSchema, err := scan.Projection()
		if err != nil {
			return err
		}
		projection, err := tbl.Scan(table.WithSnapshotID(request.SnapshotID), table.WithSelectedFields(request.Fields...)).Projection()
		if err != nil {
			return err
		}
		expression, err := predicates(readSchema, request.Predicates)
		if err != nil {
			return err
		}
		wrap, err := prepareRecords(ctx, readSchema, projection, expression, false)
		if err != nil {
			return err
		}
		outputSchema, err := table.SchemaToArrowSchema(projection, nil, false, false)
		if err != nil {
			return err
		}
		tasks, err := scan.PlanFiles(ctx)
		if err != nil {
			return err
		}
		if err = validateTasks(s, tasks); err != nil {
			return err
		}
		_, records, err := scan.ReadTasks(ctx, tasks)
		if err != nil {
			return err
		}
		return collectRecords(data, outputSchema, wrap(records), request.Limit, s.MaxBatchBytes)
	})
}
func collectRecords(data *resultData, schema *arrow.Schema, records iter.Seq2[arrow.RecordBatch, error], limit, maximum int) error {
	buffer := &limitedBuffer{maximum: maximum - 8}
	writer := ipc.NewWriter(buffer, ipc.WithSchema(schema))
	var primary error
	goodBytes := 0
	writeFailed := false
	data.complete = true
	for record, err := range records {
		if err != nil {
			primary = err
			data.complete = false
			break
		}
		if record == nil {
			primary = failure(ErrProtocol, "nil-record")
			data.complete = false
			break
		}
		available := int64(limit) - data.rows
		if record.NumRows() > available {
			selected := record.NewSlice(0, available)
			err = writer.Write(selected)
			selected.Release()
			if err == nil {
				data.rows += available
			}
			data.complete = false
		} else {
			err = writer.Write(record)
			if err == nil {
				data.rows += record.NumRows()
			}
		}
		record.Release()
		if err != nil {
			writeFailed = true
			buffer.Truncate(goodBytes)
			primary = err
			data.complete = false
			break
		}
		goodBytes = buffer.Len()
		if !data.complete {
			break
		}
	}
	// Flat scalar output has no dictionary deltas. On a failed record write,
	// the pinned stream writer can close a rolled-back complete-record prefix;
	// no further record writes are allowed. Reserve its eight-byte EOS upfront.
	buffer.maximum = maximum
	closeErr := writer.Close()
	if closeErr == nil && (!writeFailed || goodBytes > 0) {
		data.ipc = buffer.Bytes()
	}
	if closeErr != nil {
		data.complete = false
	}
	return errors.Join(primary, closeErr)
}

// Compact rewrites the bounded unpartitioned, delete-free current file set into
// one staged rewrite, then commits once and reloads. It does not delete old files,
// expire snapshots, publish per-group progress, or retry conflicts.
func (client *Client) Compact(ctx context.Context, id fault.Correlation, name string) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx, name, true); err != nil {
		return nil, err
	}
	return client.start(ctx, id, "compact", nil, func(ctx context.Context, state *exchange, data *resultData) error {
		tbl, err := client.load(ctx, state, name)
		if err != nil {
			return err
		}
		if !tbl.Spec().IsUnpartitioned() {
			return failure(ErrUnsupported, "partitioned-compaction")
		}
		tasks, err := tbl.Scan(table.WitMaxConcurrency(1)).PlanFiles(ctx)
		if err != nil {
			return err
		}
		if err = validateTasks(client.owner.settings, tasks); err != nil {
			return err
		}
		if err = validateRewriteRows(client.owner.settings, tasks); err != nil {
			return err
		}
		if len(tasks) < 2 {
			data.complete = true
			return capture(data, tbl)
		}
		combined, err := rewrittenRows(ctx, client.owner.settings, tbl, tasks, native.AlwaysFalse{})
		if err != nil {
			return err
		}
		if combined != nil {
			defer combined.Release()
		}
		files, err := writeRecord(ctx, client.owner.settings, tbl, combined)
		if err != nil {
			return err
		}
		removed := make([]native.DataFile, len(tasks))
		for index, task := range tasks {
			removed[index] = task.File
		}
		tx := tbl.NewTransaction()
		rewrite := tx.NewRewrite(nil)
		rewrite.Apply(removed, files, nil)
		err = rewrite.Commit(ctx)
		if err != nil {
			return err
		}
		return client.commit(ctx, state, data, name, tbl, tx)
	})
}

func validateRewriteRows(s settings, tasks []table.FileScanTask) error {
	var rows int64
	for _, task := range tasks {
		if task.File.Count() > int64(s.MaxRows)-rows {
			return failure(ErrLimit, "rewrite-rows")
		}
		rows += task.File.Count()
	}
	return nil
}
