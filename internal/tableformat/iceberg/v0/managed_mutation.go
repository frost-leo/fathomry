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
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/variant"
	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

type managedReader struct {
	array.RecordReader
	ctx         context.Context
	settings    ManagedSettings
	rows, bytes int64
	err         error
}

func (r *managedReader) Next() bool {
	if r.err != nil {
		return false
	}
	if r.ctx.Err() != nil {
		r.err = errors.Join(r.ctx.Err(), context.Cause(r.ctx))
		return false
	}
	if !r.RecordReader.Next() {
		return false
	}
	record := r.RecordReader.RecordBatch()
	if record == nil || typedNil(record) {
		r.err = failure(ErrProtocol, "nil-record")
		return false
	}
	count := RecordBytes(record)
	if record.NumRows() < 0 || record.NumRows() > r.settings.MaxRows-r.rows || count > r.settings.MaxBatchBytes || count > r.settings.MaxIOBytes-r.bytes {
		r.err = failure(ErrLimit, "input-record")
		return false
	}
	r.rows += record.NumRows()
	r.bytes += count
	return true
}
func (r *managedReader) Err() error { return errors.Join(r.err, r.RecordReader.Err()) }

func (t *ManagedTransaction) Append(reader array.RecordReader, props native.Properties) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "append", grantWrite, func(ctx context.Context) error {
		if reader == nil || typedNil(reader) {
			return failure(ErrInput, "reader")
		}
		if err := t.valid(); err != nil {
			return err
		}
		copied, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		bounded := &managedReader{RecordReader: reader, ctx: ctx, settings: t.scope.source.prepared.settings}
		t.write.Store(true)
		return errors.Join(t.transaction.Append(ctx, bounded, copied), bounded.Err())
	})
}
func (t *ManagedTransaction) AppendTable(value arrow.Table, batchSize int64, props native.Properties) error {
	if err := t.valid(); err != nil {
		return err
	}
	if value == nil || typedNil(value) || batchSize < 1 {
		return failure(ErrInput, "arrow-table")
	}
	return managedDo(t.scope, "append-table", grantWrite, func(ctx context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		copied, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		reader := array.NewTableReader(value, batchSize)
		defer reader.Release()
		bounded := &managedReader{RecordReader: reader, ctx: ctx, settings: t.scope.source.prepared.settings}
		t.write.Store(true)
		return errors.Join(t.transaction.Append(ctx, bounded, copied), bounded.Err())
	})
}
func (t *ManagedTransaction) Overwrite(reader array.RecordReader, props native.Properties, opts ...table.OverwriteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "overwrite", grantWrite, func(ctx context.Context) error {
		if reader == nil || typedNil(reader) {
			return failure(ErrInput, "reader")
		}
		if err := t.valid(); err != nil {
			return err
		}
		for _, opt := range opts {
			if opt == nil {
				return failure(ErrInput, "option")
			}
		}
		copied, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		bounded := &managedReader{RecordReader: reader, ctx: ctx, settings: t.scope.source.prepared.settings}
		options := append(slices.Clone(opts), table.WithOverwriteConcurrency(t.scope.source.prepared.settings.Workers))
		t.write.Store(true)
		return errors.Join(t.transaction.Overwrite(ctx, bounded, copied, options...), bounded.Err())
	})
}
func (t *ManagedTransaction) Delete(filter native.BooleanExpression, props native.Properties, opts ...table.DeleteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "delete", grantWrite, func(ctx context.Context) error {
		if filter == nil || typedNil(filter) {
			return failure(ErrInput, "filter")
		}
		if err := t.valid(); err != nil {
			return err
		}
		for _, opt := range opts {
			if opt == nil {
				return failure(ErrInput, "option")
			}
		}
		copied, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		options := append(slices.Clone(opts), table.WithDeleteConcurrency(t.scope.source.prepared.settings.Workers))
		t.write.Store(true)
		return t.transaction.Delete(ctx, filter, copied, options...)
	})
}
func (f *ManagedSession) checkDataFiles(files []native.DataFile) error {
	if len(files) > f.source.prepared.settings.MaxListEntries {
		return failure(ErrLimit, "data-files")
	}
	for _, file := range files {
		if file == nil || typedNil(file) {
			return failure(ErrInput, "data-file")
		}
		if err := f.location(file.FilePath()); err != nil {
			return err
		}
		if file.FileSizeBytes() < 0 || file.FileSizeBytes() > f.source.prepared.settings.MaxObjectBytes || file.Count() < 0 {
			return failure(ErrLimit, "data-file")
		}
	}
	return nil
}

func (f *ManagedSession) checkAdoption(files []native.DataFile) error {
	if err := f.checkDataFiles(files); err != nil {
		return err
	}
	if f.allowed(grantImport) {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, file := range files {
		owned := false
		for _, effect := range f.data.files {
			if effect.URI == file.FilePath() && effect.Effect == Acknowledged && (effect.Operation == "put" || effect.Operation == "multipart-complete") {
				owned = true
				break
			}
		}
		if !owned {
			return failure(ErrAuthority, "file-adoption")
		}
	}
	return nil
}
func (t *ManagedTransaction) AddFiles(paths []string, props native.Properties, ignoreDuplicates bool, opts ...table.AddFilesOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "add-files", grantImport, func(ctx context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		if len(paths) > t.scope.source.prepared.settings.MaxListEntries {
			return failure(ErrLimit, "import-files")
		}
		for _, path := range paths {
			if err := t.scope.location(path); err != nil {
				return err
			}
		}
		for _, opt := range opts {
			if opt == nil {
				return failure(ErrInput, "option")
			}
		}
		props, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		options := append(slices.Clone(opts), table.WithAddFilesConcurrency(t.scope.source.prepared.settings.Workers))
		t.write.Store(true)
		return t.transaction.AddFiles(ctx, slices.Clone(paths), props, ignoreDuplicates, options...)
	})
}
func (t *ManagedTransaction) AddDataFiles(files []native.DataFile, props native.Properties, opts ...table.WriteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "add-data-files", grantImport, func(ctx context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		if err := t.scope.checkDataFiles(files); err != nil {
			return err
		}
		for _, opt := range opts {
			if opt == nil {
				return failure(ErrInput, "option")
			}
		}
		props, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		t.write.Store(true)
		return t.transaction.AddDataFiles(ctx, slices.Clone(files), props, slices.Clone(opts)...)
	})
}
func (t *ManagedTransaction) RowDelta(rows, deletes, removeDeletes []native.DataFile, props native.Properties) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "row-delta", grantWrite, func(ctx context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		for _, files := range [][]native.DataFile{rows, deletes, removeDeletes} {
			if err := t.scope.checkDataFiles(files); err != nil {
				return err
			}
		}
		if err := t.scope.checkAdoption(rows); err != nil {
			return err
		}
		if err := t.scope.checkAdoption(deletes); err != nil {
			return err
		}
		props, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		t.write.Store(true)
		return t.transaction.NewRowDelta(props).AddRows(slices.Clone(rows)...).AddDeletes(slices.Clone(deletes)...).RemoveDeletes(slices.Clone(removeDeletes)...).Commit(ctx)
	})
}

// WriteRecords writes staged Parquet files without submitting a snapshot. Each
// yielded input reference transfers to the native writer; callers retaining a
// batch must Retain it first. The input iterator must return after early stop.
func (t *ManagedTable) WriteRecords(schema *arrow.Schema, records iter.Seq2[arrow.RecordBatch, error], opts ...table.WriteRecordOption) ([]native.DataFile, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "write-records", grantWrite, func(ctx context.Context) ([]native.DataFile, error) {
		if schema == nil || records == nil {
			return nil, failure(ErrInput, "records")
		}
		for _, opt := range opts {
			if opt == nil {
				return nil, failure(ErrInput, "option")
			}
		}
		child, cancel := context.WithCancel(ctx)
		defer cancel()
		input := &managedRecordInput{context: child, cancel: cancel, settings: t.scope.source.prepared.settings, records: records, transfer: true}
		var result []native.DataFile
		for file, err := range table.WriteRecords(child, t.table, schema, input.bounded, slices.Clone(opts)...) {
			if err != nil {
				cancel()
				return result, err
			}
			if len(result) >= t.scope.source.prepared.settings.MaxListEntries {
				cancel()
				return result, failure(ErrLimit, "write-files")
			}
			if err = t.scope.checkDataFiles([]native.DataFile{file}); err != nil {
				cancel()
				return result, err
			}
			result = append(result, file)
		}
		return result, errors.Join(input.err, child.Err(), context.Cause(child))
	})
}

type SchemaEdit struct {
	Action         string
	Path, Relative []string
	Type           native.Type
	Doc, Name      string
	Required       bool
	Default        native.Literal
	Column         table.ColumnUpdate
	Move           table.MoveOp
	Identifiers    [][]string
	Schema         *native.Schema
}

func cloneManagedLiteral(value native.Literal) (native.Literal, error) {
	if typedNil(value) {
		return nil, failure(ErrInput, "literal")
	}
	switch value := value.(type) {
	case native.BinaryLiteral:
		return native.BinaryLiteral(slices.Clone(value)), nil
	case native.FixedLiteral:
		return native.FixedLiteral(slices.Clone(value)), nil
	case native.VariantLiteral:
		item := variant.Value(value)
		copy, err := variant.New(slices.Clone(item.Metadata().Bytes()), slices.Clone(item.Bytes()))
		if err != nil {
			return nil, err
		}
		return native.VariantLiteral(copy), nil
	case native.GeoLiteral:
		data, err := value.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return native.LiteralFromBytes(value.Type(), slices.Clone(data))
	}
	return value, nil
}

func (f *ManagedSession) freezeSchemaEdit(change SchemaEdit) (SchemaEdit, error) {
	copyType := func(value native.Type) (native.Type, error) {
		if value == nil || typedNil(value) {
			return nil, failure(ErrInput, "field-type")
		}
		field := native.NestedField{ID: 1, Name: "field", Type: value}
		var frozen native.NestedField
		if err := f.cloneData(field, &frozen); err != nil {
			return nil, err
		}
		return frozen.Type, nil
	}
	var err error
	if change.Type != nil {
		change.Type, err = copyType(change.Type)
		if err != nil {
			return SchemaEdit{}, err
		}
	}
	change.Default, err = cloneManagedLiteral(change.Default)
	if err != nil {
		return SchemaEdit{}, err
	}
	if change.Column.FieldType.Valid {
		change.Column.FieldType.Val, err = copyType(change.Column.FieldType.Val)
		if err != nil {
			return SchemaEdit{}, err
		}
	}
	if change.Column.WriteDefault.Valid {
		change.Column.WriteDefault.Val, err = cloneManagedLiteral(change.Column.WriteDefault.Val)
		if err != nil {
			return SchemaEdit{}, err
		}
	}
	if change.Schema != nil {
		var frozen native.Schema
		if err = f.cloneData(change.Schema, &frozen); err != nil {
			return SchemaEdit{}, err
		}
		change.Schema = &frozen
	}
	return change, nil
}

func (t *ManagedTransaction) EditSchema(caseSensitive, allowIncompatible bool, mapping native.NameMapping, changes []SchemaEdit) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "edit-schema", grantMetadata, func(context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		if len(changes) > t.scope.source.prepared.settings.MaxListEntries {
			return failure(ErrLimit, "schema-edits")
		}
		var copied native.NameMapping
		if err := t.scope.cloneData(mapping, &copied); err != nil {
			return err
		}
		update := t.transaction.UpdateSchema(caseSensitive, allowIncompatible, table.WithNameMapping(copied))
		for _, change := range changes {
			change, err := t.scope.freezeSchemaEdit(change)
			if err != nil {
				return err
			}
			switch change.Action {
			case "add":
				update.AddColumn(slices.Clone(change.Path), change.Type, change.Doc, change.Required, change.Default)
			case "delete":
				update.DeleteColumn(slices.Clone(change.Path))
			case "update":
				update.UpdateColumn(slices.Clone(change.Path), change.Column)
			case "rename":
				update.RenameColumn(slices.Clone(change.Path), change.Name)
			case "move":
				update.MoveColumn(change.Move, slices.Clone(change.Path), slices.Clone(change.Relative))
			case "identifiers":
				paths := make([][]string, len(change.Identifiers))
				for i, path := range change.Identifiers {
					paths[i] = slices.Clone(path)
				}
				update.SetIdentifierField(paths)
			case "union":
				update.UnionByNameWith(change.Schema)
			default:
				return failure(ErrInput, "schema-edit")
			}
		}
		return update.Commit()
	})
}

type PartitionEdit struct {
	Action, Source, Name, NewName string
	Transform                     native.Transform
}

func (t *ManagedTransaction) EditPartitions(caseSensitive bool, changes []PartitionEdit) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "edit-partitions", grantMetadata, func(context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		if len(changes) > t.scope.source.prepared.settings.MaxListEntries {
			return failure(ErrLimit, "partition-edits")
		}
		update := t.transaction.UpdateSpec(caseSensitive)
		for _, change := range changes {
			switch change.Action {
			case "add":
				update.AddField(change.Source, change.Transform, change.Name)
			case "identity":
				update.AddIdentity(change.Source)
			case "remove":
				update.RemoveField(change.Name)
			case "rename":
				update.RenameField(change.Name, change.NewName)
			default:
				return failure(ErrInput, "partition-edit")
			}
		}
		return update.Commit()
	})
}
func (t *ManagedTransaction) SetProperties(props native.Properties) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "setproperties", grantMetadata, func(context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		copy, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		return t.transaction.SetProperties(copy)
	})
}
func (t *ManagedTransaction) RemoveProperties(keys []string) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "removeproperties", grantMetadata, func(context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		return t.transaction.RemoveProperties(slices.Clone(keys))
	})
}
func (t *ManagedTransaction) UpgradeFormatVersion(version int) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "upgradeformatversion", grantMetadata, func(context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		return t.transaction.UpgradeFormatVersion(version)
	})
}
func (t *ManagedTransaction) RollbackToSnapshot(snapshotID int64) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "rollbacktosnapshot", grantMetadata, func(context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		return t.transaction.RollbackToSnapshot(snapshotID)
	})
}
func (t *ManagedTransaction) ReplaceSortOrder(order table.SortOrder) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "replacesortorder", grantMetadata, func(context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		if order.Len() > t.scope.source.prepared.settings.MaxListEntries {
			return failure(ErrLimit, "sort-fields")
		}
		var copy table.SortOrder
		if err := t.scope.cloneData(order, &copy); err != nil {
			return err
		}
		return t.transaction.ReplaceSortOrder(copy)
	})
}
func (t *ManagedTransaction) AssertRefSnapshotID(branch string) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "assertrefsnapshotid", grantMetadata, func(context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		return t.transaction.AssertRefSnapshotID(branch)
	})
}
func (t *ManagedTransaction) AssertDefaultShape() error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "assert-default-shape", grantMetadata, func(context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		return t.transaction.AssertDefaultShape()
	})
}
