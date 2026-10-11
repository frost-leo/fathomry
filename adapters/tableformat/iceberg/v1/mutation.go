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
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
)

type SchemaEdit struct {
	Action         string
	Path, Relative []string
	Type           sdk.Type
	Doc, Name      string
	Required       bool
	Default        sdk.Literal
	Column         table.ColumnUpdate
	Move           table.MoveOp
	Identifiers    [][]string
	Schema         *sdk.Schema
}
type PartitionEdit struct {
	Action, Source, Name, NewName string
	Transform                     sdk.Transform
}

func (t *Transaction) Append(reader array.RecordReader, props sdk.Properties) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.Append(reader, props), "append")
}
func (t *Transaction) AppendTable(value arrow.Table, batchSize int64, props sdk.Properties) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.AppendTable(value, batchSize, props), "appendtable")
}
func (t *Transaction) Overwrite(reader array.RecordReader, props sdk.Properties, opts ...table.OverwriteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.Overwrite(reader, props, opts...), "overwrite")
}
func (t *Transaction) Delete(filter sdk.BooleanExpression, props sdk.Properties, opts ...table.DeleteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.Delete(filter, props, opts...), "delete")
}

// AddFiles imports native Parquet metadata. ignoreDuplicates is the native
// duplicate-check bypass, not "skip existing files"; true can duplicate rows.
func (t *Transaction) AddFiles(paths []string, props sdk.Properties, ignoreDuplicates bool, opts ...table.AddFilesOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.AddFiles(paths, props, ignoreDuplicates, opts...), "addfiles")
}
func (t *Transaction) AddDataFiles(files []sdk.DataFile, props sdk.Properties, opts ...table.WriteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.AddDataFiles(files, props, opts...), "adddatafiles")
}
func (t *Transaction) RowDelta(rows, deletes, removeDeletes []sdk.DataFile, props sdk.Properties) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.RowDelta(rows, deletes, removeDeletes, props), "rowdelta")
}

// WriteEqualityDeletes borrows each batch until yield returns; callers retain
// their input references. It returns staged delete files, without a Catalog commit.
func (t *Transaction) WriteEqualityDeletes(fieldIDs []int, records iter.Seq2[arrow.RecordBatch, error]) ([]sdk.DataFile, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	files, err := t.native.WriteEqualityDeletes(fieldIDs, records)
	return files, translate(err, "write-equality-deletes")
}

// CommitEqualityDeletes stages the native delete-only RowDelta shortcut. Call
// Commit to submit it; this method alone does not make it visible in the Catalog.
// Like WriteEqualityDeletes, its iterator lends rather than transfers batches.
func (t *Transaction) CommitEqualityDeletes(fieldIDs []int, records iter.Seq2[arrow.RecordBatch, error], props sdk.Properties) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.CommitEqualityDeletes(fieldIDs, records, props), "commit-equality-deletes")
}
func (t *Transaction) SetProperties(props sdk.Properties) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.SetProperties(props), "setproperties")
}
func (t *Transaction) RemoveProperties(keys []string) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.RemoveProperties(keys), "removeproperties")
}
func (t *Transaction) UpgradeFormatVersion(version int) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.UpgradeFormatVersion(version), "upgradeformatversion")
}
func (t *Transaction) RollbackToSnapshot(snapshotID int64) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.RollbackToSnapshot(snapshotID), "rollbacktosnapshot")
}
func (t *Transaction) ReplaceSortOrder(order table.SortOrder) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.ReplaceSortOrder(order), "replacesortorder")
}
func (t *Transaction) AssertRefSnapshotID(branch string) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.AssertRefSnapshotID(branch), "assertrefsnapshotid")
}
func (t *Transaction) AssertDefaultShape() error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.AssertDefaultShape(), "assertdefaultshape")
}

func (t *Transaction) EditSchema(caseSensitive, allowIncompatible bool, mapping sdk.NameMapping, changes []SchemaEdit) error {
	if err := t.valid(); err != nil {
		return err
	}
	input := make([]native.SchemaEdit, len(changes))
	for i, v := range changes {
		input[i] = native.SchemaEdit(v)
	}
	return translate(t.native.EditSchema(caseSensitive, allowIncompatible, mapping, input), "edit-schema")
}
func (t *Transaction) EditPartitions(caseSensitive bool, changes []PartitionEdit) error {
	if err := t.valid(); err != nil {
		return err
	}
	input := make([]native.PartitionEdit, len(changes))
	for i, v := range changes {
		input[i] = native.PartitionEdit(v)
	}
	return translate(t.native.EditPartitions(caseSensitive, input), "edit-partitions")
}
func (t *Table) WriteRecords(schema *arrow.Schema, records iter.Seq2[arrow.RecordBatch, error], options ...table.WriteRecordOption) ([]sdk.DataFile, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.WriteRecords(schema, records, options...)
	return v, translate(err, "write-records")
}
