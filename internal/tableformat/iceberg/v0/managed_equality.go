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
	sdk "github.com/apache/iceberg-go"
)

type managedRecordInput struct {
	context  context.Context
	cancel   context.CancelFunc
	settings ManagedSettings
	records  iter.Seq2[arrow.RecordBatch, error]
	transfer bool
	err      error
}

func (input *managedRecordInput) bounded(yield func(arrow.RecordBatch, error) bool) {
	var rows, bytes int64
	for record, err := range input.records {
		if err == nil && input.context.Err() != nil {
			err = errors.Join(input.context.Err(), context.Cause(input.context))
		}
		if err == nil && (record == nil || typedNil(record)) {
			err = failure(ErrProtocol, "nil-record")
		}
		if err == nil {
			count := RecordBytes(record)
			if record.NumRows() < 0 || record.NumRows() > input.settings.MaxRows-rows || count > input.settings.MaxBatchBytes || count > input.settings.MaxIOBytes-bytes {
				err = failure(ErrLimit, "input-record")
			}
			rows += record.NumRows()
			bytes += count
		}
		if err != nil {
			if input.transfer && record != nil && !typedNil(record) {
				record.Release()
			}
			input.err = err
			yield(nil, err)
			return
		}
		if !yield(record, nil) {
			input.cancel()
			return
		}
	}
}

// WriteEqualityDeletes writes owned delete files without committing a snapshot.
// Yielded Arrow batches are borrowed until yield returns; the native equality
// writer retains its queued input. Unlike WriteRecords, ownership does not transfer.
// Format/field/partition validation and equality sequencing remain native.
func (t *ManagedTransaction) WriteEqualityDeletes(fieldIDs []int, records iter.Seq2[arrow.RecordBatch, error]) ([]sdk.DataFile, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "write-equality-deletes", grantWrite, func(ctx context.Context) ([]sdk.DataFile, error) {
		if err := t.valid(); err != nil {
			return nil, err
		}
		if records == nil || len(fieldIDs) > t.scope.source.prepared.settings.MaxListEntries {
			return nil, failure(ErrInput, "equality-deletes")
		}
		child, cancel := context.WithCancel(ctx)
		defer cancel()
		input := &managedRecordInput{context: child, cancel: cancel, settings: t.scope.source.prepared.settings, records: records}
		t.write.Store(true)
		files, err := t.transaction.WriteEqualityDeletes(child, slices.Clone(fieldIDs), input.bounded)
		err = errors.Join(err, input.err, child.Err(), context.Cause(child))
		if err == nil {
			err = t.scope.checkDataFiles(files)
		}
		return files, err
	})
}

// CommitEqualityDeletes stages the native delete-only RowDelta shortcut. It
// does not submit the Catalog commit; Commit still consumes the transaction.
func (t *ManagedTransaction) CommitEqualityDeletes(fieldIDs []int, records iter.Seq2[arrow.RecordBatch, error], props sdk.Properties) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "commit-equality-deletes", grantWrite, func(ctx context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		if records == nil || len(fieldIDs) > t.scope.source.prepared.settings.MaxListEntries {
			return failure(ErrInput, "equality-deletes")
		}
		copied, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		child, cancel := context.WithCancel(ctx)
		defer cancel()
		input := &managedRecordInput{context: child, cancel: cancel, settings: t.scope.source.prepared.settings, records: records}
		t.write.Store(true)
		err = t.transaction.CommitEqualityDeletes(child, slices.Clone(fieldIDs), input.bounded, copied)
		return errors.Join(err, input.err, child.Err(), context.Cause(child))
	})
}
