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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"slices"
)

func (t *ManagedTransaction) OverwriteTable(value arrow.Table, batchSize int64, props sdk.Properties, options ...table.OverwriteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "overwrite-table", grantWrite, func(ctx context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		if value == nil || typedNil(value) || batchSize < 1 {
			return failure(ErrInput, "arrow-table")
		}
		for _, option := range options {
			if option == nil {
				return failure(ErrInput, "option")
			}
		}
		copied, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		reader := array.NewTableReader(value, batchSize)
		defer reader.Release()
		bounded := &managedReader{RecordReader: reader, ctx: ctx, settings: t.scope.source.prepared.settings}
		options = append(slices.Clone(options), table.WithOverwriteConcurrency(t.scope.source.prepared.settings.Workers))
		t.write.Store(true)
		return errors.Join(t.transaction.Overwrite(ctx, bounded, copied, options...), bounded.Err())
	})
}
func (t *ManagedTransaction) ReplaceDataFiles(remove, add []string, props sdk.Properties) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "replace-data-files", grantImport, func(ctx context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		if len(remove)+len(add) > t.scope.source.prepared.settings.MaxListEntries {
			return failure(ErrLimit, "replace-files")
		}
		for _, paths := range [][]string{remove, add} {
			for _, path := range paths {
				if err := t.scope.location(path); err != nil {
					return err
				}
			}
		}
		copied, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		t.write.Store(true)
		return t.transaction.ReplaceDataFiles(ctx, slices.Clone(remove), slices.Clone(add), copied)
	})
}
func (t *ManagedTransaction) ReplaceDataFilesWithDataFiles(remove, add []sdk.DataFile, props sdk.Properties, options ...table.WriteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "replacedatafileswithdatafiles", grantWrite, func(ctx context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		for _, files := range [][]sdk.DataFile{remove, add} {
			if err := t.scope.checkDataFiles(files); err != nil {
				return err
			}
		}
		if err := t.scope.checkAdoption(add); err != nil {
			return err
		}

		for _, option := range options {
			if option == nil {
				return failure(ErrInput, "option")
			}
		}
		copied, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		t.write.Store(true)
		return t.transaction.ReplaceDataFilesWithDataFiles(ctx, slices.Clone(remove), slices.Clone(add), copied, slices.Clone(options)...)
	})
}
func (t *ManagedTransaction) ReplaceFiles(remove, add, removeDeletes []sdk.DataFile, props sdk.Properties, options ...table.WriteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "replacefiles", grantWrite, func(ctx context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		for _, files := range [][]sdk.DataFile{remove, add, removeDeletes} {
			if err := t.scope.checkDataFiles(files); err != nil {
				return err
			}
		}
		if err := t.scope.checkAdoption(add); err != nil {
			return err
		}

		for _, option := range options {
			if option == nil {
				return failure(ErrInput, "option")
			}
		}
		copied, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		t.write.Store(true)
		return t.transaction.ReplaceFiles(ctx, slices.Clone(remove), slices.Clone(add), slices.Clone(removeDeletes), copied, slices.Clone(options)...)
	})
}
func (t *ManagedTransaction) ReplaceFilesWithDeleteFiles(remove, add, removeDeletes []sdk.DataFile, addDeletes []table.DeleteFileAddition, props sdk.Properties, options ...table.WriteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "replacefileswithdeletefiles", grantWrite, func(ctx context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		for _, files := range [][]sdk.DataFile{remove, add, removeDeletes} {
			if err := t.scope.checkDataFiles(files); err != nil {
				return err
			}
		}
		if err := t.scope.checkAdoption(add); err != nil {
			return err
		}
		for _, addition := range addDeletes {
			if err := t.scope.checkAdoption([]sdk.DataFile{addition.File}); err != nil {
				return err
			}
		}
		for _, option := range options {
			if option == nil {
				return failure(ErrInput, "option")
			}
		}
		copied, err := t.scope.boundedProperties(props)
		if err != nil {
			return err
		}
		t.write.Store(true)
		return t.transaction.ReplaceFilesWithDeleteFiles(ctx, slices.Clone(remove), slices.Clone(add), slices.Clone(removeDeletes), slices.Clone(addDeletes), copied, slices.Clone(options)...)
	})
}

// Close consumes staging locally. It does not commit, compensate, or delete files.
func (t *ManagedTransaction) Close() error {
	if t == nil || t.scope == nil || t.transaction == nil {
		return failure(ErrInput, "transaction")
	}
	if !t.scope.gate.TryLock() {
		return failure(ErrAuthority, "transaction-busy")
	}
	defer t.scope.gate.Unlock()
	t.consumed.Store(true)
	t.transaction.MarkCommitted()
	return nil
}
