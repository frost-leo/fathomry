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
	"github.com/apache/arrow-go/v18/arrow"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

func (t *Transaction) OverwriteTable(value arrow.Table, batchSize int64, props sdk.Properties, options ...table.OverwriteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.OverwriteTable(value, batchSize, props, options...), "overwritetable")
}
func (t *Transaction) ReplaceDataFiles(remove, add []string, props sdk.Properties) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.ReplaceDataFiles(remove, add, props), "replacedatafiles")
}
func (t *Transaction) ReplaceDataFilesWithDataFiles(remove, add []sdk.DataFile, props sdk.Properties, options ...table.WriteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.ReplaceDataFilesWithDataFiles(remove, add, props, options...), "replacedatafileswithdatafiles")
}
func (t *Transaction) ReplaceFiles(remove, add, removeDeletes []sdk.DataFile, props sdk.Properties, options ...table.WriteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.ReplaceFiles(remove, add, removeDeletes, props, options...), "replacefiles")
}
func (t *Transaction) ReplaceFilesWithDeleteFiles(remove, add, removeDeletes []sdk.DataFile, addDeletes []table.DeleteFileAddition, props sdk.Properties, options ...table.WriteOption) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.ReplaceFilesWithDeleteFiles(remove, add, removeDeletes, addDeletes, props, options...), "replacefileswithdeletefiles")
}
func (t *Transaction) Close() error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.Close(), "close")
}
