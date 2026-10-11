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
	"github.com/apache/arrow-go/v18/arrow/array"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"strconv"
	"testing"
)

func TestManagedMergeOnReadDeleteAndOwnedRowDelta(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			_, options := newPeer(t)
			settings := managedSettingsFor(options)
			settings.Permissions.Import = false
			f := bindManagedFixture(t, settings, 1)
			initial, added := makeBatch(t, 0, 257, "initial"), makeBatch(t, 300, 7, "delta")
			f.run(t, "mor", func(s *ManagedSession) error {
				tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"), catalog.WithProperties(sdk.Properties{"format-version": strconv.Itoa(version), "write.delete.mode": "merge-on-read"}))
				if err != nil {
					return err
				}
				tx, err := tbl.NewTransaction("")
				if err != nil {
					return err
				}
				reader, _ := array.NewRecordReader(initial.Schema(), []arrow.RecordBatch{initial})
				err = tx.Append(reader, nil)
				reader.Release()
				if err != nil {
					return err
				}
				tbl, err = tx.Commit()
				if err != nil {
					return err
				}
				files, err := tbl.WriteRecords(added.Schema(), func(yield func(arrow.RecordBatch, error) bool) { added.Retain(); yield(added, nil) }, table.WithClusteredWrite())
				if err != nil {
					return err
				}
				tx, err = tbl.NewTransaction("")
				if err != nil {
					return err
				}
				if err = tx.RowDelta(files, nil, nil, nil); err != nil {
					return err
				}
				tbl, err = tx.Commit()
				if err != nil {
					return err
				}
				tx, err = tbl.NewTransaction("")
				if err != nil {
					return err
				}
				if err = tx.Delete(sdk.EqualTo(sdk.Reference("id"), int64(1)), nil); err != nil {
					return err
				}
				tbl, err = tx.Commit()
				if err != nil {
					return err
				}
				scan, err := tbl.Scan(ManagedScanOptions{BatchSize: 32})
				if err != nil {
					return err
				}
				tasks, err := scan.PlanFiles()
				if err != nil {
					return err
				}
				deletes := 0
				for _, task := range tasks {
					deletes += len(task.DeleteFiles) + len(task.EqualityDeleteFiles) + len(task.DeletionVectorFiles)
				}
				if deletes == 0 {
					t.Error("merge-on-read silently became copy-on-write")
				}
				expected := expectedRows(0, 257, "initial")
				delete(expected, 1)
				for id, value := range expectedRows(300, 7, "delta") {
					expected[id] = value
				}
				managedExactRows(t, scan, expected)
				return nil
			})
		})
	}
}
