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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
)

func TestManagedImportAndReplacementExactRows(t *testing.T) {
	for _, route := range []string{"paths", "data-files", "replace-paths", "replace-data-files", "replace-files"} {
		t.Run(route, func(t *testing.T) {
			_, options := newPeer(t)
			f := bindManagedFixture(t, managedSettingsFor(options), 1)
			initial, later := makeBatch(t, 0, 7, "initial"), makeBatch(t, 100, 9, "later")
			f.run(t, "import-replace", func(s *ManagedSession) error {
				tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"))
				if err != nil {
					return err
				}
				files, err := tbl.WriteRecords(initial.Schema(), func(yield func(arrow.RecordBatch, error) bool) { initial.Retain(); yield(initial, nil) })
				if err != nil {
					return err
				}
				paths := make([]string, len(files))
				for index, file := range files {
					paths[index] = file.FilePath()
				}
				tx, err := tbl.NewTransaction("")
				if err != nil {
					return err
				}
				if route == "data-files" {
					err = tx.AddDataFiles(files, nil)
				} else {
					err = tx.AddFiles(paths, nil, false)
				}
				if err != nil {
					return err
				}
				tbl, err = tx.Commit()
				if err != nil {
					return err
				}
				scan, err := tbl.Scan(ManagedScanOptions{})
				if err != nil {
					return err
				}
				managedExactRows(t, scan, expectedRows(0, 7, "initial"))
				if route == "paths" {
					tx, err = tbl.NewTransaction("")
					if err != nil {
						return err
					}
					if err = tx.AddFiles(paths, nil, true); err != nil {
						return err
					}
					tbl, err = tx.Commit()
					if err != nil {
						return err
					}
					scan, err = tbl.Scan(ManagedScanOptions{})
					if err != nil {
						return err
					}
					counts := map[int64]int{}
					expected := expectedRows(0, 7, "initial")
					summary, err := scan.Records(func(record arrow.RecordBatch) (bool, error) {
						ids, values := record.Column(0).(*array.Int64), record.Column(1).(*array.String)
						for row := range ids.Len() {
							id := ids.Value(row)
							want, found := expected[id]
							if !found || want.valid != values.IsValid(row) || want.valid && want.value != values.Value(row) {
								return false, fmt.Errorf("duplicate-check bypass changed values")
							}
							counts[id]++
						}
						return true, nil
					})
					if err != nil {
						return err
					}
					if !summary.Complete || summary.Rows != 14 || len(counts) != 7 {
						return fmt.Errorf("native duplicate-check bypass semantics changed")
					}
					for _, count := range counts {
						if count != 2 {
							return fmt.Errorf("duplicate multiset differs")
						}
					}
					return nil
				}
				if route == "data-files" {
					return nil
				}
				added, err := tbl.WriteRecords(later.Schema(), func(yield func(arrow.RecordBatch, error) bool) { later.Retain(); yield(later, nil) })
				if err != nil {
					return err
				}
				tx, err = tbl.NewTransaction("")
				if err != nil {
					return err
				}
				switch route {
				case "replace-paths":
					addPaths := make([]string, len(added))
					for index, file := range added {
						addPaths[index] = file.FilePath()
					}
					err = tx.ReplaceDataFiles(paths, addPaths, nil)
				case "replace-data-files":
					err = tx.ReplaceDataFilesWithDataFiles(files, added, nil)
				case "replace-files":
					err = tx.ReplaceFiles(files, added, nil, nil)
				default:
					return fmt.Errorf("unknown fixture route")
				}
				if err != nil {
					return err
				}
				tbl, err = tx.Commit()
				if err != nil {
					return err
				}
				scan, err = tbl.Scan(ManagedScanOptions{BatchSize: 2})
				if err != nil {
					return err
				}
				managedExactRows(t, scan, expectedRows(100, 9, "later"))
				return nil
			})
		})
	}
}
