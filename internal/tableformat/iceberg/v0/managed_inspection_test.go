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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
)

func TestManagedInspectRejectsTypedNilBeforeNativeIO(t *testing.T) {
	peer, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	f.run(t, "setup", func(s *ManagedSession) error {
		_, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"))
		return err
	})
	var before, after int
	called := false
	receipt, err := f.client.Use(deadline(t), correlation("nil-allocator"), func(s *ManagedSession) error {
		tbl, err := s.LoadTable([]string{"isolated", "data"})
		if err != nil {
			return err
		}
		var allocator *memory.CheckedAllocator
		peer.mu.Lock()
		before = peer.requests
		peer.mu.Unlock()
		_, err = tbl.Inspect("all-manifests", allocator, func(arrow.RecordBatch) (bool, error) { called = true; return true, nil })
		peer.mu.Lock()
		after = peer.requests
		peer.mu.Unlock()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil || !errors.Is(result.Err(), ErrInput) || called || before != after {
		t.Fatal("typed-nil inspection entered native work", err, result.Err())
	}
	delivery, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = delivery.Release()
}

func TestManagedAllInspectionFamiliesAndRelease(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			_, options := newPeer(t)
			f := bindManagedFixture(t, managedSettingsFor(options), 1)
			allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer allocator.AssertSize(t, 0)
			f.run(t, "inspect", func(s *ManagedSession) error {
				tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"), catalog.WithProperties(sdk.Properties{"format-version": strconv.Itoa(version), "write.delete.mode": "merge-on-read"}))
				if err != nil {
					return err
				}
				for index := range 2 {
					record := makeBatch(t, index*10, 3, "inspect")
					reader, err := array.NewRecordReader(record.Schema(), []arrow.RecordBatch{record})
					if err != nil {
						return err
					}
					tx, err := tbl.NewTransaction("")
					if err == nil {
						err = tx.Append(reader, nil)
					}
					reader.Release()
					if err != nil {
						return err
					}
					tbl, err = tx.Commit()
					if err != nil {
						return err
					}
				}
				tx, err := tbl.NewTransaction("")
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
				scan, err := tbl.Scan(ManagedScanOptions{})
				if err != nil {
					return err
				}
				tasks, err := scan.PlanFiles()
				if err != nil {
					return err
				}
				expectedFiles := map[string]int64{}
				var deletedPath string
				for _, task := range tasks {
					expectedFiles[task.File.FilePath()] = task.File.Count()
					for _, file := range append(task.DeleteFiles, task.DeletionVectorFiles...) {
						expectedFiles[file.FilePath()] = file.Count()
						deletedPath = task.File.FilePath()
					}
				}
				for _, name := range []string{"history", "snapshots", "manifests", "refs", "metadata-log", "entries", "all-entries", "files", "data-files", "delete-files", "all-files", "all-data-files", "all-delete-files", "all-manifests", "partitions", "position-deletes"} {
					var rows []map[string]json.RawMessage
					summary, err := tbl.Inspect(name, allocator, func(record arrow.RecordBatch) (bool, error) {
						raw, err := record.MarshalJSON()
						if err != nil {
							return false, err
						}
						var batch []map[string]json.RawMessage
						if err = json.Unmarshal(raw, &batch); err != nil {
							return false, err
						}
						rows = append(rows, batch...)
						return true, nil
					})
					if err != nil {
						return fmt.Errorf("%s: %w", name, err)
					}
					if !summary.Complete || summary.Rows != int64(len(rows)) || len(rows) == 0 {
						return fmt.Errorf("%s: incomplete or empty inspection", name)
					}
					switch name {
					case "history", "snapshots":
						if len(rows) != 3 {
							return fmt.Errorf("%s: snapshot history size", name)
						}
						seen := map[int64]bool{}
						for _, row := range rows {
							var id int64
							if err := json.Unmarshal(row["snapshot_id"], &id); err != nil {
								return err
							}
							if seen[id] || tbl.table.SnapshotByID(id) == nil {
								return fmt.Errorf("%s: snapshot identity", name)
							}
							seen[id] = true
						}
					case "refs":
						if len(rows) != 1 || string(rows[0]["name"]) != "\"main\"" {
							return fmt.Errorf("reference identity")
						}
					case "files", "data-files", "delete-files", "all-files", "all-data-files", "all-delete-files":
						seen := map[string]bool{}
						for _, row := range rows {
							var path string
							var count int64
							if err := json.Unmarshal(row["file_path"], &path); err != nil {
								return err
							}
							if err := json.Unmarshal(row["record_count"], &count); err != nil {
								return err
							}
							expected, found := expectedFiles[path]
							if !found || expected != count {
								return fmt.Errorf("%s: file identity/count", name)
							}
							seen[path] = true
						}
						if name == "files" && len(seen) != len(expectedFiles) {
							return fmt.Errorf("files: missing exact file")
						}
					case "position-deletes":
						if len(rows) != 1 || string(rows[0]["pos"]) != "1" || string(rows[0]["file_path"]) != strconv.Quote(deletedPath) {
							return fmt.Errorf("position-delete target/position")
						}
					}
					allocator.AssertSize(t, 0)
					early, err := tbl.Inspect(name, allocator, func(arrow.RecordBatch) (bool, error) { return false, nil })
					if err != nil || early.Complete {
						return fmt.Errorf("%s: early break became complete: %w", name, err)
					}
					allocator.AssertSize(t, 0)
				}
				return nil
			})
		})
	}
}
