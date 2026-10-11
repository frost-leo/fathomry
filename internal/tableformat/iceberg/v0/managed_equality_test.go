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
	"errors"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func TestManagedEqualityDeleteFidelity(t *testing.T) {
	for _, version := range []int{2, 3} {
		for _, partitioned := range []bool{false, true} {
			t.Run(fmt.Sprintf("v%d/partitioned=%t", version, partitioned), func(t *testing.T) {
				_, options := newPeer(t)
				settings := managedSettingsFor(options)
				settings.Permissions.Import = false
				f := bindManagedFixture(t, settings, 1)
				allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer allocator.AssertSize(t, 0)
				input := makeBatch(t, 0, 25, "eq")
				f.run(t, "equality-deletes", func(s *ManagedSession) error {
					tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"), catalog.WithProperties(sdk.Properties{"format-version": strconv.Itoa(version)}))
					if err != nil {
						return err
					}
					if partitioned {
						tx, err := tbl.NewTransaction("")
						if err != nil {
							return err
						}
						if err = tx.EditPartitions(true, []PartitionEdit{{Action: "add", Source: "id", Name: "id_bucket", Transform: sdk.BucketTransform{NumBuckets: 2}}}); err != nil {
							return err
						}
						if _, err = tx.Commit(); err != nil {
							return err
						}
					}
					tx, err := stageManagedAppend(s, "data", input)
					if err != nil {
						return err
					}
					tbl, err = tx.Commit()
					if err != nil {
						return err
					}
					fieldIDs, fieldName, data := []int{2}, "value", `[{"value":null},{"value":"eq-1"}]`
					expected := expectedRows(0, 25, "eq")
					for id := range expected {
						if id%7 == 0 || id == 1 {
							delete(expected, id)
						}
					}
					if partitioned {
						fieldIDs, fieldName, data = []int{1}, "id", `[{"id":2},{"id":4},{"id":6}]`
						expected = expectedRows(0, 25, "eq")
						delete(expected, 2)
						delete(expected, 4)
						delete(expected, 6)
					}
					selected, err := tbl.table.Schema().Select(true, fieldName)
					if err != nil {
						return err
					}
					arrowSchema, err := table.SchemaToArrowSchema(selected, nil, true, false)
					if err != nil {
						return err
					}
					records := func(yield func(arrow.RecordBatch, error) bool) {
						keys, _, err := array.RecordFromJSON(allocator, arrowSchema, strings.NewReader(data))
						if err != nil {
							yield(nil, err)
							return
						}
						defer keys.Release()
						yield(keys, nil)
					}
					tx, err = tbl.NewTransaction("")
					if err != nil {
						return err
					}
					files, err := tx.WriteEqualityDeletes(fieldIDs, records)
					if err != nil {
						return err
					}
					if len(files) == 0 {
						return fmt.Errorf("equality writer produced no delete files")
					}
					for _, file := range files {
						if file.ContentType() != sdk.EntryContentEqDeletes || !slices.Equal(file.EqualityFieldIDs(), fieldIDs) {
							return fmt.Errorf("equality file content/IDs changed")
						}
					}
					if err = tx.RowDelta(nil, files, nil, sdk.Properties{"source": "owned-delete"}); err != nil {
						return err
					}
					tbl, err = tx.Commit()
					if err != nil {
						return err
					}
					scan, err := tbl.Scan(ManagedScanOptions{BatchSize: 3})
					if err != nil {
						return err
					}
					firstDeleteSequence := tbl.table.CurrentSnapshot().SequenceNumber
					managedExactRows(t, scan, expected)
					tx, err = tbl.NewTransaction("")
					if err != nil {
						return err
					}
					if err = tx.CommitEqualityDeletes(fieldIDs, records, sdk.Properties{"source": "native-shortcut"}); err != nil {
						return err
					}
					tbl, err = tx.Commit()
					if err != nil {
						return err
					}
					if tbl.table.CurrentSnapshot().Summary.Properties["source"] != "native-shortcut" {
						return fmt.Errorf("native snapshot properties lost")
					}
					scan, err = tbl.Scan(ManagedScanOptions{BatchSize: 7})
					if err != nil {
						return err
					}
					managedExactRows(t, scan, expected)
					sequence := firstDeleteSequence
					laterID, laterValue := int64(100), "eq-1"
					if partitioned {
						laterID, laterValue = 2, "later-2"
					}
					laterBuilder := array.NewRecordBuilder(allocator, input.Schema())
					laterBuilder.Field(0).(*array.Int64Builder).Append(laterID)
					laterBuilder.Field(1).(*array.StringBuilder).Append(laterValue)
					later := laterBuilder.NewRecordBatch()
					laterBuilder.Release()
					defer later.Release()
					tx, err = stageManagedAppend(s, "data", later)
					if err != nil {
						return err
					}
					tbl, err = tx.Commit()
					if err != nil {
						return err
					}
					expected[laterID] = cell{value: laterValue, valid: true}
					tx, err = tbl.NewTransaction("")
					if err != nil {
						return err
					}
					replacements, err := tx.WriteEqualityDeletes(fieldIDs, records)
					if err != nil {
						return err
					}
					add := make([]table.DeleteFileAddition, len(replacements))
					for index, file := range replacements {
						add[index] = table.DeleteFileAddition{File: file, DataSequenceNumber: sequence}
					}
					if err = tx.ReplaceFilesWithDeleteFiles(nil, nil, files, add, nil); err != nil {
						return err
					}
					tbl, err = tx.Commit()
					if err != nil {
						return err
					}
					scan, err = tbl.Scan(ManagedScanOptions{BatchSize: 3})
					if err != nil {
						return err
					}
					managedExactRows(t, scan, expected)
					return nil
				})
			})
		}
	}
}

func TestManagedEqualityDeleteValidationPrecedesInputAndFileIO(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			peer, options := newPeer(t)
			f := bindManagedFixture(t, managedSettingsFor(options), 1)
			var consumed bool
			result, err := f.outcome(t, "invalid-equality", func(s *ManagedSession) error {
				tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"), catalog.WithProperties(sdk.Properties{"format-version": strconv.Itoa(version)}))
				if err != nil {
					return err
				}
				tx, err := tbl.NewTransaction("")
				if err != nil {
					return err
				}
				peer.mu.Lock()
				before := peer.requests
				peer.mu.Unlock()
				var input iter.Seq2[arrow.RecordBatch, error] = func(func(arrow.RecordBatch, error) bool) { consumed = true }
				fields := []int{1}
				if version == 2 {
					fields = nil
				}
				_, err = tx.WriteEqualityDeletes(fields, input)
				peer.mu.Lock()
				after := peer.requests
				peer.mu.Unlock()
				if before != after || consumed {
					return fmt.Errorf("invalid equality input reached native input/FileIO")
				}
				return err
			})
			if err == nil || version == 2 && !errors.Is(err, table.ErrEmptyEqualityFieldIDs) {
				t.Fatalf("invalid equality request not rejected: %s", causes(err))
			}
			if consumed {
				t.Fatal("rejected equality request consumed caller iterator")
			}
			for _, file := range result.Files() {
				if file.Operation == "put" || file.Operation == "multipart-complete" {
					t.Fatal("invalid equality request wrote data")
				}
			}
		})
	}
}
