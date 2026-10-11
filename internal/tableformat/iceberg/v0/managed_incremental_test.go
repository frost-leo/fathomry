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
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func TestManagedIncrementalSnapshotRangesAndChangelogOrder(t *testing.T) {
	_, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	var snapshots []int64
	f.run(t, "history-setup", func(s *ManagedSession) error {
		tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"))
		if err != nil {
			return err
		}
		for index := range 3 {
			record := makeBatch(t, index*10, 3, "history")
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
			snapshots = append(snapshots, tbl.table.CurrentSnapshot().SnapshotID)
		}
		tbl, err = s.UpdateTable(tbl.Identifier(), nil, []table.Update{table.NewSetSnapshotRefUpdate("saved", snapshots[1], table.TagRef, 0, 0, 0)})
		if err != nil {
			return err
		}
		tx, err := tbl.NewTransaction("")
		if err != nil {
			return err
		}
		if err = tx.Delete(sdk.LessThan(sdk.Reference("id"), int64(3)), nil); err != nil {
			return err
		}
		tbl, err = tx.Commit()
		if err == nil {
			snapshots = append(snapshots, tbl.table.CurrentSnapshot().SnapshotID)
		}
		return err
	})
	for _, test := range []struct {
		name    string
		options ManagedScanOptions
		span    IncrementalRange
		starts  []int
	}{
		{"all", ManagedScanOptions{}, IncrementalRange{}, []int{0, 10, 20}},
		{"exclusive", ManagedScanOptions{}, IncrementalRange{From: snapshots[0], To: snapshots[1]}, []int{10}},
		{"inclusive", ManagedScanOptions{}, IncrementalRange{From: snapshots[1], To: snapshots[1], Inclusive: true}, []int{10}},
		{"ref", ManagedScanOptions{Ref: "saved"}, IncrementalRange{From: snapshots[0]}, []int{10}},
		{"snapshot", ManagedScanOptions{SnapshotID: snapshots[1]}, IncrementalRange{From: snapshots[0]}, []int{10}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f.run(t, "range-"+test.name, func(s *ManagedSession) error {
				tbl, err := s.LoadTable([]string{"isolated", "data"})
				if err != nil {
					return err
				}
				tasks, err := tbl.IncrementalAppend(test.options, test.span)
				if err != nil {
					return err
				}
				if len(tasks) != len(test.starts) {
					return fmt.Errorf("incremental tasks=%d, want %d", len(tasks), len(test.starts))
				}
				scan, err := tbl.Scan(ManagedScanOptions{})
				if err != nil {
					return err
				}
				actual := map[int64]cell{}
				summary, err := scan.ReadTasks(tasks, func(record arrow.RecordBatch) (bool, error) {
					ids, values := record.Column(0).(*array.Int64), record.Column(1).(*array.String)
					for row := range ids.Len() {
						if _, found := actual[ids.Value(row)]; found {
							return false, fmt.Errorf("duplicate incremental row")
						}
						actual[ids.Value(row)] = cell{value: values.Value(row), valid: values.IsValid(row)}
					}
					return true, nil
				})
				if err != nil {
					return err
				}
				expected := map[int64]cell{}
				for _, start := range test.starts {
					for id, value := range expectedRows(start, 3, "history") {
						expected[id] = value
					}
				}
				if !summary.Complete || !reflect.DeepEqual(actual, expected) {
					return fmt.Errorf("incremental exact values differ")
				}
				return nil
			})
		})
	}
	readSettings := managedSettingsFor(options)
	readSettings.Permissions = Grants{Read: true}
	readOnly := bindManagedFixture(t, readSettings, 1)
	readOnly.run(t, "incremental-read-without-import", func(s *ManagedSession) error {
		tbl, err := s.LoadTable([]string{"isolated", "data"})
		if err != nil {
			return err
		}
		seen := map[int64]cell{}
		summary, err := tbl.IncrementalAppendRecords(ManagedScanOptions{Ref: "saved", BatchSize: 1}, IncrementalRange{From: snapshots[0]}, func(record arrow.RecordBatch) (bool, error) {
			ids, values := record.Column(0).(*array.Int64), record.Column(1).(*array.String)
			for row := range ids.Len() {
				seen[ids.Value(row)] = cell{value: values.Value(row), valid: values.IsValid(row)}
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		if !summary.Complete || summary.Rows != 3 || !reflect.DeepEqual(seen, expectedRows(10, 3, "history")) {
			return fmt.Errorf("owned incremental append differs")
		}
		changes := make([]ManagedChange, 0, 9)
		ids := make([]int64, 0, 9)
		summary, err = tbl.IncrementalChangelogRecords(ManagedScanOptions{BatchSize: 1}, IncrementalRange{From: snapshots[0]}, func(change ManagedChange, record arrow.RecordBatch) (bool, error) {
			for row := range int(record.NumRows()) {
				changes = append(changes, change)
				ids = append(ids, record.Column(0).(*array.Int64).Value(row))
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		if !summary.Complete || summary.Rows != 9 || !reflect.DeepEqual(ids, []int64{10, 11, 12, 20, 21, 22, 0, 1, 2}) {
			return fmt.Errorf("owned changelog row order differs")
		}
		for index, change := range changes {
			ordinal := index / 3
			operation := table.ChangelogOpInsert
			if ordinal == 2 {
				operation = table.ChangelogOpDelete
			}
			if change.Ordinal != ordinal || change.SnapshotID != snapshots[ordinal+1] || change.Operation != operation {
				return fmt.Errorf("owned changelog identity differs")
			}
		}
		count := int64(0)
		summary, err = tbl.IncrementalChangelogRecords(ManagedScanOptions{BatchSize: 1, Limit: 4}, IncrementalRange{From: snapshots[0]}, func(_ ManagedChange, record arrow.RecordBatch) (bool, error) {
			count += record.NumRows()
			return true, nil
		})
		if err != nil || !summary.Complete || count != 4 {
			return fmt.Errorf("changelog row limit applied per task: %w", err)
		}
		summary, err = tbl.IncrementalChangelogRecords(ManagedScanOptions{BatchSize: 1}, IncrementalRange{From: snapshots[0]}, func(_ ManagedChange, _ arrow.RecordBatch) (bool, error) { return false, nil })
		if err != nil || summary.Complete || summary.Rows != 1 {
			return fmt.Errorf("changelog early break resumed another task: %w", err)
		}
		return nil
	})
	limitedSettings := readSettings
	limitedSettings.MaxRows = 4
	limited := bindManagedFixture(t, limitedSettings, 1)
	var limitedSummary ScanSummary
	var visited int64
	limitedReceipt, err := limited.client.Use(deadline(t), correlation("changelog-quota"), func(s *ManagedSession) error {
		tbl, err := s.LoadTable([]string{"isolated", "data"})
		if err != nil {
			return err
		}
		limitedSummary, err = tbl.IncrementalChangelogRecords(ManagedScanOptions{BatchSize: 1}, IncrementalRange{From: snapshots[0]}, func(_ ManagedChange, record arrow.RecordBatch) (bool, error) {
			visited += record.NumRows()
			return true, nil
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	limitedResult, err := limitedReceipt.WaitReleased(deadline(t))
	if err != nil || !errors.Is(limitedResult.Err(), ErrLimit) || limitedSummary.Complete || limitedSummary.Rows != 4 || visited != 4 {
		t.Fatal("changelog quota reset between tasks")
	}
	delivery, err := limited.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = delivery.Release()
	f.run(t, "changelog", func(s *ManagedSession) error {
		tbl, err := s.LoadTable([]string{"isolated", "data"})
		if err != nil {
			return err
		}
		tasks, err := tbl.IncrementalChangelog(ManagedScanOptions{}, IncrementalRange{From: snapshots[0], To: snapshots[3]})
		if err != nil {
			return err
		}
		if len(tasks) != 3 {
			return fmt.Errorf("changelog tasks=%d want 3", len(tasks))
		}
		for index, task := range tasks {
			operation := table.ChangelogOpInsert
			if index == 2 {
				operation = table.ChangelogOpDelete
			}
			if task.ChangeOrdinal() != index || task.CommitSnapshotID() != snapshots[index+1] || task.Operation() != operation {
				return fmt.Errorf("incorrect changelog ordering at %d", index)
			}
			scan, err := tbl.Scan(ManagedScanOptions{})
			if err != nil {
				return err
			}
			start := (index + 1) * 10
			if index == 2 {
				start = 0
			}
			actual := map[int64]cell{}
			summary, err := scan.ReadTasks([]table.FileScanTask{task.ScanTask()}, func(record arrow.RecordBatch) (bool, error) {
				ids, values := record.Column(0).(*array.Int64), record.Column(1).(*array.String)
				for row := range ids.Len() {
					actual[ids.Value(row)] = cell{value: values.Value(row), valid: values.IsValid(row)}
				}
				return true, nil
			})
			if err != nil {
				return err
			}
			if !summary.Complete || summary.Rows != 3 || !reflect.DeepEqual(actual, expectedRows(start, 3, "history")) {
				return fmt.Errorf("changelog values differ at ordinal %d", index)
			}
		}
		return nil
	})
	for _, test := range []struct {
		name      string
		changelog bool
		span      IncrementalRange
	}{
		{"unknown-append", false, IncrementalRange{From: 1}},
		{"unknown-changelog", true, IncrementalRange{From: 1}},
		{"exclusive-equal-append", false, IncrementalRange{From: snapshots[1], To: snapshots[1]}},
		{"exclusive-equal-changelog", true, IncrementalRange{From: snapshots[1], To: snapshots[1]}},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt, err := f.client.Use(deadline(t), correlation("unknown-range"), func(s *ManagedSession) error {
				tbl, err := s.LoadTable([]string{"isolated", "data"})
				if err != nil {
					return err
				}
				if test.changelog {
					_, err = tbl.IncrementalChangelog(ManagedScanOptions{}, test.span)
				} else {
					_, err = tbl.IncrementalAppend(ManagedScanOptions{}, test.span)
				}
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := receipt.WaitReleased(deadline(t))
			if err != nil || !errors.Is(result.Err(), sdk.ErrInvalidArgument) {
				t.Fatalf("unknown boundary ignored: wait=%v result=%v", err, result.Err())
			}
			delivery, err := f.inbox.Next(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			_ = delivery.Release()
		})
	}
}
