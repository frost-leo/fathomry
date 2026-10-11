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
	"github.com/apache/iceberg-go/table/compaction"
	"testing"
)

func TestManagedEvolutionCompactionNativeFidelity(t *testing.T) {
	_, options := newPeer(t)
	settings := managedSettingsFor(options)
	settings.MaxHandles = 256
	f := bindManagedFixture(t, settings, 1)
	first, later := makeBatch(t, 0, 257, "first"), makeBatch(t, 300, 8, "later")
	f.run(t, "compaction", func(s *ManagedSession) error {
		tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"))
		if err != nil {
			return err
		}
		appendRecord := func(input arrow.RecordBatch) error {
			tx, err := tbl.NewTransaction("")
			if err != nil {
				return err
			}
			reader, _ := array.NewRecordReader(input.Schema(), []arrow.RecordBatch{input})
			err = tx.Append(reader, nil)
			reader.Release()
			if err != nil {
				return err
			}
			tbl, err = tx.Commit()
			return err
		}
		if err = appendRecord(first); err != nil {
			return err
		}
		tx, err := tbl.NewTransaction("")
		if err != nil {
			return err
		}
		if err = tx.EditSchema(true, false, nil, []SchemaEdit{{Action: "add", Path: []string{"extra"}, Type: sdk.PrimitiveTypes.Int64}}); err != nil {
			return err
		}
		if err = tx.EditPartitions(true, []PartitionEdit{{Action: "add", Source: "id", Name: "id_bucket", Transform: sdk.BucketTransform{NumBuckets: 4}}}); err != nil {
			return err
		}
		order, err := table.NewSortOrder(1, []table.SortField{{SourceIDs: []int{1}, Transform: sdk.IdentityTransform{}, Direction: table.SortDirection("asc"), NullOrder: table.NullsFirst}})
		if err != nil {
			return err
		}
		if err = tx.ReplaceSortOrder(order); err != nil {
			return err
		}
		tbl, err = tx.Commit()
		if err != nil {
			return err
		}
		if err = appendRecord(later); err != nil {
			return err
		}
		cfg := compaction.DefaultConfig()
		cfg.TargetFileSizeBytes = 1 << 20
		cfg.MinFileSizeBytes = 3 << 18
		cfg.MaxFileSizeBytes = 2 << 20
		cfg.MinInputFiles = 1
		plan, err := tbl.PlanCompaction(cfg)
		if err != nil {
			return err
		}
		groups := make([]table.CompactionTaskGroup, len(plan.Groups))
		for i, group := range plan.Groups {
			groups[i] = table.CompactionTaskGroup{PartitionKey: group.PartitionKey, Tasks: group.Tasks, TotalSizeBytes: group.TotalSizeBytes}
		}
		rewrite, err := tbl.RewriteDataFiles(groups, table.RewriteDataFilesOptions{GroupOptions: []table.CompactionGroupOption{table.WithCompactionArrowBatchSize(128), table.WithCompactionTargetFileSize(1 << 20)}})
		if err != nil {
			return err
		}
		tbl = rewrite.Table
		scan, err := tbl.Scan(ManagedScanOptions{Fields: []string{"id", "value"}})
		if err != nil {
			return err
		}
		expected := expectedRows(0, 257, "first")
		for id, value := range expectedRows(300, 8, "later") {
			expected[id] = value
		}
		managedExactRows(t, scan, expected)
		return nil
	})
}
