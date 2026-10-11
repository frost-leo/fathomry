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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"testing"
)

func TestManagedBranchIdentityAndNoopCommit(t *testing.T) {
	_, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	input := makeBatch(t, 0, 7, "main")
	more := makeBatch(t, 10, 3, "branch")
	var mainID, branchID int64
	result := f.run(t, "branch", func(s *ManagedSession) error {
		tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"))
		if err != nil {
			return err
		}
		write := func(tbl *ManagedTable, branch string, record arrow.RecordBatch) (*ManagedTable, error) {
			tx, err := tbl.NewTransaction(branch)
			if err != nil {
				return nil, err
			}
			reader, _ := array.NewRecordReader(record.Schema(), []arrow.RecordBatch{record})
			err = tx.Append(reader, nil)
			reader.Release()
			if err != nil {
				return nil, err
			}
			return tx.Commit()
		}
		tbl, err = write(tbl, "", input)
		if err != nil {
			return err
		}
		mainID = tbl.table.CurrentSnapshot().SnapshotID
		tbl, err = s.UpdateTable(tbl.Identifier(), []table.Requirement{table.AssertTableUUID(tbl.table.Metadata().TableUUID())}, []table.Update{table.NewSetSnapshotRefUpdate("branch", mainID, table.BranchRef, 0, 0, 0), table.NewSetSnapshotRefUpdate("saved", mainID, table.TagRef, 0, 0, 0)})
		if err != nil {
			return err
		}
		tbl, err = write(tbl, "branch", more)
		if err != nil {
			return err
		}
		branchID = tbl.table.SnapshotByName("branch").SnapshotID
		if branchID == mainID || tbl.table.CurrentSnapshot().SnapshotID != mainID {
			t.Error("branch write retargeted main")
		}
		scan, err := tbl.Scan(ManagedScanOptions{Ref: "saved"})
		if err != nil {
			return err
		}
		managedExactRows(t, scan, expectedRows(0, 7, "main"))
		scan, err = tbl.Scan(ManagedScanOptions{Ref: "branch"})
		if err != nil {
			return err
		}
		expected := expectedRows(0, 7, "main")
		for id, value := range expectedRows(10, 3, "branch") {
			expected[id] = value
		}
		managedExactRows(t, scan, expected)
		noop, err := tbl.NewTransaction("")
		if err != nil {
			return err
		}
		_, err = noop.Commit()
		return err
	})
	found := false
	effects := result.Effects()
	for _, effect := range effects {
		if effect.Operation == "commit" && effect.Ref == "branch" {
			found = true
			if effect.BaseSnapshot != mainID || effect.IntendedSnapshot != branchID || effect.CommittedSnapshot != branchID {
				t.Error("wrong branch identity")
			}
		}
	}
	if !found {
		t.Fatal("branch evidence absent")
	}
	last := effects[len(effects)-1]
	if last.Submitted || last.CommittedSnapshot != 0 || last.MetadataLocation != "" {
		t.Fatal("no-op invented a commit identity")
	}
}
func TestManagedUpdateAuthorityBeforeSubmission(t *testing.T) {
	peer, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	f.run(t, "table", func(s *ManagedSession) error {
		_, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"))
		return err
	})
	receipt, err := f.client.Use(deadline(t), correlation("outside"), func(s *ManagedSession) error {
		_, err := s.UpdateTable([]string{"isolated", "data"}, nil, []table.Update{table.NewSetPropertiesUpdate(sdk.Properties{"write.data.path": "s3://fixture/outside/"})})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil || !errors.Is(result.Err(), ErrAuthority) {
		t.Fatal("outside write path not rejected")
	}
	peer.mu.Lock()
	count := peer.commits
	peer.mu.Unlock()
	if count != 0 {
		t.Fatal("authority checked after mutation")
	}
	delivery, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = delivery.Release()
}
