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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go/catalog"
)

func TestManagedPlanRetainsIndependentScanAndLimit(t *testing.T) {
	_, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	input := makeBatch(t, 0, 13, "planned")
	var escaped *ManagedPlan
	f.run(t, "plans", func(s *ManagedSession) error {
		if _, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data")); err != nil {
			return err
		}
		tx, err := stageManagedAppend(s, "data", input)
		if err != nil {
			return err
		}
		tbl, err := tx.Commit()
		if err != nil {
			return err
		}
		scan, err := tbl.Scan(ManagedScanOptions{Limit: 5, BatchSize: 2})
		if err != nil {
			return err
		}
		first, err := scan.Plan()
		if err != nil {
			return err
		}
		second, err := scan.Plan()
		if err != nil {
			return err
		}
		if first.scan == second.scan || first.scan == scan.scan || second.scan == scan.scan {
			return fmt.Errorf("replanning can retarget an earlier plan")
		}
		if err = scan.Close(); err != nil {
			return err
		}
		for _, plan := range []*ManagedPlan{first, second} {
			summary, err := plan.Records(func(record arrow.RecordBatch) (bool, error) { return true, nil })
			if err != nil || !summary.Complete || summary.Rows != 5 {
				return fmt.Errorf("closed parent or replanning changed retained plan: rows=%d: %w", summary.Rows, err)
			}
		}
		if err = first.Close(); err != nil {
			return err
		}
		if err = first.Close(); err != nil {
			return err
		}
		escaped = second
		return nil
	})
	if _, err := escaped.Records(func(arrow.RecordBatch) (bool, error) { return true, nil }); !errors.Is(err, ErrAuthority) {
		t.Fatal("plan escaped its admitted source generation")
	}
	if err := escaped.Close(); err != nil {
		t.Fatal("repeated cleanup failed", err)
	}
}

func TestManagedScanCloseDoesNotRequireNewOperationCapacity(t *testing.T) {
	_, options := newPeer(t)
	settings := managedSettingsFor(options)
	settings.MaxOperations = 2
	f := bindManagedFixture(t, settings, 1)
	f.run(t, "scan-cleanup", func(s *ManagedSession) error {
		tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"))
		if err != nil {
			return err
		}
		scan, err := tbl.Scan(ManagedScanOptions{})
		if err != nil {
			return err
		}
		if _, err = tbl.Metadata(); !errors.Is(err, ErrLimit) {
			return fmt.Errorf("operation quota control did not saturate")
		}
		if err = scan.Close(); err != nil {
			return err
		}
		return scan.Close()
	})
}
