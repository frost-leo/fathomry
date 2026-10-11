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
	"github.com/apache/iceberg-go/catalog"
	"sync"
	"testing"
)

func TestManagedOutputQuotaEarlyBreakAndScopeCleanup(t *testing.T) {
	_, options := newPeer(t)
	settings := managedSettingsFor(options)
	settings.MaxBatchBytes = 2048
	f := bindManagedFixture(t, settings, 1)
	var inputs []arrow.RecordBatch
	for start := 0; start < 1024; start += 32 {
		inputs = append(inputs, makeBatch(t, start, 32, "bounded"))
	}
	var escaped *ManagedArrowTable
	receipt, err := f.client.Use(deadline(t), correlation("outputs"), func(s *ManagedSession) error {
		tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"), catalog.WithProperties(sdk.Properties{"write.parquet.row-group-limit": "32", "write.parquet.page-row-limit": "32"}))
		if err != nil {
			return err
		}
		tx, err := tbl.NewTransaction("")
		if err != nil {
			return err
		}
		reader, _ := array.NewRecordReader(inputs[0].Schema(), inputs)
		err = tx.Append(reader, nil)
		reader.Release()
		if err != nil {
			return err
		}
		tbl, err = tx.Commit()
		if err != nil {
			return err
		}
		scan, err := tbl.Scan(ManagedScanOptions{BatchSize: 16})
		if err != nil {
			return err
		}
		summary, err := scan.Records(func(arrow.RecordBatch) (bool, error) { return false, nil })
		if err != nil {
			return err
		}
		if summary.Complete || summary.Rows != 16 {
			t.Error("early break claimed exhaustion")
		}
		output, summary, err := scan.ToArrowTable()
		if !errors.Is(err, ErrLimit) || output == nil || summary.Complete || summary.Rows == 0 {
			t.Errorf("partial materialization lost: rows=%d err=%v", summary.Rows, err)
		}
		if output != nil {
			visitErr := output.Visit(func(table arrow.Table) error {
				if table.NumRows() != summary.Rows {
					t.Error("partial result rows overstated")
				}
				return nil
			})
			if visitErr != nil {
				t.Error(visitErr)
			}
		}
		escaped = output
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(result.Err(), ErrLimit) {
		t.Fatal("handled output limit lost independent evidence")
	}
	if escaped == nil {
		for _, effect := range result.Outcome.Value.Effects() {
			t.Logf("stage=%s cause=%s", effect.Operation, causes(effect.Err))
		}
		t.Fatal("missing output")
	}
	if err = escaped.Visit(func(arrow.Table) error { return nil }); !errors.Is(err, ErrAuthority) {
		t.Fatal("output escaped scope")
	}
	if err = escaped.Close(); err != nil {
		t.Fatal("repeat cleanup", err)
	}
	delivery, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = delivery.Release()
}
func TestManagedInternalSaturationAndConcurrentRevocation(t *testing.T) {
	_, options := newPeer(t)
	settings := managedSettingsFor(options)
	settings.MaxActive = 1
	f := bindManagedFixture(t, settings, 2)
	entered, release := make(chan struct{}), make(chan struct{})
	receipt, err := f.client.Use(deadline(t), correlation("active"), func(*ManagedSession) error { close(entered); <-release; return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	called := false
	_, err = f.client.Use(deadline(t), correlation("overload"), func(*ManagedSession) error { called = true; return nil })
	if err == nil || called {
		t.Fatal("Internal capacity bypass")
	}
	close(release)
	if _, err = receipt.WaitReleased(deadline(t)); err != nil {
		t.Fatal(err)
	}
	delivery, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = delivery.Release()
	var captured *ManagedSession
	f.run(t, "capture", func(s *ManagedSession) error { captured = s; return nil })
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 20 {
				_, _, err := captured.RemotePlanningCapabilities()
				if !errors.Is(err, ErrAuthority) {
					t.Error("revocation lost")
				}
			}
		}()
	}
	wait.Wait()
}
func TestManagedCancellationCauseIsNotExhaustion(t *testing.T) {
	_, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	cause := errors.New("caller-cause")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	receipt, err := f.client.Use(ctx, correlation("canceled"), func(*ManagedSession) error { cancel(cause); return nil })
	if err != nil {
		t.Fatal(err)
	}
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(result.Err(), cause) || !errors.Is(result.Err(), context.Canceled) {
		t.Fatal("cancel cause lost")
	}
	record, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = record.Release()
}
