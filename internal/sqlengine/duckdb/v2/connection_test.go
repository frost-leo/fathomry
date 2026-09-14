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

package duckdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	sdk "github.com/duckdb/duckdb-go/v2"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestIssue40PersistentOwnership(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "gh40-owned.duckdb")
	options := OptionsV1{Path: path}
	fixture := openFixture(t, options)
	fixture.exec(t, "CREATE TABLE gh40_persistent AS SELECT i AS id FROM range(4097) r(i)")
	fixture.exec(t, "CHECKPOINT")
	duplicate, err := Select(OptionsV1{Name: "gh40-second-owner", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	duplicate = resource.WithLimits(duplicate, LimitsV1(options))
	other, err := resource.Assemble(deadline(t), deadline(t), "gh40-duplicate", duplicate)
	if err == nil {
		other.Close(deadline(t))
		t.Fatal("independent native file owner bypassed sealed configuration")
	}
	if other != nil {
		for _, source := range other.Snapshot().Sources {
			if !source.Released {
				t.Fatal("failed initialization leaked native owner")
			}
		}
	}
	fixture.rows(t, "SELECT 1")
	if err := fixture.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("authorized native file not created", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != "gh40-owned.duckdb" {
			t.Fatal("unexpected persistent/WAL/spill artifact after checkpoint/close")
		}
	}
	reopened := openFixture(t, options)
	rows := reopened.rows(t, "SELECT id FROM gh40_persistent ORDER BY id")
	if len(rows) != 4097 {
		t.Fatal("persistent reopen lost rows")
	}
	for index, row := range rows {
		if row[0] != int64(index) {
			t.Fatal("persistent exact readback mismatch")
		}
	}
	memory := openFixture(t, OptionsV1{})
	if result := memory.run(t, Request{Mode: Query, SQL: "SELECT * FROM gh40_persistent"}); result.Err() == nil {
		t.Fatal("memory instance not isolated")
	}
}

func TestEvidenceSaturationAndBorrowedOwnership(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	fixture.exec(t, "CREATE TABLE gh40_rows (id BIGINT)")
	inbox, err := invocation.NewInbox[Result](1, defaults(OptionsV1{}).evidenceReservation())
	if err != nil {
		t.Fatal(err)
	}
	db, err := Bind(fixture.assembly, fixture.selection, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := db.Run(deadline(t), deadline(t), fault.Correlation{Call: "gh40-held"}, Request{Mode: Query, SQL: "SELECT 1"})
	requireOK(t, settle(t, receipt, err))
	rejected, err := db.Run(deadline(t), deadline(t), fault.Correlation{Call: "gh40-saturated"}, Request{Mode: Execute, SQL: "INSERT INTO gh40_rows VALUES (1)"})
	if !errors.Is(err, invocation.ErrEvidence) || rejected != nil {
		t.Fatal("saturated evidence permitted native work")
	}
	if fixture.rows(t, "SELECT count(*) FROM gh40_rows")[0][0] != int64(0) {
		t.Fatal("saturated call wrote")
	}
	record, err := inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := record.Release(); err != nil {
		t.Fatal(err)
	}
	alias := resource.Borrow("alias", fixture.assembly, fixture.selection)
	borrower, err := resource.Assemble(deadline(t), deadline(t), "borrower", alias)
	if err != nil {
		t.Fatal(err)
	}
	borrowed, err := Bind(borrower, alias, fixture.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if borrowed.access.Info().Configuration.Identity.Name != "gh40" {
		t.Fatal("borrow relabeled source")
	}
	if err := borrower.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fixture.rows(t, "SELECT 42"), [][]any{{int32(42)}}) {
		t.Fatal("borrowed close destroyed native owner")
	}
	if receipt, err := borrowed.Run(deadline(t), deadline(t), fixture.id(), Request{Mode: Query, SQL: "SELECT 42"}); receipt != nil || err == nil {
		t.Fatal("closed borrower retained admission")
	}
}

func TestNativeMemorySettingAndNoSpill(t *testing.T) {
	fixture := openFixture(t, OptionsV1{MemoryBytes: 16 << 20})
	result := fixture.run(t, Request{Mode: Query, SQL: "SELECT i, list(i) FROM range(1000000) r(i) GROUP BY i"})
	var native *sdk.Error
	if !errors.As(result.Err(), &native) || native.Type != sdk.ErrorTypeOutOfMemory {
		t.Fatal("native memory rejection not observed", result.Err())
	}
	conformance.Private(t, result.Err(), "SELECT i, list(i)")
	conformance.Cause[*sdk.Error](t, result.Err(), func(actual *sdk.Error) bool { return actual == native && actual.Type == sdk.ErrorTypeOutOfMemory })
	if !result.Outcome.Value.Snapshot().ConnectionClosed {
		t.Fatal("OOM did not release connection")
	}
	if rows := fixture.rows(t, "SELECT current_setting('temp_directory'),current_setting('memory_limit')"); rows[0][0] != "" {
		t.Fatal("spill not disabled")
	}
	fixture.rows(t, "SELECT 1")
}

func TestConcurrentNativeCallsAndShutdown(t *testing.T) {
	fixture := openFixture(t, OptionsV1{Connections: 4, QueuedCalls: 8})
	fixture.exec(t, "CREATE TABLE gh40_rows (id BIGINT PRIMARY KEY)")
	var workers sync.WaitGroup
	for worker := range 4 {
		workers.Go(func() {
			receipt, err := fixture.database.Run(deadline(t), deadline(t), fault.Correlation{Call: []string{"gh40-a", "gh40-b", "gh40-c", "gh40-d"}[worker]},
				Request{Mode: Append, Table: "gh40_rows", Rows: [][]any{{int64(worker)}}})
			if err != nil {
				t.Error(err)
				return
			}
			result, err := receipt.WaitReleased(deadline(t))
			if err != nil || result.Err() != nil {
				t.Error("concurrent native call failed")
			}
		})
	}
	workers.Wait()
	for range 4 {
		record, err := fixture.inbox.Next(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := record.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if len(fixture.rows(t, "SELECT * FROM gh40_rows")) != 4 {
		t.Fatal("concurrent committed rows missing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := fixture.assembly.Close(ctx); err == nil {
		t.Fatal("canceled owner close unexpectedly completed")
	}
	if err := fixture.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if receipt, err := fixture.database.Run(deadline(t), deadline(t), fixture.id(), Request{Mode: Query, SQL: "SELECT 1"}); receipt != nil || err == nil {
		t.Fatal("closed owner retained native access")
	}
}

func TestCleanupCancellationKeepsEffectUncertainty(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	fixture.exec(t, "CREATE TABLE gh40_rows (id BIGINT)")
	work, stop := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer stop()
	cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	receipt, err := fixture.database.Transaction(work, cleanup, fixture.id(), []Request{
		{Mode: Execute, SQL: "INSERT INTO gh40_rows VALUES (1)"},
		{Mode: Query, SQL: "SELECT sum(a.i*b.i) FROM range(1000000) a(i), range(1000000) b(i)"},
	})
	result := settle(t, receipt, err)
	fixture.drain(t, receipt)
	progress := result.Outcome.Value.Snapshot()
	if !errors.Is(result.Outcome.Primary, context.DeadlineExceeded) || !errors.Is(result.Outcome.Cleanup, context.DeadlineExceeded) ||
		progress.RolledBack || progress.RollbackAttempted || progress.Committed || !progress.ConnectionClosed {
		t.Fatal("cleanup timeout invented rollback acknowledgement")
	}
	if len(fixture.rows(t, "SELECT * FROM gh40_rows")) != 0 {
		t.Fatal("native disconnect left uncommitted rows")
	}
}
