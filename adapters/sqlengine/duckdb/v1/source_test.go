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
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func testSettings() Settings {
	return Settings{Name: "acceptance", Connections: 2, Threads: 1, MemoryBytes: 64 << 20,
		Timeout: 10 * time.Second, CleanupTimeout: time.Second, InputBytes: 1 << 20, ResultBytes: 1 << 20}
}

func testOwner(t testing.TB, selected Settings, capacity int) (*Owner, *adapters.Inbox[Result], *adapters.Runtime) {
	t.Helper()
	return testOwnerWithObserver(t, selected, capacity, nil)
}

func testOwnerWithObserver(t testing.TB, selected Settings, capacity int, observer *adapters.Observer) (*Owner, *adapters.Inbox[Result], *adapters.Runtime) {
	t.Helper()
	policy, err := Recommend(selected)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	if capacity > 0 {
		policy.Evidence.Capacity = capacity
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, openErr := Open(context.Background(), selected, Dependencies{Runtime: runtime, Evidence: inbox, Observer: observer})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if owner != nil {
			if err := owner.Close(ctx); err != nil {
				t.Error("owner cleanup", err)
			}
			if !owner.ShutdownComplete() {
				t.Error("native owner was not released")
			}
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error("runtime cleanup", err)
		}
		for status, _ := inbox.Inspect(); status.Outstanding > 0; status, _ = inbox.Inspect() {
			delivery, err := inbox.NextReleased(ctx)
			if err != nil {
				t.Error("cleanup evidence", err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error("cleanup acknowledgement", err)
				break
			}
		}
	})
	if openErr != nil {
		t.Fatal(openErr)
	}
	return owner, inbox, runtime
}

func receive(t testing.TB, inbox *adapters.Inbox[Result]) adapters.Delivery[Result] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	delivery, err := inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return delivery
}

func ack(t testing.TB, inbox *adapters.Inbox[Result]) Result {
	t.Helper()
	delivery := receive(t, inbox)
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := receipt.Snapshot()
	if !ok || !snapshot.Info().Released {
		t.Fatal("independent evidence was not released")
	}
	value, _ := snapshot.ValueCopy()
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	return value
}

func execute(t testing.TB, owner *Owner, inbox *adapters.Inbox[Result], sql string, args ...any) Progress {
	t.Helper()
	value, err := owner.Client().Execute(context.Background(), context.Background(), sql, args...)
	if err != nil {
		t.Fatal("execute", err)
	}
	ack(t, inbox)
	return value.Snapshot()
}

func query(t testing.TB, owner *Owner, inbox *adapters.Inbox[Result], sql string, args ...any) Result {
	t.Helper()
	value, err := owner.Client().Query(context.Background(), context.Background(), sql, args...)
	if err != nil {
		t.Fatal("query", err)
	}
	if !value.HasData() || !value.Snapshot().ConnectionClosed || len(value.Snapshot().Steps) != 1 || !value.Snapshot().Steps[0].Complete {
		t.Fatal("query failed to retain completed native progress")
	}
	ack(t, inbox)
	return value
}

func TestPersistentOwnershipReopenAndIsolation(t *testing.T) {
	selected := testSettings()
	directory := t.TempDir()
	selected.Path = filepath.Join(directory, "exclusive.duckdb")
	owner, inbox, _ := testOwner(t, selected, 0)
	execute(t, owner, inbox, "CREATE TABLE persistent AS SELECT range AS id FROM range(4097)")
	execute(t, owner, inbox, "CHECKPOINT")
	if err := owner.Close(context.Background()); err != nil || !owner.ShutdownComplete() {
		t.Fatal("source did not release before reopen", err)
	}
	ack(t, inbox)
	if _, err := os.Stat(selected.Path); err != nil {
		t.Fatal("native file was not retained", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 || entries[0].Name() != "exclusive.duckdb" {
		t.Fatal("unexpected persistent, WAL, or spill artifact after cleanup", err)
	}
	reopened, secondInbox, _ := testOwner(t, selected, 0)
	rows := query(t, reopened, secondInbox, "SELECT id FROM persistent ORDER BY id").Snapshot().Steps[0].Rows
	if len(rows) != 4097 {
		t.Fatal("reopen lost committed rows")
	}
	for index, row := range rows {
		if row[0] != int64(index) {
			t.Fatal("reopen changed positional values")
		}
	}
	memory, memoryInbox, _ := testOwner(t, testSettings(), 0)
	if value, err := memory.Client().Query(context.Background(), context.Background(), "SELECT * FROM persistent"); err == nil || value.Snapshot().Steps[0].Submitted {
		t.Fatal("fresh memory instance inherited another source's database")
	}
	ack(t, memoryInbox)
}

func TestTimedOutSourceCloseRetainsActualWorkAndCleanupAuthority(t *testing.T) {
	selected := testSettings()
	owner, inbox, runtime := testOwner(t, selected, 0)
	reader, err := owner.Client().Read(context.Background(), "SELECT range FROM range(10)")
	if err != nil {
		t.Fatal(err)
	}
	before, _ := runtime.Inspect()
	if before.Active != 2 || before.WorkBytes < selected.MemoryBytes {
		t.Fatal("source engine and retained reader were not charged")
	}
	reader.session.family.gate.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			reader.session.family.gate.Unlock()
		}
	}()
	deadline, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := owner.Close(deadline); !errors.Is(err, context.DeadlineExceeded) || owner.ShutdownComplete() {
		t.Fatal("timed-out shutdown discarded actual-work ownership", err)
	}
	if result := owner.Release(deadline); result.Complete || !errors.Is(result.Err, context.DeadlineExceeded) {
		t.Fatal("resource release invented completed cleanup")
	}
	if after, _ := runtime.Inspect(); after.Active != before.Active || after.WorkBytes != before.WorkBytes {
		t.Fatal("timeout released source or reader budgets")
	}
	if state, _ := reader.Receipt().Snapshot(); state.Info().Released {
		t.Fatal("timeout released terminal evidence before actual join")
	}
	reader.session.family.gate.Unlock()
	unlocked = true
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := owner.Close(cleanup); err != nil || !owner.ShutdownComplete() {
		t.Fatal("same owner could not continue shutdown", err)
	}
	terminal, err := reader.Result(cleanup)
	if !errors.Is(err, context.Canceled) || !terminal.Snapshot().ConnectionClosed || terminal.Snapshot().Steps[0].Complete {
		t.Fatal("continued cleanup lost reader cancellation/partial facts", err)
	}
	ack(t, inbox)
	ack(t, inbox)
	if after, _ := runtime.Inspect(); after.Active != 0 || after.WorkBytes != 0 {
		t.Fatal("continued cleanup retained work reservations")
	}
}

func TestIgnoredReturnsAndDroppedObservationKeepIndependentEvidence(t *testing.T) {
	observer, err := adapters.NewObserver(1)
	if err != nil {
		t.Fatal(err)
	}
	owner, inbox, _ := testOwnerWithObserver(t, testSettings(), 0, observer)
	ctx := context.Background()
	source, err := inbox.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := source.Retry(); err != nil {
			t.Error(err)
		}
	}()
	owner.Client().Execute(ctx, ctx, "CREATE SEQUENCE ignored_result")
	if value := ack(t, inbox); !value.HasData() || !value.Snapshot().Steps[0].Submitted || !value.Snapshot().ConnectionClosed {
		t.Fatal("ignored result erased native progress")
	}
	owner.Client().Query(ctx, ctx, "SELECT nextval('ignored_result')")
	sinkFailure := errors.New("fixture sink rejected delivery")
	var sequence uint64
	if err := inbox.DeliverOne(ctx, func(_ context.Context, snapshot adapters.Snapshot[Result]) error {
		value, present := snapshot.ValueCopy()
		if !present || value.Snapshot().Steps[0].Rows[0][0] != int64(1) {
			t.Fatal("independent sink did not receive exact ignored query")
		}
		sequence = value.Attribution().Sequence
		return sinkFailure
	}); !errors.Is(err, sinkFailure) {
		t.Fatal("sink failure lost its original cause", err)
	}
	if err := inbox.DeliverOne(ctx, func(_ context.Context, snapshot adapters.Snapshot[Result]) error {
		value, present := snapshot.ValueCopy()
		if !present || value.Attribution().Sequence != sequence || value.Snapshot().Steps[0].Rows[0][0] != int64(1) {
			t.Fatal("receiver retry replaced original required evidence")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := query(t, owner, inbox, "SELECT nextval('ignored_result')").Snapshot().Steps[0].Rows[0][0]; got != int64(2) {
		t.Fatal("receiver retry replayed SQL")
	}
	if status, err := observer.Inspect(); err != nil || status.Dropped == 0 {
		t.Fatal("lossy-observer control did not actually saturate", err)
	}
	if status, _ := inbox.Inspect(); status.Outstanding != 1 {
		t.Fatal("ignored results leaked or silently discarded evidence")
	}
}

func TestPartialOpenKeepsReachableOwner(t *testing.T) {
	selected := testSettings()
	selected.Path = filepath.Join(t.TempDir(), "exclusive.duckdb")
	original, originalInbox, _ := testOwner(t, selected, 0)
	execute(t, original, originalInbox, "CREATE TABLE retained (id BIGINT)")
	policy, err := Recommend(selected)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := Open(context.Background(), selected, Dependencies{Runtime: runtime, Evidence: inbox})
	if duplicate != nil {
		defer duplicate.Close(context.Background())
	}
	if err == nil || duplicate == nil {
		t.Fatal("conflicting native owner did not retain partial acquisition authority", err)
	}
	if result := duplicate.Release(context.Background()); !result.Complete || !duplicate.ShutdownComplete() {
		t.Fatal("partially opened owner could not confirm native release", result.Err)
	}
	delivery := receive(t, inbox)
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := receipt.Snapshot()
	if snapshot.Primary() == nil || !snapshot.Info().Released {
		t.Fatal("partial Open discarded independently retained failure")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	query(t, original, originalInbox, "SELECT * FROM retained")
	if status, _ := runtime.Inspect(); status.Active != 0 || status.WorkBytes != 0 {
		t.Fatal("partial Open retained public budget after confirmed release")
	}
}
