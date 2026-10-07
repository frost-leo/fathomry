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
	"reflect"
	"testing"
	"time"
)

func TestFiniteModesAndIndependentEffects(t *testing.T) {
	owner, inbox, _ := testOwner(t, testSettings(), 0)
	ctx := context.Background()
	client := owner.Client()
	created := execute(t, owner, inbox, "CREATE TABLE records (id BIGINT PRIMARY KEY, value VARCHAR DEFAULT 'default')")
	if len(created.Steps) != 1 || !created.Steps[0].Prepared || !created.Steps[0].Submitted || created.Steps[0].Executions != 1 || !created.ConnectionClosed {
		t.Fatal("DDL lost finite stage evidence")
	}
	batch, err := client.ExecuteMany(ctx, ctx, "INSERT INTO records VALUES (?,?)", [][]any{{int64(1), "one"}, {int64(2), "two"}})
	if err != nil {
		t.Fatal(err)
	}
	progress := batch.Snapshot()
	if !progress.Transaction || !progress.Began || !progress.CommitAttempted || !progress.Committed || progress.RollbackAttempted || !progress.ConnectionClosed || !progress.Steps[0].Prepared || progress.Steps[0].Executions != 2 || progress.Steps[0].RowsChanged != 2 || !progress.Steps[0].RowsChangedKnown {
		t.Fatal("prepare-once batch progress incomplete")
	}
	if !reflect.DeepEqual(ack(t, inbox).Snapshot(), progress) {
		t.Fatal("independent batch evidence changed")
	}
	appended, err := client.Append(ctx, ctx, "", "records", []string{"id"}, [][]any{{int64(3)}})
	if err != nil {
		t.Fatal(err)
	}
	progress = appended.Snapshot()
	stage := progress.Steps[0]
	if !progress.Committed || stage.AcceptedRows != 1 || !stage.FlushAttempted || !stage.Flushed || stage.FlushedRows != 1 || !stage.AppenderClosed || !stage.AppenderCloseSucceeded {
		t.Fatal("column-subset Appender stage evidence incomplete")
	}
	ack(t, inbox)
	failed, err := client.Append(ctx, ctx, "", "records", nil, [][]any{{int64(4), "buffered"}, {int64(1), "duplicate"}})
	progress = failed.Snapshot()
	if err == nil || progress.Committed || !progress.RolledBack || !progress.RollbackAttempted || progress.Steps[0].AcceptedRows != 2 || progress.Steps[0].FlushedRows != 0 || !progress.Steps[0].AppenderClosed {
		t.Fatal("constraint failure erased buffered/rollback evidence")
	}
	ack(t, inbox)
	want := [][]any{{int64(1), "one"}, {int64(2), "two"}, {int64(3), "default"}}
	if rows := query(t, owner, inbox, "SELECT id,value FROM records ORDER BY id").Snapshot().Steps[0].Rows; !reflect.DeepEqual(rows, want) {
		t.Fatal("failed Appender published a partial batch")
	}
	failed, err = client.ExecuteMany(ctx, ctx, "INSERT INTO records VALUES (?,?)", [][]any{{int64(4), "four"}, {int64(1), "duplicate"}})
	progress = failed.Snapshot()
	if err == nil || !progress.RolledBack || progress.Committed || progress.Steps[0].Executions != 1 || progress.Steps[0].RowsChanged != 1 {
		t.Fatal("failed parameter batch lost acknowledged prefix")
	}
	ack(t, inbox)
	if rows := query(t, owner, inbox, "SELECT id,value FROM records ORDER BY id").Snapshot().Steps[0].Rows; !reflect.DeepEqual(rows, want) {
		t.Fatal("failed prepared batch published a partial batch")
	}
	mixed, err := client.Transaction(ctx, ctx, []Request{
		{Mode: Append, Table: "records", Columns: []string{"id"}, Rows: [][]any{{int64(4)}}},
		{Mode: ExecuteMany, SQL: "INSERT INTO records VALUES (?,?)", Rows: [][]any{{int64(5), "five"}}},
		{Mode: Query, SQL: "SELECT id FROM records ORDER BY id"},
		{Mode: Execute, SQL: "UPDATE records SET value='changed' WHERE id=4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	progress = mixed.Snapshot()
	if !progress.Committed || len(progress.Steps) != 4 || len(progress.Steps[2].Rows) != 5 || !progress.Steps[2].Complete || progress.Steps[3].RowsChanged != 1 {
		t.Fatal("mixed transaction lost ordered visibility or progress")
	}
	ack(t, inbox)
	if got := query(t, owner, inbox, "SELECT value FROM records WHERE id=4").Snapshot().Steps[0].Rows[0][0]; got != "changed" {
		t.Fatal("independent read did not observe committed transaction")
	}
	for _, request := range []Request{{Mode: Append, Table: "records"}, {Mode: ExecuteMany, SQL: "INSERT INTO records VALUES (?,?)"}} {
		value, err := client.Run(ctx, ctx, request)
		if err != nil || !value.Snapshot().Committed || value.Snapshot().Steps[0].Executions != 0 {
			t.Fatal("empty batch did not preserve zero-work commit", err)
		}
		if request.Mode == Append && (!value.Snapshot().Steps[0].FlushAttempted || !value.Snapshot().Steps[0].Flushed) {
			t.Fatal("empty Appender flush acknowledgement absent")
		}
		ack(t, inbox)
	}
	empty := query(t, owner, inbox, "SELECT id FROM records WHERE false").Snapshot().Steps[0]
	if !empty.Complete || len(empty.Columns) != 1 || empty.Rows == nil || len(empty.Rows) != 0 {
		t.Fatal("successful empty result conflated with unavailable output")
	}
	for _, sql := range []string{"ALTER TABLE records ADD COLUMN note VARCHAR", "ANALYZE records", "VACUUM records", "CHECKPOINT", "DROP TABLE records"} {
		execute(t, owner, inbox, sql)
	}
}

func TestSafetyRefusalsPreserveEffects(t *testing.T) {
	owner, inbox, _ := testOwner(t, testSettings(), 0)
	ctx := context.Background()
	execute(t, owner, inbox, "CREATE TABLE effects (id BIGINT PRIMARY KEY)")
	for _, sql := range []string{"INSERT INTO effects VALUES (1); INSERT INTO effects VALUES (2)", "EXPLAIN ANALYZE INSERT INTO effects VALUES (3)", "BEGIN TRANSACTION", "INSTALL httpfs", "SET enable_external_access=true"} {
		if _, err := owner.Client().Execute(ctx, ctx, sql); err == nil {
			t.Fatal("unsafe statement accepted")
		}
		ack(t, inbox)
	}
	if rows := query(t, owner, inbox, "SELECT * FROM effects").Snapshot().Steps[0].Rows; len(rows) != 0 {
		t.Fatal("rejected statements produced effects")
	}
	result, err := owner.Client().Query(ctx, ctx, "INSERT INTO effects VALUES (7) RETURNING [id]")
	if !errors.Is(err, ErrUnsupported) || !result.Snapshot().Steps[0].Submitted || result.Snapshot().Steps[0].Executions != 1 || result.Snapshot().Steps[0].Complete {
		t.Fatal("unsupported post-DML result lost submitted effect evidence", err)
	}
	ack(t, inbox)
	if query(t, owner, inbox, "SELECT id FROM effects").Snapshot().Steps[0].Rows[0][0] != int64(7) {
		t.Fatal("unsupported RETURNING hid independently committed mutation")
	}
	execute(t, owner, inbox, "CREATE SEQUENCE effects_sequence")
	result, err = owner.Client().Transaction(ctx, ctx, []Request{{Mode: Query, SQL: "SELECT nextval('effects_sequence')"}, {Mode: Execute, SQL: "INSERT INTO effects VALUES (7)"}})
	if err == nil || !result.Snapshot().RolledBack || result.Snapshot().Steps[0].Rows[0][0] != int64(1) {
		t.Fatal("rollback lost sequence observation")
	}
	ack(t, inbox)
	if query(t, owner, inbox, "SELECT nextval('effects_sequence')").Snapshot().Steps[0].Rows[0][0] != int64(2) {
		t.Fatal("rollback was incorrectly treated as absence of all effects")
	}
}

func TestCancellationAfterFlushAndAggregateResultLimit(t *testing.T) {
	selected := testSettings()
	selected.MaxRows = 2
	owner, inbox, _ := testOwner(t, selected, 0)
	ctx := context.Background()
	execute(t, owner, inbox, "CREATE TABLE buffered (id BIGINT)")
	deadline, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	result, err := owner.Client().Transaction(deadline, ctx, []Request{
		{Mode: Append, Table: "buffered", Rows: [][]any{{int64(1)}}},
		{Mode: Query, SQL: "SELECT sum(a.i*b.i) FROM range(1000000) a(i),range(1000000) b(i)"},
	})
	cancel()
	progress := result.Snapshot()
	if !errors.Is(err, context.DeadlineExceeded) || len(progress.Steps) != 2 || progress.Steps[0].FlushedRows != 1 || !progress.RolledBack || progress.Committed || !progress.ConnectionClosed {
		t.Fatal("cancellation erased flushed-but-not-committed evidence", err)
	}
	ack(t, inbox)
	if len(query(t, owner, inbox, "SELECT * FROM buffered").Snapshot().Steps[0].Rows) != 0 {
		t.Fatal("canceled transaction committed table effects")
	}
	result, err = owner.Client().Transaction(ctx, ctx, []Request{
		{Mode: Execute, SQL: "INSERT INTO buffered VALUES (2)"},
		{Mode: Query, SQL: "SELECT range FROM range(2)"},
		{Mode: Query, SQL: "SELECT range FROM range(1)"},
	})
	progress = result.Snapshot()
	if !errors.Is(err, ErrLimit) || !progress.RolledBack || progress.Committed || len(progress.Steps) != 3 || len(progress.Steps[1].Rows) != 2 || !progress.Steps[2].Limited {
		t.Fatal("transaction reset result envelope or erased prefix", err)
	}
	ack(t, inbox)
	if len(query(t, owner, inbox, "SELECT * FROM buffered").Snapshot().Steps[0].Rows) != 0 {
		t.Fatal("limited transaction committed table mutation")
	}
}
