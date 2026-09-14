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
	"math"
	"reflect"
	"testing"
	"time"
)

func TestBatchAtomicityAndStages(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	fixture.exec(t, "CREATE TABLE gh40_rows (id BIGINT PRIMARY KEY, value VARCHAR DEFAULT 'default')")
	good := requireOK(t, fixture.run(t, Request{Mode: Append, Table: "gh40_rows", Columns: []string{"id"}, Rows: [][]any{{int64(1)}, {int64(2)}}}))
	if good.Steps[0].FlushedRows != 2 || !good.Committed {
		t.Fatal("missing acknowledged append")
	}
	bad := fixture.run(t, Request{Mode: Append, Table: "gh40_rows", Rows: [][]any{{int64(3), "buffered"}, {int64(1), "duplicate"}}})
	progress := bad.Outcome.Value.Snapshot()
	if bad.Err() == nil || !progress.RolledBack || progress.Committed || progress.Steps[0].AcceptedRows != 2 ||
		progress.Steps[0].FlushedRows != 0 || !progress.Steps[0].AppenderClosed {
		t.Fatal("failed flush evidence missing")
	}
	if !reflect.DeepEqual(fixture.rows(t, "SELECT id,value FROM gh40_rows ORDER BY id"), [][]any{{int64(1), "default"}, {int64(2), "default"}}) {
		t.Fatal("batch failure lost earlier commit or published partial batch")
	}
	result := fixture.run(t, Request{Mode: ExecuteMany, SQL: "INSERT INTO gh40_rows VALUES (?,?)", Rows: [][]any{{3, "third"}, {4, "fourth"}, {1, "duplicate"}}})
	progress = result.Outcome.Value.Snapshot()
	if result.Err() == nil || !progress.RolledBack || progress.Steps[0].Executions != 2 || progress.Steps[0].RowsChanged != 2 {
		t.Fatal("prepared batch partial progress erased")
	}
	if len(fixture.rows(t, "SELECT * FROM gh40_rows")) != 2 {
		t.Fatal("prepared batch not atomic")
	}
	good = requireOK(t, fixture.run(t, Request{Mode: ExecuteMany, SQL: "INSERT INTO gh40_rows VALUES (?,?)", Rows: [][]any{{3, "third"}, {4, "fourth"}}}))
	if !good.Committed || !good.Steps[0].Prepared || good.Steps[0].Executions != 2 {
		t.Fatal("prepared statement batch did not succeed")
	}
	for _, request := range []Request{
		{Mode: Append, Table: "gh40_rows"}, {Mode: ExecuteMany, SQL: "INSERT INTO gh40_rows VALUES (?,?)"},
	} {
		if !requireOK(t, fixture.run(t, request)).Committed {
			t.Fatal("empty batch missing transaction evidence")
		}
	}
	empty := requireOK(t, fixture.run(t, Request{Mode: Append, Table: "gh40_rows"})).Steps[0]
	if !empty.FlushAttempted || !empty.Flushed || empty.FlushedRows != 0 {
		t.Fatal("empty successful flush has no explicit acknowledgement")
	}
}

func TestAppenderNarrowingRejectsBeforeBuffering(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	fixture.exec(t, "CREATE TABLE gh40_rows (signed TINYINT, unsigned UBIGINT)")
	for _, row := range [][]any{{int64(128), uint64(1)}, {int64(-129), uint64(1)}, {int64(1), int64(-1)}, {1.25, uint64(1)}} {
		result := fixture.run(t, Request{Mode: Append, Table: "gh40_rows", Rows: [][]any{row}})
		if !errors.Is(result.Err(), ErrUnsupported) || result.Outcome.Value.Snapshot().Steps[0].AcceptedRows != 0 {
			t.Fatal("lossy native narrowing accepted")
		}
	}
	requireOK(t, fixture.run(t, Request{Mode: Append, Table: "gh40_rows", Rows: [][]any{{int64(-128), uint64(math.MaxUint64)}, {int64(127), uint64(0)}}}))
	if !reflect.DeepEqual(fixture.rows(t, "SELECT * FROM gh40_rows ORDER BY signed"), [][]any{{int8(-128), uint64(math.MaxUint64)}, {int8(127), uint64(0)}}) {
		t.Fatal("integer boundary values changed")
	}
}

func TestTransactionIncludesAppendQueryAndRollback(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	fixture.exec(t, "CREATE TABLE gh40_rows (id BIGINT PRIMARY KEY)")
	requests := []Request{
		{Mode: Append, Table: "gh40_rows", Rows: [][]any{{int64(1)}, {int64(2)}}},
		{Mode: Query, SQL: "SELECT id FROM gh40_rows ORDER BY id"},
		{Mode: Execute, SQL: "INSERT INTO gh40_rows VALUES (1)"},
	}
	receipt, err := fixture.database.Transaction(deadline(t), deadline(t), fixture.id(), requests)
	result := settle(t, receipt, err)
	fixture.drain(t, receipt)
	progress := result.Outcome.Value.Snapshot()
	if result.Err() == nil || !progress.RolledBack || progress.Committed || len(progress.Steps) != 3 ||
		progress.Steps[0].FlushedRows != 2 || !reflect.DeepEqual(progress.Steps[1].Rows, [][]any{{int64(1)}, {int64(2)}}) {
		t.Fatal("same-connection transaction evidence changed")
	}
	if len(fixture.rows(t, "SELECT * FROM gh40_rows")) != 0 {
		t.Fatal("explicit rollback failed")
	}
	requests = requests[:2]
	receipt, err = fixture.database.Transaction(deadline(t), deadline(t), fixture.id(), requests)
	requireOK(t, settle(t, receipt, err))
	fixture.drain(t, receipt)
	if len(fixture.rows(t, "SELECT * FROM gh40_rows")) != 2 {
		t.Fatal("transaction commit failed")
	}
}

func TestCancellationAfterFlushRollsBack(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	fixture.exec(t, "CREATE TABLE gh40_rows (id BIGINT)")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	receipt, err := fixture.database.Transaction(ctx, deadline(t), fixture.id(), []Request{
		{Mode: Append, Table: "gh40_rows", Rows: [][]any{{int64(1)}}},
		{Mode: Query, SQL: "SELECT sum(a.i*b.i) FROM range(1000000) a(i),range(1000000) b(i)"},
	})
	result := settle(t, receipt, err)
	fixture.drain(t, receipt)
	progress := result.Outcome.Value.Snapshot()
	if !errors.Is(result.Err(), context.DeadlineExceeded) || !progress.RolledBack || progress.Committed ||
		len(progress.Steps) != 2 || progress.Steps[0].FlushedRows != 1 {
		t.Fatal("canceled transaction lost earlier flush evidence")
	}
	if len(fixture.rows(t, "SELECT * FROM gh40_rows")) != 0 {
		t.Fatal("canceled transaction committed")
	}
}
