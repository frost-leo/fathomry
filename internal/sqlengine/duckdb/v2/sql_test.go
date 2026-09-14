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

func TestSQLBoundaries(t *testing.T) {
	fixture := openFixture(t, OptionsV1{MaxRows: 3, ResultBytes: 1024})
	fixture.exec(t, "CREATE TABLE gh40_rows (id BIGINT)")
	for _, query := range []string{
		"INSERT INTO gh40_rows VALUES (1); SELECT 42",
		"SELECT 1; SELECT 2",
	} {
		result := fixture.run(t, Request{Mode: Query, SQL: query})
		if result.Err() == nil {
			t.Fatal("multiple statements accepted")
		}
	}
	if !reflect.DeepEqual(fixture.rows(t, "SELECT count(*) FROM gh40_rows"), [][]any{{int64(0)}}) {
		t.Fatal("preparation executed earlier statement")
	}
	if got := fixture.rows(t, "SELECT ';' AS value /* ; */;"); !reflect.DeepEqual(got, [][]any{{";"}}) {
		t.Fatal("native parser mishandled quoted semicolon")
	}
	for _, query := range []string{
		"SELECT MAP([from_hex('00'),from_hex('01')], [1,2])",
		"SELECT MAP([UUID '00000000-0000-0000-0000-000000000001',UUID '00000000-0000-0000-0000-000000000002'],[1,2])",
		"SELECT MAP(['one','two'],[1,2])",
		"SELECT [1,2]", "SELECT {'nested': MAP([from_hex('00'),from_hex('01')], [1,2])}",
		"SELECT '9007199254740993'::JSON",
	} {
		result := fixture.run(t, Request{Mode: Query, SQL: query})
		if !errors.Is(result.Err(), ErrUnsupported) {
			t.Fatal("unsafe/unqualified decoder was entered", result.Err())
		}
		progress := result.Outcome.Value.Snapshot()
		if !progress.ConnectionClosed || len(progress.Steps[0].Rows) != 0 {
			t.Fatal("unsupported result leaked data/resource")
		}
	}
	for _, query := range []string{
		"BEGIN", "COMMIT", "ROLLBACK", "SET threads=2", "ATTACH ':memory:' AS other", "INSTALL iceberg",
		"COPY gh40_rows TO '/not-authorized-gh40.csv'",
	} {
		result := fixture.run(t, Request{Mode: Execute, SQL: query})
		if result.Err() == nil {
			t.Fatal("unsupported capability accepted")
		}
	}
	for _, query := range []string{"SELECT * FROM read_csv('/not-authorized-gh40.csv')", "SELECT * FROM read_parquet('https://invalid.example/gh40.parquet')"} {
		if result := fixture.run(t, Request{Mode: Query, SQL: query}); result.Err() == nil {
			t.Fatal("external access accepted")
		}
	}
	if rows := fixture.rows(t, "SELECT i FROM range(3) r(i) ORDER BY i"); len(rows) != 3 {
		t.Fatal("exact row bound rejected")
	}
	result := fixture.run(t, Request{Mode: Query, SQL: "SELECT i FROM range(4) r(i) ORDER BY i"})
	progress := result.Outcome.Value.Snapshot()
	if !errors.Is(result.Err(), ErrLimit) || len(progress.Steps[0].Rows) != 3 || progress.Steps[0].Complete || !progress.Steps[0].Limited {
		t.Fatal("partial result limit evidence changed")
	}
	result = fixture.run(t, Request{Mode: Query, SQL: "SELECT i,repeat('x',700) FROM range(3) r(i) ORDER BY i"})
	if !errors.Is(result.Err(), ErrLimit) || len(result.Outcome.Value.Snapshot().Steps[0].Rows) != 1 {
		t.Fatal("byte bound did not retain exact prefix")
	}
	empty := requireOK(t, fixture.run(t, Request{Mode: Query, SQL: "SELECT id FROM gh40_rows"})).Steps[0]
	if !empty.Complete || len(empty.Rows) != 0 || len(empty.Columns) != 1 {
		t.Fatal("empty and incomplete results conflated")
	}
}

func TestQueryCancellationAndReuse(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	receipt, err := fixture.database.Run(ctx, deadline(t), fixture.id(), Request{Mode: Query, SQL: "SELECT sum(a.i*b.i) FROM range(1000000) a(i), range(1000000) b(i)"})
	result := settle(t, receipt, err)
	fixture.drain(t, receipt)
	if !errors.Is(result.Err(), context.DeadlineExceeded) || !result.Outcome.Value.Snapshot().ConnectionClosed {
		t.Fatal("cancellation lost ownership/error")
	}
	t.Logf("stable cancellation elapsed=%s (cooperative, not a hard deadline)", time.Since(start))
	if !reflect.DeepEqual(fixture.rows(t, "SELECT 42::BIGINT"), [][]any{{int64(42)}}) {
		t.Fatal("later native user was interrupted")
	}
}

func TestExecutionPreservesCallerCancellationCause(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	cause := errors.New("gh40-caller-cancellation")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	timer := time.AfterFunc(20*time.Millisecond, func() { cancel(cause) })
	defer timer.Stop()
	receipt, err := fixture.database.Run(ctx, deadline(t), fixture.id(), Request{Mode: Query, SQL: "SELECT sum(a.i*b.i) FROM range(1000000) a(i), range(1000000) b(i)"})
	result := settle(t, receipt, err)
	fixture.drain(t, receipt)
	if !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), cause) {
		t.Fatal("caller cancellation cause was lost during native execution")
	}
}

func TestResultTypeFailureCanFollowCommittedDML(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	fixture.exec(t, "CREATE TABLE gh40_effects (id BIGINT)")
	result := fixture.run(t, Request{Mode: Query, SQL: "INSERT INTO gh40_effects VALUES (9) RETURNING MAP([from_hex('00'),from_hex('01')],[1,2])"})
	progress := result.Outcome.Value.Snapshot()
	if !errors.Is(result.Err(), ErrUnsupported) || progress.Transaction || !progress.Steps[0].Submitted || progress.Steps[0].Executions != 1 {
		t.Fatal("post-execution decoding failure lost effect evidence")
	}
	if !reflect.DeepEqual(fixture.rows(t, "SELECT id FROM gh40_effects"), [][]any{{int64(9)}}) {
		t.Fatal("autocommit DML effect was not independently observed")
	}
}

func TestTransactionSharesResultEnvelope(t *testing.T) {
	fixture := openFixture(t, OptionsV1{MaxRows: 3})
	fixture.exec(t, "CREATE TABLE gh40_rows (id BIGINT)")
	receipt, err := fixture.database.Transaction(deadline(t), deadline(t), fixture.id(), []Request{
		{Mode: Execute, SQL: "INSERT INTO gh40_rows VALUES (1),(2)"},
		{Mode: Query, SQL: "SELECT id FROM gh40_rows ORDER BY id"},
		{Mode: Query, SQL: "SELECT id FROM gh40_rows ORDER BY id"},
	})
	result := settle(t, receipt, err)
	fixture.drain(t, receipt)
	progress := result.Outcome.Value.Snapshot()
	if !errors.Is(result.Err(), ErrLimit) || !progress.RolledBack || len(progress.Steps[1].Rows) != 2 || len(progress.Steps[2].Rows) != 1 {
		t.Fatal("transaction reset result row envelope or lost partial output")
	}
	if len(fixture.rows(t, "SELECT * FROM gh40_rows")) != 0 {
		t.Fatal("result-limit failure committed transaction")
	}
}

func TestExplainCannotEscapeOwnedTransaction(t *testing.T) {
	for _, query := range []string{"EXPLAIN ANALYZE COMMIT", "EXPLAIN ANALYZE ROLLBACK", "EXPLAIN SELECT 1"} {
		t.Run(query, func(t *testing.T) {
			fixture := openFixture(t, OptionsV1{})
			fixture.exec(t, "CREATE TABLE gh40_explain (id BIGINT)")
			receipt, err := fixture.database.Transaction(deadline(t), deadline(t), fixture.id(), []Request{
				{Mode: Execute, SQL: "INSERT INTO gh40_explain VALUES (1)"},
				{Mode: Query, SQL: query},
				{Mode: Execute, SQL: "INSERT INTO gh40_explain VALUES (2)"},
			})
			result := settle(t, receipt, err)
			fixture.drain(t, receipt)
			progress := result.Outcome.Value.Snapshot()
			if !errors.Is(result.Err(), ErrUnsupported) || !progress.RolledBack || progress.CommitAttempted || len(progress.Steps) != 2 {
				t.Fatal("EXPLAIN entered the owned transaction")
			}
			if len(fixture.rows(t, "SELECT * FROM gh40_explain")) != 0 {
				t.Fatal("EXPLAIN bypass published rows")
			}
		})
	}
}

func TestNativeRollbackDoesNotUndoSequenceAdvancement(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	fixture.exec(t, "CREATE SEQUENCE gh40_sequence START 1")
	receipt, err := fixture.database.Transaction(deadline(t), deadline(t), fixture.id(), []Request{
		{Mode: Query, SQL: "SELECT nextval('gh40_sequence')"},
		{Mode: Execute, SQL: "INSERT INTO gh40_missing_table VALUES (1)"},
	})
	result := settle(t, receipt, err)
	fixture.drain(t, receipt)
	if result.Err() == nil || !result.Outcome.Value.Snapshot().RolledBack {
		t.Fatal("rollback control did not execute")
	}
	if fixture.rows(t, "SELECT nextval('gh40_sequence')")[0][0] != int64(2) {
		t.Fatal("native sequence rollback semantics changed")
	}
}
