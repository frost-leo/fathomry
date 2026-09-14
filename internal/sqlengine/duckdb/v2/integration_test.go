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
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type fixture struct {
	database  *Database
	assembly  *resource.Assembly
	selection resource.Selection[Source]
	inbox     *invocation.Inbox[Result]
	next      atomic.Int64
}

func deadline(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func openFixture(t testing.TB, options OptionsV1) *fixture {
	t.Helper()
	if options.Name == "" {
		options.Name = "gh40"
	}
	selection, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selection = resource.WithLimits(selection, LimitsV1(options))
	assembly, err := resource.Assemble(deadline(t), deadline(t), "gh40", selection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := assembly.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	inbox, err := invocation.NewInbox[Result](16, 32*defaults(options).evidenceReservation())
	if err != nil {
		t.Fatal(err)
	}
	database, err := Bind(assembly, selection, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{database: database, assembly: assembly, selection: selection, inbox: inbox}
}

func (fixture *fixture) id() fault.Correlation {
	return fault.Correlation{Call: fmt.Sprintf("gh40-call-%d", fixture.next.Add(1))}
}

func settle(t testing.TB, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	result, waitErr := receipt.WaitReleased(deadline(t))
	if waitErr != nil || !result.Final || !result.Released {
		t.Fatal("call did not release", waitErr)
	}
	return result
}

func (fixture *fixture) drain(t testing.TB, receipt *invocation.Receipt[Result]) {
	t.Helper()
	record, err := fixture.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	left, _ := record.Receipt().Result()
	right, _ := receipt.Result()
	if left.Context.Correlation != right.Context.Correlation {
		t.Fatal("evidence correlation changed")
	}
	if err := record.Release(); err != nil {
		t.Fatal(err)
	}
}

func (fixture *fixture) run(t testing.TB, request Request) invocation.Result[Result] {
	t.Helper()
	receipt, err := fixture.database.Run(deadline(t), deadline(t), fixture.id(), request)
	result := settle(t, receipt, err)
	fixture.drain(t, receipt)
	return result
}

func requireOK(t testing.TB, result invocation.Result[Result]) Progress {
	t.Helper()
	if result.Err() != nil {
		t.Logf("failure kinds: input=%t unsupported=%t native=%t cleanup=%t", errors.Is(result.Err(), ErrInput), errors.Is(result.Err(), ErrUnsupported), errors.Is(result.Err(), ErrNative), errors.Is(result.Err(), ErrCleanup))
		t.Fatal(result.Err())
	}
	if !result.Outcome.Present {
		t.Fatal("missing progress")
	}
	progress := result.Outcome.Value.Snapshot()
	if !progress.ConnectionClosed {
		t.Fatal("native connection not released")
	}
	return progress
}

func (fixture *fixture) exec(t testing.TB, query string) Progress {
	t.Helper()
	return requireOK(t, fixture.run(t, Request{Mode: Execute, SQL: query}))
}

func (fixture *fixture) rows(t testing.TB, query string) [][]any {
	t.Helper()
	return requireOK(t, fixture.run(t, Request{Mode: Query, SQL: query})).Steps[0].Rows
}

func TestIssue40NativeComposition(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	fixture.exec(t, "CREATE TABLE gh40_rows (id BIGINT PRIMARY KEY, value VARCHAR, payload BLOB)")
	input := make([][]any, 4097)
	for index := range input {
		var value any
		if index%7 != 0 {
			value = fmt.Sprintf("row-%d", index)
		}
		input[index] = []any{int64(index), value, []byte{byte(index), 0, 255}}
	}
	request := Request{Mode: Append, Table: "gh40_rows", Rows: input}
	id := fixture.id()
	receipt, err := fixture.database.Run(deadline(t), deadline(t), id, request)
	result := settle(t, receipt, err)
	_, info, err := resource.Bind(fixture.assembly, fixture.selection)
	if err != nil {
		t.Fatal(err)
	}
	want := conformance.Expected[Result]{
		Context: fault.Context{Provider: ProviderID, Source: "gh40", Scope: "gh40", Operation: "transaction", Correlation: id},
		Source:  info, Limits: LimitsV1(OptionsV1{}), Shape: invocation.Finite, Present: true, Final: true, Released: true,
		Value: func(t testing.TB, result Result) {
			progress := result.Snapshot()
			if !progress.Began || !progress.Committed || progress.RolledBack || !progress.ConnectionClosed ||
				len(progress.Steps) != 1 {
				t.Fatal("transaction progress mismatch")
			}
			step := progress.Steps[0]
			if step.AcceptedRows != len(input) || step.FlushedRows != len(input) || !step.AppenderClosed || !step.AppenderCloseSucceeded {
				t.Fatal("appender stage mismatch")
			}
		},
	}
	conformance.Result(t, result, want)
	conformance.Receive(t, deadline(t), fixture.inbox, []conformance.Expected[Result]{want})
	rows := fixture.rows(t, "SELECT id,value,payload FROM gh40_rows ORDER BY id")
	if !reflect.DeepEqual(rows, input) {
		t.Fatal("exact 4097-row/chunk/null/blob roundtrip changed")
	}
	fixture.exec(t, "CREATE TABLE gh40_copy AS SELECT * FROM gh40_rows WHERE id < 3")
	fixture.exec(t, "UPDATE gh40_copy SET value='changed' WHERE id=0")
	fixture.exec(t, "DELETE FROM gh40_copy WHERE id=2")
	fixture.exec(t, "MERGE INTO gh40_copy AS target USING (VALUES (0::BIGINT,'merged'),(3::BIGINT,'new')) AS incoming(id,value) ON target.id=incoming.id WHEN MATCHED THEN UPDATE SET value=incoming.value WHEN NOT MATCHED THEN INSERT (id,value) VALUES (incoming.id,incoming.value)")
	wantRows := [][]any{{int64(0), "merged"}, {int64(1), "row-1"}, {int64(3), "new"}}
	if !reflect.DeepEqual(fixture.rows(t, "SELECT id,value FROM gh40_copy ORDER BY id"), wantRows) {
		t.Fatal("DML/MERGE result mismatch")
	}
	fixture.exec(t, "ALTER TABLE gh40_copy RENAME COLUMN value TO label")
	fixture.exec(t, "CREATE OR REPLACE TABLE gh40_copy AS SELECT 99::BIGINT AS id")
	if !reflect.DeepEqual(fixture.rows(t, "SELECT id FROM gh40_copy"), [][]any{{int64(99)}}) {
		t.Fatal("replacement mismatch")
	}
	fixture.exec(t, "ANALYZE gh40_copy")
	fixture.exec(t, "VACUUM")
	fixture.exec(t, "DROP TABLE gh40_copy")
	conformance.Facade(t, fixture.database, "Profile", "Run", "Transaction", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
	conformance.Runtime(t, fixture.database, new(Database), "gh40_rows")
}

func TestEvidenceSurvivesIgnoredReceiptsAndDroppedObservation(t *testing.T) {
	const canary = "gh40-private-evidence-canary"
	fixture := openFixture(t, OptionsV1{})
	observer, err := invocation.NewObserver(1)
	if err != nil {
		t.Fatal(err)
	}
	database, err := Bind(fixture.assembly, fixture.selection, fixture.inbox, observer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Run(deadline(t), deadline(t), fixture.id(), Request{Mode: Query, SQL: "SELECT '" + canary + "' AS value"}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Run(deadline(t), deadline(t), fixture.id(), Request{Mode: Query, SQL: "SELECT * FROM gh40_missing_table"}); err != nil {
		t.Fatal(err)
	}
	for index := range 2 {
		record, err := fixture.inbox.Next(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		result, err := record.Receipt().WaitReleased(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		progress := result.Outcome.Value.Snapshot()
		if !progress.ConnectionClosed {
			t.Fatal("independent evidence lost native completion")
		}
		if index == 0 {
			if result.Err() != nil || result.Observation != invocation.ObservationQueued || !reflect.DeepEqual(progress.Steps[0].Rows, [][]any{{canary}}) {
				t.Fatal("ignored receipt lost successful payload evidence")
			}
			conformance.Runtime(t, progress, new(Progress), canary)
			conformance.Runtime(t, progress.Steps[0], new(Step), canary)
		} else if !errors.Is(result.Err(), ErrNative) || result.Observation != invocation.ObservationDropped {
			t.Fatal("dropped diagnostics changed required failure evidence")
		}
		conformance.Private(t, result, canary)
		if err := record.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if fixture.inbox.Usage().Outstanding != 0 {
		t.Fatal("independent evidence reservation leaked")
	}
}
