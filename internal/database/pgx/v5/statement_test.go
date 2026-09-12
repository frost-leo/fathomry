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

package pgx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPreparedReuseOwnsOneConnectionAndOriginalEvidence(t *testing.T) {
	peer := newProtocolPeer(t, true)
	fixture := bindFixture(t, peer.options(), 2)
	ctx, cancel := context.WithCancel(context.Background())
	statement, receipt, err := fixture.database.Prepare(ctx, correlation("prepare"), "SELECT $1::text")
	if err != nil || statement == nil {
		t.Fatal("preparation failed", err)
	}
	cancel()
	t.Cleanup(func() { _, _ = statement.Close(context.Background()) })
	initial, ready := receipt.Result()
	if !ready || initial.Final || initial.Released || !initial.Outcome.Value.Complete() {
		t.Fatal("preparation lifetime changed")
	}
	root, err := fixture.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.database.Query(context.Background(), correlation("full"), "SELECT cells"); !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("retained preparation multiplied native capacity")
	}
	for _, value := range []string{"first", "second", ""} {
		receipt, err := statement.Query(context.Background(), correlation("reuse"), value)
		result := operationResult(t, receipt, err)
		row, rowErr := result.Outcome.Value.First()
		if result.Err() != nil || rowErr != nil || string(row.ValuesCopy()[0]) != value || !result.Nested || result.Context.Correlation.Parent != "prepare" {
			t.Fatal("prepared reuse changed native result/ownership", result.Err())
		}
		drain(t, fixture.inbox, 1)
	}
	receipt, err = statement.Exec(context.Background(), correlation("prepared-exec"), "discard")
	if result := operationResult(t, receipt, err); result.Err() != nil || result.Outcome.Value.RowsCopy() != nil {
		t.Fatal("prepared Exec changed bounded consumption", result.Err())
	}
	drain(t, fixture.inbox, 1)
	copied := *statement
	receipt, err = copied.Close(context.Background())
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err = statement.Query(context.Background(), correlation("closed"), "later"); !errors.Is(err, ErrState) {
		t.Fatal("closed copy retained native authority")
	}
	conformance.Facade(t, statement, "Query", "Exec", "Close", "Format", "String", "GoString", "LogValue", "MarshalJSON", "UnmarshalJSON")
	conformance.Runtime(t, statement, new(Statement), "credential-canary")
}

func TestTransactionalPreparationCopiesReleaseSlots(t *testing.T) {
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 2)
	tx := beginTransaction(t, fixture, "root")
	root, err := fixture.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for range MaxPreparedStatements + 1 {
		statement, _, err := tx.Prepare(context.Background(), correlation("nested-prepare"), "SELECT $1::text")
		if err != nil || statement == nil {
			t.Fatal("closed copy consumed a preparation slot", err)
		}
		copy := *statement
		receipt, err := copy.Close(context.Background())
		if result := operationResult(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		drain(t, fixture.inbox, 1)
	}
	receipt, err := tx.Commit(context.Background())
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparationSurvivesConfiguredLifetimeUntilClose(t *testing.T) {
	peer := newProtocolPeer(t, false)
	options := peer.options()
	options.MaxLifetime = time.Millisecond
	fixture := bindFixture(t, options, 2)
	statement, _, err := fixture.database.Prepare(context.Background(), correlation("held"), "SELECT $1::text")
	if err != nil || statement == nil {
		t.Fatal(err)
	}
	root, err := fixture.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	receipt, err := statement.Query(context.Background(), correlation("old"), "still-owned")
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal("expiry interrupted held resource", result.Err())
	}
	drain(t, fixture.inbox, 1)
	receipt, err = statement.Close(context.Background())
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}
	if fixture.database.Stats().TotalResources() != 0 || fixture.inbox.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("expired statement connection remained pooled")
	}
}

func TestDeallocateFailureCannotInventCommitAcknowledgement(t *testing.T) {
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 2)
	tx := beginTransaction(t, fixture, "root")
	statement, _, err := tx.Prepare(context.Background(), correlation("retained"), "SELECT $1::text")
	if err != nil || statement == nil {
		t.Fatal("prepare failed", err)
	}
	peer.failDeallocate.Store(true)
	peer.dropCommit.Store(true)
	receipt, err := tx.Commit(context.Background())
	result := operationResult(t, receipt, err)
	drain(t, fixture.inbox, 2)
	var native *pgconn.PgError
	if !errors.As(result.Outcome.Cleanup, &native) || native.Code != "57014" {
		t.Fatal("deallocate-error negative control did not execute", result.Err())
	}
	if result.Outcome.Value.TransactionOutcome() == CommitAcknowledged {
		t.Fatalf("COMMIT was falsely acknowledged using previous Deallocate ReadyForQuery; primary=%v cleanup=%v", result.Outcome.Primary, result.Outcome.Cleanup)
	}
}

func TestDeallocateFailureCannotInventRollbackAcknowledgement(t *testing.T) {
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 2)
	tx := beginTransaction(t, fixture, "root")
	statement, _, err := tx.Prepare(context.Background(), correlation("retained"), "SELECT $1::text")
	if err != nil || statement == nil {
		t.Fatal("prepare failed", err)
	}
	peer.failDeallocate.Store(true)
	peer.dropRollback.Store(true)
	receipt, err := statement.Close(context.Background())
	closed := operationResult(t, receipt, err)
	var native *pgconn.PgError
	if !errors.As(closed.Outcome.Cleanup, &native) || native.Code != "57014" {
		t.Fatal("deallocate-error negative control did not execute", closed.Err())
	}
	receipt, err = tx.Rollback(context.Background())
	result := operationResult(t, receipt, err)
	drain(t, fixture.inbox, 2)
	if result.Outcome.Value.TransactionOutcome() == RollbackAcknowledged {
		t.Fatalf("ROLLBACK was falsely acknowledged using previous Deallocate ReadyForQuery; primary=%v cleanup=%v", result.Outcome.Primary, result.Outcome.Cleanup)
	}
}
