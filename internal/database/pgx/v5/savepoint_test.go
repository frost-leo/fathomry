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

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestSavepointRecoversErrorAndClosesItsServerScope(t *testing.T) {
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 3)
	tx := beginTransaction(t, fixture, "transaction")
	root, err := fixture.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	point, receipt, err := tx.Savepoint(context.Background(), correlation("savepoint"))
	if err != nil || point == nil {
		t.Fatal("savepoint creation failed", err)
	}
	if _, ready := receipt.Result(); ready {
		t.Fatal("live savepoint falsely finalized")
	}
	saved, err := fixture.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	receipt, err = tx.Query(context.Background(), correlation("failed"), "SELECT partial")
	if result := operationResult(t, receipt, err); result.Err() == nil {
		t.Fatal("native error did not occur")
	}
	drain(t, fixture.inbox, 1)
	receipt, err = point.Rollback(context.Background())
	result := operationResult(t, receipt, err)
	if result.Err() != nil || result.Outcome.Value.SavepointOutcome() != SavepointRolledBack || result.Outcome.Value.TransactionOutcome() != TransactionUnobserved {
		t.Fatal("savepoint recovery changed native effect evidence", result.Err())
	}
	if err = saved.Release(); err != nil {
		t.Fatal(err)
	}
	receipt, err = tx.Query(context.Background(), correlation("after"), "SELECT cells")
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal("savepoint recovery did not permit further work", result.Err())
	}
	drain(t, fixture.inbox, 1)
	receipt, err = tx.Commit(context.Background())
	if result := operationResult(t, receipt, err); result.Err() != nil || result.Outcome.Value.TransactionOutcome() != CommitAcknowledged {
		t.Fatal(result.Err())
	}
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}
	conformance.Facade(t, point, "Release", "Rollback", "Format", "String", "GoString", "LogValue", "MarshalJSON", "UnmarshalJSON")
	conformance.Runtime(t, point, new(Savepoint), "credential-canary")
}
func TestSavepointsRequireLIFOAndParentFinalizesAtSaturation(t *testing.T) {
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 3)
	tx := beginTransaction(t, fixture, "transaction")
	first, _, err := tx.Savepoint(context.Background(), correlation("first"))
	if err != nil || first == nil {
		t.Fatal(err)
	}
	second, _, err := tx.Savepoint(context.Background(), correlation("second"))
	if err != nil || second == nil {
		t.Fatal(err)
	}
	if _, err = first.Release(context.Background()); !errors.Is(err, ErrState) {
		t.Fatal("out-of-order savepoint changed server state")
	}
	receipt, err := tx.Rollback(context.Background())
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal("saturated parent rollback failed", result.Err())
	}
	for _, point := range []*Savepoint{first, second} {
		receipt, err := point.Release(context.Background())
		if result := operationResult(t, receipt, err); result.Err() != nil || result.Outcome.Value.SavepointOutcome() != SavepointParentEnded || result.Outcome.Value.Complete() {
			t.Fatal("parent cleanup invented individual acknowledgement")
		}
	}
	drain(t, fixture.inbox, 3)
}
func TestRepeatedSavepointRollbackDoesNotExhaustDepth(t *testing.T) {
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 2)
	tx := beginTransaction(t, fixture, "transaction")
	root, err := fixture.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for range MaxSavepoints + 1 {
		point, _, err := tx.Savepoint(context.Background(), correlation("point"))
		if err != nil || point == nil {
			t.Fatal(err)
		}
		copy := *point
		receipt, err := copy.Rollback(context.Background())
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

func TestRollbackActuallyReleasesTheServerSavepoint(t *testing.T) {
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 2)
	tx := beginTransaction(t, fixture, "transaction")
	root, err := fixture.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	point, _, err := tx.Savepoint(context.Background(), correlation("point"))
	if err != nil || point == nil {
		t.Fatal(err)
	}
	name := point.name
	receipt, err := point.Rollback(context.Background())
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, fixture.inbox, 1)
	receipt, err = tx.Exec(context.Background(), correlation("missing"), "ROLLBACK TO SAVEPOINT "+name)
	result := operationResult(t, receipt, err)
	var native *pgconn.PgError
	if !errors.As(result.Err(), &native) || native.Code != "3B001" {
		t.Fatal("server savepoint survived terminal rollback")
	}
	drain(t, fixture.inbox, 1)
	receipt, err = tx.Rollback(context.Background())
	_ = operationResult(t, receipt, err)
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}
}
