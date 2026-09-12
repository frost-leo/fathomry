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

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/jackc/pgx/v5"
)

func beginTransaction(t testing.TB, fixture boundFixture, id string) *Transaction {
	t.Helper()
	transaction, receipt, err := fixture.database.Begin(context.Background(), correlation(id), TxOptionsV1{Isolation: sdk.ReadCommitted, Access: sdk.ReadWrite})
	if err != nil {
		t.Fatal(err)
	}
	if transaction == nil {
		result, _ := receipt.Result()
		t.Fatal("transaction begin failed", result.Err())
	}
	t.Cleanup(func() {
		if !transaction.closed {
			_, _ = transaction.Rollback(context.Background())
		}
	})
	return transaction
}
func TestTransactionFinalizationNativeOutcomes(t *testing.T) {
	for _, mode := range []string{"commit", "rollback", "aborted", "lost"} {
		t.Run(mode, func(t *testing.T) {
			peer := newProtocolPeer(t, false)
			fixture := bindFixture(t, peer.options(), 3)
			transaction := beginTransaction(t, fixture, "transaction")
			entries := 1
			if mode == "aborted" {
				receipt, err := transaction.Query(context.Background(), correlation("failed-statement"), "SELECT partial")
				result := operationResult(t, receipt, err)
				if result.Err() == nil {
					t.Fatal("native error control did not abort the transaction")
				}
				entries++
			}
			if mode == "lost" {
				peer.dropCommit.Store(true)
			}
			var receipt *invocation.Receipt[Result]
			var err error
			if mode == "rollback" {
				receipt, err = transaction.Rollback(context.Background())
			} else {
				receipt, err = transaction.Commit(context.Background())
			}
			result := operationResult(t, receipt, err)
			want := CommitAcknowledged
			switch mode {
			case "rollback":
				want = RollbackAcknowledged
			case "aborted":
				want = CommitRolledBack
				if !errors.Is(result.Err(), sdk.ErrTxCommitRollback) {
					t.Fatal("aborted commit identity lost")
				}
			case "lost":
				want = FinalizationUnknown
				if result.Err() == nil {
					t.Fatal("lost reply became acknowledged commit")
				}
			}
			if result.Outcome.Value.TransactionOutcome() != want || peer.connects.Load() != 1 ||
				peer.commits.Load()+peer.rollbacks.Load() != 1 {
				t.Fatal("native transaction outcome, connection ownership or retry count changed")
			}
			if _, err := transaction.Rollback(context.Background()); !errors.Is(err, sdk.ErrTxClosed) {
				t.Fatal("repeated finalization invented success")
			}
			retained, _ := receipt.Result()
			if retained.Outcome.Value.TransactionOutcome() != want || !errors.Is(retained.Err(), result.Err()) {
				t.Fatal("later no-op finalization changed historical evidence")
			}
			drain(t, fixture.inbox, entries)
		})
	}
}
func TestTransactionSaturationAndSharedBorrowing(t *testing.T) {
	peer := newProtocolPeer(t, false)
	options := peer.options()
	fixture := bindFixture(t, options, 2)
	borrowed := resource.Borrow("alias", fixture.assembly, fixture.selected)
	aliasOwner, err := resource.Assemble(context.Background(), context.Background(), "borrower", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	defer aliasOwner.Close(context.Background())
	alias, err := Bind(aliasOwner, borrowed, fixture.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	transaction := beginTransaction(t, fixture, "root")
	if receipt, err := alias.Query(context.Background(), correlation("other-root"), "SELECT cells"); receipt != nil || !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("alias multiplied the original connection allowance")
	}
	receipt, err := transaction.Exec(context.Background(), correlation("statement"), "INSERT INTO fixture VALUES ($1)", "value")
	child := operationResult(t, receipt, err)
	if child.Err() != nil || !child.Nested || child.Context.Correlation.Parent != "root" || child.Outcome.Value.RowsAffected() != 1 ||
		fixture.assembly.Snapshot().Sources[0].Usage.Active != 1 {
		t.Fatal("transaction statement reacquired its own root capacity", child.Err())
	}
	if receipt, err := transaction.Query(context.Background(), correlation("evidence-full"), "SELECT cells"); receipt != nil || !errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("full nested evidence did not reject immediately")
	}
	receipt, err = transaction.Commit(context.Background())
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal("saturated finalization needed a new slot", result.Err())
	}
	if peer.connects.Load() != 1 || peer.queries.Load() != 1 {
		t.Fatal("nested source or native attempt count changed")
	}
	drain(t, fixture.inbox, 2)
}
func TestBeginCancellationDoesNotRollbackAndCloseRetainsTransaction(t *testing.T) {
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 1)
	ctx, cancel := context.WithCancel(context.Background())
	transaction, receipt, err := fixture.database.Begin(ctx, correlation("root"), TxOptionsV1{Isolation: sdk.Serializable, Access: sdk.ReadOnly, Deferrable: true})
	if err != nil || transaction == nil {
		t.Fatal("begin failed", err)
	}
	cancel()
	if peer.rollbacks.Load() != 0 || transaction.handle.Value().native.PgConn().TxStatus() != 'T' {
		t.Fatal("BEGIN cancellation became rollback")
	}
	if err := fixture.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("live transaction lost owner")
	}
	if _, err := transaction.Commit(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("expired budget reached native finalization")
	}
	if _, ok := receipt.Result(); ok {
		t.Fatal("pre-entry refusal finalized transaction evidence")
	}
	final, err := transaction.Rollback(context.Background())
	if result := operationResult(t, final, err); result.Err() != nil {
		t.Fatal("explicit independent cleanup failed", result.Err())
	}
	drain(t, fixture.inbox, 1)
}
func TestConcurrentTransactionUseIsRefusedWithoutNativeRace(t *testing.T) {
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 2)
	transaction := beginTransaction(t, fixture, "root")
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan *invocation.Receipt[Result], 1)
	go func() {
		receipt, err := transaction.Query(ctx, correlation("blocking"), "SELECT wait")
		if err != nil {
			t.Error(err)
		}
		finished <- receipt
	}()
	select {
	case <-peer.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("native query not entered")
	}
	copy := *transaction
	if _, err := copy.Commit(context.Background()); !errors.Is(err, ErrState) {
		t.Fatal("concurrent commit raced the active native rows")
	}
	cancel()
	var receipt *invocation.Receipt[Result]
	select {
	case receipt = <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("canceled transaction query did not return")
	}
	if result := operationResult(t, receipt, nil); !errors.Is(result.Err(), context.Canceled) {
		t.Fatal("cancellation evidence lost")
	}
	final, err := transaction.Rollback(context.Background())
	result := operationResult(t, final, err)
	if result.Err() == nil || result.Outcome.Value.TransactionOutcome() != FinalizationUnknown {
		t.Fatal("dead-connection rollback became success")
	}
	drain(t, fixture.inbox, 2)
}

func TestTransactionCopiesShareFinalizationAndZeroState(t *testing.T) {
	for _, transaction := range []*Transaction{nil, {}} {
		if _, err := transaction.Commit(context.Background()); !errors.Is(err, ErrState) {
			t.Fatal("zero transaction invented finalization")
		}
	}
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 1)
	transaction := beginTransaction(t, fixture, "copy")
	copy := *transaction
	receipt, err := copy.Commit(context.Background())
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal("copied transaction lost shared authority")
	}
	if _, err := transaction.Rollback(context.Background()); !errors.Is(err, sdk.ErrTxClosed) || peer.commits.Load() != 1 || peer.rollbacks.Load() != 0 {
		t.Fatal("transaction copy split native ownership")
	}
	drain(t, fixture.inbox, 1)
}
