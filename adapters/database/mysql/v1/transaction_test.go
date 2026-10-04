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

package mysql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func txOptions() TxOptions { return TxOptions{} }

func TestTransactionCancellationAndUnknownCommit(t *testing.T) {
	t.Run("context", func(t *testing.T) {
		peer := newPeer(t, false, false)
		owner, inbox, _ := testOwner(t, peer.options(), 0)
		ctx, cancel := context.WithCancel(context.Background())
		transaction, err := owner.Client().Begin(ctx, txOptions())
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		snapshot, err := transaction.Receipt().WaitReleased(wait)
		if err != nil {
			t.Fatal(err)
		}
		value, _ := snapshot.ValueCopy()
		if value.TransactionOutcome() != RollbackAcknowledged || !errors.Is(snapshot.Primary(), context.Canceled) {
			t.Fatal("automatic rollback or lifetime cause lost")
		}
		ack(t, inbox)
	})
	t.Run("lost_commit", func(t *testing.T) {
		peer := newPeer(t, false, false)
		owner, inbox, _ := testOwner(t, peer.options(), 0)
		transaction, err := owner.Client().Begin(context.Background(), txOptions())
		if err != nil {
			t.Fatal(err)
		}
		peer.dropCommit.Store(true)
		value, err := transaction.Commit(context.Background())
		if err == nil || value.TransactionOutcome() != FinalizationUnknown || peer.commits.Load() != 1 {
			t.Fatal("lost commit was guessed or retried", err)
		}
		ack(t, inbox)
	})
	t.Run("owner_stop", func(t *testing.T) {
		peer := newPeer(t, false, false)
		owner, _, _ := testOwner(t, peer.options(), 0)
		statement, err := owner.Client().Prepare(context.Background(), "SELECT cells")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := owner.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := statement.Receipt().WaitReleased(ctx); err != nil {
			t.Fatal("native handle leaked", err)
		}
	})
}

func TestTransactionStatementAndEndStates(t *testing.T) {
	for _, mode := range []string{"statement_only", "transaction_ended", "sql_commit"} {
		t.Run(mode, func(t *testing.T) {
			peer := newPeer(t, false, false)
			owner, inbox, _ := testOwner(t, peer.options(), 0)
			ctx := context.Background()
			transaction, err := owner.Client().Begin(ctx, txOptions())
			if err != nil {
				t.Fatal(err)
			}
			if mode == "sql_commit" {
				value, err := transaction.Exec(ctx, "COMMIT")
				if err != nil || value.TransactionOutcome() != TransactionUnobserved {
					t.Fatal("SQL acknowledgement invented controlled finalization", err)
				}
			} else {
				query := "SELECT duplicate"
				if mode == "transaction_ended" {
					query = "SELECT partial"
				}
				_, err := transaction.Query(ctx, query)
				native, found := InspectError(err)
				if err == nil || !found || mode == "statement_only" && native.Number != 1062 {
					t.Fatal("native statement error lost", err)
				}
			}
			ack(t, inbox)
			if mode == "statement_only" {
				if _, err = transaction.Query(ctx, "SELECT cells"); err != nil {
					t.Fatal("statement-only error ended transaction", err)
				}
				ack(t, inbox)
				if value, err := transaction.Commit(ctx); err != nil || value.TransactionOutcome() != CommitAcknowledged {
					t.Fatal("recoverable transaction refused", err)
				}
			} else {
				before := peer.queries.Load()
				if _, err = transaction.Query(ctx, "SELECT cells"); !errors.Is(err, ErrState) {
					t.Fatal("ended transaction admitted SQL", err)
				}
				ack(t, inbox)
				if statement, err := transaction.Prepare(ctx, "SELECT ?"); statement != nil || !errors.Is(err, ErrState) {
					t.Fatal("ended transaction admitted preparation", err)
				}
				ack(t, inbox)
				if _, err = transaction.Commit(ctx); !errors.Is(err, ErrState) {
					t.Fatal("ended transaction committed again", err)
				}
				if peer.queries.Load() != before {
					t.Fatal("ended transaction dispatched new SQL")
				}
				if _, err = transaction.Rollback(ctx); err != nil {
					t.Fatal("explicit cleanup failed", err)
				}
				if mode == "sql_commit" && peer.commits.Load() != 1 {
					t.Fatal("transaction-ending SQL replayed")
				}
			}
			ack(t, inbox)
		})
	}
}

func TestConcurrentTransactionCancellationKeepsCause(t *testing.T) {
	peer := newPeer(t, false, false)
	settings := peer.options()
	settings.Timeout = 5 * time.Second
	owner, inbox, _ := testOwner(t, settings, 0)
	transaction, err := owner.Client().Begin(context.Background(), txOptions())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("caller-cancellation-canary")
	done := make(chan error, 1)
	go func() { _, err := transaction.Query(ctx, "SELECT wait"); done <- err }()
	select {
	case <-peer.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("query not dispatched")
	}
	if _, err = transaction.Commit(context.Background()); !errors.Is(err, ErrState) {
		t.Fatal("concurrent finalization reached native work", err)
	}
	cancel(cause)
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
			t.Fatal("caller cause lost", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled query did not terminate")
	}
	ack(t, inbox)
	value, err := transaction.Rollback(context.Background())
	if err == nil || value.TransactionOutcome() != FinalizationUnknown {
		t.Fatal("dead-connection rollback falsely acknowledged", err)
	}
	ack(t, inbox)
}
