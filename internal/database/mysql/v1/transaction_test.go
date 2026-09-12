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
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func beginTx(t *testing.T, f boundFixture, ctx context.Context) *Transaction {
	t.Helper()
	tx, receipt, err := f.db.Begin(ctx, correlation("transaction"), TxOptionsV1{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	if tx == nil {
		result := observe(t, receipt, nil)
		t.Fatal("transaction startup failed", result.Err())
	}
	t.Cleanup(func() { _, _ = tx.Rollback(context.Background()) })
	return tx
}
func TestTransactionFinalizationAtSaturation(t *testing.T) {
	for _, mode := range []string{"commit", "rollback", "lost", "automatic"} {
		t.Run(mode, func(t *testing.T) {
			peer := newPeer(t, true, false)
			f := bindFixture(t, peer.options(), 1)
			lifetime, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			tx := beginTx(t, f, lifetime)
			if !errors.Is(f.assembly.Close(context.Background()), resource.ErrIncomplete) {
				t.Fatal("live transaction released its source")
			}
			var receipt *invocation.Receipt[Result]
			var err error
			expected := CommitAcknowledged
			switch mode {
			case "commit":
				receipt, err = tx.Commit(context.Background())
			case "rollback":
				expected = RollbackAcknowledged
				receipt, err = tx.Rollback(context.Background())
			case "lost":
				expected = FinalizationUnknown
				peer.dropCommit.Store(true)
				receipt, err = tx.Commit(context.Background())
			case "automatic":
				expected = RollbackAcknowledged
				cancel(errors.New("lifetime-canary"))
				receipt = tx.call.Receipt()
			}
			result := observe(t, receipt, err)
			if result.Outcome.Value.TransactionOutcome() != expected {
				t.Fatal("wrong finalization evidence", result.Err())
			}
			if mode == "lost" && result.Err() == nil {
				t.Fatal("missing commit reply became success")
			}
			if mode == "automatic" && !errors.Is(result.Err(), context.Canceled) {
				t.Fatal("automatic rollback lost caller cancellation")
			}
			if peer.commits.Load()+peer.rollbacks.Load() != 1 {
				t.Fatal("native finalizer was replayed")
			}
			if _, err := tx.Rollback(context.Background()); !errors.Is(err, sql.ErrTxDone) {
				t.Fatal("repeat finalizer changed evidence")
			}
			drain(t, f.inbox, 1)
		})
	}
}
func TestNestedStatementsAndFailedTransactionRefusal(t *testing.T) {
	peer := newPeer(t, false, false)
	f := bindFixture(t, peer.options(), 2)
	tx := beginTx(t, f, context.Background())
	receipt, err := tx.Exec(context.Background(), correlation("child"), "UPDATE fixture SET value=?", "changed")
	result := observe(t, receipt, err)
	if result.Err() != nil || !result.Nested || result.Context.Correlation.Parent != "transaction" {
		t.Fatal("nested work lost its root", result.Err())
	}
	if _, err := tx.Query(context.Background(), correlation("full"), "SELECT cells"); !errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("saturated nested evidence was not refused")
	}
	record, err := f.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Retain the unresolved root while child evidence drains independently.
	drain(t, f.inbox, 1)
	receipt, err = tx.Query(context.Background(), correlation("failure"), "SELECT partial")
	if result := observe(t, receipt, err); result.Err() == nil {
		t.Fatal("native statement failure missing")
	}
	if _, err := tx.Commit(context.Background()); !errors.Is(err, ErrState) {
		t.Fatal("failed transaction committed a potentially incomplete effect set")
	}
	receipt, err = tx.Rollback(context.Background())
	if result := observe(t, receipt, err); result.Outcome.Value.TransactionOutcome() != RollbackAcknowledged {
		t.Fatal("explicit cleanup failed", result.Err())
	}
	drain(t, f.inbox, 1)
	if err = record.Release(); err != nil {
		t.Fatal(err)
	}
}
func TestQueryCancellationAndConcurrentTransactionRefusal(t *testing.T) {
	peer := newPeer(t, true, false)
	f := bindFixture(t, peer.options(), 2)
	tx := beginTx(t, f, context.Background())
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("cancellation-canary")
	done := make(chan *invocation.Receipt[Result], 1)
	go func() {
		receipt, err := tx.Query(ctx, correlation("wait"), "SELECT wait")
		if err != nil {
			t.Error(err)
		}
		done <- receipt
	}()
	select {
	case <-peer.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("query never entered")
	}
	if _, err := tx.Commit(context.Background()); !errors.Is(err, ErrState) {
		t.Fatal("concurrent finalizer reached native work")
	}
	cancel(cause)
	result := observe(t, <-done, nil)
	if !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), cause) {
		t.Fatal("cancellation cause lost", result.Err())
	}
	receipt, err := tx.Rollback(context.Background())
	result = observe(t, receipt, err)
	if result.Outcome.Value.TransactionOutcome() != FinalizationUnknown {
		t.Fatal("dead-connection rollback falsely acknowledged")
	}
	drain(t, f.inbox, 2)
}
