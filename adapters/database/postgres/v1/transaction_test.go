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

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	sdk "github.com/jackc/pgx/v5"
)

func txOptions() TxOptions { return TxOptions{Isolation: ReadCommitted, Access: ReadWrite} }

func TestTransactionCancellationAndUnknownCommit(t *testing.T) {
	t.Run("context", func(t *testing.T) {
		peer := newProtocolPeer(t, false)
		owner, inbox, _ := testOwner(t, peer.options(), 0)
		ctx, cancel := context.WithCancel(context.Background())
		transaction, err := owner.Client().Begin(ctx, txOptions())
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		if _, err := transaction.Query(context.Background(), "SELECT cells"); err != nil {
			t.Fatal("setup cancellation revoked transaction", err)
		}
		ack(t, inbox)
		if value, err := transaction.Rollback(context.Background()); err != nil || value.TransactionOutcome() != RollbackAcknowledged {
			t.Fatal(err)
		}
		ack(t, inbox)
	})
	t.Run("lost_commit", func(t *testing.T) {
		peer := newProtocolPeer(t, false)
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
		peer := newProtocolPeer(t, false)
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

func TestAbortedCommitPreservesNativeOutcome(t *testing.T) {
	peer := newProtocolPeer(t, false)
	owner, inbox, _ := testOwner(t, peer.options(), 0)
	ctx := context.Background()
	transaction, err := owner.Client().Begin(ctx, txOptions())
	if err != nil {
		t.Fatal(err)
	}
	value, err := transaction.Query(ctx, "SELECT partial")
	server, ok := InspectError(err)
	if err == nil || value.Complete() || !ok || server.Code != "22012" {
		t.Fatal("aborting statement did not retain its native error")
	}
	ack(t, inbox)
	value, err = transaction.Commit(ctx)
	if !errors.Is(err, sdk.ErrTxCommitRollback) || value.TransactionOutcome() != CommitRolledBack || peer.commits.Load() != 1 {
		t.Fatal("aborted commit was hidden, guessed or retried", err)
	}
	snapshot, valid := transaction.Receipt().Snapshot()
	if !valid || !snapshot.Info().Released || !errors.Is(snapshot.Primary(), sdk.ErrTxCommitRollback) {
		t.Fatal("independent final evidence differs from direct result")
	}
	ack(t, inbox)
}

func TestConcurrentTransactionCancellationKeepsCause(t *testing.T) {
	peer := newProtocolPeer(t, false)
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
