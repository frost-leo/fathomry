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
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// TxOptionsV1 supports the native default and all four MySQL isolation levels.
// ReadOnly defaults false; LevelDefault preserves the server's session setting.
// All accessed tables/trigger effects must satisfy the declared InnoDB profile.
type TxOptionsV1 struct {
	Isolation sql.IsolationLevel
	ReadOnly  bool
}
type TransactionOutcome string

const (
	TransactionUnobserved TransactionOutcome = ""
	CommitAcknowledged    TransactionOutcome = "commit-acknowledged"
	RollbackAcknowledged  TransactionOutcome = "rollback-acknowledged"
	FinalizationUnknown   TransactionOutcome = "finalization-unknown"
)

// Transaction holds one pinned connection/root allowance. The Begin context
// owns its whole bounded lifetime, including database/sql automatic rollback.
// Copies share authority; concurrent calls are refused. A failed statement does
// not imply transaction rollback. The response to an owned native Ping checks
// whether the server still reports an active transaction before further use.
type Transaction struct {
	private
	*transactionState
}
type transactionState struct {
	mu          sync.Mutex
	db          *Database
	conn        *sql.Conn
	owned       *managedConn
	native      *sql.Tx
	finalizer   *nativeTransaction
	call        *invocation.Call[Result]
	correlation fault.Correlation
	lifetime    context.Context
	cancel      context.CancelFunc
	closed      bool
	ended       bool
	statements  map[*Statement]struct{}
}

func (db *Database) Begin(ctx context.Context, id fault.Correlation, options TxOptionsV1) (*Transaction, *invocation.Receipt[Result], error) {
	switch options.Isolation {
	case sql.LevelDefault, sql.LevelReadUncommitted, sql.LevelReadCommitted, sql.LevelRepeatableRead, sql.LevelSerializable:
	default:
		return nil, nil, failure(ErrInput, "isolation")
	}
	call, err := db.beginCall(ctx, id, "transaction", invocation.Session, nil)
	if err != nil {
		return nil, nil, err
	}
	receipt := call.Receipt()
	lifetime, cancel := context.WithTimeout(ctx, db.owner.settings.TransactionTimeout)
	work, stopWork := context.WithTimeout(lifetime, db.owner.settings.Timeout)
	conn, owned, err := db.owner.take(work)
	if err != nil {
		stopWork()
		cancel()
		call.Complete(connectionOutcome(err))
		return nil, receipt, nil
	}
	_, _ = call.Attempt()
	native, err := conn.BeginTx(nativeContext{lifetime}, &sql.TxOptions{Isolation: options.Isolation, ReadOnly: options.ReadOnly})
	stopWork()
	if err != nil {
		cancel()
		call.Complete(invocation.Outcome[Result]{Primary: operationError(lifetime, err, owned), Cleanup: give(conn, owned, true)})
		return nil, receipt, nil
	}
	tx := &Transaction{transactionState: &transactionState{db: db, conn: conn, owned: owned, native: native, finalizer: owned.transaction, call: call, correlation: id, lifetime: lifetime, cancel: cancel, statements: make(map[*Statement]struct{})}}
	guard, err := call.Scope().Hold()
	if err != nil {
		cancel()
		_ = native.Rollback()
		<-tx.finalizer.done
		call.Complete(invocation.Outcome[Result]{Primary: err, Cleanup: joined(ErrCleanup, "begin-rollback", tx.finalizer.err, give(conn, owned, true))})
		return nil, receipt, nil
	}
	go func() {
		defer guard.End()
		<-lifetime.Done()
		tx.mu.Lock()
		defer tx.mu.Unlock()
		if tx.closed {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), db.owner.settings.CloseTimeout)
		defer stop()
		tx.finishLocked(cleanup, false, true)
	}()
	return tx, receipt, nil
}
func (tx *Transaction) lock() error {
	if tx == nil || tx.transactionState == nil {
		return failure(ErrState, "transaction")
	}
	if !tx.mu.TryLock() {
		return failure(ErrState, "concurrent-transaction")
	}
	if tx.closed {
		tx.mu.Unlock()
		return failure(ErrState, "transaction", sql.ErrTxDone)
	}
	return nil
}
func (tx *Transaction) Query(ctx context.Context, id fault.Correlation, query string, args ...any) (*invocation.Receipt[Result], error) {
	if err := tx.lock(); err != nil {
		return nil, err
	}
	defer tx.mu.Unlock()
	if tx.ended || tx.lifetime.Err() != nil {
		return nil, failure(ErrState, "transaction", tx.lifetime.Err())
	}
	return tx.db.statement(ctx, id, query, args, true, tx, nil)
}
func (tx *Transaction) Exec(ctx context.Context, id fault.Correlation, query string, args ...any) (*invocation.Receipt[Result], error) {
	if err := tx.lock(); err != nil {
		return nil, err
	}
	defer tx.mu.Unlock()
	if tx.ended || tx.lifetime.Err() != nil {
		return nil, failure(ErrState, "transaction", tx.lifetime.Err())
	}
	return tx.db.statement(ctx, id, query, args, false, tx, nil)
}
func (tx *Transaction) Commit(ctx context.Context) (*invocation.Receipt[Result], error) {
	return tx.finish(ctx, true)
}
func (tx *Transaction) Rollback(ctx context.Context) (*invocation.Receipt[Result], error) {
	return tx.finish(ctx, false)
}
func (tx *Transaction) finish(ctx context.Context, commit bool) (*invocation.Receipt[Result], error) {
	if err := tx.lock(); err != nil {
		return nil, err
	}
	defer tx.mu.Unlock()
	if ctx == nil {
		return nil, failure(ErrInput, "finalize")
	}
	if err := ctx.Err(); err != nil {
		return nil, failure(ErrState, "finalize", err, context.Cause(ctx))
	}
	if commit && tx.ended {
		return nil, failure(ErrState, "transaction-ended")
	}
	tx.finishLocked(ctx, commit, false)
	return tx.call.Receipt(), nil
}
func (tx *Transaction) finishLocked(ctx context.Context, commit, automatic bool) {
	var statementCleanup []error
	for statement := range tx.statements {
		statementCleanup = append(statementCleanup, statement.closeLocked(ctx))
	}
	tx.finalizer.setContext(ctx)
	_, _ = tx.call.Attempt()
	var err error
	if commit {
		err = tx.native.Commit()
	} else {
		err = tx.native.Rollback()
	}
	// ErrTxDone can precede the native finalizer's return. A canceled Commit can
	// instead leave automatic rollback scheduled: always join the original result.
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		_ = tx.native.Rollback()
	}
	<-tx.finalizer.done
	data := &resultData{serverVersion: tx.owned.wire.version, transaction: FinalizationUnknown}
	primary := tx.finalizer.err
	if primary == nil {
		if tx.finalizer.committed {
			data.transaction = CommitAcknowledged
		} else {
			data.transaction = RollbackAcknowledged
		}
		data.complete = true
	}
	if automatic || tx.lifetime.Err() != nil {
		primary = joined(ErrQuery, "transaction-lifetime", primary, tx.lifetime.Err(), context.Cause(tx.lifetime))
	}
	cleanup := joined(ErrCleanup, "transaction-cleanup", give(tx.conn, tx.owned, primary != nil || tx.ended || tx.owned.wire.closed.Load()), errors.Join(statementCleanup...))
	tx.closed = true
	tx.cancel()
	tx.conn = nil
	tx.native = nil
	tx.owned = nil
	tx.finalizer = nil
	tx.call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: primary, Cleanup: cleanup})
}
