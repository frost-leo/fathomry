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
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	sdk "github.com/jackc/pgx/v5"
	"github.com/jackc/puddle/v2"
)

// TxOptionsV1 requires an explicit native isolation and access mode. Deferrable
// is supported only for serializable/read-only. No custom BEGIN/COMMIT SQL,
// savepoint or transaction retry is accepted.
type TxOptionsV1 struct {
	Isolation  sdk.TxIsoLevel
	Access     sdk.TxAccessMode
	Deferrable bool
}

// TransactionOutcome reports only the observed finalization, not replica
// visibility, crash durability or effect ownership.
type TransactionOutcome string

const (
	TransactionUnobserved TransactionOutcome = ""
	CommitAcknowledged    TransactionOutcome = "commit-acknowledged"
	RollbackAcknowledged  TransactionOutcome = "rollback-acknowledged"
	CommitRolledBack      TransactionOutcome = "commit-rolled-back"
	FinalizationUnknown   TransactionOutcome = "finalization-unknown"
)

// Transaction retains one native connection and one root allowance until explicit
// Commit/Rollback. BEGIN context cancellation does not roll it back. Copies share
// one authority; concurrent/reentrant operations are refused rather than racing native handles.
type Transaction struct {
	private
	*transactionState
}

type transactionState struct {
	mu          sync.Mutex
	database    *Database
	handle      *puddle.Resource[*connection]
	native      sdk.Tx
	call        *invocation.Call[Result]
	correlation fault.Correlation
	closed      bool
}

func transactionOptions(options TxOptionsV1) (sdk.TxOptions, error) {
	switch options.Isolation {
	case sdk.ReadCommitted, sdk.RepeatableRead, sdk.Serializable:
	default:
		return sdk.TxOptions{}, failure(ErrInput, "isolation")
	}
	if options.Access != sdk.ReadOnly && options.Access != sdk.ReadWrite ||
		options.Deferrable && (options.Isolation != sdk.Serializable || options.Access != sdk.ReadOnly) {
		return sdk.TxOptions{}, failure(ErrInput, "transaction-options")
	}
	deferrable := sdk.NotDeferrable
	if options.Deferrable {
		deferrable = sdk.Deferrable
	}
	return sdk.TxOptions{IsoLevel: options.Isolation, AccessMode: options.Access, DeferrableMode: deferrable}, nil
}

// Begin returns a live transaction and its unresolved finalization receipt.
// Accepted startup failure instead returns nil transaction, a completed receipt,
// and nil setup error. Inspect that receipt even if a direct caller handles failure.
func (database *Database) Begin(ctx context.Context, correlation fault.Correlation, options TxOptionsV1) (*Transaction, *invocation.Receipt[Result], error) {
	nativeOptions, err := transactionOptions(options)
	if err != nil {
		return nil, nil, err
	}
	call, err := database.beginCall(ctx, correlation, "transaction", invocation.Session, nil)
	if err != nil {
		return nil, nil, err
	}
	receipt := call.Receipt()
	work, cancel, err := (invocation.Budget{Limit: database.owner.settings.Timeout}).Context(ctx, invocation.Establish)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return nil, receipt, nil
	}
	defer cancel()
	handle, err := database.owner.take(work)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return nil, receipt, nil
	}
	if work.Err() != nil {
		call.Complete(invocation.Outcome[Result]{Primary: failure(ErrQuery, "begin", work.Err(), context.Cause(work)), Cleanup: database.owner.give(handle)})
		return nil, receipt, nil
	}
	_, _ = call.Attempt()
	native, err := handle.Value().native.BeginTx(nativeContext{work}, nativeOptions)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: nativeFailure(ErrQuery, "begin", work, err), Cleanup: database.owner.give(handle)})
		return nil, receipt, nil
	}
	return &Transaction{transactionState: &transactionState{database: database, handle: handle, native: native, call: call, correlation: correlation}}, receipt, nil
}
func (transaction *Transaction) lock() error {
	if transaction == nil || transaction.transactionState == nil || transaction.call == nil {
		return failure(ErrState, "transaction")
	}
	if !transaction.mu.TryLock() {
		return failure(ErrState, "concurrent-transaction")
	}
	if transaction.closed {
		transaction.mu.Unlock()
		return failure(ErrState, "transaction", sdk.ErrTxClosed)
	}
	return nil
}
func (transaction *Transaction) Query(ctx context.Context, correlation fault.Correlation, sql string, args ...any) (*invocation.Receipt[Result], error) {
	if err := transaction.lock(); err != nil {
		return nil, err
	}
	defer transaction.mu.Unlock()
	return transaction.database.statement(ctx, correlation, sql, args, true, transaction)
}
func (transaction *Transaction) Exec(ctx context.Context, correlation fault.Correlation, sql string, args ...any) (*invocation.Receipt[Result], error) {
	if err := transaction.lock(); err != nil {
		return nil, err
	}
	defer transaction.mu.Unlock()
	return transaction.database.statement(ctx, correlation, sql, args, false, transaction)
}

// Commit uses the existing root evidence slot and connection, even at saturation.
// A setup-budget refusal leaves the transaction live for an explicit later cleanup.
// Once native finalization runs, a lost response remains unknown; it is never retried.
func (transaction *Transaction) Commit(ctx context.Context) (*invocation.Receipt[Result], error) {
	return transaction.finish(ctx, true)
}

// Rollback explicitly requests rollback; it does not establish that no earlier
// transaction committed. A repeated finalization is ErrTxClosed, not new evidence.
func (transaction *Transaction) Rollback(ctx context.Context) (*invocation.Receipt[Result], error) {
	return transaction.finish(ctx, false)
}
func (transaction *Transaction) finish(ctx context.Context, commit bool) (*invocation.Receipt[Result], error) {
	if err := transaction.lock(); err != nil {
		return nil, err
	}
	defer transaction.mu.Unlock()
	work, cancel, err := (invocation.Budget{Limit: transaction.database.owner.settings.Timeout}).Context(ctx, invocation.Cleanup)
	if err != nil {
		return nil, err
	}
	defer cancel()
	_, _ = transaction.call.Attempt()
	data := &resultData{transaction: FinalizationUnknown, serverVersion: transaction.handle.Value().native.PgConn().ParameterStatus("server_version")}
	operation := "rollback"
	if commit {
		operation = "commit"
		err = transaction.native.Commit(nativeContext{work})
		if err == nil {
			data.transaction = CommitAcknowledged
		}
		if errors.Is(err, sdk.ErrTxCommitRollback) {
			data.transaction = CommitRolledBack
		}
	} else {
		err = transaction.native.Rollback(nativeContext{work})
		if err == nil {
			data.transaction = RollbackAcknowledged
		}
	}
	transaction.closed = true
	data.complete = err == nil
	primary := nativeFailure(ErrQuery, operation, work, err)
	cleanup := transaction.database.owner.give(transaction.handle)
	transaction.handle = nil
	transaction.native = nil
	transaction.call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: primary, Cleanup: cleanup})
	return transaction.call.Receipt(), nil
}
