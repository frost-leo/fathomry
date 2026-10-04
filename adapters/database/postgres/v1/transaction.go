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

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/database/pgx/v5"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	sdk "github.com/jackc/pgx/v5"
)

// Transaction retains one source generation and connection through finalization.
// Overlapping use, including statements and savepoints, is refused.
type Transaction struct {
	private
	session *session
	native  *native.Transaction
}

// Begin uses ctx for admission and setup only; the Client lifetime owns the
// retained transaction. Canceling a successful setup does not roll it back.
// An accepted startup failure returns a semantic error, not a hidden nil error.
func (client *Client) Begin(ctx context.Context, options TxOptions) (*Transaction, error) {
	if client == nil {
		return nil, fail(ErrInput, "begin")
	}
	var transaction *Transaction
	receipt, err := client.dispatch(ctx, client.lifetime, "transaction", func(value *session, database *native.Database) {
		guard, err := value.call.Hold()
		if err != nil {
			_ = value.call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		setup, stop := value.setup(ctx)
		defer stop()
		handle, nativeReceipt, err := database.Begin(setup, value.family.correlation(value.call), native.TxOptionsV1{Isolation: sdk.TxIsoLevel(options.Isolation), Access: sdk.TxAccessMode(options.Access), Deferrable: options.Deferrable})
		if handle == nil {
			value.finish(nativeReceipt, err)
			_ = guard.Release()
			return
		}
		transaction = &Transaction{session: value, native: handle}
		if err = value.keep(guard, nativeReceipt, func(ctx context.Context) { _, _ = handle.Rollback(ctx) }); err != nil {
			_ = value.call.Resolve(adapters.Outcome[Result]{Primary: err})
		}
	})
	_, err = result(receipt, err)
	return transaction, err
}

// Receipt exposes read-only final evidence, not producer/finalization authority.
func (transaction *Transaction) Receipt() *adapters.Receipt[Result] {
	if transaction == nil || transaction.session == nil {
		return nil
	}
	return transaction.session.call.Receipt()
}

// Query runs on this transaction's original connection, never a new generation.
func (transaction *Transaction) Query(ctx context.Context, query string, args ...any) (Result, error) {
	if transaction == nil || transaction.session == nil {
		return Result{}, fail(ErrState, "query")
	}
	return transaction.session.finite(ctx, "query", func(work context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return transaction.native.Query(work, id, query, args...)
	})
}

// Exec does not turn a successful statement into commit evidence.
func (transaction *Transaction) Exec(ctx context.Context, query string, args ...any) (Result, error) {
	if transaction == nil || transaction.session == nil {
		return Result{}, fail(ErrState, "exec")
	}
	return transaction.session.finite(ctx, "exec", func(work context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return transaction.native.Exec(work, id, query, args...)
	})
}

// Commit uses existing ownership at saturation. Unknown finalization is never retried.
func (transaction *Transaction) Commit(ctx context.Context) (Result, error) {
	if transaction == nil || transaction.session == nil {
		return Result{}, fail(ErrState, "commit")
	}
	return transaction.session.finalize(ctx, "commit", transaction.native.Commit)
}

// Rollback cannot prove that previous statements or transactions never committed.
func (transaction *Transaction) Rollback(ctx context.Context) (Result, error) {
	if transaction == nil || transaction.session == nil {
		return Result{}, fail(ErrState, "rollback")
	}
	return transaction.session.finalize(ctx, "rollback", transaction.native.Rollback)
}
