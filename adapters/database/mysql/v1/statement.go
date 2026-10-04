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

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/database/mysql/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Statement pins one preparation and connection, or borrows its transaction's
// connection. Close finalizes the original receipt; parent finalization also
// closes transaction-owned preparations. Copies share the same authority.
type Statement struct {
	private
	session *session
	native  *native.Statement
	ready   Result
}

// Prepare uses ctx only for setup, not the retained lifetime. A standalone
// statement pins a connection until Close or the explicit Client lifetime ends.
func (client *Client) Prepare(ctx context.Context, query string) (*Statement, error) {
	if client == nil {
		return nil, fail(ErrInput, "prepare")
	}
	var statement *Statement
	receipt, err := client.dispatch(ctx, client.lifetime, "prepare", func(value *session, database *native.Database) {
		statement = prepare(ctx, value, func(work context.Context, id fault.Correlation) (*native.Statement, *invocation.Receipt[native.Result], error) {
			return database.Prepare(work, id, query)
		})
	})
	_, err = result(receipt, err)
	return statement, err
}

// Prepare retains bounded preparation on this transaction's original generation.
func (transaction *Transaction) Prepare(ctx context.Context, query string) (*Statement, error) {
	if transaction == nil || transaction.session == nil {
		return nil, fail(ErrState, "prepare")
	}
	var statement *Statement
	receipt, err := transaction.session.child(ctx, transaction.session.lifetime, "prepare", func(value *session) {
		statement = prepare(ctx, value, func(work context.Context, id fault.Correlation) (*native.Statement, *invocation.Receipt[native.Result], error) {
			return transaction.native.Prepare(work, id, query)
		})
	})
	_, err = result(receipt, err)
	return statement, err
}

func prepare(ctx context.Context, value *session, work func(context.Context, fault.Correlation) (*native.Statement, *invocation.Receipt[native.Result], error)) *Statement {
	guard, err := value.call.Hold()
	if err != nil {
		_ = value.call.Resolve(adapters.Outcome[Result]{Primary: err})
		return nil
	}
	setup, stop := value.setup(ctx)
	defer stop()
	handle, receipt, err := work(setup, value.family.correlation(value.call))
	if handle == nil {
		value.finish(receipt, err)
		_ = guard.Release()
		return nil
	}
	initial, _ := receipt.Result()
	metadata, _ := value.call.Receipt().Snapshot()
	statement := &Statement{session: value, native: handle, ready: project(initial, metadata.Info())}
	if err = value.keep(guard, receipt, func(ctx context.Context) { _, _ = handle.Close(ctx) }); err != nil {
		_ = value.call.Resolve(adapters.Outcome[Result]{Primary: err})
	}
	return statement
}

// Ready is immutable initial preparation metadata, not final lifetime evidence.
// Late cleanup failures appear in Receipt and Close, never overwrite this snapshot.
func (statement *Statement) Ready() Result {
	if statement == nil {
		return Result{}
	}
	return statement.ready
}

// Receipt remains unresolved until all native cleanup facts are known.
func (statement *Statement) Receipt() *adapters.Receipt[Result] {
	if statement == nil || statement.session == nil {
		return nil
	}
	return statement.session.call.Receipt()
}

// Query reuses the same native preparation without automatic reprepare or retry.
func (statement *Statement) Query(ctx context.Context, args ...any) (Result, error) {
	if statement == nil || statement.session == nil {
		return Result{}, fail(ErrState, "query")
	}
	return statement.session.finite(ctx, "query", func(work context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return statement.native.Query(work, id, args...)
	})
}

// Exec reuses preparation while consuming, but not retaining, bounded rows.
func (statement *Statement) Exec(ctx context.Context, args ...any) (Result, error) {
	if statement == nil || statement.session == nil {
		return Result{}, fail(ErrState, "exec")
	}
	return statement.session.finite(ctx, "exec", func(work context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return statement.native.Exec(work, id, args...)
	})
}

// Close uses its original reservations. Repeated Close preserves earlier errors.
func (statement *Statement) Close(ctx context.Context) (Result, error) {
	if statement == nil || statement.session == nil {
		return Result{}, fail(ErrState, "close")
	}
	return statement.session.finalize(ctx, "close", statement.native.Close)
}
