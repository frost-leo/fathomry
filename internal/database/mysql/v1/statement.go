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
	"database/sql/driver"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

const MaxPreparedStatements = 8

// Statement owns one native prepared statement, not the pool. A database-level
// statement pins one connection/root allowance; a transaction statement borrows
// that transaction. Methods serialize with their actual connection owner.
// Close is required and finalizes the original preparation evidence at saturation.
type Statement struct {
	private
	*statementState
}
type statementState struct {
	mu     *sync.Mutex
	db     *Database
	parent *Transaction
	conn   *sql.Conn
	owned  *managedConn
	native driver.Stmt
	query  string
	call   *invocation.Call[Result]
	closed bool
}

// Prepare retains a reusable server statement on one pinned connection. ctx
// governs preparation only, as in the native SDK; Close owns later cleanup.
func (db *Database) Prepare(ctx context.Context, id fault.Correlation, query string) (*Statement, *invocation.Receipt[Result], error) {
	return db.prepare(ctx, id, query, nil)
}

// Prepare retains a statement within this transaction. Finalization also closes
// all of its remaining statements; errors stay in their original evidence slots.
func (tx *Transaction) Prepare(ctx context.Context, id fault.Correlation, query string) (*Statement, *invocation.Receipt[Result], error) {
	if err := tx.lock(); err != nil {
		return nil, nil, err
	}
	defer tx.mu.Unlock()
	if tx.ended || tx.lifetime.Err() != nil {
		return nil, nil, failure(ErrState, "transaction", tx.lifetime.Err())
	}
	if len(tx.statements) >= MaxPreparedStatements {
		return nil, nil, failure(ErrLimit, "prepared-statements")
	}
	return tx.db.prepare(ctx, id, query, tx)
}
func (db *Database) prepare(ctx context.Context, id fault.Correlation, query string, parent *Transaction) (*Statement, *invocation.Receipt[Result], error) {
	if err := validStatement(query, nil); err != nil {
		return nil, nil, err
	}
	var parentCall *invocation.Call[Result]
	if parent != nil {
		parentCall = parent.call
	}
	call, err := db.beginCall(ctx, id, "prepare", invocation.Session, parentCall)
	if err != nil {
		return nil, nil, err
	}
	receipt := call.Receipt()
	work, cancel, err := (invocation.Budget{Limit: db.owner.settings.Timeout}).Context(ctx, invocation.Establish)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return nil, receipt, nil
	}
	defer cancel()
	var conn *sql.Conn
	var owned *managedConn
	mutex := new(sync.Mutex)
	if parent == nil {
		conn, owned, err = db.owner.take(work)
		if err != nil {
			call.Complete(connectionOutcome(err))
			return nil, receipt, nil
		}
	} else {
		conn, owned = parent.conn, parent.owned
		mutex = &parent.mu
		stop := context.AfterFunc(parent.lifetime, cancel)
		defer stop()
		if parent.lifetime.Err() != nil {
			cancel()
		}
	}
	var native driver.Stmt
	var cleanup error
	err = rawNative(conn, func(c *managedConn) error {
		c.start()
		stop := c.wire.activate(work)
		defer stop()
		if work.Err() != nil {
			return work.Err()
		}
		_, _ = call.Attempt()
		var err error
		native, err = c.Conn.(driver.ConnPrepareContext).PrepareContext(nativeContext{work}, query)
		stop()
		cleanup = c.cleanup()
		return operationError(work, err, c)
	})
	if err != nil {
		if parent == nil {
			cleanup = give(conn, owned, true)
		}
		call.Complete(invocation.Outcome[Result]{Primary: err, Cleanup: cleanup})
		return nil, receipt, nil
	}
	stmt := &Statement{statementState: &statementState{mu: mutex, db: db, parent: parent, conn: conn, owned: owned, native: native, query: query, call: call}}
	if parent != nil {
		parent.statements[stmt] = struct{}{}
	}
	call.Resolve(invocation.Outcome[Result]{Present: true, Value: Result{data: &resultData{complete: true, serverVersion: owned.wire.version}}})
	return stmt, receipt, nil
}
func (stmt *Statement) lock() error {
	if stmt == nil || stmt.statementState == nil {
		return failure(ErrState, "statement")
	}
	if !stmt.mu.TryLock() {
		return failure(ErrState, "concurrent-statement")
	}
	if stmt.closed || stmt.parent != nil && (stmt.parent.closed || stmt.parent.ended || stmt.parent.lifetime.Err() != nil) {
		stmt.mu.Unlock()
		return failure(ErrState, "statement")
	}
	return nil
}
func (stmt *Statement) Query(ctx context.Context, id fault.Correlation, args ...any) (*invocation.Receipt[Result], error) {
	if err := stmt.lock(); err != nil {
		return nil, err
	}
	defer stmt.mu.Unlock()
	return stmt.db.statement(ctx, id, stmt.query, args, true, stmt.parent, stmt)
}
func (stmt *Statement) Exec(ctx context.Context, id fault.Correlation, args ...any) (*invocation.Receipt[Result], error) {
	if err := stmt.lock(); err != nil {
		return nil, err
	}
	defer stmt.mu.Unlock()
	return stmt.db.statement(ctx, id, stmt.query, args, false, stmt.parent, stmt)
}
func (stmt *Statement) Close(ctx context.Context) (*invocation.Receipt[Result], error) {
	if stmt == nil || stmt.statementState == nil {
		return nil, failure(ErrState, "statement")
	}
	if !stmt.mu.TryLock() {
		return nil, failure(ErrState, "concurrent-statement")
	}
	defer stmt.mu.Unlock()
	if stmt.closed {
		return stmt.call.Receipt(), nil
	}
	work, cancel, err := (invocation.Budget{Limit: stmt.db.owner.settings.CloseTimeout}).Context(ctx, invocation.Cleanup)
	if err != nil {
		return nil, err
	}
	defer cancel()
	stmt.closeLocked(work)
	return stmt.call.Receipt(), nil
}
func (stmt *Statement) closeLocked(ctx context.Context) error {
	if stmt.closed {
		return nil
	}
	var cleanup error
	err := rawNative(stmt.conn, func(c *managedConn) error {
		c.start()
		stop := c.wire.activate(ctx)
		defer stop()
		_, _ = stmt.call.Attempt()
		err := stmt.native.Close()
		c.record(err)
		stop()
		cleanup = c.cleanup()
		return err
	})
	cleanup = joined(ErrCleanup, "statement-close", cleanup, err)
	stmt.closed = true
	stmt.native = nil
	if stmt.parent == nil {
		cleanup = joined(ErrCleanup, "statement-close", cleanup, give(stmt.conn, stmt.owned, cleanup != nil || stmt.owned.wire.closed.Load() || stmt.owned.wire.inTransaction))
	} else {
		delete(stmt.parent.statements, stmt)
	}
	stmt.conn = nil
	stmt.owned = nil
	stmt.call.Finish(cleanup)
	stmt.call.Release()
	return cleanup
}
