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
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"
)

// Statement retains one explicitly owned native preparation. A standalone
// statement pins a root connection; a transaction statement borrows its owner.
// Copies share authority and must not be used concurrently. Close is required;
// transaction finalization also closes its remaining preparations.
type Statement struct {
	private
	*statementState
}
type statementState struct {
	mu          *sync.Mutex
	database    *Database
	parent      *Transaction
	handle      *puddle.Resource[*connection]
	connection  *connection
	description *pgconn.StatementDescription
	call        *invocation.Call[Result]
	closed      bool
}

func (Statement) Format(state fmt.State, _ rune) { restricted(state) }
func (*Statement) LogValue() slog.Value          { return slog.StringValue("pgx[restricted]") }

// Prepare uses a caller context for preparation only, not retained lifetime.
func (database *Database) Prepare(ctx context.Context, id fault.Correlation, sql string) (*Statement, *invocation.Receipt[Result], error) {
	return database.prepare(ctx, id, sql, nil)
}

// Prepare creates a bounded statement on this transaction's original connection.
func (transaction *Transaction) Prepare(ctx context.Context, id fault.Correlation, sql string) (*Statement, *invocation.Receipt[Result], error) {
	if err := transaction.lock(); err != nil {
		return nil, nil, err
	}
	defer transaction.mu.Unlock()
	if transaction.ended || transaction.rollbackOnly {
		return nil, nil, failure(ErrState, "transaction-ended")
	}
	if len(transaction.statements) >= MaxPreparedStatements {
		return nil, nil, failure(ErrLimit, "preparations")
	}
	return transaction.database.prepare(ctx, id, sql, transaction)
}
func (database *Database) prepare(ctx context.Context, id fault.Correlation, sql string, parent *Transaction) (*Statement, *invocation.Receipt[Result], error) {
	if err := validStatement(sql, nil); err != nil {
		return nil, nil, err
	}
	var parentCall *invocation.Call[Result]
	if parent != nil {
		parentCall = parent.call
	}
	call, err := database.beginCall(ctx, id, "prepare", invocation.Session, parentCall)
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
	var handle *puddle.Resource[*connection]
	mutex := new(sync.Mutex)
	if parent == nil {
		handle, err = database.owner.take(work)
		if err != nil {
			call.Complete(invocation.Outcome[Result]{Primary: err})
			return nil, receipt, nil
		}
	} else {
		handle = parent.handle
		mutex = &parent.mu
	}
	connection := handle.Value()
	name, err := connection.nextName("fathomry_statement_")
	var description *pgconn.StatementDescription
	if err == nil {
		_, _ = call.Attempt()
		description, err = connection.native.PgConn().Prepare(nativeContext{work}, name, sql, nil)
	}
	primary := nativeFailure(ErrQuery, "prepare", work, err)
	if err == nil {
		primary = validateDescription(description, database.owner.settings)
	}
	if parent != nil {
		parent.observeState(Result{})
	}
	if primary != nil {
		var cleanup error
		var prepareErr *pgconn.PrepareError
		if description != nil || errors.As(err, &prepareErr) && prepareErr.ParseComplete {
			cleanup = deallocate(connection, name, database.owner.settings, call)
		}
		if parent == nil {
			cleanup = failureOrNil(ErrCleanup, "prepare", cleanup, database.owner.give(handle))
		} else if cleanup != nil {
			parent.rollbackOnly = true
		}
		call.Complete(invocation.Outcome[Result]{Primary: primary, Cleanup: cleanup})
		return nil, receipt, nil
	}
	statement := &Statement{statementState: &statementState{mu: mutex, database: database, parent: parent, handle: handle, connection: connection, description: description, call: call}}
	if parent != nil {
		parent.statements[statement.statementState] = statement
	}
	data := &resultData{complete: true, serverVersion: connection.native.PgConn().ParameterStatus("server_version")}
	for _, field := range description.Fields {
		data.columns = append(data.columns, Column{Name: strings.Clone(field.Name), OID: field.DataTypeOID})
	}
	call.Resolve(invocation.Outcome[Result]{Present: true, Value: Result{data: data}})
	return statement, receipt, nil
}
func validateDescription(description *pgconn.StatementDescription, settings settings) error {
	if len(description.ParamOIDs) > MaxArguments || len(description.Fields) > MaxColumns {
		return failure(ErrLimit, "prepare-metadata")
	}
	total := 0
	for _, field := range description.Fields {
		total += len(field.Name)
		if total > settings.MaxResultBytes {
			return failure(ErrLimit, "prepare-metadata")
		}
	}
	return nil
}
func (statement *Statement) lock() error {
	if statement == nil || statement.statementState == nil {
		return failure(ErrState, "statement")
	}
	if !statement.mu.TryLock() {
		return failure(ErrState, "concurrent-statement")
	}
	if statement.closed || statement.parent != nil && (statement.parent.closed || statement.parent.ended || statement.parent.rollbackOnly) {
		statement.mu.Unlock()
		return failure(ErrState, "statement")
	}
	return nil
}

// Query reuses the native preparation with the same text parameter/result contract
// as Database.Query. It does not retry or automatically reprepare invalidated SQL.
func (statement *Statement) Query(ctx context.Context, id fault.Correlation, args ...any) (*invocation.Receipt[Result], error) {
	if err := statement.lock(); err != nil {
		return nil, err
	}
	defer statement.mu.Unlock()
	return statement.database.statement(ctx, id, statement.description.SQL, args, true, statement.parent, statement)
}

// Exec reuses the preparation while consuming but not retaining bounded rows.
func (statement *Statement) Exec(ctx context.Context, id fault.Correlation, args ...any) (*invocation.Receipt[Result], error) {
	if err := statement.lock(); err != nil {
		return nil, err
	}
	defer statement.mu.Unlock()
	return statement.database.statement(ctx, id, statement.description.SQL, args, false, statement.parent, statement)
}

// Close ends this preparation's original evidence slot without new admission.
// A repeated close returns the original receipt, including its earlier errors.
func (statement *Statement) Close(ctx context.Context) (*invocation.Receipt[Result], error) {
	if statement == nil || statement.statementState == nil {
		return nil, failure(ErrState, "statement")
	}
	if !statement.mu.TryLock() {
		return nil, failure(ErrState, "concurrent-statement")
	}
	defer statement.mu.Unlock()
	if statement.closed {
		return statement.call.Receipt(), nil
	}
	work, cancel, err := (invocation.Budget{Limit: statement.database.owner.settings.CloseTimeout}).Context(ctx, invocation.Cleanup)
	if err != nil {
		return nil, err
	}
	defer cancel()
	statement.closeLocked(work)
	return statement.call.Receipt(), nil
}
func (statement *Statement) closeLocked(ctx context.Context) error {
	if statement.closed {
		return nil
	}
	_, _ = statement.call.Attempt()
	err := statement.connection.native.PgConn().Deallocate(nativeContext{ctx}, statement.description.Name)
	cleanup := nativeFailure(ErrCleanup, "deallocate", ctx, err)
	if err != nil {
		cleanup = failureOrNil(ErrCleanup, "deallocate-retire", cleanup, retirePreparation(statement.connection, statement.database.owner.settings))
	}
	statement.closed = true
	if statement.parent == nil {
		cleanup = failureOrNil(ErrCleanup, "statement-close", cleanup, statement.database.owner.give(statement.handle))
	} else {
		delete(statement.parent.statements, statement.statementState)
		if cleanup != nil {
			statement.parent.rollbackOnly = true
		}
	}
	statement.connection = nil
	statement.handle = nil
	statement.description = nil
	statement.call.Finish(cleanup)
	statement.call.Release()
	return cleanup
}
func deallocate(connection *connection, name string, settings settings, call *invocation.Call[Result]) error {
	if connection.native.IsClosed() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), settings.CloseTimeout)
	defer cancel()
	_, _ = call.Attempt()
	err := connection.native.PgConn().Deallocate(nativeContext{ctx}, name)
	cleanup := nativeFailure(ErrCleanup, "deallocate", ctx, err)
	if err != nil {
		cleanup = failureOrNil(ErrCleanup, "deallocate-retire", cleanup, retirePreparation(connection, settings))
	}
	return cleanup
}

// A protocol Close error can return before ReadyForQuery. Never run another
// command on that stream and mistake the leftover message for its completion.
func retirePreparation(connection *connection, settings settings) error {
	ctx, cancel := context.WithTimeout(context.Background(), settings.CloseTimeout)
	defer cancel()
	return connection.close(ctx)
}
