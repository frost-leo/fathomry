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
	"fmt"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// SavepointOutcome is local subtransaction evidence, never outer durability.
type SavepointOutcome string

const (
	SavepointUnobserved   SavepointOutcome = ""
	SavepointReleased     SavepointOutcome = "savepoint-released"
	SavepointRolledBack   SavepointOutcome = "savepoint-rolled-back-and-released"
	SavepointRollbackOnly SavepointOutcome = "savepoint-rollback-acknowledged"
	SavepointUnknown      SavepointOutcome = "savepoint-finalization-unknown"
	SavepointParentEnded  SavepointOutcome = "savepoint-ended-with-parent"
)

// Savepoint owns one bounded LIFO subtransaction scope. Queries and preparation
// remain on Transaction and execute inside its current stack. Copies share the
// original scope; Release or terminal Rollback is required unless the parent ends.
type Savepoint struct {
	private
	*savepointState
}
type savepointState struct {
	parent *Transaction
	name   string
	call   *invocation.Call[Result]
	closed bool
}

func (Savepoint) Format(state fmt.State, _ rune) { restricted(state) }
func (*Savepoint) LogValue() slog.Value          { return slog.StringValue("pgx[restricted]") }

// Savepoint establishes a bounded named scope with a code-owned identifier.
func (transaction *Transaction) Savepoint(ctx context.Context, id fault.Correlation) (*Savepoint, *invocation.Receipt[Result], error) {
	if err := transaction.lock(); err != nil {
		return nil, nil, err
	}
	defer transaction.mu.Unlock()
	if transaction.ended || transaction.rollbackOnly {
		return nil, nil, failure(ErrState, "transaction-ended")
	}
	if len(transaction.savepoints) >= MaxSavepoints {
		return nil, nil, failure(ErrLimit, "savepoints")
	}
	call, err := transaction.database.beginCall(ctx, id, "savepoint", invocation.Session, transaction.call)
	if err != nil {
		return nil, nil, err
	}
	receipt := call.Receipt()
	work, cancel, err := (invocation.Budget{Limit: transaction.database.owner.settings.Timeout}).Context(ctx, invocation.Establish)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return nil, receipt, nil
	}
	defer cancel()
	name, err := transaction.handle.Value().nextName("fathomry_savepoint_")
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return nil, receipt, nil
	}
	_, _ = call.Attempt()
	result, primary, cleanup := consume(work, transaction.handle.Value().native, transaction.database.owner.settings, "SAVEPOINT "+name, nil, false)
	if primary != nil || cleanup != nil {
		transaction.observeState(Result{})
		call.Complete(invocation.Outcome[Result]{Present: true, Value: result, Primary: primary, Cleanup: cleanup})
		return nil, receipt, nil
	}
	point := &Savepoint{savepointState: &savepointState{parent: transaction, name: name, call: call}}
	transaction.savepoints = append(transaction.savepoints, point.savepointState)
	return point, receipt, nil
}

// Release keeps subtransaction changes inside the parent, not committed outside it.
func (point *Savepoint) Release(ctx context.Context) (*invocation.Receipt[Result], error) {
	return point.finish(ctx, false)
}

// Rollback rolls back to the point and releases its server scope. Merely rolling
// back to a PostgreSQL savepoint would leave its stack entry alive.
func (point *Savepoint) Rollback(ctx context.Context) (*invocation.Receipt[Result], error) {
	return point.finish(ctx, true)
}
func (point *Savepoint) finish(ctx context.Context, rollback bool) (*invocation.Receipt[Result], error) {
	if point == nil || point.savepointState == nil {
		return nil, failure(ErrState, "savepoint")
	}
	parent := point.parent
	if !parent.mu.TryLock() {
		return nil, failure(ErrState, "concurrent-savepoint")
	}
	defer parent.mu.Unlock()
	if point.closed {
		return point.call.Receipt(), nil
	}
	if parent.closed || parent.ended || parent.rollbackOnly {
		return nil, failure(ErrState, "savepoint-parent")
	}
	stack := parent.savepoints
	if len(stack) == 0 || stack[len(stack)-1] != point.savepointState {
		return nil, failure(ErrState, "savepoint-order")
	}
	work, cancel, err := (invocation.Budget{Limit: parent.database.owner.settings.CloseTimeout}).Context(ctx, invocation.Cleanup)
	if err != nil {
		return nil, err
	}
	defer cancel()
	connection := parent.handle.Value().native
	data := &resultData{savepoint: SavepointUnknown, serverVersion: connection.PgConn().ParameterStatus("server_version")}
	var primary, cleanup error
	if rollback {
		_, _ = point.call.Attempt()
		_, primary, cleanup = consume(work, connection, parent.database.owner.settings, "ROLLBACK TO SAVEPOINT "+point.name, nil, false)
		if primary == nil && cleanup == nil {
			data.savepoint = SavepointRollbackOnly
		}
	}
	if primary == nil && cleanup == nil {
		_, _ = point.call.Attempt()
		_, release, drain := consume(work, connection, parent.database.owner.settings, "RELEASE SAVEPOINT "+point.name, nil, false)
		cleanup = failureOrNil(ErrCleanup, "savepoint-release", release, drain)
		if cleanup == nil {
			data.complete = true
			data.savepoint = SavepointReleased
			if rollback {
				data.savepoint = SavepointRolledBack
			}
		}
	}
	point.closed = true
	parent.savepoints = stack[:len(stack)-1]
	if primary != nil || cleanup != nil {
		parent.rollbackOnly = true
	}
	parent.observeState(Result{})
	point.call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: primary, Cleanup: cleanup})
	return point.call.Receipt(), nil
}
func (transaction *Transaction) settleSavepoints() {
	for _, state := range transaction.savepoints {
		state.closed = true
		state.call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: &resultData{savepoint: SavepointParentEnded}}})
	}
	transaction.savepoints = nil
}
