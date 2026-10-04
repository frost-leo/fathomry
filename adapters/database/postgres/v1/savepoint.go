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
)

// Savepoint owns a bounded LIFO subtransaction scope. Queries still use the
// Transaction; finalizing the parent settles every outstanding savepoint.
type Savepoint struct {
	private
	session *session
	native  *native.Savepoint
}

// Savepoint establishes a code-named scope on this transaction's connection.
func (transaction *Transaction) Savepoint(ctx context.Context) (*Savepoint, error) {
	if transaction == nil || transaction.session == nil {
		return nil, fail(ErrState, "savepoint")
	}
	var point *Savepoint
	receipt, err := transaction.session.child(ctx, transaction.session.lifetime, "savepoint", func(value *session) {
		guard, err := value.call.Hold()
		if err != nil {
			_ = value.call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		setup, stop := value.setup(ctx)
		defer stop()
		handle, receipt, err := transaction.native.Savepoint(setup, value.family.correlation(value.call))
		if handle == nil {
			value.finish(receipt, err)
			_ = guard.Release()
			return
		}
		point = &Savepoint{session: value, native: handle}
		// Only parent finalization can safely cancel an arbitrary LIFO stack entry.
		if err = value.keep(guard, receipt, nil); err != nil {
			_ = value.call.Resolve(adapters.Outcome[Result]{Primary: err})
		}
	})
	_, err = result(receipt, err)
	return point, err
}

// Receipt exposes read-only subtransaction finalization evidence.
func (point *Savepoint) Receipt() *adapters.Receipt[Result] {
	if point == nil || point.session == nil {
		return nil
	}
	return point.session.call.Receipt()
}

// Release retains changes within the parent; it does not commit them externally.
func (point *Savepoint) Release(ctx context.Context) (Result, error) {
	if point == nil || point.session == nil {
		return Result{}, fail(ErrState, "savepoint_release")
	}
	return point.session.finalize(ctx, "savepoint_release", point.native.Release)
}

// Rollback rolls back to this point and releases its server scope.
func (point *Savepoint) Rollback(ctx context.Context) (Result, error) {
	if point == nil || point.session == nil {
		return Result{}, fail(ErrState, "savepoint_rollback")
	}
	return point.session.finalize(ctx, "savepoint_rollback", point.native.Rollback)
}
