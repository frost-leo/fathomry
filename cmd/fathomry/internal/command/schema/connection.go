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

package schema

import (
	"context"
	"errors"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
)

type connection struct {
	ctx     context.Context
	cancel  context.CancelFunc
	pg      *postgres.Transaction
	mysql   *mysql.Session
	calls   int
	cleanup error
}

func (state *runnerState) begin(ctx context.Context, write bool) (*connection, error) {
	work, cancel := context.WithTimeout(ctx, state.options.Timeout)
	session := &connection{ctx: work, cancel: cancel}
	var err error
	if state.pg != nil {
		access := postgres.ReadOnly
		if write {
			access = postgres.ReadWrite
		}
		session.pg, err = state.pg.Begin(work, postgres.TxOptions{Isolation: postgres.ReadCommitted, Access: access})
	} else {
		session.mysql, err = state.mysql.Retain(work, mysql.SessionOptions{Timeout: state.options.Timeout})
	}
	if session.pg != nil || session.mysql != nil {
		state.pending = session
	} else {
		cancel()
	}
	if err != nil {
		return nil, fail(databasecmd.ErrDatabase, err)
	}
	if state.pending == nil {
		return nil, fail(databasecmd.ErrDatabase)
	}
	return session, nil
}
func (session *connection) admit() error {
	if session.calls >= maxSQLCalls {
		return fail(databasecmd.ErrLimit)
	}
	session.calls++
	if session.ctx.Err() != nil {
		return fail(databasecmd.ErrDatabase, session.ctx.Err(), context.Cause(session.ctx))
	}
	return nil
}
func (session *connection) query(sql string, args ...any) ([][][]byte, error) {
	if err := session.admit(); err != nil {
		return nil, err
	}
	var rows [][][]byte
	if session.pg != nil {
		value, err := session.pg.Query(session.ctx, sql, args...)
		if err != nil {
			return nil, fail(databasecmd.ErrDatabase, err)
		}
		if !value.Complete() {
			return nil, fail(databasecmd.ErrDatabase)
		}
		nativeRows := value.RowsCopy()
		if len(nativeRows) > maxObjects {
			return nil, fail(databasecmd.ErrLimit)
		}
		for _, row := range nativeRows {
			rows = append(rows, row.ValuesCopy())
		}
	} else {
		value, err := session.mysql.Query(session.ctx, sql, args...)
		if err != nil {
			return nil, fail(databasecmd.ErrDatabase, err)
		}
		if !value.Complete() {
			return nil, fail(databasecmd.ErrDatabase)
		}
		nativeRows := value.RowsCopy()
		if len(nativeRows) > maxObjects {
			return nil, fail(databasecmd.ErrLimit)
		}
		for _, row := range nativeRows {
			rows = append(rows, row.ValuesCopy())
		}
	}
	size := 0
	if len(rows) > maxObjects {
		return nil, fail(databasecmd.ErrLimit)
	}
	for _, row := range rows {
		for _, cell := range row {
			if len(cell) > maxStructureBytes-size {
				return nil, fail(databasecmd.ErrLimit)
			}
			size += len(cell)
		}
	}
	return rows, nil
}
func (session *connection) exec(sql string, args ...any) (int64, error) {
	if err := session.admit(); err != nil {
		return 0, err
	}
	if session.pg != nil {
		value, err := session.pg.Exec(session.ctx, sql, args...)
		if err != nil {
			return 0, fail(databasecmd.ErrDatabase, err)
		}
		count := value.RowsAffected()
		if !value.Complete() {
			return 0, fail(databasecmd.ErrDatabase)
		}
		return count, nil
	}
	value, err := session.mysql.Exec(session.ctx, sql, args...)
	if err != nil {
		return 0, fail(databasecmd.ErrDatabase, err)
	}
	count, known := value.RowsAffected()
	if !value.Complete() {
		return 0, fail(databasecmd.ErrDatabase)
	}
	if !known {
		return 0, nil
	}
	return count, nil
}
func (session *connection) scalar(sql string, args ...any) (string, error) {
	rows, err := session.query(sql, args...)
	if err != nil {
		return "", err
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] == nil {
		return "", fail(databasecmd.ErrDatabase)
	}
	return string(rows[0][0]), nil
}
func (session *connection) close(ctx context.Context) (released bool, outcome postgres.TransactionOutcome, err error) {
	if session.pg != nil {
		snapshot, _ := session.pg.Receipt().Snapshot()
		if !snapshot.Info().Released {
			value, closeError := session.pg.Rollback(ctx)
			outcome = value.TransactionOutcome()
			err = closeError
		}
		final, waitError := session.pg.Receipt().WaitReleased(ctx)
		released = final.Info().Released
		if released {
			session.cleanup = final.Cleanup()
		}
		if value, present := final.ValueCopy(); present && value.TransactionOutcome() != postgres.TransactionUnobserved {
			outcome = value.TransactionOutcome()
		}
		err = errors.Join(err, waitError, final.Cleanup())
	} else {
		err = session.mysql.Close(ctx)
		final, waitError := session.mysql.Receipt().WaitReleased(ctx)
		released = final.Info().Released
		if released {
			session.cleanup = final.Cleanup()
		}
		err = errors.Join(err, waitError, final.Cleanup())
	}
	if released {
		session.cancel()
	}
	if err != nil {
		return released, outcome, fail(databasecmd.ErrCleanup, err)
	}
	return released, outcome, nil
}
func (state *runnerState) status(value State, reason string) Status {
	return Status{State: value, Family: family, Dialect: state.dialect, Revision: "1", Reason: reason}
}
