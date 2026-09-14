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

package duckdb

import (
	"context"

	sdk "github.com/duckdb/duckdb-go/v2"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Mode chooses one finite DuckDB operation, not a cross-engine protocol.
type Mode uint8

const (
	// Execute runs one SQL statement and retains the native changed-row count.
	Execute Mode = iota + 1
	// Query consumes one SQL result, retaining a bounded positional snapshot.
	Query
	// ExecuteMany prepares once and executes each Rows parameter set in order.
	// Run always wraps the whole batch in a local transaction.
	ExecuteMany
	// Append uses the native Appender. Run always uses a local transaction.
	Append
)

const (
	MaxSQLBytes = 64 << 10
	MaxColumns  = 64
	MaxSteps    = 32
)

// Request is borrowed until Run/Transaction returns. No field may be mutated
// concurrently. Execute/Query use SQL and Args; ExecuteMany uses SQL and Rows;
// Append uses Schema, Table, optional Columns and Rows. Other fields must be empty.
// An empty Rows batch is allowed. Arguments are positional, scalar values only;
// arbitrary driver.Valuer callbacks and SDK/runtime handles are not accepted.
// uint binds as uint64. UUID/Decimal parameters bind as canonical text; the SQL
// target or explicit cast supplies the native type. Temporal parameters use the
// same UTC, precision and representability rules as Appender inputs.
type Request struct {
	private
	Mode    Mode
	SQL     string
	Args    []any
	Rows    [][]any
	Schema  string
	Table   string
	Columns []string
}

// Run performs one finite request. A returned error means validation/admission
// failed before acceptance. Once accepted, native/cleanup failures and partial
// progress are in the returned receipt AND the required independent inbox.
// cleanupCtx is separately supplied authority for rollback/cleanup, not detached
// from ctx. Native destruction still runs synchronously if cleanup expires.
func (database *Database) Run(ctx, cleanupCtx context.Context, id fault.Correlation, request Request) (*invocation.Receipt[Result], error) {
	transaction := request.Mode == Append || request.Mode == ExecuteMany
	return database.run(ctx, cleanupCtx, id, []Request{request}, transaction)
}

// Transaction runs 1–MaxSteps requests in order on the same native connection,
// committing only after every request and its cleanup succeeds. Only DuckDB's
// default local transaction mode is supplied. There are no user-issued transaction
// commands, savepoints, automatic retries or detached transaction handles.
// Total input/result/row bounds apply across all requests, not afresh per step.
func (database *Database) Transaction(ctx, cleanupCtx context.Context, id fault.Correlation, requests []Request) (*invocation.Receipt[Result], error) {
	return database.run(ctx, cleanupCtx, id, requests, true)
}

func (database *Database) run(ctx, cleanupCtx context.Context, id fault.Correlation, requests []Request, transaction bool) (*invocation.Receipt[Result], error) {
	if database == nil || database.owner == nil || database.access == nil || ctx == nil || cleanupCtx == nil {
		return nil, failure(ErrInput, "run")
	}
	if err := cleanupCtx.Err(); err != nil {
		return nil, failure(ErrInput, "cleanup-context", err, context.Cause(cleanupCtx))
	}
	config := database.owner.config
	if err := validateRequests(config, requests); err != nil {
		return nil, err
	}
	name := "run"
	if transaction {
		name = "transaction"
	}
	call, err := invocation.Begin[Result](ctx, database.access, invocation.Request{
		Name: name, Correlation: id, Shape: invocation.Finite, Bytes: config.reservation(),
		EvidenceBytes: config.evidenceReservation(), Admission: invocation.Budget{Limit: config.Timeout},
	}, database.inbox, database.observer)
	if err != nil {
		return nil, err
	}
	progress := Progress{Transaction: transaction}
	var primary, cleanup error
	work, cancel, err := (invocation.Budget{Limit: config.Timeout}).Context(ctx, invocation.Execute)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return call.Receipt(), nil
	}
	defer cancel()
	native, err := database.owner.connector.Connect(work)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: failure(ErrNative, "connect", err, work.Err(), context.Cause(work))})
		return call.Receipt(), nil
	}
	conn := native.(*sdk.Conn)
	if err := work.Err(); err != nil {
		primary = failure(ErrNative, "connect", err, context.Cause(work))
	}
	if primary == nil && transaction {
		_, err := conn.ExecContext(work, "BEGIN TRANSACTION", nil)
		if err != nil {
			primary = failure(ErrNative, "begin", err)
		} else {
			progress.Began = true
		}
	}
	resultBudget := config.ResultBytes
	rowsLeft := config.MaxRows
	for _, request := range requests {
		if primary != nil || cleanup != nil {
			break
		}
		if err := work.Err(); err != nil {
			primary = failure(ErrNative, "execute", err, context.Cause(work))
			break
		}
		step := Step{Mode: request.Mode}
		if request.Mode == Append {
			primary, cleanup = appendRows(work, cleanupCtx, config, conn, request, &step)
		} else {
			primary, cleanup = runSQL(work, conn, request, transaction, &step, &resultBudget, &rowsLeft)
		}
		progress.Steps = append(progress.Steps, step)
	}
	if work.Err() != nil {
		primary = joined(ErrNative, "execute", primary, work.Err(), context.Cause(work))
	}
	if progress.Began && primary == nil && cleanup == nil {
		progress.CommitAttempted = true
		_, err := conn.ExecContext(work, "COMMIT", nil)
		if err != nil {
			primary = failure(ErrNative, "commit", err, work.Err(), context.Cause(work))
		} else {
			progress.Committed = true
		}
	}
	if progress.Began && !progress.Committed {
		clean, stop, err := (invocation.Budget{Limit: config.CleanupTimeout}).Context(cleanupCtx, invocation.Cleanup)
		if err != nil {
			cleanup = joined(ErrCleanup, "rollback", cleanup, err)
		} else {
			progress.RollbackAttempted = true
			_, err = conn.ExecContext(clean, "ROLLBACK", nil)
			progress.RolledBack = err == nil
			if err != nil {
				cleanup = joined(ErrCleanup, "rollback", cleanup, err, clean.Err(), context.Cause(clean))
			}
			stop()
		}
	}
	// Conn.Close synchronously disconnects and destroys any remaining transaction.
	// It has no context API; do not release the resource lease before it returns.
	closeErr := conn.Close()
	progress.ConnectionClosed = true
	cleanup = joined(ErrCleanup, "connection-close", cleanup, closeErr)
	call.Complete(invocation.Outcome[Result]{Value: Result{progress: &progress}, Present: true, Primary: primary, Cleanup: cleanup})
	return call.Receipt(), nil
}

func validateRequests(config settings, requests []Request) error {
	if len(requests) < 1 || len(requests) > MaxSteps {
		return failure(ErrLimit, "steps")
	}
	remaining := config.InputBytes
	rows := 0
	for _, request := range requests {
		if request.Mode < Execute || request.Mode > Append {
			return failure(ErrInput, "mode")
		}
		if request.Mode == Append {
			if request.SQL != "" || len(request.Args) != 0 || !validText(request.Table, 256, false) ||
				!validText(request.Schema, 256, true) || len(request.Columns) > MaxColumns {
				return failure(ErrInput, "append")
			}
			for index, column := range request.Columns {
				if !validText(column, 256, false) {
					return failure(ErrInput, "column")
				}
				for _, earlier := range request.Columns[:index] {
					if earlier == column {
						return failure(ErrInput, "column")
					}
				}
				remaining -= int64(len(column)) + 32
			}
		} else {
			if !validText(request.SQL, MaxSQLBytes, false) || request.Table != "" || request.Schema != "" || len(request.Columns) != 0 {
				return failure(ErrInput, "sql")
			}
		}
		remaining -= int64(len(request.SQL)+len(request.Table)+len(request.Schema)) + 256
		if request.Mode == Execute || request.Mode == Query {
			if len(request.Rows) != 0 {
				return failure(ErrInput, "arguments")
			}
			if err := inputRow(request.Args, &remaining); err != nil {
				return err
			}
		} else {
			if len(request.Args) != 0 {
				return failure(ErrInput, "arguments")
			}
			if len(request.Rows) > config.MaxBatchRows-rows {
				return failure(ErrLimit, "batch-rows")
			}
			rows += len(request.Rows)
			for _, row := range request.Rows {
				if err := inputRow(row, &remaining); err != nil {
					return err
				}
			}
		}
		if remaining < 0 {
			return failure(ErrLimit, "input-bytes")
		}
	}
	return nil
}

func inputRow(row []any, remaining *int64) error {
	if len(row) > MaxColumns {
		return failure(ErrLimit, "columns")
	}
	*remaining -= 32
	for _, value := range row {
		size, err := scalarSize(value)
		if err != nil {
			return err
		}
		if size > *remaining {
			return failure(ErrLimit, "input-bytes")
		}
		*remaining -= size
	}
	return nil
}
