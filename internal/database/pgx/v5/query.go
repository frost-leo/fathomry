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
	"math"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	sdk "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	MaxSQLBytes           = 64 << 10
	MaxArguments          = 128
	MaxArgumentBytes      = 1 << 20
	MaxColumns            = 64
	MaxPreparedStatements = 8
	MaxSavepoints         = 8
)

// Column is copied native text-result metadata. Name is deliberate sensitive
// inspection, not a diagnostic label. No type map or connection accompanies it.
type Column struct {
	private
	Name string
	OID  uint32
}
type cell struct {
	text string
	null bool
}

// Row owns immutable raw PostgreSQL text-format cells. It contains no SDK
// scanner or decoded arbitrary value. Copying Row shares only immutable storage.
type Row struct {
	private
	cells []cell
}

// ValuesCopy returns independently owned protocol text bytes. nil means SQL
// NULL; a non-nil zero-length slice means a present empty value.
func (row Row) ValuesCopy() [][]byte {
	values := make([][]byte, len(row.cells))
	for index, cell := range row.cells {
		if !cell.null {
			values[index] = []byte(cell.text)
		}
	}
	return values
}

// Result is an immutable, process-local observation shared by the direct receipt
// and independent evidence. A complete statement is not a transaction commit,
// replica visibility guarantee or business outcome.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	columns       []Column
	rows          []Row
	rowsRead      int
	command       string
	affected      int64
	complete      bool
	retained      bool
	transaction   TransactionOutcome
	savepoint     SavepointOutcome
	serverVersion string
}

// ColumnsCopy returns independent metadata storage, or nil when unavailable.
func (result Result) ColumnsCopy() []Column {
	if result.data == nil {
		return nil
	}
	return append([]Column(nil), result.data.columns...)
}

// RowsCopy copies the immutable row list, including partial data. Nil means no
// retained query result (including Exec); successful empty Query returns []Row{}.
func (result Result) RowsCopy() []Row {
	if result.data == nil || !result.data.retained {
		return nil
	}
	return append([]Row{}, result.data.rows...)
}

// RowsRead counts observed rows, including the first over-limit witness if any.
func (result Result) RowsRead() int {
	if result.data == nil {
		return 0
	}
	return result.data.rowsRead
}

// CommandTag is native command evidence; empty means unavailable, not failure.
func (result Result) CommandTag() string {
	if result.data == nil {
		return ""
	}
	return result.data.command
}

// RowsAffected retains native command-tag semantics, not per-Item effect proof.
func (result Result) RowsAffected() int64 {
	if result.data == nil {
		return 0
	}
	return result.data.affected
}

// Complete means the bounded result was consumed through native terminal error
// checking with no failure. It says nothing about other statements or durability.
func (result Result) Complete() bool { return result.data != nil && result.data.complete }

// First returns the first row only after complete successful consumption. Like
// native QueryRow it does not require uniqueness. Empty success retains ErrNoRows.
func (result Result) First() (Row, error) {
	if !result.Complete() || !result.data.retained {
		return Row{}, failure(ErrState, "first")
	}
	if len(result.data.rows) == 0 {
		return Row{}, failure(ErrQuery, "first", sdk.ErrNoRows)
	}
	return result.data.rows[0], nil
}

// ServerVersion returns the bounded native startup observation, not an attested
// deployment identity. Empty means unavailable; do not use this text as a label.
func (result Result) ServerVersion() string {
	if result.data == nil {
		return ""
	}
	return result.data.serverVersion
}

// TransactionOutcome is absent for ordinary statements, including Tx statements.
func (result Result) TransactionOutcome() TransactionOutcome {
	if result.data == nil {
		return TransactionUnobserved
	}
	return result.data.transaction
}

// SavepointOutcome never certifies an outer transaction commit.
func (result Result) SavepointOutcome() SavepointOutcome {
	if result.data == nil {
		return SavepointUnobserved
	}
	return result.data.savepoint
}

// Query executes one extended-protocol statement and consumes bounded raw rows
// synchronously. Setup/admission failures return no receipt. Once accepted,
// errors and partial data are in Receipt.Result().Err(), independently in Inbox.
// Context values are not forwarded to native transport hooks.
func (database *Database) Query(ctx context.Context, correlation fault.Correlation, sql string, args ...any) (*invocation.Receipt[Result], error) {
	return database.statement(ctx, correlation, sql, args, true, nil, nil)
}

// Exec uses the same bounded consumption path as Query but retains no row data.
// RETURNING/SELECT rows still count against row/byte limits; it never calls
// pgx.Exec's unbounded ResultReader.Read convenience path.
func (database *Database) Exec(ctx context.Context, correlation fault.Correlation, sql string, args ...any) (*invocation.Receipt[Result], error) {
	return database.statement(ctx, correlation, sql, args, false, nil, nil)
}

func validStatement(sql string, args []any) error {
	if len(sql) == 0 || len(sql) > MaxSQLBytes || !validText(sql, MaxSQLBytes, false) || len(args) > MaxArguments {
		return failure(ErrInput, "statement")
	}
	total := 0
	for _, value := range args {
		// Exec mode sends text: scalar Go widths do not bound decimal encodings,
		// particularly fixed-point subnormal floats. Reserve conservative maxima.
		switch value := value.(type) {
		case nil, bool, int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint:
			total += 32
		case float32:
			if math.IsInf(float64(value), 0) || math.IsNaN(float64(value)) {
				return failure(ErrInput, "argument")
			}
			total += 64
		case float64:
			if math.IsInf(value, 0) || math.IsNaN(value) {
				return failure(ErrInput, "argument")
			}
			total += 512
		case string:
			if !validText(value, MaxArgumentBytes, true) {
				return failure(ErrInput, "argument")
			}
			total += len(value)
		case []byte:
			if len(value) > MaxArgumentBytes {
				return failure(ErrLimit, "argument")
			}
			total += 2*len(value) + 2
		case time.Time:
			total += 64
		default:
			return failure(ErrUnsupported, "argument")
		}
		if total > MaxArgumentBytes {
			return failure(ErrLimit, "argument")
		}
	}
	return nil
}

func (database *Database) beginCall(ctx context.Context, correlation fault.Correlation, name string, shape invocation.Shape, parent *invocation.Call[Result]) (*invocation.Call[Result], error) {
	if database == nil || database.owner == nil || ctx == nil {
		return nil, failure(ErrInput, name)
	}
	value := database.owner.settings
	request := invocation.Request{Name: name, Correlation: correlation, Shape: shape, Bytes: value.reservation(),
		EvidenceBytes: value.evidenceReservation(), Admission: invocation.Budget{Limit: value.Timeout}}
	if parent != nil {
		request.Bytes = 0
		if request.Correlation.Parent == "" {
			metadata, _ := parent.Receipt().Result()
			request.Correlation.Parent = metadata.Context.Correlation.Call
		}
		return invocation.BeginNested(ctx, parent.Scope(), request, database.inbox, database.observer)
	}
	return invocation.Begin(ctx, database.access, request, database.inbox, database.observer)
}
func (database *Database) statement(ctx context.Context, correlation fault.Correlation, sql string, args []any, keep bool, parent *Transaction, prepared *Statement) (*invocation.Receipt[Result], error) {
	if err := validStatement(sql, args); err != nil {
		return nil, err
	}
	name := "query"
	if !keep {
		name = "exec"
	}
	var parentCall *invocation.Call[Result]
	if parent != nil {
		parentCall = parent.call
	}
	if prepared != nil {
		parentCall = prepared.call
	}
	call, err := database.beginCall(ctx, correlation, name, invocation.Finite, parentCall)
	if err != nil {
		return nil, err
	}
	receipt := call.Receipt()
	value := database.owner.settings
	work, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(ctx, invocation.Execute)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return receipt, nil
	}
	defer cancel()
	var connection *connection
	var give func() error
	if prepared != nil {
		connection = prepared.connection
		give = func() error { return nil }
	} else if parent != nil {
		connection = parent.handle.Value()
		give = func() error { return nil }
	} else {
		handle, err := database.owner.take(work)
		if err != nil {
			call.Complete(invocation.Outcome[Result]{Primary: err})
			return receipt, nil
		}
		connection = handle.Value()
		give = func() error { return database.owner.give(handle) }
	}
	if work.Err() != nil {
		call.Complete(invocation.Outcome[Result]{Primary: failure(ErrQuery, name, work.Err(), context.Cause(work)), Cleanup: give()})
		return receipt, nil
	}
	_, _ = call.Attempt()
	var description *pgconn.StatementDescription
	if prepared != nil {
		description = prepared.description
	}
	result, primary, cleanup := consumeStatement(work, connection.native, value, sql, args, keep, description)
	if parent != nil {
		parent.observeState(result)
	}
	cleanup = failureOrNil(ErrCleanup, "consume", cleanup, give())
	call.Complete(invocation.Outcome[Result]{Present: true, Value: result, Primary: primary, Cleanup: cleanup})
	return receipt, nil
}

func consume(ctx context.Context, connection *sdk.Conn, limits settings, sql string, args []any, keep bool) (Result, error, error) {
	return consumeStatement(ctx, connection, limits, sql, args, keep, nil)
}
func consumeStatement(ctx context.Context, connection *sdk.Conn, limits settings, sql string, args []any, keep bool, description *pgconn.StatementDescription) (Result, error, error) {
	data := &resultData{retained: keep, serverVersion: connection.PgConn().ParameterStatus("server_version")}
	result := Result{data: data}
	var rows sdk.Rows
	var err error
	if description == nil {
		rows, err = connection.Query(nativeContext{ctx}, sql, args...)
	} else {
		if len(args) != len(description.ParamOIDs) {
			return result, failure(ErrInput, "argument-count"), nil
		}
		var builder sdk.ExtendedQueryBuilder
		err = builder.Build(connection.TypeMap(), nil, args)
		if err == nil {
			reader := connection.PgConn().ExecStatement(nativeContext{ctx}, description, builder.ParamValues, builder.ParamFormats, []int16{sdk.TextFormatCode})
			rows = sdk.RowsFromResultReader(connection.TypeMap(), reader)
		}
	}
	if err != nil {
		return result, nativeFailure(ErrQuery, "query", ctx, err), nil
	}
	columns := rows.FieldDescriptions()
	total := 0
	var primary error
	if len(columns) > MaxColumns {
		primary = failure(ErrLimit, "columns")
	} else {
		for _, column := range columns {
			total += len(column.Name)
			if total > limits.MaxResultBytes || column.Format != sdk.TextFormatCode {
				primary = failure(ErrLimit, "columns")
				break
			}
			data.columns = append(data.columns, Column{Name: strings.Clone(column.Name), OID: column.DataTypeOID})
		}
	}
	for primary == nil && rows.Next() {
		raw := rows.RawValues()
		data.rowsRead++
		if data.rowsRead > limits.MaxRows || len(raw) != len(data.columns) {
			primary = failure(ErrLimit, "rows")
			break
		}
		for _, value := range raw {
			total += len(value)
		}
		if total > limits.MaxResultBytes {
			primary = failure(ErrLimit, "result")
			break
		}
		if keep {
			row := Row{cells: make([]cell, len(raw))}
			for index, value := range raw {
				row.cells[index] = cell{text: string(value), null: value == nil}
			}
			data.rows = append(data.rows, row)
		}
	}
	rows.Close()
	nativeErr := rows.Err()
	tag := rows.CommandTag()
	data.command, data.affected = tag.String(), tag.RowsAffected()
	if primary != nil {
		return result, primary, nativeFailure(ErrQuery, "drain", ctx, nativeErr)
	}
	if nativeErr != nil {
		return result, nativeFailure(ErrQuery, "consume", ctx, nativeErr), nil
	}
	data.complete = true
	return result, nil, nil
}
