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
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

const (
	MaxSQLBytes      = 64 << 10
	MaxArguments     = 128
	MaxArgumentBytes = 1 << 20
	MaxColumns       = 64
)

// Column is copied native metadata. Names/type inspection deliberately expose
// query data, not diagnostic labels. Precision/scale are present only when known.
type Column struct {
	private
	Name           string
	DatabaseType   string
	Nullable       bool
	NullableKnown  bool
	Precision      int64
	Scale          int64
	PrecisionKnown bool
}
type cell struct {
	text string
	null bool
}

// Row contains immutable cell encodings. Dates/decimal/JSON stay native text;
// integer/float encodings follow database/sql.RawBytes, never float64 coercion.
type Row struct {
	private
	cells []cell
}

func (r Row) ValuesCopy() [][]byte {
	out := make([][]byte, len(r.cells))
	for index, c := range r.cells {
		if !c.null {
			out[index] = []byte(c.text)
		}
	}
	return out
}

// Result is immutable evidence shared by a receipt and independent inbox.
// Acknowledgement does not strengthen the server's durability/visibility settings.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	columns       []Column
	rows          []Row
	retained      bool
	rowsRead      int
	affected      int64
	insertID      int64
	commandKnown  bool
	complete      bool
	transaction   TransactionOutcome
	serverVersion string
}

func (r Result) ColumnsCopy() []Column {
	if r.data == nil {
		return nil
	}
	return append([]Column(nil), r.data.columns...)
}
func (r Result) RowsCopy() []Row {
	if r.data == nil || !r.data.retained {
		return nil
	}
	return append([]Row{}, r.data.rows...)
}
func (r Result) RowsRead() int {
	if r.data == nil {
		return 0
	}
	return r.data.rowsRead
}
func (r Result) Complete() bool { return r.data != nil && r.data.complete }
func (r Result) RowsAffected() (int64, bool) {
	if r.data == nil {
		return 0, false
	}
	return r.data.affected, r.data.commandKnown
}
func (r Result) LastInsertID() (int64, bool) {
	if r.data == nil {
		return 0, false
	}
	return r.data.insertID, r.data.commandKnown
}
func (r Result) ServerVersion() string {
	if r.data == nil {
		return ""
	}
	return r.data.serverVersion
}
func (r Result) TransactionOutcome() TransactionOutcome {
	if r.data == nil {
		return TransactionUnobserved
	}
	return r.data.transaction
}
func (r Result) First() (Row, error) {
	if !r.Complete() || !r.data.retained {
		return Row{}, failure(ErrState, "first")
	}
	if len(r.data.rows) == 0 {
		return Row{}, failure(ErrQuery, "first", sql.ErrNoRows)
	}
	return r.data.rows[0], nil
}
func validStatement(query string, args []any) error {
	if !textValid(query, MaxSQLBytes, false) || len(args) > MaxArguments {
		return failure(ErrInput, "statement")
	}
	size := 0
	for _, arg := range args {
		switch v := arg.(type) {
		case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			size += 16
		case float32:
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				return failure(ErrInput, "argument")
			}
			size += 16
		case float64:
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return failure(ErrInput, "argument")
			}
			size += 16
		case string:
			if !textValid(v, MaxArgumentBytes, true) {
				return failure(ErrInput, "argument")
			}
			size += len(v) + 16
		case []byte:
			if len(v) > MaxArgumentBytes {
				return failure(ErrLimit, "argument")
			}
			size += len(v) + 16
		case json.RawMessage:
			if len(v) > MaxArgumentBytes {
				return failure(ErrLimit, "argument")
			}
			size += len(v) + 16
		case time.Time:
			if !v.IsZero() && (v.UTC().Year() < 1 || v.UTC().Year() > 9999) {
				return failure(ErrInput, "argument-time")
			}
			size += 64
		default:
			return failure(ErrUnsupported, "argument")
		}
		if size > MaxArgumentBytes {
			return failure(ErrLimit, "arguments")
		}
	}
	return nil
}
func (db *Database) beginCall(ctx context.Context, id fault.Correlation, name string, shape invocation.Shape, parent *invocation.Call[Result]) (*invocation.Call[Result], error) {
	if db == nil || db.owner == nil || ctx == nil {
		return nil, failure(ErrInput, name)
	}
	s := db.owner.settings
	request := invocation.Request{Name: name, Shape: shape, Correlation: id, Bytes: s.reservation(), EvidenceBytes: s.evidenceReservation(), Admission: invocation.Budget{Limit: s.Timeout}}
	if parent != nil {
		request.Bytes = 0
		if request.Correlation.Parent == "" {
			metadata, _ := parent.Receipt().Result()
			request.Correlation.Parent = metadata.Context.Correlation.Call
		}
		return invocation.BeginNested(ctx, parent.Scope(), request, db.inbox, db.observer)
	}
	return invocation.Begin(ctx, db.access, request, db.inbox, db.observer)
}

// Query executes and consumes one bounded native result synchronously. Setup errors
// have no receipt; accepted failures/partial results remain in receipt and Inbox.
func (db *Database) Query(ctx context.Context, id fault.Correlation, query string, args ...any) (*invocation.Receipt[Result], error) {
	return db.statement(ctx, id, query, args, true, nil, nil)
}

// Exec accepts authorized native SQL and preserves changed-row/insert-ID evidence.
// Native parameter binding is used without a database/sql statement retry loop.
func (db *Database) Exec(ctx context.Context, id fault.Correlation, query string, args ...any) (*invocation.Receipt[Result], error) {
	return db.statement(ctx, id, query, args, false, nil, nil)
}
func (db *Database) statement(ctx context.Context, id fault.Correlation, query string, args []any, keep bool, parent *Transaction, prepared *Statement) (*invocation.Receipt[Result], error) {
	if err := validStatement(query, args); err != nil {
		return nil, err
	}
	name := "exec"
	if keep {
		name = "query"
	}
	var parentCall *invocation.Call[Result]
	if parent != nil {
		parentCall = parent.call
	}
	if prepared != nil {
		parentCall = prepared.call
	}
	call, err := db.beginCall(ctx, id, name, invocation.Finite, parentCall)
	if err != nil {
		return nil, err
	}
	receipt := call.Receipt()
	work, cancel, err := (invocation.Budget{Limit: db.owner.settings.Timeout}).Context(ctx, invocation.Execute)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return receipt, nil
	}
	defer cancel()
	if parent != nil {
		stop := context.AfterFunc(parent.lifetime, cancel)
		defer stop()
		if parent.lifetime.Err() != nil {
			cancel()
		}
	}
	var conn *sql.Conn
	var owned *managedConn
	switch {
	case parent != nil:
		conn, owned = parent.conn, parent.owned
	case prepared != nil:
		conn, owned = prepared.conn, prepared.owned
	default:
		conn, owned, err = db.owner.take(work)
		if err != nil {
			outcome := connectionOutcome(err)
			call.Complete(outcome)
			return receipt, nil
		}
	}
	data := &resultData{retained: keep, serverVersion: owned.wire.version}
	var primary, cleanup error
	err = rawNative(conn, func(c *managedConn) error {
		c.start()
		stop := c.wire.activate(work)
		defer stop()
		if err := work.Err(); err != nil {
			return err
		}
		if parent != nil && parent.lifetime.Err() != nil {
			return parent.lifetime.Err()
		}
		var stmt driver.Stmt
		if prepared != nil {
			stmt = prepared.native
		}
		operationErr := executeNative(work, c, call, query, args, keep, stmt, data)
		data.rowsRead = max(data.rowsRead, c.wire.rowCount)
		primary = operationError(work, operationErr, c)
		if parent != nil {
			parent.observeState(work, c, call, operationErr)
		}
		stop()
		if parent != nil && c.wire.closed.Load() {
			parent.ended = true
		}
		cleanup = c.cleanup()
		return operationErr
	})
	if primary == nil && err != nil {
		primary = failure(ErrQuery, "execute", err, contextCause(work, err))
	}
	data.complete = primary == nil
	if parent == nil && prepared == nil {
		cleanup = give(conn, owned, primary != nil || owned.wire.closed.Load() || owned.wire.inTransaction)
	}
	call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: primary, Cleanup: cleanup})
	return receipt, nil
}
func connectionOutcome(err error) invocation.Outcome[Result] {
	var failed *connectFailure
	outcome := invocation.Outcome[Result]{Primary: failure(ErrConnect, "acquire", err)}
	if errors.As(err, &failed) {
		outcome.Primary, outcome.Cleanup = failed.primary, failed.cleanup
	}
	return outcome
}
func operationError(ctx context.Context, err error, c *managedConn) error {
	if err == nil {
		return nil
	}
	transport, _ := c.wire.evidence()
	var timedOut net.Error
	if errors.As(transport, &timedOut) && timedOut.Timeout() {
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			// Poller deadlines can wake before the context timer is scheduled.
			// Synchronize the actual owning deadline rather than losing its cause.
			<-ctx.Done()
			return joined(ErrQuery, "execute", err, transport, ctx.Err(), context.Cause(ctx))
		}
	}
	return joined(ErrQuery, "execute", err, transport, contextCause(ctx, err))
}

// The native error is captured separately from Raw's callback return so neither
// SQL statement retries nor implicit Conn release can erase cleanup evidence.
func rawNative(conn *sql.Conn, run func(*managedConn) error) error {
	var nativeError error
	boundary := conn.Raw(func(value any) error {
		c, ok := value.(*managedConn)
		if !ok {
			return failure(ErrState, "raw-connection")
		}
		nativeError = run(c)
		return nil
	})
	return errors.Join(nativeError, boundary)
}
func arguments(c *managedConn, args []any) ([]driver.NamedValue, error) {
	values := make([]driver.NamedValue, len(args))
	for index, value := range args {
		values[index] = driver.NamedValue{Ordinal: index + 1, Value: value}
		if err := c.Conn.(driver.NamedValueChecker).CheckNamedValue(&values[index]); err != nil {
			return nil, err
		}
	}
	return values, nil
}
func executeNative(ctx context.Context, c *managedConn, call *invocation.Call[Result], query string, args []any, keep bool, prepared driver.Stmt, data *resultData) error {
	values, err := arguments(c, args)
	if err != nil {
		return err
	}
	var rows driver.Rows
	var result driver.Result
	if prepared == nil {
		_, _ = call.Attempt()
		if keep {
			rows, err = c.Conn.(driver.QueryerContext).QueryContext(nativeContext{ctx}, query, values)
		} else {
			result, err = c.Conn.(driver.ExecerContext).ExecContext(nativeContext{ctx}, query, values)
		}
		if err == driver.ErrSkip {
			_, _ = call.Attempt()
			prepared, err = c.Conn.(driver.ConnPrepareContext).PrepareContext(nativeContext{ctx}, query)
			if err != nil {
				return err
			}
			defer func() { c.record(prepared.Close()) }()
		} else if err != nil {
			return err
		}
	}
	if prepared != nil {
		if count := prepared.NumInput(); count >= 0 && count != len(values) {
			return failure(ErrInput, "argument-count")
		}
		_, _ = call.Attempt()
		if keep {
			rows, err = prepared.(driver.StmtQueryContext).QueryContext(nativeContext{ctx}, values)
		} else {
			result, err = prepared.(driver.StmtExecContext).ExecContext(nativeContext{ctx}, values)
		}
		if err != nil {
			if c.wire.longDataPending {
				_ = c.wire.fail(failure(ErrState, "incomplete-parameters", err))
			}
			return err
		}
	}
	if keep {
		defer func() { c.record(rows.Close()) }()
		return consumeNative(rows, c.pool.settings, data)
	}
	data.affected, err = result.RowsAffected()
	if err == nil {
		data.insertID, err = result.LastInsertId()
	}
	data.commandKnown = err == nil
	return err
}
func consumeNative(rows driver.Rows, s settings, data *resultData) error {
	names := rows.Columns()
	if len(names) > MaxColumns {
		return failure(ErrLimit, "columns")
	}
	total := 0
	for index, name := range names {
		nullable, known := rows.(driver.RowsColumnTypeNullable).ColumnTypeNullable(index)
		precision, scale, precisionKnown := rows.(driver.RowsColumnTypePrecisionScale).ColumnTypePrecisionScale(index)
		kind := rows.(driver.RowsColumnTypeDatabaseTypeName).ColumnTypeDatabaseTypeName(index)
		total += len(name) + len(kind)
		if total > s.MaxResultBytes {
			return failure(ErrLimit, "metadata")
		}
		data.columns = append(data.columns, Column{Name: name, DatabaseType: kind, Nullable: nullable, NullableKnown: known, Precision: precision, Scale: scale, PrecisionKnown: precisionKnown})
	}
	raw := make([]driver.Value, len(names))
	for {
		err := rows.Next(raw)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		data.rowsRead++
		row := Row{cells: make([]cell, len(raw))}
		for index, value := range raw {
			cell, err := encodedCell(value)
			if err != nil {
				return err
			}
			if len(cell.text) > s.MaxResultBytes-total {
				return failure(ErrLimit, "result")
			}
			total += len(cell.text)
			row.cells[index] = cell
		}
		data.rows = append(data.rows, row)
	}
}
func encodedCell(value driver.Value) (cell, error) {
	switch value := value.(type) {
	case nil:
		return cell{null: true}, nil
	case []byte:
		return cell{text: string(value)}, nil
	case string:
		return cell{text: value}, nil
	case int64:
		return cell{text: strconv.FormatInt(value, 10)}, nil
	case uint64:
		return cell{text: strconv.FormatUint(value, 10)}, nil
	case float32:
		return cell{text: strconv.FormatFloat(float64(value), 'g', -1, 32)}, nil
	case float64:
		return cell{text: strconv.FormatFloat(value, 'g', -1, 64)}, nil
	case bool:
		return cell{text: strconv.FormatBool(value)}, nil
	case time.Time:
		return cell{text: value.Format(time.RFC3339Nano)}, nil
	default:
		return cell{}, failure(ErrUnsupported, "native-cell")
	}
}
