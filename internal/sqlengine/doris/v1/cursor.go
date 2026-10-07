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

package doris

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Cursor owns a single text result and its framed connection. Next is serial;
// overlapping calls are refused. Close may cancel an in-flight page. No durable
// continuation token or remote cancellation/rollback guarantee is supplied.
type Cursor struct {
	private
	state *cursorState
}
type cursorState struct {
	mu                  sync.Mutex
	client              *Client
	call                *invocation.Call[Result]
	live                context.Context
	cancel              context.CancelFunc
	wire                *wire
	connection          driver.Conn
	rows                driver.Rows
	stopWire            func()
	columns             []Column
	metadataBytes       int
	pending             *Row
	pendingBytes        int
	rowsRead, bytesRead int
	dispatched          bool
	closed              bool
}

// QueryCursor separates setup/admission ctx from the explicitly owned lifetime.
// The driver watches lifetime until Rows ends. Ending ctx after successful setup
// cannot cancel an accepted cursor. Setup, each Next and cleanup waiting use
// Timeout; CursorTimeout additionally bounds the entire accepted lifetime.
func (c *Client) QueryCursor(ctx, lifetime context.Context, id fault.Correlation, query string) (*Cursor, *invocation.Receipt[Result], error) {
	if c == nil || c.owner == nil || ctx == nil || lifetime == nil || !validText(query, MaxSQLBytes, false) {
		return nil, nil, failure(ErrInput, "query-cursor")
	}
	s := c.owner.settings
	if s.SQLAddress == "" {
		return nil, nil, failure(ErrUnsupported, "query-cursor")
	}
	if c.access.Limits().MaxLeases < 2 {
		return nil, nil, failure(ErrInput, "cursor-leases")
	}
	admission, stop := cursorJoin(ctx, lifetime)
	call, err := invocation.Begin(admission, c.access, invocation.Request{Name: "query-cursor", Correlation: id, Shape: invocation.Stream,
		Bytes: s.reservation(), EvidenceBytes: s.evidenceReservation(), Admission: invocation.Budget{Limit: s.Timeout}}, c.inbox, c.observer)
	stop()
	if err != nil {
		return nil, nil, err
	}
	live, cancel, err := (invocation.Budget{Limit: s.CursorTimeout}).Context(lifetime, invocation.Lifetime)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return nil, call.Receipt(), nil
	}
	state := &cursorState{client: c, call: call, live: live, cancel: cancel}
	setup, endSetup, err := (invocation.Budget{Limit: s.Timeout}).Context(ctx, invocation.Establish)
	if err != nil {
		state.end(err, false)
		return nil, call.Receipt(), nil
	}
	stopSetup := cursorCancelOn(setup, cancel)
	err = state.open(query)
	stopSetup()
	err = errors.Join(err, cursorContextError(setup), cursorContextError(live))
	endSetup()
	if err != nil {
		state.end(err, false)
		return nil, call.Receipt(), nil
	}
	cursor := &Cursor{state: state}
	go func() {
		<-live.Done()
		state.mu.Lock()
		defer state.mu.Unlock()
		if !state.closed {
			state.end(errors.Join(live.Err(), context.Cause(live)), false)
		}
	}()
	return cursor, call.Receipt(), nil
}

func cursorCancelOn(ctx context.Context, cancel context.CancelFunc) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); cancel() })
	if ctx.Err() != nil {
		cancel()
	}
	return func() {
		if !stop() {
			<-done
		}
	}
}
func cursorJoin(ctx, owner context.Context) (context.Context, func()) {
	work, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	stop := context.AfterFunc(owner, func() { defer close(done); cancel(errors.Join(owner.Err(), context.Cause(owner))) })
	if owner.Err() != nil {
		cancel(errors.Join(owner.Err(), context.Cause(owner)))
	}
	return work, func() {
		if !stop() {
			<-done
		}
		cancel(nil)
	}
}

// A socket deadline can fire before the context timer goroutine is scheduled.
// Retain that owning deadline without replacing the original transport cause.
func cursorContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return errors.Join(err, context.Cause(ctx))
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func (state *cursorState) open(query string) error {
	s := state.client.owner.settings
	_, _ = state.call.Attempt()
	raw, err := s.dialer().DialContext(nativeContext{state.live}, "tcp", s.SQLAddress)
	if err != nil {
		return failure(ErrTransport, "connect", err)
	}
	deadline, _ := state.live.Deadline()
	wireSettings := s
	wireSettings.MaxRows = s.MaxCursorRows
	state.wire = &wire{raw: raw, transport: raw, settings: wireSettings, ctx: nativeContext{state.live}, deadline: deadline}
	_ = state.wire.SetDeadline(deadline)
	state.stopWire = cursorCancelOn(state.live, func() { _ = state.wire.Close() })
	connector, err := sqlConnector(s, state.wire)
	if err != nil {
		return err
	}
	state.connection, err = connector.Connect(nativeContext{state.live})
	if err != nil {
		return err
	}
	if err = state.live.Err(); err != nil {
		return err
	}
	_, _ = state.call.Attempt()
	state.dispatched = true
	state.rows, err = state.connection.(driver.QueryerContext).QueryContext(nativeContext{state.live}, query, nil)
	if err != nil {
		return err
	}
	for index, name := range state.rows.Columns() {
		if index >= MaxColumns {
			return failure(ErrLimit, "columns")
		}
		nullable, known := state.rows.(driver.RowsColumnTypeNullable).ColumnTypeNullable(index)
		precision, scale, precise := state.rows.(driver.RowsColumnTypePrecisionScale).ColumnTypePrecisionScale(index)
		kind := state.rows.(driver.RowsColumnTypeDatabaseTypeName).ColumnTypeDatabaseTypeName(index)
		state.metadataBytes += len(name) + len(kind)
		if state.metadataBytes > s.MaxPageBytes {
			return failure(ErrLimit, "page-metadata")
		}
		state.columns = append(state.columns, Column{Name: name, DatabaseType: kind, Nullable: nullable, NullableKnown: known, Precision: precision, Scale: scale, PrecisionKnown: precise})
	}
	state.bytesRead = state.metadataBytes
	return nil
}

func (state *cursorState) read() (*Row, int, error) {
	if state.pending != nil {
		row, size := state.pending, state.pendingBytes
		state.pending = nil
		return row, size, nil
	}
	values := make([]driver.Value, len(state.columns))
	if err := state.rows.Next(values); err != nil {
		return nil, 0, err
	}
	state.rowsRead++
	row := &Row{cells: make([]cell, len(values))}
	size := 0
	for index, value := range values {
		var text string
		switch value := value.(type) {
		case nil:
			row.cells[index].null = true
		case []byte:
			text = string(value)
		case int64:
			text = strconv.FormatInt(value, 10)
		case uint64:
			text = strconv.FormatUint(value, 10)
		case float32:
			text = strconv.FormatFloat(float64(value), 'g', -1, 32)
		case float64:
			text = strconv.FormatFloat(value, 'g', -1, 64)
		default:
			return nil, 0, failure(ErrUnsupported, "value")
		}
		size += len(text)
		row.cells[index].text = text
	}
	state.bytesRead += size
	return row, size, nil
}

func (state *cursorState) snapshot() *resultData {
	data := &resultData{columns: state.columns, dispatched: state.dispatched, rowsRead: state.rowsRead, bytesRead: state.bytesRead}
	if state.wire != nil {
		data.serverVersion, data.responseBytes = state.wire.version, state.wire.responseBytes
	}
	return data
}

// Next returns one copied page and its independent receipt. EOF is recognized
// through the framed driver, including a full final page via one-row lookahead.
// An admitted canceled/failed read ends the cursor; admission refusal does not.
// The whole-response row/byte guards never reset between pages.
func (cursor *Cursor) Next(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	if cursor == nil || cursor.state == nil || ctx == nil {
		return nil, failure(ErrInput, "cursor-next")
	}
	state := cursor.state
	if !state.mu.TryLock() {
		return nil, failure(ErrState, "cursor-busy")
	}
	defer state.mu.Unlock()
	if state.closed {
		return nil, failure(ErrState, "cursor-closed")
	}
	s := state.client.owner.settings
	call, err := invocation.BeginNested(ctx, state.call.Scope(), invocation.Request{Name: "cursor-next", Correlation: id, Shape: invocation.Finite,
		EvidenceBytes: s.evidenceReservation(), Admission: invocation.Budget{Limit: s.Timeout}}, state.client.inbox, state.client.observer)
	if err != nil {
		return nil, err
	}
	work, cancel, primary := (invocation.Budget{Limit: s.Timeout}).Context(ctx, invocation.Consume)
	var rows []Row
	complete := false
	if primary == nil {
		stop := cursorCancelOn(work, state.cancel)
		size := state.metadataBytes
		for {
			row, bytes, err := state.read()
			if err == io.EOF {
				complete = true
				break
			}
			if err != nil {
				primary = err
				break
			}
			if bytes > s.MaxPageBytes-state.metadataBytes {
				primary = failure(ErrLimit, "page-row")
				break
			}
			if len(rows) == s.MaxPageRows || bytes > s.MaxPageBytes-size {
				state.pending, state.pendingBytes = row, bytes
				break
			}
			rows = append(rows, *row)
			size += bytes
		}
		stop()
		wireErr, _ := state.wire.evidence()
		primary = errors.Join(primary, wireErr, cursorContextError(work), cursorContextError(state.live))
		cancel()
	}
	data := state.snapshot()
	data.rows, data.complete = rows, complete && primary == nil
	if primary != nil || complete {
		state.end(primary, data.complete)
	}
	call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: joined(ErrSQL, "cursor-next", primary)})
	return call.Receipt(), nil
}

// end runs only with exclusive native access. Retire the socket BEFORE Rows.Close:
// the selected driver's Close can otherwise synchronously drain unread results.
func (state *cursorState) end(primary error, complete bool) {
	if state.closed {
		return
	}
	state.closed = true
	var cleanup error
	if state.wire != nil {
		state.stopWire()
		if state.connection != nil {
			cleanup = joined(ErrCleanup, "connection", state.connection.Close())
		}
		_ = state.wire.Close()
		if state.rows != nil {
			cleanup = joined(ErrCleanup, "rows", cleanup, state.rows.Close())
		}
		wireErr, closeErr := state.wire.evidence()
		primary = errors.Join(primary, wireErr)
		cleanup = joined(ErrCleanup, "socket", cleanup, closeErr)
	}
	data := state.snapshot()
	data.complete = complete && primary == nil
	state.rows, state.connection, state.pending = nil, nil, nil
	state.call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: joined(ErrSQL, "query-cursor", primary), Cleanup: cleanup})
	state.cancel()
}

// Close cancels local work and waits under ctx and Timeout. It reuses the root
// reservation at saturation. Repeated calls observe the same terminal evidence;
// a wait timeout leaves cleanup reachable. Released never proves remote rollback.
func (cursor *Cursor) Close(ctx context.Context) error {
	if cursor == nil || cursor.state == nil || ctx == nil {
		return failure(ErrInput, "cursor-close")
	}
	state := cursor.state
	state.cancel()
	wait, cancel, err := (invocation.Budget{Limit: state.client.owner.settings.Timeout}).Context(ctx, invocation.Cleanup)
	if err != nil {
		return err
	}
	defer cancel()
	value, err := state.call.Receipt().WaitReleased(wait)
	return errors.Join(err, value.Err())
}
