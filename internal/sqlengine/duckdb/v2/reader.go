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
	"database/sql/driver"
	"errors"
	"io"
	"sync"

	sdk "github.com/duckdb/duckdb-go/v2"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Reader owns one finite SELECT-shaped native result, statement and connection.
// The pinned driver materializes native results; Next bounds detached Go delivery,
// not native materialization, decoder transients or RSS. It never re-executes SQL.
// Next and Close must not overlap: concurrent use is refused. Lifetime cancellation
// reclaims abandonment without requiring another admission or evidence slot.
type Reader struct {
	private
	mu            sync.Mutex
	database      *Database
	call          *invocation.Call[Result]
	id            fault.Correlation
	life          context.Context
	cancel        context.CancelFunc
	finished      chan struct{}
	conn          *sdk.Conn
	statement     *sdk.Stmt
	rows          driver.Rows
	values        []driver.Value
	pending       []any
	pendingSize   int64
	metadataBytes int64
	progress      Progress
	cleanup       error
}

// Read sets up one SELECT-shaped Query. setup governs admission and native setup;
// lifetime independently owns the accepted reader and is capped by ReaderLifetime.
// Neither context is detached. Canceling setup after successful return does not
// cancel the reader. SELECT can have effects (for example sequence advancement).
// Rejected admission has no reader or receipt. After acceptance, setup failures
// are recorded in the terminal receipt and a closed reader is returned.
func (database *Database) Read(setup, lifetime context.Context, id fault.Correlation, request Request) (*Reader, *invocation.Receipt[Result], error) {
	if database == nil || database.owner == nil || database.access == nil || setup == nil || lifetime == nil || request.Mode != Query {
		return nil, nil, failure(ErrInput, "read")
	}
	if err := lifetime.Err(); err != nil {
		return nil, nil, failure(ErrInput, "reader-lifetime", err, context.Cause(lifetime))
	}
	config := database.owner.config
	if err := validateRequests(config, []Request{request}); err != nil {
		return nil, nil, err
	}
	call, err := invocation.Begin[Result](setup, database.access, invocation.Request{
		Name: "read", Correlation: id, Shape: invocation.Stream, Bytes: config.readerReservation(),
		EvidenceBytes: config.readerEvidenceReservation(), Admission: invocation.Budget{Limit: config.Timeout},
	}, database.inbox, database.observer)
	if err != nil {
		return nil, nil, err
	}
	life, cancel := context.WithTimeout(lifetime, config.ReaderLifetime)
	reader := &Reader{database: database, call: call, id: id, life: life, cancel: cancel, finished: make(chan struct{}),
		progress: Progress{Steps: []Step{{Mode: Query}}, Reader: &ReadProgress{}}}
	work, stop, err := (invocation.Budget{Limit: config.Timeout}).Context(setup, invocation.Establish)
	if err == nil {
		phase, end := context.WithCancelCause(work)
		lifeJoined := make(chan struct{})
		stopLife := context.AfterFunc(life, func() {
			defer close(lifeJoined)
			end(errors.Join(life.Err(), context.Cause(life)))
		})
		err = reader.open(phase, request)
		if !stopLife() {
			<-lifeJoined
		}
		if phase.Err() != nil {
			err = joined(ErrNative, "reader-setup", err, phase.Err(), context.Cause(phase))
		}
		if life.Err() != nil {
			err = joined(ErrNative, "reader-lifetime", err, life.Err(), context.Cause(life))
		}
		end(nil)
		stop()
	}
	if err != nil {
		reader.finish(err, nil)
	} else {
		go reader.watch()
	}
	return reader, call.Receipt(), nil
}

func (reader *Reader) open(ctx context.Context, request Request) error {
	if err := ctx.Err(); err != nil {
		return failure(ErrNative, "reader-connect", err, context.Cause(ctx))
	}
	native, err := reader.database.owner.connector.Connect(ctx)
	if err != nil {
		return failure(ErrNative, "reader-connect", err)
	}
	reader.conn = native.(*sdk.Conn)
	reader.statement, err = prepare(ctx, reader.conn, request.SQL)
	if err != nil {
		return err
	}
	step := &reader.progress.Steps[0]
	step.Prepared = true
	kind, err := reader.statement.StatementType()
	if err != nil {
		return failure(ErrNative, "reader-statement-type", err)
	}
	if kind != sdk.STATEMENT_TYPE_SELECT {
		return failure(ErrUnsupported, "reader-statement-type")
	}
	if len(request.Args) != reader.statement.NumInput() {
		return failure(ErrInput, "arguments")
	}
	bound, err := boundArguments(reader.statement, request.Args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return failure(ErrNative, "reader-query", err, context.Cause(ctx))
	}
	step.Submitted = true
	reader.rows, err = reader.statement.QueryContext(ctx, bound)
	if err != nil {
		return failure(ErrNative, "reader-query", err)
	}
	step.Executions = 1
	names := reader.rows.Columns()
	if len(names) > MaxColumns {
		return failure(ErrLimit, "columns")
	}
	metadata := reader.rows.(driver.RowsColumnTypeDatabaseTypeName)
	config := reader.database.owner.config
	for index, name := range names {
		typeName := metadata.ColumnTypeDatabaseTypeName(index)
		if !queryType(typeName) {
			return failure(ErrUnsupported, "result-type")
		}
		size := int64(len(name)+len(typeName)) + 64
		if size > min(config.ReaderChunkBytes, config.ReaderTotalBytes)-reader.metadataBytes {
			step.Limited = true
			return failure(ErrLimit, "reader-metadata")
		}
		reader.metadataBytes += size
		step.Columns = append(step.Columns, Column{Name: name, Type: typeName})
	}
	reader.progress.Reader.TotalBytes = reader.metadataBytes
	reader.values = make([]driver.Value, len(names))
	return nil
}

func (reader *Reader) watch() {
	select {
	case <-reader.life.Done():
		reader.mu.Lock()
		if !reader.progress.Reader.Closed {
			reader.finish(failure(ErrNative, "reader-lifetime", reader.life.Err(), context.Cause(reader.life)), nil)
		}
		reader.mu.Unlock()
	case <-reader.finished:
	}
}

// Receipt is the pre-reserved terminal evidence. It becomes ready only after
// real native cleanup and contains aggregate progress and schema, never row history.
func (reader *Reader) Receipt() *invocation.Receipt[Result] {
	if reader == nil {
		return nil
	}
	return reader.call.Receipt()
}

// Snapshot returns detached current metadata and counters, not delivered rows.
func (reader *Reader) Snapshot() Progress {
	if reader == nil {
		return Progress{}
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return (Result{progress: &reader.progress}).Snapshot()
}

// Next reserves separate evidence before consuming any row. Capacity rejection
// does not advance the result. A chunk at its bound is not EOF. At a total bound,
// at most one native row is inspected to distinguish exact EOF from a limited
// prefix. One byte-bound lookahead row may be retained; previous chunks are not.
// Its receipt can contain partial rows and an error. Ack/release evidence between
// calls to keep backlog bounded. A canceled accepted Next terminates the reader.
func (reader *Reader) Next(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	if reader == nil || reader.call == nil || ctx == nil {
		return nil, failure(ErrInput, "reader-next")
	}
	if !reader.mu.TryLock() {
		return nil, failure(ErrState, "reader-concurrent-use")
	}
	defer reader.mu.Unlock()
	if reader.progress.Reader.Closed {
		return nil, failure(ErrState, "reader-closed")
	}
	if id.Parent == "" {
		id.Parent = reader.id.Call
	}
	config := reader.database.owner.config
	call, err := invocation.BeginNested[Result](ctx, reader.call.Scope(), invocation.Request{
		Name: "reader-next", Correlation: id, Shape: invocation.Finite,
		EvidenceBytes: config.readerEvidenceReservation(), Admission: invocation.Budget{Limit: config.Timeout},
	}, reader.database.inbox, reader.database.observer)
	if err != nil {
		return nil, err
	}
	state := reader.progress.Reader
	state.Chunk++
	state.Offset, state.Rows, state.Bytes = state.TotalRows, 0, reader.metadataBytes
	var rows [][]any
	work, stop, primary := (invocation.Budget{Limit: config.Timeout}).Context(ctx, invocation.Consume)
	if primary == nil {
		rows, primary = reader.consume(work)
		stop()
	}
	if primary != nil || reader.progress.Steps[0].Complete {
		reader.finish(primary, nil)
	}
	progress := (Result{progress: &reader.progress}).Snapshot()
	progress.Steps[0].Rows = rows
	call.Complete(invocation.Outcome[Result]{Value: Result{progress: &progress}, Present: true, Primary: primary, Cleanup: reader.cleanup})
	return call.Receipt(), nil
}

func (reader *Reader) consume(ctx context.Context) ([][]any, error) {
	config := reader.database.owner.config
	state, step := reader.progress.Reader, &reader.progress.Steps[0]
	var rows [][]any
	for {
		if err := ctx.Err(); err != nil {
			return rows, failure(ErrNative, "reader-consume", err, context.Cause(ctx))
		}
		if err := reader.life.Err(); err != nil {
			return rows, failure(ErrNative, "reader-lifetime", err, context.Cause(reader.life))
		}
		totalBound := state.TotalRows == config.ReaderTotalRows || state.TotalBytes == config.ReaderTotalBytes
		if len(rows) == config.ReaderChunkRows && !totalBound {
			return rows, nil
		}
		row, size := reader.pending, reader.pendingSize
		if row == nil {
			clear(reader.values)
			err := reader.rows.Next(reader.values)
			if ctx.Err() != nil || reader.life.Err() != nil {
				return rows, joined(ErrNative, "reader-consume", err, ctx.Err(), context.Cause(ctx), reader.life.Err(), context.Cause(reader.life))
			}
			if err == io.EOF {
				step.Complete = true
				return rows, nil
			}
			if err != nil {
				return rows, failure(ErrNative, "reader-consume", err)
			}
			if totalBound {
				step.Limited = true
				return rows, failure(ErrLimit, "reader-total")
			}
			size = 32
			for column, value := range reader.values {
				cost, err := resultScalarSize(value, step.Columns[column].Type)
				if err != nil {
					return rows, err
				}
				if cost > config.ReaderChunkBytes-size {
					step.Limited = true
					return rows, failure(ErrLimit, "reader-row-bytes")
				}
				size += cost
			}
			if size > config.ReaderChunkBytes-reader.metadataBytes || size > config.ReaderTotalBytes-state.TotalBytes {
				step.Limited = true
				return rows, failure(ErrLimit, "reader-bytes")
			}
			row = make([]any, len(reader.values))
			for column, value := range reader.values {
				row[column] = copyScalar(value)
			}
			clear(reader.values)
		}
		if size > config.ReaderChunkBytes-state.Bytes {
			reader.pending, reader.pendingSize = row, size
			return rows, nil
		}
		reader.pending, reader.pendingSize = nil, 0
		rows = append(rows, row)
		state.Rows++
		state.Bytes += size
		state.TotalRows++
		state.TotalBytes += size
	}
}

// Close ends the reader without asserting EOF. It requires no free evidence or
// admission slot, and always joins native destruction even if ctx expires.
// Repeated calls return the same cleanup facts. Concurrent Next/Close is refused.
func (reader *Reader) Close(ctx context.Context) error {
	if reader == nil || reader.call == nil || ctx == nil {
		return failure(ErrInput, "reader-close")
	}
	select {
	case <-reader.finished:
		return reader.cleanup
	default:
	}
	if !reader.mu.TryLock() {
		select {
		case <-reader.finished:
			return reader.cleanup
		default:
		}
		return failure(ErrState, "reader-concurrent-use")
	}
	defer reader.mu.Unlock()
	if !reader.progress.Reader.Closed {
		reader.finish(joined(ErrNative, "reader-lifetime", reader.life.Err(), context.Cause(reader.life)), ctx)
	}
	return reader.cleanup
}

func (reader *Reader) finish(primary error, cleanupCtx context.Context) {
	if reader.progress.Reader.Closed {
		return
	}
	reader.cancel()
	if reader.rows != nil {
		reader.cleanup = joined(ErrCleanup, "reader-rows-close", reader.cleanup, reader.rows.Close())
		reader.rows = nil
	}
	if reader.statement != nil {
		reader.cleanup = joined(ErrCleanup, "reader-statement-close", reader.cleanup, reader.statement.Close())
		reader.statement = nil
	}
	if reader.conn != nil {
		reader.cleanup = joined(ErrCleanup, "reader-connection-close", reader.cleanup, reader.conn.Close())
		reader.conn = nil
		reader.progress.ConnectionClosed = true
	}
	if cleanupCtx != nil && cleanupCtx.Err() != nil {
		reader.cleanup = joined(ErrCleanup, "reader-close", reader.cleanup, cleanupCtx.Err(), context.Cause(cleanupCtx))
	}
	reader.values, reader.pending, reader.pendingSize = nil, nil, 0
	reader.progress.Reader.Closed = true
	progress := (Result{progress: &reader.progress}).Snapshot()
	reader.call.Complete(invocation.Outcome[Result]{Value: Result{progress: &progress}, Present: true, Primary: primary, Cleanup: reader.cleanup})
	close(reader.finished)
}
