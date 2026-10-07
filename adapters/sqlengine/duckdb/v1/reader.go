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

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2"
)

// Reader retains one result and source generation. It never re-executes SQL and
// retains no history of delivered rows. Concurrent Next/Close is refused.
// Native materialization remains: chunk limits are not process RSS limits.
type Reader struct {
	private
	native  *native.Reader
	session *session
}

// Read opens a bounded SELECT-shaped result. ctx controls setup; accepted work
// belongs to the Client's owning lifetime and configured ReaderLifetime. SELECT
// can have effects (for example sequences); cancellation never authorizes replay.
func (client *Client) Read(ctx context.Context, sql string, args ...any) (*Reader, error) {
	if client == nil || client.lifetime == nil {
		return nil, fail(ErrInput, "read")
	}
	var reader *Reader
	receipt, err := client.dispatch(ctx, client.lifetime, "read", func(value *session, database *native.Database) {
		requests, inputErr := inward([]Request{{Mode: Query, SQL: sql, Args: args}}, value.state.prepared.Options())
		if inputErr != nil {
			value.finish(nil, inputErr)
			return
		}
		guard, holdErr := value.call.Hold()
		if holdErr != nil {
			value.finish(nil, holdErr)
			return
		}
		setup, stop := value.setup(ctx)
		owned, nativeReceipt, readErr := database.Read(setup, value.lifetime, value.family.correlation(value.call), requests[0])
		stop()
		if nativeReceipt == nil {
			if owned != nil {
				_ = owned.Close(context.Background())
			}
			value.finish(nil, readErr)
			_ = guard.Release()
			return
		}
		if observed, final := nativeReceipt.Result(); final && observed.Released {
			value.finish(nativeReceipt, readErr)
			_ = guard.Release()
			return
		}
		if owned == nil {
			value.finish(nativeReceipt, fail(ErrState, "read"))
			_ = guard.Release()
			return
		}
		if keepErr := value.keep(guard, nativeReceipt, func(cleanup context.Context) { _ = owned.Close(cleanup) }); keepErr != nil {
			_ = owned.Close(context.Background())
			value.finish(nativeReceipt, keepErr)
			_ = guard.Release()
			return
		}
		reader = &Reader{native: owned, session: value}
	})
	if err != nil {
		return reader, err
	}
	if reader == nil {
		_, err = result(receipt, nil)
		return nil, err
	}
	return reader, nil
}

// Next reserves public and native evidence before advancing rows. The result's
// Steps[0].Complete is true only after observed native EOF; prior chunks remain
// provisional, not completed business ranges. Receive/Ack each independent chunk
// before requesting unboundedly many further chunks.
func (reader *Reader) Next(ctx context.Context) (Result, error) {
	if reader == nil || reader.native == nil || reader.session == nil {
		return Result{}, fail(ErrInput, "next")
	}
	return result(reader.session.child(ctx, ctx, "next", func(value *session) {
		receipt, err := reader.native.Next(value.lifetime, value.family.correlation(value.call))
		value.finish(receipt, err)
	}))
}

// Result waits for terminal public evidence and actual generation release without
// canceling work when the waiter expires. It does not implicitly drain a reader.
func (reader *Reader) Result(ctx context.Context) (Result, error) {
	if reader == nil || reader.session == nil || reader.session.retained == nil {
		return Result{}, fail(ErrInput, "result")
	}
	return reader.session.retained.result(ctx)
}

// Close ends an unfinished reader without claiming EOF. It reuses terminal
// reservations under saturated admission/evidence. A timed-out wait retains this
// same cleanup authority; native destruction is cooperative, not preemptible.
func (reader *Reader) Close(ctx context.Context) (Result, error) {
	if reader == nil || reader.native == nil || reader.session == nil || ctx == nil {
		return Result{}, fail(ErrInput, "close")
	}
	if !reader.session.family.gate.TryLock() {
		return Result{}, fail(ErrState, "close")
	}
	err := reader.native.Close(ctx)
	reader.session.family.gate.Unlock()
	if err != nil {
		if _, final := reader.native.Receipt().Result(); !final {
			return Result{}, translate(err, "close")
		}
	}
	return reader.Result(ctx)
}

// Receipt observes the already-reserved terminal result without granting producer
// or shutdown authority. Copies cannot erase the independent evidence record.
func (reader *Reader) Receipt() *adapters.Receipt[Result] {
	if reader == nil || reader.session == nil {
		return nil
	}
	return reader.session.call.Receipt()
}
