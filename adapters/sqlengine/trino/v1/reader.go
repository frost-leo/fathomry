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

package trino

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/sqlengine/trino/v0"
)

// ReadProgress is provisional page position. It is neither a restart token nor
// completed business output; successful terminal Result certifies full consumption.
type ReadProgress struct {
	private
	Sequence uint64
	Offset   int
}

// ReadProgress returns page position; zero sequence identifies a non-page result.
func (value Result) ReadProgress() ReadProgress {
	return ReadProgress{Sequence: value.native.Sequence(), Offset: value.native.Offset()}
}

// Reader owns one query and borrowed generation until actual native cleanup.
// It bounds page transfer rather than retaining all rows. Methods Next/Close may
// not overlap. Canceling a Next waiter alone does not cancel the owning query.
type Reader struct {
	private
	native  *native.Reader
	session *session
	busy    atomic.Bool
}

// Read admits one bounded query. ctx governs setup/admission; the Client's owning
// lifetime and ReadTimeout govern accepted work. cleanupCtx remains explicit
// retained authority for bounded DELETE cleanup. Inputs are frozen before return.
func (client *Client) Read(ctx, cleanupCtx context.Context, input Statement) (*Reader, error) {
	if client == nil || client.lifetime == nil || cleanupCtx == nil {
		return nil, fail(ErrInput, "read")
	}
	var reader *Reader
	receipt, err := client.dispatch(ctx, client.lifetime, "read", func(value *session, nativeClient *native.Client) {
		statement, inputErr := inwardStatement(input, value.state.prepared.Options())
		if inputErr != nil {
			value.finish(nil, inputErr)
			return
		}
		guard, holdErr := value.call.Hold()
		if holdErr != nil {
			value.finish(nil, holdErr)
			return
		}
		owned, nativeReceipt, readErr := nativeClient.QueryPages(value.lifetime, cleanupCtx, value.family.correlation(value.call), statement)
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
		if keepErr := value.keep(guard, nativeReceipt, func(clean context.Context) { _ = owned.Close(clean) }); keepErr != nil {
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

// Next reserves public evidence before receiving a page. Page results always
// remain provisional. EOF means terminal processing finished successfully;
// inspect Result for whole-query evidence. A canceled waiter leaves work owned.
func (reader *Reader) Next(ctx context.Context) (Result, error) {
	if reader == nil || reader.native == nil || reader.session == nil || ctx == nil {
		return Result{}, fail(ErrInput, "next")
	}
	if !reader.busy.CompareAndSwap(false, true) {
		return Result{}, fail(ErrState, "next")
	}
	defer reader.busy.Store(false)
	select {
	case <-reader.native.Done():
		return reader.terminalNext(ctx)
	default:
	}
	eof := false
	receipt, err := reader.session.child(ctx, ctx, "next", func(value *session) {
		page, nextErr := reader.native.Next(value.lifetime)
		if errors.Is(nextErr, io.EOF) {
			eof = true
			_ = value.call.Resolve(adapters.Outcome[Result]{})
			return
		}
		metadata, _ := value.call.Receipt().Snapshot()
		projected := Result{native: page, present: nextErr == nil, source: info(value.state.owner.info),
			attribution: attribution(metadata.Info()), attempts: sqlengine.Attempts{}}
		_ = value.call.Resolve(adapters.Outcome[Result]{Value: projected, Present: nextErr == nil, Primary: translate(nextErr, "next")})
	})
	value, err := result(receipt, err)
	if err != nil {
		select {
		case <-reader.native.Done():
			return reader.terminalNext(ctx)
		default:
		}
	}
	if err == nil && eof {
		return value, io.EOF
	}
	return value, err
}

func (reader *Reader) terminalNext(ctx context.Context) (Result, error) {
	value, err := reader.Result(ctx)
	if err != nil {
		return value, err
	}
	if !value.Complete() {
		return value, fail(ErrState, "next-terminal")
	}
	return Result{}, io.EOF
}

// Result waits for compact terminal evidence and actual generation release.
// A timed-out waiter does not discard evidence or stop the query.
func (reader *Reader) Result(ctx context.Context) (Result, error) {
	if reader == nil || reader.session == nil || reader.session.retained == nil {
		return Result{}, fail(ErrInput, "result")
	}
	return reader.session.retained.result(ctx)
}

// Close cancels and joins using existing terminal reservations even when the
// inbox is full/sealed. If its wait expires, retry the same owner's Close.
func (reader *Reader) Close(ctx context.Context) (Result, error) {
	if reader == nil || reader.native == nil || reader.session == nil || ctx == nil {
		return Result{}, fail(ErrInput, "close")
	}
	if !reader.busy.CompareAndSwap(false, true) {
		return Result{}, fail(ErrState, "close")
	}
	defer reader.busy.Store(false)
	if !reader.session.family.gate.TryLock() {
		return reader.Result(ctx)
	}
	err := reader.native.Close(ctx)
	reader.session.family.gate.Unlock()
	if err != nil {
		select {
		case <-reader.native.Done():
		default:
			return Result{}, translate(err, "close")
		}
	}
	return reader.Result(ctx)
}

// Receipt observes terminal facts without producer/cancellation authority.
func (reader *Reader) Receipt() *adapters.Receipt[Result] {
	if reader == nil || reader.session == nil {
		return nil
	}
	return reader.session.call.Receipt()
}
