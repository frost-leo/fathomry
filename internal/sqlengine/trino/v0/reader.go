/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package trino

import (
	"context"
	"io"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/trinodb/trino-go-client/trino"
)

// Reader owns one asynchronous direct query and its original source reservation.
// Next transfers at most one validated provisional protocol page. There is no
// unbounded queue or per-page native evidence backlog; the required root receipt
// retains the terminal summary independently. No page is a completed business
// range. Consumers must stage provisional output until terminal completeness.
type Reader struct {
	private
	pages   chan Result
	done    chan struct{}
	cancel  context.CancelFunc
	receipt *invocation.Receipt[Result]
	reading atomic.Bool
}

// QueryPages admits one query, freezes all admitted input values before return,
// and starts its bounded lifetime. ctx owns admission AND native lifetime, limited
// by ReadTimeout; cleanup independently authorizes cancellation after that lifetime.
// With no consumer, at most one untransferred page is fetched; lifetime expiration
// cancels transfer and joins native resources. Returning or ignoring Reader does
// not release its root receipt's evidence obligation.
func (c *Client) QueryPages(ctx, cleanup context.Context, id fault.Correlation, statement Statement) (*Reader, *invocation.Receipt[Result], error) {
	if c == nil || c.owner == nil || ctx == nil || cleanup == nil {
		return nil, nil, failure(ErrInput, "query-pages")
	}
	s := c.owner.settings
	call, err := invocation.Begin(ctx, c.access, invocation.Request{Name: "query-pages", Correlation: id, Shape: invocation.Stream,
		Bytes: s.readerReservation(), EvidenceBytes: s.readerEvidenceBytes(), Admission: invocation.Budget{Limit: s.Timeout}}, c.inbox, c.observer)
	if err != nil {
		return nil, nil, err
	}
	reader := &Reader{pages: make(chan Result), done: make(chan struct{}), cancel: func() {}, receipt: call.Receipt()}
	complete := func(data *resultData, primary, cleanupErr error) {
		call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: primary, Cleanup: cleanupErr})
		close(reader.pages)
		close(reader.done)
	}
	if _, err = prepare(statement, s, true); err != nil {
		complete(&resultData{json: []byte("[]")}, err, nil)
		return reader, reader.receipt, nil
	}
	statement = cloneStatement(statement)
	work, cancel, err := (invocation.Budget{Limit: s.ReadTimeout}).Context(ctx, invocation.Lifetime)
	if err != nil {
		complete(&resultData{json: []byte("[]")}, err, nil)
		return reader, reader.receipt, nil
	}
	reader.cancel = cancel
	s.MaxRows, s.MaxPages, s.MaxWireBytes = s.MaxReadRows, s.MaxReadPages, s.MaxReadWireBytes
	s.Timeout = s.ReadTimeout
	exchange := newExchange(s, false, true, call)
	exchange.transfer = reader.pages
	go func() {
		err := exchange.executeNative(work, statement)
		if err != nil {
			err = failed(ErrOperation, "native", err, work.Err(), context.Cause(work))
		}
		cancel()
		exchange.finish(cleanup)
		data, primary, cleanupErr := exchange.outcome(err)
		complete(data, primary, cleanupErr)
	}()
	return reader, reader.receipt, nil
}

func cloneStatement(statement Statement) Statement {
	args := make([]any, len(statement.Args))
	for index, value := range statement.Args {
		args[index] = cloneArgument(value)
	}
	return Statement{SQL: strings.Clone(statement.SQL), Args: args}
}

func cloneArgument(value any) any {
	switch typed := value.(type) {
	case []byte:
		return slices.Clone(typed)
	case string:
		return strings.Clone(typed)
	case native.Numeric:
		return native.Numeric(strings.Clone(string(typed)))
	case time.Time:
		_, offset := typed.Zone()
		return typed.In(time.FixedZone("", offset))
	}
	ref := reflect.ValueOf(value)
	if !ref.IsValid() || ref.Kind() != reflect.Slice {
		return value
	}
	copy := reflect.MakeSlice(ref.Type(), ref.Len(), ref.Len())
	for index := 0; index < ref.Len(); index++ {
		cloned := cloneArgument(ref.Index(index).Interface())
		if cloned != nil {
			copy.Index(index).Set(reflect.ValueOf(cloned))
		}
	}
	return copy.Interface()
}

// Next transfers immutable exact rows/metadata for one provisional page. Only
// one Next may run at a time. Its context bounds waiting, not query lifetime.
// io.EOF means the terminal receipt is complete and all native work has released;
// a failed prefix or cleanup returns its retained error instead. Empty progress
// pages are visible and remain bounded by MaxReadPages and ReadTimeout.
func (r *Reader) Next(ctx context.Context) (Result, error) {
	if r == nil || r.receipt == nil || ctx == nil {
		return Result{}, failure(ErrInput, "next")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, failure(ErrOperation, "next-wait", err, context.Cause(ctx))
	}
	if !r.reading.CompareAndSwap(false, true) {
		return Result{}, failure(ErrInput, "concurrent-next")
	}
	defer r.reading.Store(false)
	select {
	case page, ok := <-r.pages:
		if ok {
			return page, nil
		}
		result, _ := r.receipt.Result()
		if err := result.Err(); err != nil {
			return Result{}, err
		}
		if !result.Final || !result.Released || !result.Outcome.Value.Complete() {
			return Result{}, failure(ErrProtocol, "reader-terminal")
		}
		return Result{}, io.EOF
	case <-ctx.Done():
		return Result{}, failure(ErrOperation, "next-wait", ctx.Err(), context.Cause(ctx))
	}
}

// Done closes only after native statement/connection/HTTP cleanup and root release.
func (r *Reader) Done() <-chan struct{} {
	if r == nil {
		return nil
	}
	return r.done
}

// Close cancels this query's lifetime and waits for actual native cleanup. A
// canceled wait never discards ownership; call Close again to join later. The
// terminal receipt retains primary and cleanup facts even when Close is ignored.
func (r *Reader) Close(ctx context.Context) error {
	if r == nil || r.receipt == nil || ctx == nil {
		return failure(ErrInput, "reader-close")
	}
	r.cancel()
	select {
	case <-r.done:
		result, _ := r.receipt.Result()
		return result.Err()
	default:
	}
	select {
	case <-r.done:
		result, _ := r.receipt.Result()
		return result.Err()
	case <-ctx.Done():
		return failure(ErrCleanup, "reader-close-wait", ctx.Err(), context.Cause(ctx))
	}
}
