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

package nethttp

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/frost-leo/fathomry/internal/invocation"
)

// Stream is a non-owning response reader. It permits one Read at a time and
// concurrent Close. Close is mandatory, even after EOF. Its context bounds waiting
// for one owned cleanup operation; an early wait return does not release native use.
// Retain the stream/receipt and call Close again to observe later cleanup.
type Stream struct {
	private
	op       *operation
	response *http.Response
	reads    *activity
	reading  atomic.Bool
}

func (stream *Stream) Metadata() Metadata {
	if stream == nil || stream.op == nil {
		return Metadata{}
	}
	stream.op.mu.Lock()
	defer stream.op.mu.Unlock()
	return stream.op.data.metadata
}
func (stream *Stream) Read(data []byte) (int, error) {
	if stream == nil || stream.op == nil || stream.response == nil {
		return 0, failure(ErrState, "read")
	}
	if len(data) == 0 {
		return 0, nil
	}
	if !stream.reading.CompareAndSwap(false, true) {
		return 0, failure(ErrState, "concurrent-read")
	}
	defer stream.reading.Store(false)
	if !stream.reads.enter() {
		return 0, failure(ErrState, "read-closed")
	}
	defer stream.reads.leave()
	count, err := stream.response.Body.Read(data)
	if errors.Is(err, io.EOF) {
		stream.op.mu.Lock()
		stream.op.data.complete = true
		stream.op.mu.Unlock()
	} else if err != nil {
		kind := ErrRead
		if errors.Is(err, io.ErrUnexpectedEOF) {
			kind = ErrIntegrity
		}
		err = failure(kind, "read", err, stream.op.ctx.Err(), context.Cause(stream.op.ctx))
		stream.op.fail(err)
	}
	return count, err
}
func (stream *Stream) Close(ctx context.Context) error {
	if stream == nil || stream.op == nil || ctx == nil {
		return failure(ErrInput, "close")
	}
	stream.reads.stop()
	stream.op.finish()
	return stream.op.wait(ctx)
}

type requestBody struct {
	raw   io.ReadCloser
	op    *operation
	reads *activity
	once  sync.Once
	done  chan struct{}
	err   error
}

func (op *operation) wrapRequestBody(raw io.ReadCloser) *requestBody {
	body := &requestBody{raw: raw, op: op, reads: newActivity(), done: make(chan struct{})}
	op.mu.Lock()
	op.requests = append(op.requests, body)
	closing := op.closing
	op.mu.Unlock()
	if closing {
		_ = body.Close()
	}
	return body
}
func (body *requestBody) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if !body.reads.enter() {
		return 0, failure(ErrState, "request-body-closed")
	}
	defer body.reads.leave()
	if err := body.op.ctx.Err(); err != nil {
		return 0, err
	}
	length, err := body.op.reserveRead(len(data), true)
	if err != nil {
		return 0, err
	}
	if length == 0 {
		return 0, failure(ErrLimit, "request-body")
	}
	count, readErr := body.raw.Read(data[:length])
	return body.op.finishRead(count, length, readErr, true)
}
func (body *requestBody) Close() error {
	body.once.Do(func() {
		done := body.reads.stop()
		body.err = body.raw.Close()
		<-done
		close(body.done)
	})
	<-body.done
	return body.err
}

type responseBody struct {
	raw   io.ReadCloser
	op    *operation
	reads *activity
	once  sync.Once
	done  chan struct{}
	err   error
}

func (body *responseBody) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if !body.reads.enter() {
		return 0, failure(ErrState, "response-body-closed")
	}
	defer body.reads.leave()
	length, err := body.op.reserveRead(len(data), false)
	if err != nil {
		return 0, err
	}
	if length == 0 {
		return 0, failure(ErrLimit, "response-body")
	}
	count, readErr := body.raw.Read(data[:length])
	return body.op.finishRead(count, length, readErr, false)
}
func (body *responseBody) Close() error {
	body.once.Do(func() {
		done := body.reads.stop()
		body.err = body.raw.Close()
		<-done
		close(body.done)
	})
	<-body.done
	return body.err
}

func (op *operation) reserveRead(length int, request bool) (int, error) {
	op.mu.Lock()
	defer op.mu.Unlock()
	used, reserved, maximum := op.received, &op.responseReserved, op.client.owner.settings.MaxResponseBytes
	if request {
		used, reserved, maximum = op.sent, &op.requestReserved, op.client.owner.settings.MaxRequestBytes
	}
	if used > maximum {
		return 0, failure(ErrLimit, "body")
	}
	available := maximum + 1 - used - *reserved
	if available <= 0 {
		return 0, failure(ErrLimit, "concurrent-body-budget")
	}
	if int64(length) > available {
		length = int(available)
	}
	*reserved += int64(length)
	return length, nil
}
func (op *operation) finishRead(count, reserved int, readErr error, request bool) (int, error) {
	op.mu.Lock()
	defer op.mu.Unlock()
	used, pending, maximum := &op.received, &op.responseReserved, op.client.owner.settings.MaxResponseBytes
	if request {
		used, pending, maximum = &op.sent, &op.requestReserved, op.client.owner.settings.MaxRequestBytes
	}
	*pending -= int64(reserved)
	if count < 0 || count > reserved {
		err := failure(ErrInput, "invalid-reader")
		if op.primary == nil {
			op.primary = err
		}
		return 0, err
	}
	*used += int64(count)
	if *used > maximum {
		overflow := *used - maximum
		count -= int(overflow)
		if count < 0 {
			count = 0
		}
		err := failure(ErrLimit, "body")
		if op.primary == nil {
			op.primary = err
		}
		return count, err
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) && op.primary == nil {
		kind := ErrRead
		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			kind = ErrIntegrity
		}
		op.primary = failure(kind, "body", readErr)
	}
	return count, readErr
}

// Receipt exposes observation only; it does not own completion or inbox capacity.
func (stream *Stream) Receipt() *invocation.Receipt[Result] {
	if stream == nil || stream.op == nil {
		return nil
	}
	return stream.op.call.Receipt()
}
