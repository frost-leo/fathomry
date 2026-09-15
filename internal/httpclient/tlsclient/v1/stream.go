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

package tlsclient

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"

	http "github.com/bogdanfinn/fhttp"
)

// Stream permits one Read and a concurrent Close. Close is required after EOF.
// Its context bounds waiting, not the owned native cleanup operation itself.
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
	raw     io.ReadCloser
	op      *operation
	reads   *activity
	once    sync.Once
	done    chan struct{}
	err     error
	readErr error
}

func (op *operation) input(raw io.ReadCloser) (*requestBody, error) {
	op.mu.Lock()
	if op.closing || len(op.inputs) >= op.client.owner.settings.MaxReplays+1 {
		op.mu.Unlock()
		return nil, failure(ErrLimit, "replay-readers")
	}
	if reflect.TypeOf(raw).Comparable() {
		for _, prior := range op.inputs {
			if prior.raw == raw {
				op.mu.Unlock()
				return prior, failure(ErrInput, "aliased-replay-reader")
			}
		}
	}
	body := &requestBody{raw: raw, op: op, reads: newActivity(), done: make(chan struct{})}
	op.inputs = append(op.inputs, body)
	op.mu.Unlock()
	return body, nil
}
func (body *requestBody) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if !body.reads.enter() {
		return 0, failure(ErrState, "input-closed")
	}
	defer body.reads.leave()
	select {
	case body.op.inputGate <- struct{}{}:
	case <-body.op.ctx.Done():
		return 0, body.op.ctx.Err()
	}
	defer func() { <-body.op.inputGate }()
	if err := body.op.ctx.Err(); err != nil {
		return 0, err
	}
	body.op.mu.Lock()
	remaining := body.op.client.owner.settings.MaxRequestBytes - body.op.sent
	body.op.mu.Unlock()
	if remaining < 0 {
		return 0, failure(ErrLimit, "input-bytes")
	}
	size := min(int64(len(data)), remaining+1)
	count, err := body.raw.Read(data[:int(size)])
	if count < 0 || count > int(size) {
		err = failure(ErrIntegrity, "input-count")
		count = 0
	}
	body.op.mu.Lock()
	body.op.sent += int64(count)
	body.op.mu.Unlock()
	if int64(count) > remaining {
		count = int(remaining)
		err = failure(ErrLimit, "input-bytes")
	}
	if err != nil && !errors.Is(err, io.EOF) {
		if body.readErr == nil {
			body.readErr = err
		}
		if errors.Is(err, ErrIntegrity) || errors.Is(err, ErrLimit) {
			body.op.fail(err)
		}
	}
	return count, err
}
func (body *requestBody) Close() error {
	body.once.Do(func() { done := body.reads.stop(); body.err = body.raw.Close(); <-done; close(body.done) })
	<-body.done
	return body.err
}

type responseBody struct {
	raw            io.ReadCloser
	op             *operation
	reads          *activity
	expected, seen int64
	release        func()
	once           sync.Once
	done           chan struct{}
	err            error
}

func (body *responseBody) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if !body.reads.enter() {
		return 0, failure(ErrState, "response-closed")
	}
	defer body.reads.leave()
	select {
	case body.op.outputGate <- struct{}{}:
	case <-body.op.ctx.Done():
		err := failure(ErrRead, "response", body.op.ctx.Err())
		body.op.fail(err)
		return 0, err
	}
	defer func() { <-body.op.outputGate }()
	if err := body.op.ctx.Err(); err != nil {
		wrapped := failure(ErrRead, "response", err)
		body.op.fail(wrapped)
		return 0, wrapped
	}
	body.op.mu.Lock()
	remaining := body.op.client.owner.settings.MaxResponseBytes - body.op.received
	body.op.mu.Unlock()
	if remaining < 0 {
		err := failure(ErrLimit, "response-bytes")
		body.op.fail(err)
		return 0, err
	}
	limit := remaining
	if body.expected >= 0 {
		limit = min(limit, max(int64(0), body.expected-body.seen))
	}
	size := min(int64(len(data)), limit+1)
	count, err := body.raw.Read(data[:int(size)])
	if count < 0 || count > int(size) {
		count = 0
		err = failure(ErrIntegrity, "response-count")
	}
	body.seen += int64(count)
	body.op.mu.Lock()
	body.op.received += int64(count)
	body.op.mu.Unlock()
	if int64(count) > limit {
		count = int(limit)
		if body.expected >= 0 && body.seen > body.expected {
			err = failure(ErrIntegrity, "content-length")
		} else {
			err = failure(ErrLimit, "response-bytes")
		}
	} else if errors.Is(err, io.EOF) && body.expected >= 0 && body.seen != body.expected {
		err = failure(ErrIntegrity, "content-length", io.ErrUnexpectedEOF)
	} else if err != nil && !errors.Is(err, io.EOF) {
		kind := ErrRead
		if errors.Is(err, io.ErrUnexpectedEOF) {
			kind = ErrIntegrity
		}
		err = failure(kind, "response", err, body.op.ctx.Err())
	}
	if err != nil && !errors.Is(err, io.EOF) {
		body.op.fail(err)
	}
	return count, err
}
func (body *responseBody) Close() error {
	body.once.Do(func() {
		done := body.reads.stop()
		body.err = body.raw.Close()
		<-done
		if body.release != nil {
			body.release()
		}
		close(body.done)
	})
	<-body.done
	return body.err
}
