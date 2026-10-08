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

package http3

import (
	"bufio"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/enetx/g"
	qpackdecoder "github.com/enetx/http3/internal/qpack"
	"github.com/quic-go/qpack"
	"github.com/quic-go/quic-go"
)

// FathomryQPACKLimits are technical ceilings, not advertised SETTINGS overrides.
type FathomryQPACKLimits struct {
	MaxTableCapacity   uint64
	MaxBlockedStreams  uint64
	MaxFeedbackRecords int
}

func qpackSelection(settings g.MapOrd[uint64, uint64], supplied *FathomryQPACKLimits) (FathomryQPACKLimits, uint64, uint64, error) {
	limits := FathomryQPACKLimits{MaxTableCapacity: 64 << 20, MaxBlockedStreams: 1024, MaxFeedbackRecords: 16384}
	if supplied != nil {
		limits = *supplied
	}
	if limits.MaxTableCapacity > 64<<20 || limits.MaxBlockedStreams > 1024 || limits.MaxFeedbackRecords < 1 || limits.MaxFeedbackRecords > 65536 {
		return limits, 0, 0, qpackdecoder.ErrLimit
	}
	var table, blocked uint64
	for key, value := range settings.Iter() {
		switch key {
		case 1:
			table = value
		case 7:
			blocked = value
		}
	}
	if table > limits.MaxTableCapacity || blocked > limits.MaxBlockedStreams {
		return limits, 0, 0, qpackdecoder.ErrLimit
	}
	return limits, table, blocked, nil
}

type qpackFeedback struct {
	stream, required uint64
	cancel           bool
}

type qpackConnection struct {
	conn          *quic.Conn
	decoder       *qpackdecoder.Decoder
	tableCapacity uint64
	limit         int
	mu            sync.Mutex
	queue         []qpackFeedback
	inserted      uint64
	notify        chan struct{}
	closed        bool
	err           error
}

func newQPACKConnection(conn *quic.Conn, table, blocked uint64, queue int) *qpackConnection {
	decoder, err := qpackdecoder.New(table, blocked)
	state := &qpackConnection{conn: conn, decoder: decoder, tableCapacity: table, limit: queue, queue: make([]qpackFeedback, 0, queue), notify: make(chan struct{}, 1)}
	if err != nil {
		panic("http3: unvalidated QPACK configuration")
	}
	return state
}

func (state *qpackConnection) signal() {
	select {
	case state.notify <- struct{}{}:
	default:
	}
}
func (state *qpackConnection) stop(reason error) {
	state.mu.Lock()
	state.closed = true
	state.queue = nil
	state.mu.Unlock()
	state.decoder.Close(reason)
	state.signal()
}

func (state *qpackConnection) fail(code ErrCode, cause error) error {
	if qpackConnectionEnded(cause) {
		return errors.Join(qpackdecoder.ErrClosed, cause)
	}
	if state.conn.Context().Err() != nil {
		return errors.Join(qpackdecoder.ErrClosed, context.Cause(state.conn.Context()))
	}
	problem := errors.Join(&Error{ErrorCode: code}, cause)
	state.mu.Lock()
	if state.closed {
		prior := state.err
		state.mu.Unlock()
		return errors.Join(qpackdecoder.ErrClosed, prior, context.Cause(state.conn.Context()))
	}
	if state.err == nil {
		state.err = problem
	} else {
		problem = state.err
		var prior *Error
		if errors.As(problem, &prior) {
			code = prior.ErrorCode
		}
	}
	state.mu.Unlock()
	_ = state.conn.CloseWithError(quic.ApplicationErrorCode(code), "")
	state.stop(problem)
	return problem
}

func (state *qpackConnection) enqueue(value qpackFeedback) error {
	state.mu.Lock()
	if state.closed {
		err := state.err
		state.mu.Unlock()
		return errors.Join(qpackdecoder.ErrClosed, err, context.Cause(state.conn.Context()))
	}
	if len(state.queue) >= state.limit {
		state.mu.Unlock()
		return state.fail(ErrCodeQPACKDecompressionFailed, qpackdecoder.ErrLimit)
	}
	state.queue = append(state.queue, value)
	state.mu.Unlock()
	state.signal()
	return nil
}

func (state *qpackConnection) insertedCount(count uint64) error {
	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		return qpackdecoder.ErrClosed
	}
	state.inserted = max(state.inserted, count)
	state.mu.Unlock()
	state.signal()
	return nil
}

func (state *qpackConnection) writeFeedback() {
	if state.tableCapacity == 0 {
		return
	}
	ctx := state.conn.Context()
	stream, err := state.conn.OpenUniStreamSync(ctx)
	if err != nil {
		if ctx.Err() == nil {
			state.fail(ErrCodeInternalError, err)
		}
		return
	}
	write := func(data []byte) bool {
		count, err := stream.Write(data)
		if err == nil && count != len(data) {
			err = io.ErrShortWrite
		}
		if err != nil {
			if ctx.Err() == nil {
				code := ErrCodeInternalError
				var reset *quic.StreamError
				if errors.As(err, &reset) {
					code = ErrCodeClosedCriticalStream
				}
				state.fail(code, err)
			}
			return false
		}
		return true
	}
	if !write([]byte{3}) {
		return
	}
	var known uint64
	for {
		state.mu.Lock()
		if state.closed {
			state.mu.Unlock()
			return
		}
		var data []byte
		var nextKnown = known
		if len(state.queue) > 0 {
			value := state.queue[0]
			copy(state.queue, state.queue[1:])
			state.queue[len(state.queue)-1] = qpackFeedback{}
			state.queue = state.queue[:len(state.queue)-1]
			if value.cancel {
				data = qpackdecoder.AppendInstruction(6, 0x40, value.stream)
			} else {
				data = qpackdecoder.AppendInstruction(7, 0x80, value.stream)
				nextKnown = max(known, value.required)
			}
		} else if state.inserted > known {
			nextKnown = state.inserted
			data = qpackdecoder.AppendInstruction(6, 0, nextKnown-known)
		}
		state.mu.Unlock()
		if len(data) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-stream.Context().Done():
				state.fail(ErrCodeClosedCriticalStream, context.Cause(stream.Context()))
				return
			case <-state.notify:
			}
			continue
		}
		if !write(data) {
			return
		}
		known = nextKnown
	}
}

func (state *qpackConnection) encoderStream(stream *quic.ReceiveStream) {
	err := state.decoder.ParseEncoder(stream, state.insertedCount)
	if state.conn.Context().Err() != nil || qpackConnectionEnded(err) {
		return
	}
	code := ErrCodeQPACKEncoderStreamError
	var reset *quic.StreamError
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &reset) {
		code = ErrCodeClosedCriticalStream
	}
	state.fail(code, err)
}

func (state *qpackConnection) decoderStream(stream *quic.ReceiveStream) {
	reader := bufio.NewReader(stream)
	for {
		first, err := reader.ReadByte()
		if err != nil {
			if state.conn.Context().Err() == nil && !qpackConnectionEnded(err) {
				state.fail(ErrCodeClosedCriticalStream, err)
			}
			return
		}
		prefix := uint(6)
		if first&0x80 != 0 {
			prefix = 7
		}
		_, err = qpackdecoder.ReadInstructionInteger(first, prefix, reader)
		if state.conn.Context().Err() != nil || qpackConnectionEnded(err) {
			return
		}
		if err != nil {
			code := ErrCodeQPACKDecoderStreamError
			var reset *quic.StreamError
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &reset) {
				code = ErrCodeClosedCriticalStream
			}
			state.fail(code, err)
			return
		}
		// The selected request encoder emits no dynamic references or inserts.
		// A cancellation can be redundant; ACKs and increments cannot be valid.
		if first&0x80 != 0 || first&0x40 == 0 {
			state.fail(ErrCodeQPACKDecoderStreamError, qpackdecoder.ErrEncoding)
			return
		}
	}
}

func qpackConnectionEnded(err error) bool {
	var application *quic.ApplicationError
	var transport *quic.TransportError
	var idle *quic.IdleTimeoutError
	var handshake *quic.HandshakeTimeoutError
	var reset *quic.StatelessResetError
	var version *quic.VersionNegotiationError
	return errors.As(err, &application) || errors.As(err, &transport) || errors.As(err, &idle) || errors.As(err, &handshake) || errors.As(err, &reset) || errors.As(err, &version)
}

type fieldSectionDecoder interface{ Decode([]byte) qpack.DecodeFunc }

type qpackSection struct {
	state  *qpackConnection
	ctx    context.Context
	cancel context.CancelFunc
	stream uint64
	limit  uint64
	mu     sync.Mutex
	closed bool
}

func (state *qpackConnection) section(ctx context.Context, stream uint64, limit uint64) *qpackSection {
	ctx, cancel := context.WithCancel(ctx)
	return &qpackSection{state: state, ctx: ctx, cancel: cancel, stream: stream, limit: limit}
}

func (section *qpackSection) close() {
	section.mu.Lock()
	defer section.mu.Unlock()
	if !section.closed {
		section.closed = true
		section.cancel()
		if section.state.tableCapacity > 0 && section.state.conn.Context().Err() == nil {
			_ = section.state.enqueue(qpackFeedback{stream: section.stream, cancel: true})
		}
	}
}

func (section *qpackSection) Decode(data []byte) qpack.DecodeFunc {
	var once sync.Once
	var fields []qpack.HeaderField
	var problem error
	var offset int
	return func() (qpack.HeaderField, error) {
		once.Do(func() {
			var required uint64
			fields, required, problem = section.state.decoder.Decode(section.ctx, section.stream, data, section.limit)
			if problem != nil {
				if errors.Is(problem, qpackdecoder.ErrLimit) {
					section.close()
					return
				}
				if section.ctx.Err() == nil && section.state.conn.Context().Err() == nil {
					problem = section.state.fail(ErrCodeQPACKDecompressionFailed, problem)
				}
				return
			}
			section.mu.Lock()
			defer section.mu.Unlock()
			if section.closed || section.ctx.Err() != nil {
				problem = context.Cause(section.ctx)
				return
			}
			if required > 0 {
				problem = section.state.enqueue(qpackFeedback{stream: section.stream, required: required})
			}
		})
		if problem != nil {
			return qpack.HeaderField{}, problem
		}
		if offset == len(fields) {
			return qpack.HeaderField{}, io.EOF
		}
		value := fields[offset]
		offset++
		return value, nil
	}
}
