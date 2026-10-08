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
	"context"
	"errors"
	"io"

	"github.com/sardanioss/qpack"
	"github.com/sardanioss/quic-go"
	ownedqpack "github.com/sardanioss/quic-go/internal/fathomryqpack"
)

// headerDecoder keeps the static server path unchanged while the client supplies
// its connection-owned, context-aware dynamic decoder.
type headerDecoder interface{ Decode([]byte) qpack.DecodeFunc }

type streamQPACKDecoder struct {
	cancelled bool // guarded by client.qpackMx
	client    *ClientConn
	ctx       context.Context
	streamID  quic.StreamID
	dynamic   bool
	stream    *stateTrackingStream
}

func (decoder *streamQPACKDecoder) Decode(block []byte) qpack.DecodeFunc {
	decoder.dynamic = len(block) > 0 && block[0] != 0
	return decoder.client.decoder.DecodeWithWait(decoder.ctx, block, func(changed <-chan struct{}) error {
		return decoder.stream.waitQPACK(decoder.ctx, changed)
	})
}

func (decoder *streamQPACKDecoder) finish(err error) error {
	if decoder.dynamic {
		decoder.dynamic = false
		decoder.client.qpackMx.Lock()
		var queued error
		if !decoder.cancelled {
			prefix, pattern := byte(7), byte(0x80)
			if err != nil {
				prefix, pattern = 6, 0x40
				decoder.cancelled = true
			}
			queued = decoder.client.enqueueQPACK(appendQPACKPrefixedInt(nil, prefix, pattern, uint64(decoder.streamID)))
		}
		decoder.client.qpackMx.Unlock()
		err = errors.Join(err, queued)
	}
	var malformed *qpackError
	if errors.As(err, &malformed) && !decoder.readAborted(err) {
		decoder.client.failQPACK(ErrCodeQPACKDecompressionFailed, err)
	}
	return err
}

func (decoder *streamQPACKDecoder) readAborted(err error) bool {
	if decoder.client.conn.Context().Err() != nil {
		return true
	}
	var aborted *ownedqpack.ReadError
	return errors.As(err, &aborted)
}

func (decoder *streamQPACKDecoder) cancel(error) {
	decoder.client.qpackMx.Lock()
	defer decoder.client.qpackMx.Unlock()
	if !decoder.cancelled {
		decoder.cancelled = true
		_ = decoder.client.enqueueQPACK(appendQPACKPrefixedInt(nil, 6, 0x40, uint64(decoder.streamID)))
	}
}

func (decoder *streamQPACKDecoder) watchRead() bool {
	stream := decoder.stream
	done := make(chan struct{})
	stream.readWatchDone = done
	stream.onReadAbort = decoder.cancel
	if !decoder.client.startControl(func() {
		defer close(done)
		select {
		case <-stream.Stream.FathomryReceiveAbort():
			decoder.cancel(stream.Stream.FathomryReceiveError())
		case <-decoder.ctx.Done():
			decoder.cancel(context.Cause(decoder.ctx))
		case <-stream.connectionContext.Done():
		case <-stream.readEnded:
		}
	}) {
		close(done)
		return false
	}
	return true
}

func (client *ClientConn) failQPACK(code ErrCode, err error) {
	if client.conn != nil {
		if client.conn.Context().Err() == nil {
			_ = client.conn.CloseWithError(quic.ApplicationErrorCode(code), "")
		}
		err = errors.Join(err, context.Cause(client.conn.Context()))
	}
	if client.decoder != nil {
		client.decoder.Close(err)
	}
}

func (client *ClientConn) enqueueQPACK(instruction []byte) error {
	if client.conn.Context().Err() != nil {
		return context.Cause(client.conn.Context())
	}
	select {
	case client.qpackFeedback <- instruction:
		return nil
	default:
		err := errors.New("http3: QPACK feedback capacity exceeded")
		client.failQPACK(ErrCodeExcessiveLoad, err)
		return err
	}
}

func (client *ClientConn) writeQPACKFeedback() {
	client.qpackMx.Lock()
	decoder, encoder := client.qpackDecoderStr, client.qpackEncoderStr
	client.qpackMx.Unlock()
	if decoder == nil || encoder == nil {
		client.failQPACK(ErrCodeInternalError, errors.New("http3: missing QPACK critical stream"))
		return
	}
	defer func() { client.decoder.Close(context.Cause(client.conn.Context())) }()
	for {
		select {
		case <-client.conn.Context().Done():
			return
		case <-decoder.Context().Done():
			client.failQPACK(ErrCodeClosedCriticalStream, context.Cause(decoder.Context()))
			return
		case <-encoder.Context().Done():
			client.failQPACK(ErrCodeClosedCriticalStream, context.Cause(encoder.Context()))
			return
		case instruction := <-client.qpackFeedback:
			if _, err := decoder.Write(instruction); err != nil {
				client.failQPACK(ErrCodeClosedCriticalStream, err)
				return
			}
		}
	}
}

func (client *ClientConn) handleQPACKDecoderStream(stream *quic.ReceiveStream) {
	var first [1]byte
	for {
		if _, err := io.ReadFull(stream, first[:]); err != nil {
			if client.conn.Context().Err() == nil {
				client.failQPACK(ErrCodeClosedCriticalStream, err)
			}
			return
		}
		prefix := byte(6)
		if first[0]&0x80 != 0 {
			prefix = 7
		}
		_, err := readQPACKInteger(stream, first[0], prefix)
		if err != nil {
			if client.conn.Context().Err() == nil {
				client.failQPACK(ErrCodeQPACKDecoderStreamError, err)
			}
			return
		}
		// The selected encoder emits static request sections only. Cancellation
		// can release an empty reference set; acknowledgements or increments
		// cannot refer to an outstanding dynamic request section/insertion.
		if first[0]&0xc0 != 0x40 {
			client.failQPACK(ErrCodeQPACKDecoderStreamError, errors.New("http3: unexpected decoder feedback"))
			return
		}
	}
}

func (client *ClientConn) watchQPACKCriticalStreams() {
	client.qpackMx.Lock()
	decoder, encoder := client.qpackDecoderStr, client.qpackEncoderStr
	client.qpackMx.Unlock()
	control := client.rawConn.controlSendStream
	if decoder == nil || encoder == nil || control == nil {
		return
	}
	select {
	case <-client.conn.Context().Done():
		return
	case <-decoder.Context().Done():
		client.failQPACK(ErrCodeClosedCriticalStream, context.Cause(decoder.Context()))
	case <-encoder.Context().Done():
		client.failQPACK(ErrCodeClosedCriticalStream, context.Cause(encoder.Context()))
	case <-control.Context().Done():
		client.failQPACK(ErrCodeClosedCriticalStream, context.Cause(control.Context()))
	}
}

func readQPACKInteger(reader io.Reader, first, prefix byte) (uint64, error) {
	mask := uint64(1<<prefix) - 1
	value := uint64(first) & mask
	if value < mask {
		return value, nil
	}
	var next [1]byte
	for shift := uint(0); shift < 63; shift += 7 {
		if _, err := io.ReadFull(reader, next[:]); err != nil {
			return 0, err
		}
		if shift >= 62 || uint64(next[0]&127) > (((1<<62)-1)-value)>>shift {
			return 0, errors.New("http3: QPACK integer overflow")
		}
		value += uint64(next[0]&127) << shift
		if next[0]&128 == 0 {
			return value, nil
		}
	}
	return 0, errors.New("http3: QPACK integer overflow")
}

// startControl registers before scheduling, preventing a shutdown/Add race.
// The owning profile bounds incoming unidirectional streams to at most1024;
// fixed accept/datagram/feedback workers occupy the remaining allowance.
func (client *ClientConn) startControl(work func()) bool {
	client.controlMu.Lock()
	if client.controlClosed || client.controlActive >= 1032 {
		client.controlMu.Unlock()
		return false
	}
	client.controlActive++
	client.controlWork.Add(1)
	client.controlMu.Unlock()
	go func() {
		defer func() {
			client.controlMu.Lock()
			client.controlActive--
			client.controlMu.Unlock()
			client.controlWork.Done()
		}()
		work()
	}()
	return true
}

func (client *ClientConn) acceptControlStreams() {
	client.startControl(func() {
		for {
			stream, err := client.conn.AcceptUniStream(client.conn.Context())
			if err != nil {
				return
			}
			if !client.startControl(func() { client.handleUnidirectionalStream(stream) }) {
				stream.CancelRead(quic.StreamErrorCode(ErrCodeExcessiveLoad))
				client.failQPACK(ErrCodeExcessiveLoad, errors.New("http3: critical reader capacity exceeded"))
				return
			}
		}
	})
}

func (client *ClientConn) closeControl() {
	client.controlMu.Lock()
	client.controlClosed = true
	client.controlMu.Unlock()
	if client.decoder != nil {
		cause := error(io.ErrClosedPipe)
		if client.conn != nil && client.conn.Context().Err() != nil {
			cause = context.Cause(client.conn.Context())
		}
		client.decoder.Close(cause)
	}
	client.controlWork.Wait()
}
