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
	"os"
	"sync"
	"time"

	"github.com/nukilabs/qpack"
	"github.com/nukilabs/quic-go"
	"github.com/nukilabs/quic-go/quicvarint"
)

// FathomryMaxControlWorkers bounds incoming readers, request/section watchers
// and critical-stream helpers together on one owned client connection.
const FathomryMaxControlWorkers = 8192

func (client *ClientConn) startOwnedControl(work func()) bool {
	client.controlMu.Lock()
	if client.controlClosed || client.controlActive >= FathomryMaxControlWorkers {
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

func (client *ClientConn) closeOwnedControl() {
	client.controlMu.Lock()
	client.controlClosed = true
	client.controlMu.Unlock()
	_ = client.decoder.Close()
	client.controlWork.Wait()
}

func (client *ClientConn) failQPACK(code ErrCode) {
	if client.conn.Context().Err() == nil {
		_ = client.conn.CloseWithError(quic.ApplicationErrorCode(code), "")
	}
	_ = client.decoder.Close()
}

func (client *ClientConn) acceptOwnedControl() {
	client.startOwnedControl(func() {
		for {
			stream, err := client.conn.AcceptUniStream(client.conn.Context())
			if err != nil {
				return
			}
			if !client.startOwnedControl(func() { client.handleUnidirectionalStream(stream) }) {
				stream.CancelRead(quic.StreamErrorCode(ErrCodeExcessiveLoad))
				client.failQPACK(ErrCodeExcessiveLoad)
				return
			}
		}
	})
}

func (client *ClientConn) watchCriticalSend(stream *quic.SendStream) {
	if !client.startOwnedControl(func() {
		select {
		case <-client.conn.Context().Done():
		case <-stream.Context().Done():
			client.failQPACK(ErrCodeClosedCriticalStream)
		}
	}) {
		client.failQPACK(ErrCodeExcessiveLoad)
	}
}

func (client *ClientConn) watchCriticalReceive(stream *quic.ReceiveStream) {
	finished := make(chan struct{})
	var once sync.Once
	stream.SetReceiveFinalSizeCallback(func(int64) { once.Do(func() { close(finished) }) })
	if !client.startOwnedControl(func() {
		select {
		case <-client.conn.Context().Done():
		case <-finished:
			client.failQPACK(ErrCodeClosedCriticalStream)
		}
	}) {
		client.failQPACK(ErrCodeExcessiveLoad)
	}
}

func (client *ClientConn) handleEncoderStream(stream *quic.ReceiveStream) {
	if client.additionalSettings[SettingQpackMaxTableCapacity] == 0 {
		var first [1]byte
		_, err := io.ReadFull(stream, first[:])
		if client.conn.Context().Err() == nil {
			if err != nil {
				client.failQPACK(ErrCodeClosedCriticalStream)
			} else {
				client.failQPACK(ErrCodeQPACKEncoderStreamError)
			}
		}
		return
	}
	err := client.decoder.ParseEncoderStream(stream)
	if client.conn.Context().Err() != nil {
		return
	}
	code := ErrCodeQPACKEncoderStreamError
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, qpack.ErrDecoderStream) {
		code = ErrCodeClosedCriticalStream
	}
	client.failQPACK(code)
}

func (client *ClientConn) handleDecoderStream(stream *quic.ReceiveStream) {
	var first [1]byte
	for {
		if _, err := io.ReadFull(stream, first[:]); err != nil {
			if client.conn.Context().Err() == nil {
				client.failQPACK(ErrCodeClosedCriticalStream)
			}
			return
		}
		prefix := byte(6)
		if first[0]&0x80 != 0 {
			prefix = 7
		}
		if _, err := readFeedbackInteger(stream, first[0], prefix); err != nil {
			if client.conn.Context().Err() == nil {
				client.failQPACK(ErrCodeQPACKDecoderStreamError)
			}
			return
		}
		// Native request encoding is static-only: cancellation may release an
		// empty set, but there are no inserts or dynamic sections to acknowledge.
		if first[0]&0xc0 != 0x40 {
			client.failQPACK(ErrCodeQPACKDecoderStreamError)
			return
		}
	}
}

func readFeedbackInteger(reader io.Reader, first, prefix byte) (uint64, error) {
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
			return 0, errors.New("http3: QPACK feedback integer overflow")
		}
		value += uint64(next[0]&127) << shift
		if next[0]&128 == 0 {
			return value, nil
		}
	}
	return 0, errors.New("http3: QPACK feedback integer overflow")
}

// qpackControlWriter is serialized by the selected decoder. WriteAllContext
// makes a canceled queued section atomic rather than truncating feedback.
type qpackControlWriter struct {
	client      *ClientConn
	stream      *quic.SendStream
	initialized bool
}

func (writer *qpackControlWriter) Write(data []byte) (int, error) {
	return writer.WriteContext(writer.client.conn.Context(), data)
}
func (writer *qpackControlWriter) WriteContext(ctx context.Context, data []byte) (int, error) {
	work, cancel := context.WithCancelCause(ctx)
	stopped := make(chan struct{})
	stop := context.AfterFunc(writer.client.conn.Context(), func() { defer close(stopped); cancel(context.Cause(writer.client.conn.Context())) })
	defer func() {
		if !stop() {
			<-stopped
		}
		cancel(nil)
	}()
	if writer.stream == nil {
		stream, err := writer.client.conn.OpenUniStreamSync(work)
		if err != nil {
			return 0, err
		}
		writer.stream = stream
		writer.client.watchCriticalSend(stream)
	}
	payload := data
	if !writer.initialized {
		payload = append(quicvarint.Append(nil, streamTypeQPACKDecoderStream), data...)
	}
	if _, err := writer.stream.WriteAllContext(work, payload); err != nil {
		return 0, err
	}
	writer.initialized = true
	return len(data), nil
}

func (client *ClientConn) cancelQPACK(id uint64) {
	if client.qpackCancellations == nil {
		return
	}
	select {
	case <-client.conn.Context().Done():
	case client.qpackCancellations <- id:
	default:
		client.failQPACK(ErrCodeExcessiveLoad)
	}
}
func (client *ClientConn) runQPACKCancellations() {
	for {
		select {
		case <-client.conn.Context().Done():
			return
		case id := <-client.qpackCancellations:
			if err := client.decoder.CancelStreamContext(client.conn.Context(), id); err != nil {
				client.failQPACK(ErrCodeClosedCriticalStream)
				return
			}
		}
	}
}

// requestQPACK owns receive-abort observation without conflating a successful
// request FIN with response cancellation. Completed sections own deadline
// watchers only while decoding/acknowledging, so clearing a prior deadline works.
type requestQPACK struct {
	client   *ClientConn
	stream   *stateTrackingStream
	ctx      context.Context
	cancel   context.CancelCauseFunc
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
	mu       sync.Mutex
	sections map[*context.CancelCauseFunc]struct{}
}

func (client *ClientConn) newRequestQPACK(stream *stateTrackingStream) *requestQPACK {
	ctx, cancel := context.WithCancelCause(client.conn.Context())
	scope := &requestQPACK{client: client, stream: stream, ctx: ctx, cancel: cancel, stop: make(chan struct{}), done: make(chan struct{}), sections: make(map[*context.CancelCauseFunc]struct{})}
	stream.mx.Lock()
	stream.onReadAbort = func(err error) { scope.end(err) }
	stream.mx.Unlock()
	if !client.startOwnedControl(func() {
		defer close(scope.done)
		aborted, cause := stream.Stream.FathomryReadAbort()
		if cause != nil {
			scope.end(cause)
			return
		}
		select {
		case <-scope.stop:
		case <-client.conn.Context().Done():
			scope.end(context.Cause(client.conn.Context()))
		case <-aborted:
			_, cause := stream.Stream.FathomryReadAbort()
			scope.end(cause)
		}
	}) {
		close(scope.done)
		scope.end(errors.New("http3: request control capacity exhausted"))
		client.failQPACK(ErrCodeExcessiveLoad)
	}
	return scope
}

func (scope *requestQPACK) end(err error) {
	scope.once.Do(func() {
		scope.mu.Lock()
		scope.cancel(err)
		for cancel := range scope.sections {
			(*cancel)(err)
		}
		scope.mu.Unlock()
		if err != nil && !errors.Is(err, io.EOF) {
			scope.client.cancelQPACK(uint64(scope.stream.StreamID()))
		}
		close(scope.stop)
	})
}
func (scope *requestQPACK) finish(err error) {
	scope.end(err)
	<-scope.done
}

func (scope *requestQPACK) section(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	scope.mu.Lock()
	if scope.ctx.Err() != nil {
		cancel(context.Cause(scope.ctx))
	} else {
		scope.sections[&cancel] = struct{}{}
	}
	scope.mu.Unlock()
	done := make(chan struct{})
	if !scope.client.startOwnedControl(func() {
		defer close(done)
		for {
			scope.stream.mx.Lock()
			deadline, changed := scope.stream.readDeadline, scope.stream.readChanged
			scope.stream.mx.Unlock()
			var timer *time.Timer
			var expired <-chan time.Time
			if !deadline.IsZero() {
				timer = time.NewTimer(time.Until(deadline))
				expired = timer.C
			}
			var again bool
			select {
			case <-ctx.Done():
			case <-scope.ctx.Done():
				cancel(context.Cause(scope.ctx))
			case <-changed:
				again = true
			case <-expired:
				cancel(os.ErrDeadlineExceeded)
			}
			if timer != nil {
				timer.Stop()
			}
			if !again {
				return
			}
		}
	}) {
		close(done)
		cancel(errors.New("http3: decoder control capacity exhausted"))
		scope.client.failQPACK(ErrCodeExcessiveLoad)
	}
	return ctx, func() { cancel(nil); <-done; scope.mu.Lock(); delete(scope.sections, &cancel); scope.mu.Unlock() }
}

func (client *ClientConn) classifyQPACK(ctx context.Context, err error) {
	if err == nil || ctx.Err() != nil || client.conn.Context().Err() != nil || errors.Is(err, qpack.ErrHeaderLimit) {
		return
	}
	if errors.Is(err, qpack.ErrDecoderStream) {
		client.failQPACK(ErrCodeClosedCriticalStream)
		return
	}
	var malformed *qpackError
	if errors.As(err, &malformed) {
		client.failQPACK(ErrCodeQPACKDecompressionFailed)
	}
}
