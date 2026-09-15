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

package httpcloak

import (
	"bufio"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/andybalholm/brotli"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/klauspost/compress/zstd"
	http "github.com/sardanioss/http"
)

func decode(raw io.Reader, encoding string, limit int64) (io.Reader, io.Closer, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
		return raw, nil, nil
	case "gzip":
		reader, err := gzip.NewReader(raw)
		return reader, reader, err
	case "br":
		return brotli.NewReader(raw), nil, nil
	case "deflate":
		buffer := bufio.NewReader(raw)
		prefix, err := buffer.Peek(2)
		if err != nil {
			return nil, nil, err
		}
		if prefix[0]&15 == 8 && (uint16(prefix[0])<<8|uint16(prefix[1]))%31 == 0 {
			reader, err := zlib.NewReader(buffer)
			return decoderInput{reader, buffer}, reader, err
		}
		reader := flate.NewReader(buffer)
		return decoderInput{reader, buffer}, reader, nil
	case "zstd":
		reader, err := zstd.NewReader(raw, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(uint64(limit)+1<<20))
		if err != nil {
			return nil, nil, err
		}
		closer := reader.IOReadCloser()
		return closer, closer, nil
	default:
		return nil, nil, failure(ErrUnsupported, "content-encoding")
	}
}

type decoderInput struct {
	io.Reader
	tail io.Reader
}

// Stream owns a borrowing obligation, not an SDK client. Concurrent Read calls
// are serialized; Close interrupts native I/O and waits only as long as its context.
type Stream struct {
	private
	op       *operation
	wire     *wireBody
	reader   io.Reader
	decoder  io.Closer
	readMu   sync.Mutex
	eof      bool
	terminal error
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
	if stream == nil || stream.op == nil {
		return 0, failure(ErrState, "stream")
	}
	stream.readMu.Lock()
	defer stream.readMu.Unlock()
	if len(data) == 0 {
		return 0, nil
	}
	if stream.terminal != nil {
		return 0, stream.terminal
	}
	if stream.eof {
		return 0, io.EOF
	}
	if err := stream.op.ctx.Err(); err != nil {
		return 0, failure(ErrState, "read", err, context.Cause(stream.op.ctx))
	}
	stream.op.mu.Lock()
	remaining := stream.op.client.owner.settings.MaxResponseBytes - stream.op.data.decoded
	stream.op.mu.Unlock()
	if remaining < 0 {
		return 0, failure(ErrLimit, "decoded-body")
	}
	if int64(len(data)) > remaining+1 {
		data = data[:remaining+1]
	}
	n, err := stream.reader.Read(data)
	stream.op.mu.Lock()
	stream.op.data.decoded += int64(n)
	stream.op.mu.Unlock()
	if int64(n) > remaining {
		err = errors.Join(failure(ErrLimit, "decoded-body"), err)
	}
	if err == io.EOF {
		var witness [1]byte
		var tail io.Reader = stream.wire
		if decoder, ok := stream.reader.(decoderInput); ok {
			tail = decoder.tail
		}
		count, wireErr := tail.Read(witness[:])
		if count != 0 || wireErr != io.EOF {
			err = failure(ErrIntegrity, "encoded-completion", wireErr)
		} else {
			stream.eof = true
			stream.op.mu.Lock()
			stream.op.data.complete = true
			if headerFits(stream.wire.response.Trailer, stream.op.client.owner.settings.MaxHeaderBytes) {
				stream.op.data.trailers = stream.wire.response.Trailer.Clone()
			} else {
				err = failure(ErrLimit, "trailers")
			}
			stream.op.mu.Unlock()
		}
	}
	if err != nil && err != io.EOF {
		err = failure(ErrRead, "response-body", err)
		stream.terminal = err
		stream.op.fail(err)
	}
	return n, err
}
func (stream *Stream) Close(ctx context.Context) error {
	if stream == nil || stream.op == nil || ctx == nil {
		return failure(ErrInput, "stream-close")
	}
	stream.op.stop()
	select {
	case <-stream.op.done:
		return stream.op.finalErr
	case <-ctx.Done():
		return invocation.ErrWait.New(fault.Context{Provider: ProviderID, Operation: "cleanup"}, ctx.Err(), context.Cause(ctx))
	}
}

type wireBody struct {
	raw            io.ReadCloser
	op             *operation
	binding        *binding
	response       *http.Response
	readMu         sync.Mutex
	expected, read int64
	eof            bool
	readErr        error
	closeOnce      sync.Once
	closeErr       error
}

func (body *wireBody) Read(data []byte) (int, error) {
	body.readMu.Lock()
	defer body.readMu.Unlock()
	if len(data) == 0 {
		return 0, nil
	}
	if body.readErr != nil {
		return 0, body.readErr
	}
	if body.eof {
		return 0, io.EOF
	}
	body.op.mu.Lock()
	remaining := body.op.client.owner.settings.MaxWireBytes - body.op.data.wire
	body.op.mu.Unlock()
	if remaining < 0 {
		return 0, failure(ErrLimit, "wire-body")
	}
	if int64(len(data)) > remaining+1 {
		data = data[:remaining+1]
	}
	n, err := body.raw.Read(data)
	body.read += int64(n)
	body.op.mu.Lock()
	body.op.data.wire += int64(n)
	body.op.mu.Unlock()
	if int64(n) > remaining {
		err = errors.Join(failure(ErrLimit, "wire-body"), err)
	}
	if err == io.EOF {
		if body.expected >= 0 && body.read != body.expected {
			err = failure(ErrIntegrity, "wire-length", io.ErrUnexpectedEOF)
		} else {
			body.eof = true
		}
	}
	if err != nil && err != io.EOF {
		body.readErr = errors.Join(body.readErr, err)
		body.op.fail(failure(ErrRead, "wire-body", err))
	}
	return n, err
}
func (body *wireBody) Close() error {
	body.closeOnce.Do(func() {
		body.closeErr = body.raw.Close()
		body.readMu.Lock()
		retire := !body.eof || body.readErr != nil || body.closeErr != nil
		body.readMu.Unlock()
		if !retire {
			body.op.recordNotifications(body.binding)
		}
		body.closeErr = errors.Join(body.closeErr, body.op.client.owner.returnBinding(body.binding, retire))
		if retire {
			body.op.recordNotifications(body.binding)
		}
	})
	return body.closeErr
}
