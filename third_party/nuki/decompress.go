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
	"bufio"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zlib"
	"github.com/klauspost/compress/zstd"
	"github.com/nukilabs/http"
)

// DecompressBody preserves raw-body ownership and lazy decoder errors.
// Close interrupts raw reads, joins decoder use and closes both resources.
func DecompressBody(res *http.Response) {
	if res == nil || res.Body == nil {
		return
	}
	if res.StatusCode == http.StatusNoContent || res.StatusCode == http.StatusNotModified ||
		res.StatusCode >= 100 && res.StatusCode < 200 || res.Request != nil && res.Request.Method == http.MethodHead {
		return
	}
	encoding := res.Header.Get("Content-Encoding")
	switch encoding {
	case "gzip", "br", "deflate", "zstd":
	default:
		return
	}
	res.Body = &decodedBody{raw: res.Body, encoding: encoding}
	res.Header.Del("Content-Encoding")
	res.Header.Del("Content-Length")
	res.Uncompressed = true
	res.ContentLength = -1
}

type decodedBody struct {
	raw          io.ReadCloser
	encoding     string
	init         sync.Once
	reader       io.Reader
	decoderClose func() error
	initErr      error
	mu           sync.Mutex
	closed       bool
	reading      sync.WaitGroup
	closing      sync.Once
	closeErr     error
}

func (body *decodedBody) initialize() {
	switch body.encoding {
	case "gzip":
		reader, err := gzip.NewReader(body.raw)
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		body.initErr = err
		if err == nil {
			body.reader, body.decoderClose = reader, reader.Close
		}
	case "br":
		body.reader = brotli.NewReader(body.raw)
	case "deflate":
		buffer := bufio.NewReader(body.raw)
		prefix, err := buffer.Peek(2)
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			body.initErr = err
			return
		}
		var reader io.ReadCloser
		if prefix[0]&0x0f == 8 && prefix[0]>>4 <= 7 && (uint16(prefix[0])<<8|uint16(prefix[1]))%31 == 0 {
			reader, err = zlib.NewReader(buffer)
		} else {
			reader = flate.NewReader(buffer)
		}
		body.initErr = err
		if err == nil {
			body.reader, body.decoderClose = reader, reader.Close
		}
	case "zstd":
		reader, err := zstd.NewReader(body.raw, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
		body.initErr = err
		if err == nil {
			body.reader = reader
			body.decoderClose = func() error { reader.Close(); return nil }
		}
	}
}

func (body *decodedBody) Read(data []byte) (int, error) {
	body.mu.Lock()
	if body.closed {
		body.mu.Unlock()
		return 0, net.ErrClosed
	}
	body.reading.Add(1)
	body.mu.Unlock()
	defer body.reading.Done()
	body.init.Do(body.initialize)
	if body.initErr != nil {
		return 0, body.initErr
	}
	return body.reader.Read(data)
}

func (body *decodedBody) Close() error {
	body.closing.Do(func() {
		body.mu.Lock()
		body.closed = true
		body.mu.Unlock()
		body.closeErr = body.raw.Close()
		body.reading.Wait()
		if body.decoderClose != nil {
			body.closeErr = errors.Join(body.closeErr, body.decoderClose())
		}
	})
	return body.closeErr
}
