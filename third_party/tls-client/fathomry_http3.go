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

package tls_client

import (
	"compress/gzip"
	"errors"
	"io"
	"strings"

	http "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/quic-go-utls/http3"
)

// The native H3 decoder removes encoded Content-Length before the Provider can
// check it. Keep framing validation beneath transparent gzip without changing
// explicit Accept-Encoding, Range or DisableCompression semantics.
type fathomryHTTP3Transport struct {
	native             *http3.Transport
	disableCompression bool
}

func (transport *fathomryHTTP3Transport) RoundTrip(original *http.Request) (*http.Response, error) {
	request := original.Clone(original.Context())
	automatic := !transport.disableCompression && request.Method != http.MethodHead &&
		request.Header.Get("Accept-Encoding") == "" && request.Header.Get("Range") == ""
	present := false
	for key := range request.Header {
		present = present || strings.EqualFold(key, "Accept-Encoding")
	}
	if automatic && !present {
		if request.Header == nil {
			request.Header = make(http.Header)
		}
		request.Header.Set("Accept-Encoding", "gzip")
	}
	response, err := transport.native.RoundTrip(request)
	if response == nil || response.Body == nil {
		return response, err
	}
	expected := response.ContentLength
	if request.Method == http.MethodHead || response.StatusCode == http.StatusNoContent || response.StatusCode == http.StatusNotModified {
		expected = -1
	}
	body := &fathomryFramedBody{ReadCloser: response.Body, expected: expected}
	response.Body = body
	if automatic && response.Header.Get("Content-Encoding") == "gzip" {
		response.Body = &fathomryGzipBody{body: body}
		response.Header.Del("Content-Encoding")
		response.Header.Del("Content-Length")
		response.ContentLength = -1
		response.Uncompressed = true
	}
	return response, err
}
func (transport *fathomryHTTP3Transport) CloseIdleConnections() {
	transport.native.CloseIdleConnections()
}

type fathomryFramedBody struct {
	io.ReadCloser
	expected, seen int64
}

func (body *fathomryFramedBody) Read(buffer []byte) (int, error) {
	count, err := body.ReadCloser.Read(buffer)
	body.seen += int64(count)
	if body.expected >= 0 && (body.seen > body.expected || errors.Is(err, io.EOF) && body.seen != body.expected) {
		return count, io.ErrUnexpectedEOF
	}
	return count, err
}

type fathomryGzipBody struct {
	body    io.ReadCloser
	decoder *gzip.Reader
	failure error
}

func (body *fathomryGzipBody) Read(buffer []byte) (int, error) {
	if body.failure != nil {
		return 0, body.failure
	}
	if body.decoder == nil {
		body.decoder, body.failure = gzip.NewReader(body.body)
		if body.failure != nil {
			return 0, body.failure
		}
	}
	return body.decoder.Read(buffer)
}
func (body *fathomryGzipBody) Close() error { return body.body.Close() }
