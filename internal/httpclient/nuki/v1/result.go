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

package nuki

import (
	"slices"

	http "github.com/nukilabs/http"
)

// Metadata is immutable response metadata. Copy methods return independent
// containers. It carries no response body, client, connection or callback.
type Metadata struct {
	private
	present       bool
	status        int
	protocol      string
	url           string
	headers       http.Header
	length        int64
	encodedLength int64
	uncompressed  bool
}

func (metadata Metadata) StatusCode() int          { return metadata.status }
func (metadata Metadata) Protocol() string         { return metadata.protocol }
func (metadata Metadata) URL() string              { return metadata.url }
func (metadata Metadata) HeadersCopy() http.Header { return metadata.headers.Clone() }
func (metadata Metadata) ContentLength() int64 {
	if !metadata.present {
		return -1
	}
	return metadata.length
}
func (metadata Metadata) EncodedContentLength() int64 {
	if !metadata.present {
		return -1
	}
	return metadata.encodedLength
}
func (metadata Metadata) Uncompressed() bool { return metadata.uncompressed }

// Result is immutable runtime evidence shared by a receipt and its Inbox.
// Complete proves final response EOF plus framing/decode checks, not business
// success or remote-effect completion. An early-returning stream consumer may
// finish technically with Complete false and no invented read error.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	metadata     Metadata
	body         []byte
	retained     bool
	complete     bool
	trailers     http.Header
	inputBytes   int64
	encodedBytes int64
	decodedBytes int64
	exchanges    int
	replays      int
	proxyMode    ProxyMode
	readErrors   []error
}

func (result Result) Metadata() Metadata {
	if result.data == nil {
		return Metadata{}
	}
	return result.data.metadata
}
func (result Result) DataCopy() []byte {
	if result.data == nil || !result.data.retained {
		return nil
	}
	return append([]byte{}, result.data.body...)
}
func (result Result) TrailersCopy() http.Header {
	if result.data == nil {
		return nil
	}
	return result.data.trailers.Clone()
}
func (result Result) Complete() bool { return result.data != nil && result.data.complete }

// BytesReadFromInput includes replay reads and an excess-byte witness, not
// confirmed wire delivery or an acknowledgement of a remote mutation.
func (result Result) BytesReadFromInput() int64 {
	if result.data == nil {
		return 0
	}
	return result.data.inputBytes
}
func (result Result) EncodedBytesRead() int64 {
	if result.data == nil {
		return 0
	}
	return result.data.encodedBytes
}
func (result Result) DecodedBytesRead() int64 {
	if result.data == nil {
		return 0
	}
	return result.data.decodedBytes
}

// Exchanges is a lower-bound native submission count. Hidden reconnect attempts
// and QUIC connection warming are not exact application request observations.
func (result Result) Exchanges() int {
	if result.data == nil {
		return 0
	}
	return result.data.exchanges
}
func (result Result) Replays() int {
	if result.data == nil {
		return 0
	}
	return result.data.replays
}
func (result Result) ProxyMode() ProxyMode {
	if result.data == nil {
		return ""
	}
	return result.data.proxyMode
}

// InputErrorsCopy preserves bounded native reader and asynchronous upload causes
// across exchanges, independently of the winning response or SDK retry behavior.
// Errors themselves remain borrowed immutable.
func (result Result) InputErrorsCopy() []error {
	if result.data == nil {
		return nil
	}
	return slices.Clone(result.data.readErrors)
}
