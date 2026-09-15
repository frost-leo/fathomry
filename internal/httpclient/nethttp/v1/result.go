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

import "net/http"

// Metadata is a frozen response-header observation. Returned headers are copies.
// URL and headers are deliberate sensitive inspection, not safe diagnostic labels.
// It contains no native Request, TLS handle, Body, callback or owning connection.
type Metadata struct {
	private
	present      bool
	status       int
	protocol     string
	url          string
	headers      http.Header
	length       int64
	uncompressed bool
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
func (metadata Metadata) Uncompressed() bool { return metadata.uncompressed }

// Result is immutable technical evidence, shared by receipt and independent Inbox.
// For request results, Complete means native body EOF without a primary failure.
// For Connect results it means the established session lifetime ended; cleanup is
// separate. Neither means business success, remote non-effect, mutation durability
// or completion of another call.
// An intentionally closed partial stream has Complete false, even with nil Err.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	proxyMode string
	metadata  Metadata
	trailers  http.Header
	body      []byte
	retained  bool
	complete  bool
	received  int64
	sent      int64
	exchanges int
	connected bool
}

// ProxyMode reports "provider", "direct" or "proxy" for the frozen call
// option; empty means unavailable. It never includes a proxy URI or credentials.
// Provider mode retains native callback semantics and does not attest its URI.
func (result Result) ProxyMode() string {
	if result.data == nil {
		return ""
	}
	return result.data.proxyMode
}

func (result Result) Metadata() Metadata {
	if result.data == nil {
		return Metadata{}
	}
	return result.data.metadata
}
func (result Result) TrailersCopy() http.Header {
	if result.data == nil {
		return nil
	}
	return result.data.trailers.Clone()
}

// DataCopy is nil for streams, connection lifetimes or missing responses. A
// successfully retained empty Do response is a non-nil empty slice.
func (result Result) DataCopy() []byte {
	if result.data == nil || !result.data.retained {
		return nil
	}
	return append([]byte{}, result.data.body...)
}
func (result Result) Complete() bool { return result.data != nil && result.data.complete }
func (result Result) BytesRead() int64 {
	if result.data == nil {
		return 0
	}
	return result.data.received
}

// RequestBytesRead counts bytes consumed from request bodies, including replays
// and an over-limit witness. It does not prove transmission or peer receipt.
func (result Result) RequestBytesRead() int64 {
	if result.data == nil {
		return 0
	}
	return result.data.sent
}

// Exchanges counts intercepted client RoundTrip entries, not every native wire
// attempt. Native retries and protocol establishment remain separately unobserved.
func (result Result) Exchanges() int {
	if result.data == nil {
		return 0
	}
	return result.data.exchanges
}
func (result Result) Connected() bool { return result.data != nil && result.data.connected }
