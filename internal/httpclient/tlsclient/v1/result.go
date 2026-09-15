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

import http "github.com/bogdanfinn/fhttp"

// Metadata is a frozen header observation without native owning handles.
// URL and header inspection may expose sensitive values; ordinary diagnostics do not.
type Metadata struct {
	private
	present       bool
	status        int
	protocol, url string
	headers       http.Header
	length        int64
	uncompressed  bool
}

func (value Metadata) StatusCode() int          { return value.status }
func (value Metadata) Protocol() string         { return value.protocol }
func (value Metadata) URL() string              { return value.url }
func (value Metadata) HeadersCopy() http.Header { return value.headers.Clone() }
func (value Metadata) ContentLength() int64 {
	if !value.present {
		return -1
	}
	return value.length
}
func (value Metadata) Uncompressed() bool { return value.uncompressed }

// Result is immutable local evidence. Complete means final native body EOF with
// no primary failure, not business success, non-effect or durability.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	metadata           Metadata
	trailers           http.Header
	body               []byte
	retained, complete bool
	sent, received     int64
	exchanges          int
	proxyMode          string
	warnings           []error
	inputErrors        []error
}

func (value Result) Metadata() Metadata {
	if value.data == nil {
		return Metadata{}
	}
	return value.data.metadata
}
func (value Result) Complete() bool { return value.data != nil && value.data.complete }
func (value Result) DataCopy() []byte {
	if value.data == nil || !value.data.retained {
		return nil
	}
	return append([]byte{}, value.data.body...)
}
func (value Result) TrailersCopy() http.Header {
	if value.data == nil {
		return nil
	}
	return value.data.trailers.Clone()
}
func (value Result) RequestBytesRead() int64 {
	if value.data == nil {
		return 0
	}
	return value.data.sent
}
func (value Result) BytesRead() int64 {
	if value.data == nil {
		return 0
	}
	return value.data.received
}
func (value Result) Exchanges() int {
	if value.data == nil {
		return 0
	}
	return value.data.exchanges
}
func (value Result) ProxyMode() string {
	if value.data == nil {
		return ""
	}
	return value.data.proxyMode
}

// HookErrorsCopy exposes bounded native hook notices deliberately. Native
// post-hook errors and ErrContinueHooks are not a business retry decision.
func (value Result) HookErrorsCopy() []error {
	if value.data == nil {
		return nil
	}
	return append([]error(nil), value.data.warnings...)
}

// InputErrorsCopy exposes at most one read error per input reader. A canceled
// racing loser or a native retry need not invalidate a complete winning response.
func (value Result) InputErrorsCopy() []error {
	if value.data == nil {
		return nil
	}
	return append([]error(nil), value.data.inputErrors...)
}
