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

package surf

import http "github.com/enetx/http"

// Metadata is an immutable header observation without SDK ownership. Explicit
// URL/header access may reveal sensitive values; diagnostics do not.
type Metadata struct {
	private
	present       bool
	status        int
	protocol, url string
	headers       http.Header
	length        int64
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

// Result owns bounded immutable local evidence. Complete means the final native
// response was fully read and verified, not business success or remote non-effect.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	metadata           Metadata
	trailers           http.Header
	body               []byte
	retained, complete bool
	read, sent         int64
	roundTrips         int
	proxyMode          string
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
func (value Result) BytesRead() int64 {
	if value.data == nil {
		return 0
	}
	return value.data.read
}
func (value Result) RequestBytesRead() int64 {
	if value.data == nil {
		return 0
	}
	return value.data.sent
}

// RoundTrips counts entries into Surf's transport, not exact physical attempts.
func (value Result) RoundTrips() int {
	if value.data == nil {
		return 0
	}
	return value.data.roundTrips
}
func (value Result) ProxyMode() string {
	if value.data == nil {
		return ""
	}
	return value.data.proxyMode
}
