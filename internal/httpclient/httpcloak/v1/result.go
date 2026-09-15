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

import http "github.com/sardanioss/http"

// Metadata is immutable header evidence without a native body or connection.
// Headers and URL are sensitive deliberate inspection, not diagnostic labels.
type Metadata struct {
	private
	present  bool
	status   int
	protocol string
	url      string
	headers  http.Header
	length   int64
	encoding string
	order    []string
	casing   []string
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
func (value Metadata) ContentEncoding() string { return value.encoding }

// HeaderOrderCopy returns optional native response ordering observations.
// Nil means the SDK did not provide that evidence.
func (value Metadata) HeaderOrderCopy() []string { return append([]string(nil), value.order...) }

// HeaderCasingCopy returns optional native response spelling observations.
// Nil means unavailable; bounded managed H1 parsing currently does not collect it.
func (value Metadata) HeaderCasingCopy() []string { return append([]string(nil), value.casing...) }

// Result freezes separate response integrity, input consumption and cleanup
// facts. Complete is not a business outcome, non-effect or source-release claim.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	inputComplete        bool
	writes               int
	notifications        []error
	metadata             Metadata
	body                 []byte
	retained, complete   bool
	wire, decoded, input int64
	exchanges, replays   int
	trailers             http.Header
	proxyMode            string
}

func (result Result) Metadata() Metadata {
	if result.data == nil {
		return Metadata{}
	}
	return result.data.metadata
}

// NativeErrorsCopy retains failures reported by borrowed native callbacks even
// when a later cancellation or SDK error translation replaces their return.
func (result Result) NativeErrorsCopy() []error {
	if result.data == nil {
		return nil
	}
	return append([]error(nil), result.data.notifications...)
}
func (result Result) Complete() bool { return result.data != nil && result.data.complete }

// InputComplete reports bounded reader consumption, not peer receipt or effect.
func (result Result) InputComplete() bool { return result.data != nil && result.data.inputComplete }

// WritesObserved counts native request-write notifications, not acknowledged
// effects or an exact count of every connection/protocol attempt.
func (result Result) WritesObserved() int {
	if result.data == nil {
		return 0
	}
	return result.data.writes
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
func (result Result) WireBytesRead() int64 {
	if result.data == nil {
		return 0
	}
	return result.data.wire
}
func (result Result) BytesRead() int64 {
	if result.data == nil {
		return 0
	}
	return result.data.decoded
}
func (result Result) RequestBytesRead() int64 {
	if result.data == nil {
		return 0
	}
	return result.data.input
}
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
func (result Result) ProxyMode() string {
	if result.data == nil {
		return ""
	}
	return result.data.proxyMode
}
