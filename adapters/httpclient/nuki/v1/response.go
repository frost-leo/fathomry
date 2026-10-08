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
	"errors"
	"io"
	"sync/atomic"

	native "github.com/frost-leo/fathomry/internal/httpclient/nuki/v1"
)

// Response is a read-only callback-scoped capability. Reads are serialized by
// Internal and already-entered reads remain owned until actual completion.
// It has no Close or underlying Body/PacketConn/tunnel accessor. Metadata is
// detached and may be kept; new reads after callback return are refused.
type Response struct {
	private
	native  *native.Response
	revoked atomic.Bool
}

func (response *Response) Metadata() Metadata {
	if response == nil || response.native == nil {
		return Metadata{}
	}
	return Metadata{native: response.native.Metadata()}
}
func (response *Response) Read(data []byte) (int, error) {
	if response == nil || response.native == nil || response.revoked.Load() {
		return 0, fail(ErrState, "response-read")
	}
	count, err := response.native.Read(data)
	if errors.Is(err, io.EOF) {
		return count, err
	}
	return count, translate(err, "response-read")
}

var _ io.Reader = (*Response)(nil)
