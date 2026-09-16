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
	"io"
	"sync"
	"sync/atomic"
)

// Response is a callback-scoped streaming view. Metadata is immutable. Reads
// are serialized; the view is revoked when the callback returns, and provider
// cleanup joins already-entered reads before releasing the original call.
type Response struct {
	private
	mu       sync.Mutex
	revoked  atomic.Bool
	reader   *body
	metadata Metadata
}

func (response *Response) Metadata() Metadata {
	if response == nil {
		return Metadata{}
	}
	return response.metadata
}
func (response *Response) Read(data []byte) (int, error) {
	if response == nil {
		return 0, failure(ErrInput, "stream")
	}
	response.mu.Lock()
	defer response.mu.Unlock()
	if response.revoked.Load() {
		return 0, failure(ErrState, "stream-revoked")
	}
	return response.reader.Read(data)
}
func (response *Response) revoke() {
	response.revoked.Store(true)
}

var _ io.Reader = (*Response)(nil)
