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

package pgx

import (
	"encoding/binary"
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
)

// copyFence sits above the selected TLS reader. Native message-size enforcement
// still owns allocation limits; this fence prevents unsupported COPY streams from
// being silently discarded by the ordinary ResultReader.
type copyFence struct {
	reader    io.Reader
	header    [5]byte
	offset    int
	pending   bool
	remaining uint32
}

func boundedFrontend(reader io.Reader, writer io.Writer) *pgproto3.Frontend {
	return pgproto3.NewFrontend(&copyFence{reader: reader}, writer)
}
func (fence *copyFence) Read(destination []byte) (int, error) {
	if len(destination) == 0 {
		return 0, nil
	}
	if fence.pending {
		count := copy(destination, fence.header[fence.offset:])
		fence.offset += count
		if fence.offset == len(fence.header) {
			fence.pending = false
			fence.offset = 0
		}
		return count, nil
	}
	if fence.remaining > 0 {
		size := min(uint64(len(destination)), uint64(fence.remaining))
		count, err := fence.reader.Read(destination[:int(size)])
		fence.remaining -= uint32(count)
		return count, err
	}
	if _, err := io.ReadFull(fence.reader, fence.header[:]); err != nil {
		return 0, err
	}
	length := binary.BigEndian.Uint32(fence.header[1:])
	if length < 4 {
		return 0, failure(ErrInput, "protocol-length")
	}
	switch fence.header[0] {
	case 'G', 'H', 'W', 'd', 'c':
		return 0, failure(ErrUnsupported, "copy-protocol")
	}
	fence.remaining = length - 4
	fence.pending = true
	return fence.Read(destination)
}
