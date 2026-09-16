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

package http3

import (
	"context"

	"github.com/nukilabs/quic-go"
	"github.com/nukilabs/quic-go/quicvarint"
)

// qpackControlWriter is installed before any response can be decoded. QPACK
// serializes its calls; initialization and writes use the caller and connection
// lifetimes. Atomic writes prevent one canceled section corrupting another's
// control instruction. The connection owns the underlying stream.
type qpackControlWriter struct {
	conn        *quic.Conn
	stream      *quic.SendStream
	initialized bool
}

func (client *ClientConn) cancelQPACK(id uint64) {
	if client.qpackCancellations == nil {
		return
	}
	select {
	case <-client.conn.Context().Done():
	case client.qpackCancellations <- id:
	default:
		_ = client.conn.CloseWithError(quic.ApplicationErrorCode(ErrCodeExcessiveLoad), "QPACK cancellation capacity exhausted")
	}
}

func (client *ClientConn) runQPACKCancellations() {
	defer close(client.qpackDone)
	for {
		select {
		case <-client.conn.Context().Done():
			return
		case id := <-client.qpackCancellations:
			client.decoder.CancelStream(id)
		}
	}
}

func (writer *qpackControlWriter) Write(data []byte) (int, error) {
	return writer.WriteContext(writer.conn.Context(), data)
}
func (writer *qpackControlWriter) WriteContext(ctx context.Context, data []byte) (int, error) {
	work, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(writer.conn.Context(), func() { cancel(context.Cause(writer.conn.Context())) })
	defer func() { stop(); cancel(nil) }()
	if writer.stream == nil {
		stream, err := writer.conn.OpenUniStreamSync(work)
		if err != nil {
			return 0, err
		}
		writer.stream = stream
	}
	payload := data
	if !writer.initialized {
		payload = append(quicvarint.Append(nil, streamTypeQPACKDecoderStream), data...)
	}
	_, err := writer.stream.WriteAllContext(work, payload)
	if err != nil {
		return 0, err
	}
	writer.initialized = true
	return len(data), nil
}
