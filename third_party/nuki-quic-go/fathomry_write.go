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

package quic

import (
	"context"
	"errors"
)

// WriteAllContext queues the entire small control instruction atomically or
// waits for send credit. Cancellation before acceptance leaves no partial
// instruction in this shared stream. Concurrent writers require external
// serialization, as with Write.
func (stream *SendStream) WriteAllContext(ctx context.Context, data []byte) (int, error) {
	for {
		if ctx.Err() != nil {
			return 0, context.Cause(ctx)
		}
		err := stream.TryWriteAll(data)
		if err == nil {
			return len(data), nil
		}
		if !errors.Is(err, ErrWouldBlock) {
			return 0, err
		}
		select {
		case <-ctx.Done():
			return 0, context.Cause(ctx)
		case <-stream.Context().Done():
			return 0, context.Cause(stream.Context())
		case <-stream.writeChan:
		}
	}
}
