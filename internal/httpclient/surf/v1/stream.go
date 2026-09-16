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

import (
	"context"
	"errors"
	"io"

	sdk "github.com/enetx/surf"
)

// Stream owns one admitted response. Reads are serialized and bounded; Close
// cancels unread work and waits on a caller-owned cleanup context. Returning from
// that wait does not release a still-running reader or native callback.
type Stream struct {
	private
	op   *operation
	body io.ReadCloser
	gate chan struct{}
}

func (stream *Stream) Metadata() Metadata {
	if stream == nil || stream.op == nil {
		return Metadata{}
	}
	stream.op.mu.Lock()
	defer stream.op.mu.Unlock()
	return stream.op.data.metadata
}
func (stream *Stream) Read(buffer []byte) (int, error) {
	if stream == nil || stream.op == nil {
		return 0, failure(ErrInput, "stream")
	}
	if len(buffer) == 0 {
		return 0, nil
	}
	op := stream.op
	if !op.work.enter() {
		return 0, failure(ErrState, "stream-ended")
	}
	defer op.work.leave()
	op.mu.Lock()
	terminal, complete := op.primary, op.data.complete
	op.mu.Unlock()
	if terminal != nil {
		return 0, terminal
	}
	if complete {
		return 0, io.EOF
	}
	select {
	case stream.gate <- struct{}{}:
		defer func() { <-stream.gate }()
	case <-op.ctx.Done():
		err := failure(ErrRead, "stream-wait", op.ctx.Err(), context.Cause(op.ctx))
		op.fail(err)
		return 0, err
	}
	op.mu.Lock()
	if op.primary != nil {
		err := op.primary
		op.mu.Unlock()
		return 0, err
	}
	if op.data.complete {
		op.mu.Unlock()
		return 0, io.EOF
	}
	remaining := op.client.owner.settings.MaxResponseBytes - op.data.read
	op.mu.Unlock()
	if int64(len(buffer)) > remaining+1 {
		buffer = buffer[:remaining+1]
	}
	count, err := stream.body.Read(buffer)
	op.mu.Lock()
	op.data.read += int64(count)
	exceeded := op.data.read > op.client.owner.settings.MaxResponseBytes
	if err == io.EOF && !exceeded {
		op.data.complete = true
	}
	op.mu.Unlock()
	if exceeded {
		count = min(count, int(remaining))
		err = failure(ErrLimit, "response-body", sdk.ErrFathomryBodyLimit)
	}
	if err != nil && err != io.EOF {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			err = failure(ErrIntegrity, "response-body", err)
		} else if !exceeded {
			err = failure(ErrRead, "response-body", err)
		}
		op.fail(err)
	}
	return count, err
}
func (stream *Stream) Close(ctx context.Context) error {
	if stream == nil || stream.op == nil || ctx == nil {
		return failure(ErrInput, "stream-close")
	}
	stream.op.finish()
	return stream.op.wait(ctx)
}
