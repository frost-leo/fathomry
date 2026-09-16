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

package qpack

import (
	"context"
	"errors"
	"io"
)

const FathomryCompatibilityRevision = "v1"

var ErrHeaderLimit = errors.New("qpack: decoded header limit exceeded")

// DecodeForStreamContext bounds one section's decoded bytes and permits its
// cancellation without closing the shared table or affecting another stream.
// The caller keeps ctx alive through decoding. A context-aware decoder writer
// must implement WriteContext when control-stream backpressure can block.
func (d *Decoder) DecodeForStreamContext(ctx context.Context, streamID int64, data []byte, limit uint64) DecodeFunc {
	var decode DecodeFunc
	if d.dt != nil {
		decode = d.decodeDynamicContext(ctx, streamID, data, limit)
	} else {
		decode = d.decodeStatic(data)
	}
	var size uint64
	return func() (HeaderField, error) {
		if err := ctx.Err(); err != nil {
			return HeaderField{}, context.Cause(ctx)
		}
		field, err := decode()
		if err != nil {
			return field, err
		}
		count := uint64(len(field.Name)) + uint64(len(field.Value)) + 32
		if limit > 0 && (size > limit || count > limit-size) {
			return HeaderField{}, ErrHeaderLimit
		}
		size += count
		return field, nil
	}
}

func (d *Decoder) writeDecoderStreamContext(ctx context.Context, data []byte) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case d.writeGate <- struct{}{}:
	}
	defer func() { <-d.writeGate }()
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	d.writeMutex.Lock()
	writer := d.decoderStr
	d.writeMutex.Unlock()
	if writer == nil {
		return nil
	}
	var count int
	var err error
	if controlled, ok := writer.(interface {
		WriteContext(context.Context, []byte) (int, error)
	}); ok {
		count, err = controlled.WriteContext(ctx, data)
	} else {
		count, err = writer.Write(data)
	}
	if err == nil && count != len(data) {
		err = io.ErrShortWrite
	}
	return err
}
