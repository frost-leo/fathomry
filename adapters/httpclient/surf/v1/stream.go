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

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/httpclient/surf/v1"
)

// Stream permits one Read at a time and concurrent repeatable Close. Reads
// consume its existing operation reservation; they are not separate admissions.
type Stream struct {
	private
	native  *native.Stream
	receipt *adapters.Receipt[Result]
}

func (stream *Stream) Metadata() Metadata {
	if stream == nil || stream.native == nil {
		return Metadata{}
	}
	return Metadata{native: stream.native.Metadata()}
}
func (stream *Stream) Read(data []byte) (int, error) {
	if stream == nil || stream.native == nil {
		return 0, fail(ErrState, "read")
	}
	count, err := stream.native.Read(data)
	if errors.Is(err, io.EOF) {
		return count, err
	}
	return count, translate(err, "read")
}

// Close bypasses admission/evidence saturation. A timeout ends only this wait;
// cleanup and evidence transfer continue, and the same handle remains usable.
// Native Surf completion returns primary and cleanup failures together;
func (stream *Stream) Close(ctx context.Context) error {
	if stream == nil || stream.native == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	return translate(stream.native.Close(ctx), "close")
}
func (stream *Stream) Receipt() *adapters.Receipt[Result] {
	if stream == nil {
		return nil
	}
	return stream.receipt
}
