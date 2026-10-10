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

package internal

import (
	"context"

	"go.temporal.io/sdk/converter"
)

// FathomryEncodedValueDecoderV1 is an opt-in native result-consumption hook.
// It is captured with the result, then receives each caller's Get context. It
// must retain its original ownership and join actual decoding before returning.
type FathomryEncodedValueDecoderV1 func(context.Context, converter.EncodedValue, any) error

type fathomryEncodedValueDecoderKey struct{}

func FathomryWithEncodedValueDecoderV1(ctx context.Context, decode FathomryEncodedValueDecoderV1) context.Context {
	return context.WithValue(ctx, fathomryEncodedValueDecoderKey{}, decode)
}

func fathomryEncodedValueDecoder(ctx context.Context) FathomryEncodedValueDecoderV1 {
	if ctx == nil {
		return nil
	}
	decode, _ := ctx.Value(fathomryEncodedValueDecoderKey{}).(FathomryEncodedValueDecoderV1)
	return decode
}

func fathomryDecodeEncodedValue(ctx context.Context, value converter.EncodedValue, output any, origin FathomryEncodedValueDecoderV1) error {
	if origin != nil {
		return origin(ctx, value, output)
	}
	return value.Get(output)
}
