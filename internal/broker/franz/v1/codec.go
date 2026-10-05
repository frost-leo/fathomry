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

package franz

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/twmb/franz-go/pkg/kgo"
)

func validCompression(value string) bool {
	switch value {
	case "none", "gzip", "snappy", "lz4", "zstd":
		return true
	}
	return false
}

func expandCodec(src []byte, codec kgo.CompressionCodecType, limit int) ([]byte, error) {
	if limit <= 0 {
		return nil, failure(ErrLimit, "decoded-bytes")
	}
	switch codec {
	case kgo.CodecGzip:
		return expandGzip(src, limit)
	case kgo.CodecSnappy:
		return expandSnappy(src, limit)
	case kgo.CodecLz4:
		expected, err := checkLZ4Frame(src, limit)
		if err != nil {
			return nil, err
		}
		reader := lz4.NewReader(bytes.NewReader(src))
		if err := reader.Apply(lz4.ConcurrencyOption(1)); err != nil {
			return nil, failure(ErrRead, "lz4", err)
		}
		defer reader.Reset(nil)
		decoded, err := io.ReadAll(io.LimitReader(reader, int64(limit)+1))
		if err != nil {
			return nil, failure(ErrRead, "lz4", err)
		}
		if len(decoded) > limit {
			return nil, failure(ErrLimit, "decoded-bytes")
		}
		if expected >= 0 && len(decoded) != expected {
			return nil, failure(ErrRead, "lz4-content-size")
		}
		return decoded, nil
	case kgo.CodecZstd:
		decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxMemory(uint64(max(1024, limit))), zstd.WithDecoderMaxWindow(uint64(max(1024, limit))), zstd.WithDecodeAllCapLimit(true))
		if err != nil {
			return nil, failure(ErrRead, "zstd", err)
		}
		defer decoder.Close()
		decoded, err := decoder.DecodeAll(src, make([]byte, 0, limit))
		if errors.Is(err, zstd.ErrDecoderSizeExceeded) || errors.Is(err, zstd.ErrWindowSizeExceeded) {
			return nil, failure(ErrLimit, "decoded-bytes", err)
		}
		if err != nil {
			return nil, failure(ErrRead, "zstd", err)
		}
		return decoded, nil
	default:
		return nil, failure(ErrUnsupported, "compression")
	}
}

// Kafka record-batch v2 uses one modern independent-block frame. In particular,
// legacy/skippable/concatenated frames must not reach the SDK's larger buffers
// or recursive legacy reader. Checksums and decompression remain SDK-owned.
func checkLZ4Frame(src []byte, limit int) (int, error) {
	if len(src) < 4 {
		return 0, failure(ErrRead, "lz4-frame")
	}
	if binary.LittleEndian.Uint32(src) != 0x184d2204 {
		return 0, failure(ErrUnsupported, "lz4-frame")
	}
	if len(src) < 7 {
		return 0, failure(ErrRead, "lz4-frame")
	}
	flags, descriptor := src[4], src[5]
	index := descriptor >> 4 & 7
	if flags&0xe3 != 0x60 || descriptor&0x8f != 0 || index < 4 || index > 7 {
		return 0, failure(ErrUnsupported, "lz4-frame")
	}
	maximum := uint32(1) << (2*index + 8)
	expected, cursor := -1, 7
	if flags&8 != 0 {
		if len(src) < 15 {
			return 0, failure(ErrRead, "lz4-frame")
		}
		size := binary.LittleEndian.Uint64(src[6:14])
		if size > uint64(limit) {
			return 0, failure(ErrLimit, "decoded-bytes")
		}
		expected, cursor = int(size), 15
	}
	plain := uint64(0)
	for {
		if len(src)-cursor < 4 {
			return 0, failure(ErrRead, "lz4-frame")
		}
		block := binary.LittleEndian.Uint32(src[cursor:])
		cursor += 4
		if block == 0 {
			break
		}
		size := block & 0x7fffffff
		if size > maximum {
			return 0, failure(ErrLimit, "lz4-block")
		}
		if uint64(size) > uint64(len(src)-cursor) {
			return 0, failure(ErrRead, "lz4-frame")
		}
		if block&0x80000000 != 0 {
			plain += uint64(size)
		}
		if plain > uint64(limit) {
			return 0, failure(ErrLimit, "decoded-bytes")
		}
		cursor += int(size)
		if flags&16 != 0 {
			if len(src)-cursor < 4 {
				return 0, failure(ErrRead, "lz4-frame")
			}
			cursor += 4
		}
	}
	if flags&4 != 0 {
		if len(src)-cursor < 4 {
			return 0, failure(ErrRead, "lz4-frame")
		}
		cursor += 4
	}
	if cursor != len(src) {
		return 0, failure(ErrUnsupported, "lz4-trailing")
	}
	return expected, nil
}

var xerialMagic = []byte{130, 'S', 'N', 'A', 'P', 'P', 'Y', 0}

func expandSnappy(src []byte, limit int) ([]byte, error) {
	decode := func(src []byte, remaining int) ([]byte, error) {
		size, err := s2.DecodedLen(src)
		if err != nil {
			return nil, failure(ErrRead, "snappy", err)
		}
		if size > remaining {
			return nil, failure(ErrLimit, "decoded-bytes")
		}
		decoded, err := s2.Decode(nil, src)
		if err != nil {
			return nil, failure(ErrRead, "snappy", err)
		}
		return decoded, nil
	}
	if !bytes.HasPrefix(src, xerialMagic) {
		return decode(src, limit)
	}
	if len(src) < 16 || binary.BigEndian.Uint32(src[8:12]) != 1 || binary.BigEndian.Uint32(src[12:16]) != 1 {
		return nil, failure(ErrRead, "snappy-frame")
	}
	src = src[16:]
	var result []byte
	for len(src) > 0 {
		if len(src) < 4 {
			return nil, failure(ErrRead, "snappy-frame")
		}
		size := uint64(binary.BigEndian.Uint32(src[:4]))
		src = src[4:]
		if size == 0 || size > uint64(len(src)) {
			return nil, failure(ErrRead, "snappy-frame")
		}
		chunk, err := decode(src[:int(size)], limit-len(result))
		if err != nil {
			return nil, err
		}
		result = append(result, chunk...)
		src = src[int(size):]
	}
	return result, nil
}
