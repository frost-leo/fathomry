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

package qpackdecoder

import (
	"bytes"
	"errors"
	"io"

	"github.com/quic-go/qpack"
	"golang.org/x/net/http2/hpack"
)

const maxInteger uint64 = (1 << 62) - 1

var ErrEncoding = errors.New("qpack: invalid encoding")
var ErrLimit = errors.New("qpack: field or instruction exceeds bound")
var ErrClosed = errors.New("qpack: decoder closed")

func integer(first byte, prefix uint, reader io.ByteReader) (uint64, error) {
	mask := uint64(1<<prefix) - 1
	value := uint64(first) & mask
	if value < mask {
		return value, nil
	}
	for shift := uint(0); shift < 63; shift += 7 {
		next, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		part := uint64(next & 127)
		if part > (maxInteger-value)>>shift {
			return 0, ErrEncoding
		}
		value += part << shift
		if next&128 == 0 {
			return value, nil
		}
	}
	return 0, ErrEncoding
}

func appendInteger(data []byte, prefix uint, value uint64) []byte {
	maximum := uint64(1<<prefix) - 1
	if value < maximum {
		return append(data, byte(value))
	}
	data = append(data, byte(maximum))
	value -= maximum
	for value >= 128 {
		data = append(data, byte(value&127)|128)
		value >>= 7
	}
	return append(data, byte(value))
}

type literalReader interface {
	io.Reader
	io.ByteReader
}

func literal(first byte, prefix uint, reader literalReader, maximum uint64) (string, error) {
	length, err := integer(first, prefix, reader)
	if err != nil {
		return "", err
	}
	huffman := first&(1<<prefix) != 0
	encodedMaximum := maximum
	if huffman {
		encodedMaximum = (maximum*30 + 7) / 8
	}
	if length > encodedMaximum {
		return "", ErrLimit
	}
	if bounded, ok := reader.(*bytes.Reader); ok && length > uint64(bounded.Len()) {
		return "", io.ErrUnexpectedEOF
	}
	data := make([]byte, int(length))
	if _, err := io.ReadFull(reader, data); err != nil {
		return "", err
	}
	var value string
	if huffman {
		value, err = hpack.HuffmanDecodeToString(data)
	} else {
		value = string(data)
	}
	if err != nil {
		return "", err
	}
	if uint64(len(value)) > maximum {
		return "", ErrLimit
	}
	return value, nil
}

func staticField(index uint64) (qpack.HeaderField, error) {
	data := appendInteger([]byte{0, 0}, 6, index)
	data[2] |= 0xc0
	return qpack.NewDecoder().Decode(data)()
}
