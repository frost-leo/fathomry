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
	"context"
	"testing"
)

func FuzzFathomryQPACKBoundedDecode(f *testing.F) {
	for _, seed := range [][]byte{{}, {0, 0, 0xd9}, {2, 0, 0x80}, {0xff, 0xff}, {2, 0x80, 0x10}, {0, 0, 0x21, 'x', 1, 'v'}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1024 {
			return
		}
		decoder, err := New(128, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer decoder.Close(nil)
		seed := append(AppendInstruction(5, 0x20, 128), 0x41, 'x', 1, 'v')
		_ = decoder.ParseEncoder(bytes.NewReader(seed), func(uint64) error { return nil })
		fields, _, err := decoder.Decode(context.Background(), 0, data, 1024)
		var size int
		for _, field := range fields {
			size += len(field.Name) + len(field.Value) + 32
		}
		if err == nil && size > 1024 || len(decoder.blocked) != 0 || decoder.table.size > 128 {
			t.Fatal("bounded section escaped configured state envelope")
		}
	})
}

func FuzzFathomryQPACKBoundedEncoderStream(f *testing.F) {
	for _, seed := range [][]byte{{}, AppendInstruction(5, 0x20, 128), {0x20}, {0xff, 0xff}, {0x41, 'x', 1, 'v'}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		decoder, err := New(128, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer decoder.Close(nil)
		_ = decoder.ParseEncoder(bytes.NewReader(data), func(uint64) error { return nil })
		if decoder.table.size > 128 || len(decoder.table.entries) > 4 || decoder.table.capacity > 128 {
			t.Fatal("bounded encoder instructions escaped advertised table envelope")
		}
	})
}
