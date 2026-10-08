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
	"io"
	"strings"
	"testing"
)

func TestFathomryEncoderFragmentation(t *testing.T) {
	wire := []byte{0x3f, 0x21, 0x41, 'x', 0x01, 'v'}
	for split := 1; split < len(wire); split++ {
		decoder := NewDecoderWithCapacity(64)
		if err := decoder.ProcessEncoderInstructions(wire[:split]); err != nil {
			t.Fatalf("split %d rejected an incomplete instruction: %v", split, err)
		}
		if err := decoder.ProcessEncoderInstructions(wire[split:]); err != nil {
			t.Fatalf("split %d rejected the remainder: %v", split, err)
		}
		read := decoder.Decode([]byte{2, 0, 0x80})
		field, err := read()
		if err != nil || field.Name != "x" || field.Value != "v" {
			t.Fatalf("split %d lost the dynamic field: %#v, %v", split, field, err)
		}
		if _, err := read(); err != io.EOF {
			t.Fatalf("split %d: %v", split, err)
		}
	}
}

func TestFathomryTableCapacityIsInitiallyZero(t *testing.T) {
	decoder := NewDecoderWithCapacity(64)
	if err := decoder.InsertWithoutNameReference("x", "v"); err == nil || decoder.InsertCount() != 0 {
		t.Fatal("advertised maximum was used before encoder capacity instruction")
	}
	if err := decoder.SetDynamicTableCapacity(64); err != nil {
		t.Fatal(err)
	}
	if err := decoder.InsertWithoutNameReference("x", "v"); err != nil {
		t.Fatal(err)
	}
	if decoder.InsertCount() != 1 {
		t.Fatal("valid insertion was not retained")
	}
}

func TestFathomryOversizedInsertionIsRejected(t *testing.T) {
	decoder := NewDecoderWithCapacity(64)
	if err := decoder.SetDynamicTableCapacity(64); err != nil {
		t.Fatal(err)
	}
	if err := decoder.InsertWithoutNameReference("x", strings.Repeat("v", 32)); err == nil || decoder.InsertCount() != 0 {
		t.Fatal("oversized insertion advanced state instead of rejecting")
	}
	if err := decoder.InsertWithoutNameReference("x", strings.Repeat("v", 31)); err != nil {
		t.Fatal(err)
	}
	if decoder.InsertCount() != 1 {
		t.Fatal("exact-capacity insertion was rejected")
	}
}

func TestFathomryNonzeroEncodedZeroRICIsRejected(t *testing.T) {
	decoder := NewDecoderWithCapacity(64)
	_, err := decoder.Decode([]byte{1, 0, 0xd9})()
	if err == nil {
		t.Fatal("nonzero EncodedInsertCount decoded to forbidden zero")
	}
	field, err := decoder.Decode([]byte{0, 0, 0xd9})()
	if err != nil || field.Name != ":status" || field.Value != "200" {
		t.Fatalf("unchanged static control failed: %#v %v", field, err)
	}
}

func TestFathomryRICBoundsEveryDynamicRepresentation(t *testing.T) {
	for _, test := range []struct {
		name      string
		bad, good []byte
	}{
		{"indexed", []byte{0, 1, 0x80}, []byte{2, 0, 0x80}},
		{"name", []byte{0, 1, 0x40, 1, 't'}, []byte{2, 0, 0x40, 1, 't'}},
		{"post_indexed", []byte{0, 0, 0x10}, []byte{2, 0x80, 0x10}},
		{"post_name", []byte{0, 0, 0, 1, 't'}, []byte{2, 0x80, 0, 1, 't'}},
	} {
		t.Run(test.name, func(t *testing.T) {
			decoder := NewDecoderWithCapacity(64)
			if err := decoder.SetDynamicTableCapacity(64); err != nil {
				t.Fatal(err)
			}
			if err := decoder.InsertWithoutNameReference("x", "v"); err != nil {
				t.Fatal(err)
			}
			if _, err := decoder.Decode(test.bad)(); err == nil {
				t.Error("dynamic index exceeded Required Insert Count")
			}
			read := decoder.Decode(test.good)
			field, err := read()
			if err != nil || field.Name != "x" {
				t.Fatalf("valid counterpart: %#v %v", field, err)
			}
			if _, err := read(); err != io.EOF {
				t.Fatal("valid section did not finish", err)
			}
		})
	}
}
