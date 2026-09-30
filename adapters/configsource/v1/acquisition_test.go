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

package configsource

import (
	"errors"
	"strings"
	"testing"
)

func TestBatch(t *testing.T) {
	input := []Raw{{Content: []byte("private")}, {Missing: true}, {Content: []byte{}}}
	batch, err := NewBatch(input)
	if err != nil || !batch.Valid() || batch.Len() != 3 {
		t.Fatal(err)
	}
	input[0].Content[0] = 'x'
	copy, err := batch.DocumentsCopy()
	if err != nil || string(copy[0].Content) != "private" || !copy[1].Missing || copy[2].Missing {
		t.Fatal("presence or input isolation")
	}
	copy[0].Content[0] = 'y'
	copy[1].Missing = false
	next, _ := batch.DocumentsCopy()
	if string(next[0].Content) != "private" || !next[1].Missing {
		t.Fatal("output alias")
	}
	for _, input := range [][]Raw{nil, make([]Raw, MaxDocuments+1), {{Missing: true, Content: []byte("invalid")}}, {{Content: []byte{255}}}} {
		if value, err := NewBatch(input); err == nil || value.Valid() {
			t.Fatal("invalid batch accepted")
		}
	}
	if _, err := NewBatch([]Raw{{Content: []byte(strings.Repeat("x", MaxDocumentBytes+1))}}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	var zero Batch
	if zero.Valid() || zero.Len() != 0 {
		t.Fatal("zero batch is usable")
	}
	if _, err := zero.DocumentsCopy(); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
}
