// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package qpack

import (
	"context"
	"io"
	"math"
	"testing"
)

func TestFathomryDynamicReferencesRespectRequiredInsertCount(t *testing.T) {
	for _, form := range []string{"indexed", "name", "post-indexed", "post-name"} {
		for _, count := range []uint64{0, 1, 2} {
			t.Run(form+string(rune('0'+count)), func(t *testing.T) {
				decoder := NewDecoder(WithMaxTableCapacity(256))
				defer decoder.Close()
				if err := decoder.dt.setCapacity(256); err != nil {
					t.Fatal(err)
				}
				if err := decoder.applyInsert("zero", "value"); err != nil {
					t.Fatal(err)
				}
				if err := decoder.applyInsert("one", "value"); err != nil {
					t.Fatal(err)
				}
				encoded := uint64(0)
				if count != 0 {
					encoded = count + 1
				}
				var field []byte
				base := uint64(2)
				switch form {
				case "indexed":
					field = fieldIndexedDynamic(0)
				case "name":
					field = fieldLiteralDynamicNameRef(0, "replacement")
				case "post-indexed":
					base = 1
					field = fieldPostBaseIndexed(0)
				case "post-name":
					base = 1
					field = fieldPostBaseLiteralNameRef(0, "replacement")
				}
				sign := base < count
				delta := base - count
				if sign {
					delta = count - base - 1
				}
				block := append(blockPrefix(encoded, delta, sign), field...)
				decode := decoder.DecodeForStreamContext(context.Background(), 4, block, 1024)
				value, err := decode()
				if count < 2 {
					if err == nil || err == io.EOF {
						t.Fatalf("reference abs1 outside RequiredInsertCount%d accepted: %+v", count, value)
					}
				} else if err != nil || value.Name != "one" {
					t.Fatal("legal reference rejected", value, err)
				}
			})
		}
	}
}

func TestFathomryDynamicIndexArithmeticCannotWrap(t *testing.T) {
	decoder := NewDecoder(WithMaxTableCapacity(256))
	defer decoder.Close()
	if err := decoder.dt.setCapacity(256); err != nil {
		t.Fatal(err)
	}
	if err := decoder.applyInsert("zero", "value"); err != nil {
		t.Fatal(err)
	}
	for _, field := range [][]byte{fieldPostBaseIndexed(math.MaxUint64), fieldPostBaseLiteralNameRef(math.MaxUint64, "x"), fieldIndexedDynamic(math.MaxUint64), fieldLiteralDynamicNameRef(math.MaxUint64, "x")} {
		block := append(blockPrefix(2, 0, false), field...)
		if value, err := decoder.DecodeForStreamContext(context.Background(), 4, block, 1024)(); err == nil || err == io.EOF {
			t.Fatal("wrapped dynamic reference accepted", value, err)
		}
	}
}
