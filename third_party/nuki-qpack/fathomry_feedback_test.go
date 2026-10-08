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
	"bytes"
	"context"
	"testing"
)

func TestFathomryFeedbackInsertCreditOrders(t *testing.T) {
	for _, acknowledgementFirst := range []bool{false, true} {
		name := "increment-first"
		if acknowledgementFirst {
			name = "ack-first"
		}
		t.Run(name, func(t *testing.T) {
			var wire bytes.Buffer
			decoder := NewDecoder(WithMaxTableCapacity(64), WithDecoderStream(&wire))
			defer decoder.Close()
			if err := decoder.dt.setCapacity(64); err != nil {
				t.Fatal(err)
			}
			if err := decoder.applyInsert("x", "v"); err != nil {
				t.Fatal(err)
			}
			if !acknowledgementFirst {
				decoder.sendInsertCountIncrement()
			}
			if field, err := decoder.DecodeForStreamContext(context.Background(), 4, []byte{2, 0, 0x80}, 1024)(); err != nil || field.Name != "x" {
				t.Fatal("valid dynamic field", field, err)
			}
			if acknowledgementFirst {
				decoder.sendInsertCountIncrement()
			}
			data := wire.Bytes()
			known, acks := uint64(0), 0
			for len(data) > 0 {
				first := data[0]
				prefix := byte(6)
				if first&0x80 != 0 {
					prefix = 7
				}
				value, rest, err := readVarInt(prefix, data)
				if err != nil {
					t.Fatal(err)
				}
				switch {
				case first&0x80 != 0:
					if value != 4 {
						t.Fatal("wrong section acknowledgement", value)
					}
					acks++
					known = max(known, 1)
				case first&0xc0 == 0:
					if value == 0 {
						t.Fatal("zero insert count increment")
					}
					known += value
				default:
					t.Fatal("unexpected cancellation")
				}
				if known > decoder.dt.insertCount {
					t.Fatalf("feedback over-credited peer: known=%d actual=%d wire=%x", known, decoder.dt.insertCount, wire.Bytes())
				}
				data = rest
			}
			if acks != 1 || known != 1 {
				t.Fatal("feedback lost acknowledged section or insertion", acks, known)
			}
		})
	}
}
