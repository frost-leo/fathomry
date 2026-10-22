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

package declaration

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"os"
	"testing"
)

func FuzzParse(f *testing.F) {
	raw, err := os.ReadFile("testdata/health.bundle.json")
	if err != nil {
		f.Fatal(err)
	}
	for _, seed := range [][]byte{raw, []byte("{}"), []byte(`{"format":"bad"}`), []byte{0xff}, []byte(`{"a":1,"a":2}`)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 256<<10 {
			t.Skip("bounded fuzz corpus")
		}
		bundle, err := Parse(context.Background(), input)
		if err != nil {
			if bundle != nil {
				t.Fatal("partial bundle")
			}
			return
		}
		first, err := bundle.BytesCopy()
		if err != nil {
			t.Fatal(err)
		}
		again, err := Parse(context.Background(), first)
		if err != nil {
			t.Fatal("accepted export rejected", err)
		}
		second, _ := again.BytesCopy()
		if !bytes.Equal(first, second) {
			t.Fatal("non-idempotent normalization")
		}
		report, err := Check(context.Background(), bundle, again)
		if err != nil || !report.Passed {
			t.Fatal("identical normalized documents conflict", err)
		}
	})
}

func FuzzCompile(f *testing.F) {
	seed, err := json.Marshal(fixtureDeclaration(f))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 256<<10 {
			t.Skip("bounded fuzz corpus")
		}
		var declaration Declaration
		if err := json.Unmarshal(input, &declaration, json.RejectUnknownMembers(true)); err != nil {
			return
		}
		bundle, err := Compile(context.Background(), declaration)
		if err != nil {
			if bundle != nil {
				t.Fatal("partial compilation")
			}
			return
		}
		raw, err := bundle.BytesCopy()
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := Parse(context.Background(), raw)
		if err != nil {
			t.Fatal("compiled export rejected", err)
		}
		roundTrip, _ := parsed.BytesCopy()
		if !bytes.Equal(raw, roundTrip) {
			t.Fatal("compiled export changed on parsing")
		}
	})
}
