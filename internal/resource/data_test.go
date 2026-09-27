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

package resource

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestIdentityNeutralPreparationSharesStrictEngine(t *testing.T) {
	type value struct {
		Format uint32         `json:"format"`
		Values map[string]int `json:"values"`
	}
	schema := Schema[value]{Format: 1, Defaults: value{Format: 1, Values: map[string]int{"default": 1}}}
	prepared, err := PrepareData(schema, []Layer{{Kind: Base, Content: []byte("format: 1\nvalues: {selected: 2}")}})
	if err != nil {
		t.Fatal(err)
	}
	description := prepared.Description()
	if description.Identity != (Identity{}) || description.Revision == "" {
		t.Fatal("fictitious identity or missing preparation revision")
	}
	copy, err := prepared.ValueCopy()
	if err != nil {
		t.Fatal(err)
	}
	copy.Values["default"] = 9
	schema.Defaults.Values["default"] = 8
	second, _ := prepared.ValueCopy()
	if second.Values["default"] != 1 || second.Values["selected"] != 2 {
		t.Fatal("data aliases defaults or copies")
	}
	if _, err := json.Marshal(prepared); err == nil {
		t.Fatal("prepared data serialized")
	}
	if err := json.Unmarshal([]byte("{}"), new(PreparedData[value])); err == nil {
		t.Fatal("prepared data reconstructed")
	}
	if _, err := Prepare(schema, Input{Format: 1}); !errors.Is(err, ErrConfiguration) {
		t.Fatal("named resource identity checks relaxed", err)
	}
	if _, err := PrepareData(schema, []Layer{{Kind: Base, Content: []byte("format: 1\nunknown: 1")}, {Kind: Local, Content: []byte("format: 1")}}); !errors.Is(err, ErrConfiguration) {
		t.Fatal("neutral seam lost every-layer validation", err)
	}
}
func FuzzDocumentFormat(f *testing.F) {
	f.Add([]byte("format: 1"))
	f.Add([]byte("format: 2"))
	f.Add([]byte("{\"format\":1,\"format\":1}"))
	f.Add([]byte("format: 1e0"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20+1 {
			t.Skip()
		}
		err := CheckDocumentFormat(raw, 1)
		if err == nil {
			mapping, err := parseMapping(raw)
			if err != nil {
				t.Fatal("format accepted rejected syntax", err)
			}
			if mapping["format"] != json.Number("1") {
				t.Fatal("format normalized unexpected integer spelling")
			}
		}
	})
}
