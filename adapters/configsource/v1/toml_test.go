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
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/pelletier/go-toml/v2"
)

func TestTOML(t *testing.T) {
	type row struct {
		Name   string `json:"name"`
		Values []int  `json:"values"`
	}
	type model struct {
		Precise float32           `json:"precise"`
		Count   int64             `json:"count"`
		Nested  map[string]string `json:"nested"`
		Rows    []row             `json:"rows"`
	}
	t.Run("original_numbers_and_complete_table_structure", func(t *testing.T) {
		raw := "precise=1.0000000596046448\ncount=9_007_199_254_740_993\n[nested]\n'a.b'='literal'\n[[rows]]\nname='first'\nvalues=[0x10,0o10,0b10]\n[[rows]]\nname='second'\nvalues=[]\n"
		value, err := Prepare(context.Background(), Schema[model]{Version: 1}, []Layer{{Base, TOML, []byte(raw)}})
		if err != nil {
			t.Fatal(err)
		}
		got, _ := value.ValueCopy()
		if math.Float32bits(got.Precise) != math.Float32bits(math.Nextafter32(1, 2)) || got.Count != 9007199254740993 || got.Nested["a.b"] != "literal" || len(got.Rows) != 2 || !reflect.DeepEqual(got.Rows[0].Values, []int{16, 8, 2}) {
			t.Fatalf("original TOML values changed: %+v", got)
		}
	})
	t.Run("native_structure_comparison", func(t *testing.T) {
		for _, raw := range []string{
			"", "# no overrides\n", "[empty]\n", "[a.b]\nc=1\n[a]\nd=2",
			"[[fruit]]\nname='apple'\n[fruit.physical]\ncolor='red'\n[[fruit.varieties]]\nname='red'\n[[fruit.varieties]]\nname='green'\n[[fruit]]\nname='banana'\n[[fruit.varieties]]\nname='plantain'",
			"a={b.c=1, d=[{e=true},{e=false}]}\n",
			"x='''[[[[literal]]]]'''\ny=\"\\\"quoted\\\"\"",
		} {
			got, err := parseTOML([]byte(raw))
			if err != nil {
				t.Fatal(raw, err)
			}
			var native map[string]any
			if err := toml.Unmarshal([]byte(raw), &native); err != nil {
				t.Fatal(err)
			}
			if native == nil {
				native = map[string]any{}
			}
			left, _ := json.Marshal(got)
			right, _ := json.Marshal(native)
			if string(left) != string(right) {
				t.Fatalf("native structure differs: %s / %s", left, right)
			}
		}
	})
	t.Run("reject_original_layer_before_overlay", func(t *testing.T) {
		for _, raw := range []string{
			"count=1\ncount=2", "[nested]\na='x'\n[nested]\nb='y'",
			"count=9223372036854775808", "precise=nan", "precise=+inf",
			"count=1979-05-27", "count=1.0", "count='1'", "Count=1",
			"rows=[{name='x',name='y'}]", "nested={a='x'}\n[nested]\nb='y'",
			"precise=1e999", "count=1\ntrailing",
		} {
			value, err := Prepare(context.Background(), Schema[model]{Version: 1}, []Layer{{Base, TOML, []byte(raw)}, {Local, JSON, []byte(`{"count":1,"precise":1}`)}})
			if err == nil || value.state != nil {
				t.Fatal("invalid original TOML accepted", raw)
			}
		}
	})
	t.Run("empty_and_bounds", func(t *testing.T) {
		for _, encoding := range []Encoding{YAML, JSON, TOML} {
			_, err := Prepare(context.Background(), Schema[model]{Version: 1}, []Layer{{Base, encoding, nil}})
			if (err == nil) != (encoding == TOML) {
				t.Fatal("empty profile changed", encoding, err)
			}
		}
		if _, err := parseTOML([]byte("x=" + strings.Repeat("[", MaxDepth+1) + "0" + strings.Repeat("]", MaxDepth+1))); !errors.Is(err, ErrLimit) {
			t.Fatal("nesting guard missing", err)
		}
		if _, err := parseTOML([]byte("x=[" + strings.Repeat("1,", MaxNodes) + "]")); !errors.Is(err, ErrLimit) {
			t.Fatal("node guard missing", err)
		}
	})
}

func FuzzTOML(f *testing.F) {
	for _, raw := range []string{"", "value=1", "[[rows]]\nname='one'\n[[rows]]\nname='two'", "a={b=[true,false]}", "value=nan"} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 8192 {
			return
		}
		got, err := parse([]byte(raw), TOML)
		if err != nil {
			return
		}
		var native map[string]any
		if err := toml.Unmarshal([]byte(raw), &native); err != nil {
			t.Fatal("native syntax refusal was lost")
		}
		if native == nil {
			native = map[string]any{}
		}
		left, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		right, err := json.Marshal(native)
		if err != nil {
			return
		}
		var decodedLeft, decodedRight any
		if json.Unmarshal(left, &decodedLeft) != nil || json.Unmarshal(right, &decodedRight) != nil || !reflect.DeepEqual(decodedLeft, decodedRight) {
			t.Fatal("native structural interpretation changed")
		}
	})
}
