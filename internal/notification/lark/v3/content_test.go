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

package lark

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeChartTableAndExactJSON(t *testing.T) {
	for _, kind := range []string{"line", "bar", "pie", "area", "scatter"} {
		spec := jsonValue(t, `{"type":"`+kind+`","data":{"values":[{"label":"月份","value":9007199254740993}]},"xField":"label","yField":"value","legends":{"visible":true},"tooltip":{"visible":true}}`)
		chart, err := Chart("chart_one", spec)
		if err != nil {
			t.Fatal(err)
		}
		table, err := Table("table_one", jsonValue(t, `[{"name":"label","display_name":"Label","data_type":"text"}]`), jsonValue(t, `[{"label":"月份"}]`))
		if err != nil {
			t.Fatal(err)
		}
		content, err := ComposeCard("Synthetic", chart, table)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(content.JSON().Bytes(), []byte("9007199254740993")) || !bytes.Contains(content.JSON().Bytes(), []byte(`"tag":"table"`)) {
			t.Fatal("native components or exact numbers lost")
		}
		output := content.JSON().Bytes()
		output[0] = 'X'
		if !json.Valid(content.JSON().Bytes()) {
			t.Fatal("returned bytes alias frozen content")
		}
	}
}
func TestRejectMalformedAmbiguousAndOversizedJSON(t *testing.T) {
	for _, raw := range []string{"", `null`, `42`, `{"x":1,"x":2}`, `{"x":1,"\u0078":2}`, `{}{}`, `{"x":"` + string([]byte{0xff}) + `"}`, strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34), `["` + strings.Repeat("x", maxContentBytes) + `"]`} {
		if _, err := NewJSON([]byte(raw)); err == nil {
			t.Fatal("invalid or unbounded JSON accepted")
		}
	}
	if _, err := Card([]byte(`{"elements":[]}`)); err == nil {
		t.Fatal("legacy card silently called schema 2")
	}
	if _, err := NewContent("unknown", []byte("{}")); err == nil {
		t.Fatal("unknown message type accepted")
	}
	if _, err := Text(""); err == nil {
		t.Fatal("empty text accepted")
	}
}
func TestFrozenContentAndTemplates(t *testing.T) {
	data := []byte(`{"text":"before"}`)
	content, err := NewContent("text", data)
	if err != nil {
		t.Fatal(err)
	}
	copy(data, []byte(`{"text":"after!"}`))
	if !bytes.Contains(content.JSON().Bytes(), []byte("before")) {
		t.Fatal("input aliases content")
	}
	vars := jsonValue(t, `{"series":[1,2,3],"title":"Synthetic"}`)
	template, err := Template("template_fixture", "1.0", vars)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(template.JSON().Bytes(), []byte(`"template_version_name":"1.0"`)) {
		t.Fatal("template version lost")
	}
	if _, err = CardInstance("card_fixture"); err != nil {
		t.Fatal(err)
	}
}
func FuzzNativeJSON(f *testing.F) {
	f.Add([]byte(`{"schema":"2.0","body":{"elements":[]}}`))
	f.Add([]byte(`{"duplicate":0,"duplicate":1}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxContentBytes+1 {
			return
		}
		value, err := NewJSON(data)
		if err == nil && !json.Valid(value.Bytes()) {
			t.Fatal("invalid JSON accepted")
		}
	})
}
