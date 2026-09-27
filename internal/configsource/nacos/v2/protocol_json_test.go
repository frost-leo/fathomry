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

package nacos

import (
	"strings"
	"testing"
)

func TestProtocolJSONUnicodeAndBoundedStructure(t *testing.T) {
	for _, raw := range []string{
		`{"content":"\ud83d\ude00"}`, `{"content":"\ufffd"}`, `{"content":"\\ud800"}`,
		"{\"content\":\"\ufffd\u0085\uffff\"}", `{"":true}`,
	} {
		if err := validateJSON([]byte(raw)); err != nil {
			t.Fatalf("valid protocol JSON refused: %q: %v", raw, err)
		}
	}
	for _, raw := range []string{
		`{"content":"\ud800"}`, `{"content":"\udfff"}`, `{"content":"\ud800\u0041"}`,
		`{"\ud800":"text"}`, `{"content":"\u12"}`, `{"a":1,"\u0061":2}`, `{"":1,"":2}`,
		`{} {}`, `{"a":[}`, "{\"content\":\"\xff\"}",
	} {
		if err := validateJSON([]byte(raw)); err == nil {
			t.Fatalf("invalid protocol JSON admitted: %q", raw)
		}
	}
	for _, depth := range []int{64, 65} {
		raw := strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth)
		if err := validateJSON([]byte(raw)); (err == nil) != (depth == 64) {
			t.Fatal("depth profile changed", depth, err)
		}
	}
	for _, nodes := range []int{32768, 32769} {
		raw := "[" + strings.Repeat("0,", nodes-2) + "0]"
		if err := validateJSON([]byte(raw)); (err == nil) != (nodes == 32768) {
			t.Fatal("value-node profile changed", nodes, err)
		}
	}
}
