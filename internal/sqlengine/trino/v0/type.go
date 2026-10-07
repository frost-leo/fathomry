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

package trino

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

type semanticType struct {
	name     string
	numbers  []int64
	children []semanticType
	fields   []*string
}

type typeParser struct {
	text  string
	pos   int
	nodes int
}

func matchingType(text string, sig signature) bool {
	if len(text) == 0 || len(text) > 4096 || !utf8.ValidString(text) {
		return false
	}
	parser := typeParser{text: text}
	parsed, ok := parser.parse(0)
	parser.space()
	return ok && parser.pos == len(text) && parsed.matches(sig)
}

func (p *typeParser) space() {
	for p.pos < len(p.text) && strings.ContainsRune(" \t\r\n\f", rune(p.text[p.pos])) {
		p.pos++
	}
}

func (p *typeParser) take(char byte) bool {
	p.space()
	if p.pos == len(p.text) || p.text[p.pos] != char {
		return false
	}
	p.pos++
	return true
}

func (p *typeParser) word() string {
	p.space()
	start := p.pos
	for p.pos < len(p.text) {
		char := p.text[p.pos]
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char == '_' ||
			p.pos > start && char >= '0' && char <= '9') {
			break
		}
		p.pos++
	}
	return strings.ToLower(p.text[start:p.pos])
}

func (p *typeParser) keyword(word string) bool {
	old := p.pos
	if p.word() == word {
		return true
	}
	p.pos = old
	return false
}

func (p *typeParser) number() (int64, bool) {
	p.space()
	start := p.pos
	for p.pos < len(p.text) && p.text[p.pos] >= '0' && p.text[p.pos] <= '9' {
		p.pos++
	}
	number, err := strconv.ParseInt(p.text[start:p.pos], 10, 64)
	return number, err == nil
}

func (p *typeParser) field() (string, bool) {
	if !p.take('"') {
		name := p.word()
		return name, name != ""
	}
	var name strings.Builder
	for p.pos < len(p.text) {
		char := p.text[p.pos]
		p.pos++
		if char == '"' {
			if p.pos == len(p.text) || p.text[p.pos] != '"' {
				return name.String(), true
			}
			p.pos++
		}
		name.WriteByte(char)
	}
	return "", false
}

func (p *typeParser) parse(depth int) (semanticType, bool) {
	p.nodes++
	if depth > 8 || p.nodes > 1024 {
		return semanticType{}, false
	}
	value := semanticType{name: p.word()}
	switch value.name {
	case "int":
		value.name = "integer"
	case "double":
		p.keyword("precision")
	case "character":
		value.name = "char"
		if p.keyword("varying") {
			value.name = "varchar"
		}
	}
	switch value.name {
	case "array", "map", "row":
		if !p.take('(') {
			return value, false
		}
		for {
			if len(value.children) >= 256 {
				return value, false
			}
			start, nodes := p.pos, p.nodes
			nested, ok := p.parse(depth + 1)
			var field *string
			p.space()
			if value.name == "row" && (!ok || p.pos == len(p.text) || p.text[p.pos] != ',' && p.text[p.pos] != ')') {
				p.pos, p.nodes = start, nodes
				name, named := p.field()
				if !named {
					return value, false
				}
				field = &name
				nested, ok = p.parse(depth + 1)
			}
			if !ok {
				return value, false
			}
			value.children = append(value.children, nested)
			if value.name == "row" {
				value.fields = append(value.fields, field)
			}
			if !p.take(',') {
				break
			}
		}
		return value, p.take(')') && (value.name == "row" || value.name == "array" && len(value.children) == 1 ||
			value.name == "map" && len(value.children) == 2)
	case "decimal", "char", "varchar", "time", "timestamp":
		switch value.name {
		case "decimal":
			value.numbers = []int64{38, 0}
		case "char":
			value.numbers = []int64{1}
		case "varchar":
			value.numbers = []int64{2147483647}
		default:
			value.numbers = []int64{3}
		}
		if p.take('(') {
			number, ok := p.number()
			if !ok {
				return value, false
			}
			value.numbers[0] = number
			if value.name == "decimal" && p.take(',') {
				value.numbers[1], ok = p.number()
				if !ok {
					return value, false
				}
			}
			if !p.take(')') {
				return value, false
			}
		}
		if value.name == "time" || value.name == "timestamp" {
			if p.keyword("with") {
				if !p.keyword("time") || !p.keyword("zone") {
					return value, false
				}
				value.name += " with time zone"
			} else if p.keyword("without") && (!p.keyword("time") || !p.keyword("zone")) {
				return value, false
			}
		}
	case "interval":
		unit := p.word()
		if !p.keyword("to") {
			return value, false
		}
		end := p.word()
		if !(unit == "year" && end == "month" || unit == "day" && end == "second") {
			return value, false
		}
		value.name += " " + unit + " to " + end
	case "boolean", "tinyint", "smallint", "integer", "bigint", "real", "double", "date", "varbinary",
		"json", "uuid", "ipaddress", "unknown":
	default:
		return value, false
	}
	return value, true
}

func (value semanticType) matches(sig signature) bool {
	if value.name != sig.RawType || len(value.numbers)+len(value.children) != len(sig.Arguments) {
		return false
	}
	if len(value.numbers) != 0 {
		numbers := make([]int64, len(sig.Arguments))
		for index, argument := range sig.Arguments {
			if argument.Kind != "LONG" || string(argument.Value) == "null" || json.Unmarshal(argument.Value, &numbers[index]) != nil {
				return false
			}
		}
		return slices.Equal(value.numbers, numbers)
	}
	for index, nested := range value.children {
		argument := sig.Arguments[index]
		if value.name == "row" {
			var named struct {
				FieldName *struct {
					Name *string `json:"name"`
				} `json:"fieldName"`
			}
			if argument.Kind != "NAMED_TYPE" || json.Unmarshal(argument.Value, &named) != nil {
				return false
			}
			field := value.fields[index]
			if field == nil {
				if named.FieldName != nil {
					return false
				}
			} else if named.FieldName == nil || named.FieldName.Name == nil || *field != *named.FieldName.Name {
				return false
			}
		} else if argument.Kind != "TYPE" {
			return false
		}
		child, err := child(argument)
		if err != nil || !nested.matches(child) {
			return false
		}
	}
	return true
}
