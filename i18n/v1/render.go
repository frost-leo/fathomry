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

package i18n

import (
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/feature/plural"
	"golang.org/x/text/language"
)

// Capture the Rules value, not the exported mutable pointer. Its internal tables
// are unexported; later normal reassignment of plural.Cardinal cannot change us.
var cardinalRules = *plural.Cardinal

type segment struct{ literal, name string }

func compile(pattern string, arguments []Parameter, cardinal bool) ([]segment, error) {
	known := make(map[string]bool, len(arguments)+1)
	for _, argument := range arguments {
		known[argument.Name] = false
	}
	if cardinal {
		known["count"] = false
	}
	var literal strings.Builder
	var segments []segment
	references := 0
	flush := func() {
		if literal.Len() != 0 {
			segments = append(segments, segment{literal: literal.String()})
			literal.Reset()
		}
	}
	for index := 0; index < len(pattern); {
		char := pattern[index]
		if char != '{' && char != '}' {
			literal.WriteByte(char)
			index++
			continue
		}
		if index+1 < len(pattern) && pattern[index+1] == char {
			literal.WriteByte(char)
			index += 2
			continue
		}
		if char == '}' {
			return nil, reject(ErrResource)
		}
		end := strings.IndexByte(pattern[index+1:], '}')
		if end < 0 {
			return nil, reject(ErrResource)
		}
		end += index + 1
		name := pattern[index+1 : end]
		if _, exists := known[name]; !exists {
			return nil, reject(ErrResource)
		}
		if references >= MaxReferences {
			return nil, reject(ErrLimit)
		}
		references++
		known[name] = true
		flush()
		segments = append(segments, segment{name: name})
		index = end + 1
	}
	flush()
	for _, argument := range arguments {
		if !known[argument.Name] {
			return nil, reject(ErrResource)
		}
	}
	if len(segments) > MaxSegments {
		return nil, reject(ErrLimit)
	}
	return segments, nil
}

// Argument supplies exactly one declared name and builtin string/int64/uint64/bool.
// Named types, int, floats, nil, pointers and formatter objects are rejected
// without calling methods. A slice permits duplicate detection before map loss.
type Argument struct {
	Name  string
	Value any
}

// Rendered contains complete plain text or is zero on error. Category is the
// actual-resource rule result; Variant is the authored form used. Noncardinal
// messages use Other. FormFallback is separate from whole-message fallback.
type Rendered struct {
	Text         string
	Category     Form
	Variant      Form
	FormFallback bool
}

// Render validates the exact argument set and count, then charges every expansion
// before allocating output. Count is borrowed only during this call and copied
// once; nil means absent, not zero. Cardinal messages require it, other messages
// reject it. The full uint64 range selects and displays without floating point.
// Caller inputs must not change concurrently. No partial text or callbacks occur.
func (selection Selection) Render(arguments []Argument, count *uint64) (Rendered, error) {
	if selection.item == nil {
		return Rendered{}, reject(ErrCatalog)
	}
	definition := &selection.item.definition
	if len(arguments) > MaxArguments {
		return Rendered{}, reject(ErrLimit)
	}
	if len(arguments) != len(definition.Arguments) || definition.Cardinal != (count != nil) {
		return Rendered{}, reject(ErrArguments)
	}
	var number uint64
	if count != nil {
		number = *count
	}
	values := make(map[string]string, len(arguments)+1)
	total := 0
	for _, argument := range arguments {
		if !identifier(argument.Name, 64) || argument.Name == "count" {
			return Rendered{}, reject(ErrArguments)
		}
		if _, exists := values[argument.Name]; exists {
			return Rendered{}, reject(ErrArguments)
		}
		index := -1
		for position, parameter := range definition.Arguments {
			if parameter.Name == argument.Name {
				index = position
				break
			}
		}
		if index < 0 {
			return Rendered{}, reject(ErrArguments)
		}
		value, err := scalar(argument.Value, definition.Arguments[index].Kind)
		if err != nil {
			return Rendered{}, err
		}
		if len(value) > MaxArgumentBytes-total {
			return Rendered{}, reject(ErrLimit)
		}
		total += len(value)
		values[argument.Name] = value
	}
	category := Other
	if count != nil {
		category = cardinalForm(selection.item.tag, number)
		values["count"] = strconv.FormatUint(number, 10)
	}
	variant := category
	program, exists := selection.item.programs[category]
	if !exists {
		variant, program = Other, selection.item.programs[Other]
	}
	total = 0
	for _, segment := range program {
		value := segment.literal
		if segment.name != "" {
			value = values[segment.name]
		}
		if len(value) > MaxOutputBytes-total {
			return Rendered{}, reject(ErrLimit)
		}
		total += len(value)
	}
	var output strings.Builder
	output.Grow(total)
	for _, segment := range program {
		if segment.name != "" {
			output.WriteString(values[segment.name])
		} else {
			output.WriteString(segment.literal)
		}
	}
	return Rendered{Text: output.String(), Category: category, Variant: variant, FormFallback: category != variant}, nil
}

func scalar(value any, kind string) (string, error) {
	switch value := value.(type) {
	case string:
		if kind != "string" {
			break
		}
		if len(value) > MaxStringBytes {
			return "", reject(ErrLimit)
		}
		if !utf8.ValidString(value) {
			break
		}
		return value, nil
	case int64:
		if kind == "int64" {
			return strconv.FormatInt(value, 10), nil
		}
	case uint64:
		if kind == "uint64" {
			return strconv.FormatUint(value, 10), nil
		}
	case bool:
		if kind == "bool" {
			return strconv.FormatBool(value), nil
		}
	}
	return "", reject(ErrArguments)
}

func cardinalForm(tag language.Tag, number uint64) Form {
	// In the pinned interpreter all exact tests are below 10^7 and every
	// modulus divides 10^7. Preserve the large/nonzero class as well as residues.
	representative := number
	if number >= 10000000 {
		representative = 10000000 + number%10000000
	}
	switch cardinalRules.MatchPlural(tag, int(representative), 0, 0, 0, 0) {
	case plural.Zero:
		return Zero
	case plural.One:
		return One
	case plural.Two:
		return Two
	case plural.Few:
		return Few
	case plural.Many:
		return Many
	default:
		return Other
	}
}
