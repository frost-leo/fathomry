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
	"bytes"
	"encoding/json"
	"io"
	"unicode/utf16"
	"unicode/utf8"
)

// Decode only bounded buffers. The token walk preserves duplicate-member evidence
// that ordinary map/struct decoding would erase.
func decode(data []byte, nodes *int) (map[string]any, error) {
	if !utf8.Valid(data) || !validEscapes(data) {
		return nil, reject(ErrResource)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := decodeValue(decoder, 1, nodes)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, reject(ErrResource)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, reject(ErrResource)
	}
	return object, nil
}

func decodeValue(decoder *json.Decoder, depth int, nodes *int) (any, error) {
	if depth > MaxDepth || *nodes >= MaxNodes {
		return nil, reject(ErrLimit)
	}
	*nodes++
	token, err := decoder.Token()
	if err != nil {
		return nil, reject(ErrResource)
	}
	switch value := token.(type) {
	case string:
		if len(value) > MaxTokenBytes {
			return nil, reject(ErrLimit)
		}
		return value, nil
	case bool:
		return value, nil
	case json.Delim:
		switch value {
		case '{':
			object := map[string]any{}
			for decoder.More() {
				if *nodes >= MaxNodes {
					return nil, reject(ErrLimit)
				}
				*nodes++
				token, err := decoder.Token()
				if err != nil {
					return nil, reject(ErrResource)
				}
				key, ok := token.(string)
				if !ok {
					return nil, reject(ErrResource)
				}
				if len(key) > MaxKeyBytes {
					return nil, reject(ErrLimit)
				}
				if _, exists := object[key]; exists {
					return nil, reject(ErrResource)
				}
				child, err := decodeValue(decoder, depth+1, nodes)
				if err != nil {
					return nil, err
				}
				object[key] = child
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return nil, reject(ErrResource)
			}
			return object, nil
		case '[':
			array := []any{}
			for decoder.More() {
				child, err := decodeValue(decoder, depth+1, nodes)
				if err != nil {
					return nil, err
				}
				array = append(array, child)
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return nil, reject(ErrResource)
			}
			return array, nil
		}
	}
	// Null and numbers have no role in this resource schema.
	return nil, reject(ErrResource)
}

// encoding/json replaces unpaired UTF-16 surrogates. Refuse that lossy repair
// before decoding; ordinary syntax validation remains the decoder's job.
func validEscapes(data []byte) bool {
	quoted := false
	for index := 0; index < len(data); index++ {
		if data[index] == '"' {
			quoted = !quoted
			continue
		}
		if !quoted || data[index] != '\\' {
			continue
		}
		index++
		if index >= len(data) {
			return false
		}
		if data[index] != 'u' {
			continue
		}
		code, ok := hex16(data, index+1)
		if !ok {
			return false
		}
		index += 4
		if code >= 0xdc00 && code <= 0xdfff {
			return false
		}
		if utf16.IsSurrogate(rune(code)) {
			if index+6 >= len(data) || data[index+1] != '\\' || data[index+2] != 'u' {
				return false
			}
			low, ok := hex16(data, index+3)
			if !ok || low < 0xdc00 || low > 0xdfff {
				return false
			}
			index += 6
		}
	}
	return true
}

func hex16(data []byte, start int) (uint16, bool) {
	if start > len(data)-4 {
		return 0, false
	}
	var result uint16
	for _, char := range data[start : start+4] {
		result <<= 4
		switch {
		case char >= '0' && char <= '9':
			result += uint16(char - '0')
		case char >= 'a' && char <= 'f':
			result += uint16(char - 'a' + 10)
		case char >= 'A' && char <= 'F':
			result += uint16(char - 'A' + 10)
		default:
			return 0, false
		}
	}
	return result, true
}

func identifier(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for index := range len(value) {
		char := value[index]
		letter := char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z'
		if !letter && (index == 0 || !(char >= '0' && char <= '9' || char == '_' || char == '.' || char == '-')) {
			return false
		}
	}
	return true
}

func fields(object map[string]any, names ...string) bool {
	if len(object) != len(names) {
		return false
	}
	for _, name := range names {
		if _, exists := object[name]; !exists {
			return false
		}
	}
	return true
}

func textField(object map[string]any, name string, maximum int) (string, bool) {
	value, ok := object[name].(string)
	return value, ok && len(value) > 0 && len(value) <= maximum
}
