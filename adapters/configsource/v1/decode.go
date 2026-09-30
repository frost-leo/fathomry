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
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"io"
	"unicode/utf8"

	"go.yaml.in/yaml/v3"
)

func parse(raw []byte, encoding Encoding) (map[string]any, error) {
	if len(raw) == 0 || !utf8.Valid(raw) {
		return nil, fail(ErrDecode, "document")
	}
	if len(raw) > MaxDocumentBytes {
		return nil, fail(ErrLimit, "document")
	}
	budget := newBudget()
	var value any
	var err error
	switch encoding {
	case JSON:
		decoder := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowDuplicateNames(false), jsontext.AllowInvalidUTF8(false))
		value, err = jsonValue(decoder, &budget, 0)
		if err == nil {
			_, trailing := decoder.ReadToken()
			if !errors.Is(trailing, io.EOF) {
				err = fail(ErrDecode, "trailing", trailing)
			}
		}
	case YAML:
		decoder := yaml.NewDecoder(bytes.NewReader(raw))
		var document, extra yaml.Node
		if err = decoder.Decode(&document); err != nil {
			break
		}
		if trailing := decoder.Decode(&extra); !errors.Is(trailing, io.EOF) {
			err = fail(ErrDecode, "trailing", trailing)
			break
		}
		if len(document.Content) != 1 {
			err = fail(ErrDecode, "document")
			break
		}
		value, err = yamlValue(document.Content[0], indexSource(raw), &budget, 0)
	default:
		return nil, fail(ErrInput, "encoding")
	}
	if err != nil {
		return nil, fail(ErrDecode, "document", err)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fail(ErrDecode, "mapping")
	}
	return object, nil
}

func jsonValue(decoder *jsontext.Decoder, budget *valueBudget, depth int) (any, error) {
	if err := budget.take(0, depth); err != nil {
		return nil, err
	}
	token, err := decoder.ReadToken()
	if err != nil {
		return nil, err
	}
	switch token.Kind() {
	case 'n':
		return nil, nil
	case 't', 'f':
		return token.Bool(), nil
	case '"':
		return token.String(), nil
	case '0':
		return json.Number(token.String()), nil
	case '{':
		object := map[string]any{}
		for decoder.PeekKind() != '}' {
			keyToken, err := decoder.ReadToken()
			if err != nil {
				return nil, err
			}
			key := keyToken.String()
			if err := budget.take(len(key), depth+1); err != nil {
				return nil, err
			}
			value, err := jsonValue(decoder, budget, depth+1)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		_, err := decoder.ReadToken()
		return object, err
	case '[':
		items := []any{}
		for decoder.PeekKind() != ']' {
			value, err := jsonValue(decoder, budget, depth+1)
			if err != nil {
				return nil, err
			}
			items = append(items, value)
		}
		_, err := decoder.ReadToken()
		return items, err
	}
	return nil, fail(ErrDecode, "syntax")
}

type sourceIndex struct {
	text  []rune
	lines []int
}

func indexSource(raw []byte) sourceIndex {
	if !bytes.ContainsRune(raw, '!') {
		return sourceIndex{}
	}
	text := []rune(string(bytes.TrimPrefix(raw, []byte{0xEF, 0xBB, 0xBF})))
	lines := []int{0}
	for index := 0; index < len(text); index++ {
		switch text[index] {
		case '\r':
			if index+1 < len(text) && text[index+1] == '\n' {
				index++
			}
		case '\n', '\u0085', '\u2028', '\u2029':
		default:
			continue
		}
		lines = append(lines, index+1)
	}
	return sourceIndex{text, lines}
}
func (source sourceIndex) tagged(node *yaml.Node) bool {
	if node.Style&yaml.TaggedStyle != 0 {
		return true
	}
	if node.Line < 1 || node.Line > len(source.lines) || node.Column < 1 {
		return false
	}
	offset := source.lines[node.Line-1] + node.Column - 1
	return offset < len(source.text) && source.text[offset] == '!'
}
func yamlValue(node *yaml.Node, source sourceIndex, budget *valueBudget, depth int) (any, error) {
	if err := budget.take(0, depth); err != nil {
		return nil, err
	}
	if node.Anchor != "" || node.Alias != nil || source.tagged(node) {
		return nil, fail(ErrDecode, "yaml_feature")
	}
	switch node.Kind {
	case yaml.MappingNode:
		object := map[string]any{}
		for index := 0; index < len(node.Content); index += 2 {
			key := node.Content[index]
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || key.Anchor != "" || source.tagged(key) {
				return nil, fail(ErrDecode, "key")
			}
			if _, exists := object[key.Value]; exists {
				return nil, fail(ErrDecode, "duplicate_key")
			}
			if err := budget.take(len(key.Value), depth+1); err != nil {
				return nil, err
			}
			value, err := yamlValue(node.Content[index+1], source, budget, depth+1)
			if err != nil {
				return nil, err
			}
			object[key.Value] = value
		}
		return object, nil
	case yaml.SequenceNode:
		if len(node.Content) > budget.nodes {
			return nil, fail(ErrLimit, "structure")
		}
		values := make([]any, len(node.Content))
		for index, child := range node.Content {
			value, err := yamlValue(child, source, budget, depth+1)
			if err != nil {
				return nil, err
			}
			values[index] = value
		}
		return values, nil
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!null":
			return nil, nil
		case "!!str":
			return node.Value, nil
		case "!!bool":
			var result bool
			if err := node.Decode(&result); err != nil {
				return nil, err
			}
			return result, nil
		case "!!int", "!!float":
			decoder := json.NewDecoder(bytes.NewBufferString(node.Value))
			decoder.UseNumber()
			token, err := decoder.Token()
			if err != nil {
				return nil, fail(ErrDecode, "number", err)
			}
			number, ok := token.(json.Number)
			if !ok || !json.Valid([]byte(node.Value)) {
				return nil, fail(ErrDecode, "number")
			}
			return number, nil
		}
	}
	return nil, fail(ErrDecode, "yaml_feature")
}
