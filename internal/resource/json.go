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
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"io"
)

// JSONVariables borrows generated JSON bytes for the Variables layer. Ordinary
// Layer literals keep their existing YAML semantics, including JSON-as-YAML.
// This changes only decoding: schema checks, precedence and merging are shared.
func JSONVariables(content []byte) Layer {
	return Layer{Kind: Variables, Content: content, jsonVariables: true}
}

func (layer Layer) mapping() (map[string]any, error) {
	if !layer.jsonVariables {
		return parseMapping(layer.Content)
	}
	if layer.Kind != Variables {
		return nil, errors.New("source: JSON variables require Variables layer")
	}
	return parseJSONMapping(layer.Content)
}

func parseJSONMapping(data []byte) (map[string]any, error) {
	if len(data) == 0 || len(data) > 1<<20 {
		return nil, errors.New("source: document size outside supported bounds")
	}
	// Strict native tokens reject invalid Unicode and duplicate decoded names,
	// without YAML normalization or numeric coercion. No second merger is used.
	decoder := jsontext.NewDecoder(bytes.NewReader(data))
	value, err := jsonValue(decoder, 0)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.ReadToken(); err != io.EOF {
		return nil, errors.New("source: exactly one document required")
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("source: mapping required")
	}
	return object, nil
}

func jsonValue(decoder *jsontext.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("source: configuration nesting limit exceeded")
	}
	token, err := decoder.ReadToken()
	if err != nil {
		return nil, err
	}
	switch token.Kind() {
	case '{':
		object := make(map[string]any)
		for decoder.PeekKind() != '}' {
			name, err := decoder.ReadToken()
			if err != nil {
				return nil, err
			}
			if name.Kind() != '"' {
				return nil, errors.New("source: string name required")
			}
			key := name.String()
			value, err := jsonValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		_, err := decoder.ReadToken()
		return object, err
	case '[':
		items := make([]any, 0)
		for decoder.PeekKind() != ']' {
			value, err := jsonValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			items = append(items, value)
		}
		_, err := decoder.ReadToken()
		return items, err
	case '"':
		return token.String(), nil
	case '0':
		return json.Number(token.String()), nil
	case 't', 'f':
		return token.Bool(), nil
	case 'n':
		return nil, nil
	default:
		return nil, errors.New("source: unexpected JSON delimiter")
	}
}
