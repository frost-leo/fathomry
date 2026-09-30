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
	"reflect"
	"strings"
	"unicode/utf8"
)

const MaxVariables = 64

// Variable is an explicitly captured value, not authority to read the process
// environment. Unset values still undergo declaration admission, but contribute
// no override. JSON=false means literal text for string or pointer-to-string;
// JSON=true selects strict JSON values. Paths address struct fields only.
type Variable struct {
	Path    string
	Value   string
	Present bool
	JSON    bool
}

// BindVariables builds a strict Variables layer without process I/O. Whole-map JSON
// bindings overlay recursively during Prepare. Duplicate and ancestor-overlapping
// paths reject even when unset; individual map keys and list indices are refused.
// Aggregate values and the resulting document are bounded to one MiB.
func BindVariables[T any](variables []Variable) (Layer, error) {
	if len(variables) > MaxVariables {
		return Layer{}, fail(ErrLimit, "variables")
	}
	root, err := compile(reflect.TypeFor[T]())
	if err != nil {
		return Layer{}, err
	}
	if root.kind.Kind() != reflect.Struct {
		return Layer{}, fail(ErrInput, "variables")
	}
	object := map[string]any{}
	paths := make([][]string, 0, len(variables))
	budget := newBudget()
	bytesUsed := 0
	for _, variable := range variables {
		tokens, target, err := variablePath(root, variable.Path)
		if err != nil {
			return Layer{}, err
		}
		for _, previous := range paths {
			overlap := min(len(previous), len(tokens))
			equal := true
			for index := range overlap {
				if previous[index] != tokens[index] {
					equal = false
					break
				}
			}
			if equal {
				return Layer{}, fail(ErrInput, "variable_overlap")
			}
		}
		paths = append(paths, tokens)
		leaf := target
		for leaf.kind.Kind() == reflect.Pointer {
			leaf = leaf.element
		}
		if !variable.JSON && leaf.kind.Kind() != reflect.String {
			return Layer{}, fail(ErrInput, "variable_text")
		}
		if !variable.Present {
			continue
		}
		if len(variable.Value) > MaxDocumentBytes-bytesUsed {
			return Layer{}, fail(ErrLimit, "variables")
		}
		bytesUsed += len(variable.Value)
		if !utf8.ValidString(variable.Value) {
			return Layer{}, fail(ErrDecode, "variable")
		}
		var value any = variable.Value
		if variable.JSON {
			decoder := jsontext.NewDecoder(bytes.NewBufferString(variable.Value), jsontext.AllowDuplicateNames(false), jsontext.AllowInvalidUTF8(false))
			value, err = jsonValue(decoder, &budget, 0)
			if err != nil {
				return Layer{}, fail(ErrDecode, "variable", err)
			}
			if _, err := decoder.ReadToken(); !errors.Is(err, io.EOF) {
				return Layer{}, fail(ErrDecode, "variable", err)
			}
		}
		checked := newBudget()
		if _, err := toGo(target, value, &checked, 0); err != nil {
			return Layer{}, err
		}
		current := object
		for _, token := range tokens[:len(tokens)-1] {
			child, exists := current[token].(map[string]any)
			if !exists {
				child = map[string]any{}
				current[token] = child
			}
			current = child
		}
		current[tokens[len(tokens)-1]] = value
	}
	checked := newBudget()
	if _, err := toGo(root, object, &checked, 0); err != nil {
		return Layer{}, err
	}
	raw, err := json.Marshal(object)
	if err != nil {
		return Layer{}, fail(ErrDecode, "variables", err)
	}
	if len(raw) > MaxDocumentBytes {
		return Layer{}, fail(ErrLimit, "variables")
	}
	return Layer{Kind: Variables, Encoding: JSON, Content: raw}, nil
}
func variablePath(root *shape, path string) ([]string, *shape, error) {
	if len(path) == 0 || len(path) > 4096 || path[0] != '/' || !utf8.ValidString(path) {
		return nil, nil, fail(ErrInput, "variable_path")
	}
	tokens := strings.Split(path[1:], "/")
	if len(tokens) > MaxDepth {
		return nil, nil, fail(ErrLimit, "variable_path")
	}
	selected := root
	for index, token := range tokens {
		for offset := 0; offset < len(token); offset++ {
			if token[offset] == '~' {
				if offset+1 >= len(token) || token[offset+1] != '0' && token[offset+1] != '1' {
					return nil, nil, fail(ErrInput, "variable_path")
				}
				offset++
			}
		}
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		tokens[index] = token
		for selected.kind.Kind() == reflect.Pointer {
			selected = selected.element
		}
		if selected.kind.Kind() != reflect.Struct {
			return nil, nil, fail(ErrInput, "variable_path")
		}
		field, exists := selected.names[token]
		if !exists {
			return nil, nil, fail(ErrInput, "variable_path")
		}
		selected = selected.fields[field].shape
	}
	return tokens, selected, nil
}
