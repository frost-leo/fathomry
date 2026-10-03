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
	"bufio"
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

// MaxDotenvBytes bounds explicitly supplied literal variable documents.
const MaxDotenvBytes = 64 << 10

// ParseDotenv decodes an explicit literal variable document without filesystem,
// process-environment access or interpolation. It accepts ASCII environment names,
// single quotes, strict JSON double quotes and comments. Duplicates, export and
// multiline assignments reject. The returned map is caller-owned; values may be
// sensitive. Callers separately authorize which names may affect their program.
func ParseDotenv(raw []byte) (map[string]string, error) {
	if len(raw) > MaxDotenvBytes {
		return nil, fail(ErrLimit, "dotenv")
	}
	if !utf8.Valid(raw) || bytes.ContainsRune(raw, 0) {
		return nil, fail(ErrDecode, "dotenv")
	}
	result := map[string]string{}
	lines := bufio.NewScanner(bytes.NewReader(raw))
	// Scanner needs room to detect EOF after a maximum-size final line.
	lines.Buffer(make([]byte, 1024), MaxDotenvBytes+1)
	for lines.Scan() {
		line := strings.TrimSpace(lines.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, text, present := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !present || !dotenvKey(key) || len(result) == MaxVariables {
			return nil, fail(ErrInput, "dotenv")
		}
		if _, exists := result[key]; exists {
			return nil, fail(ErrInput, "dotenv_duplicate")
		}
		value, err := dotenvLiteral(strings.TrimSpace(text))
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	if err := lines.Err(); err != nil {
		return nil, fail(ErrLimit, "dotenv", err)
	}
	return result, nil
}

func dotenvLiteral(value string) (string, error) {
	invalid := func() (string, error) { return "", fail(ErrInput, "dotenv_value") }
	if value == "" {
		return "", nil
	}
	if value[0] == '\'' {
		end := strings.IndexByte(value[1:], '\'')
		if end < 0 {
			return invalid()
		}
		end++
		tail := strings.TrimSpace(value[end+1:])
		if tail != "" && !strings.HasPrefix(tail, "#") {
			return invalid()
		}
		return value[1:end], nil
	}
	if value[0] == '"' {
		end := 1
		for ; end < len(value); end++ {
			if value[end] == '\\' {
				end++
				continue
			}
			if value[end] == '"' {
				break
			}
		}
		if end >= len(value) {
			return invalid()
		}
		tail := strings.TrimSpace(value[end+1:])
		if tail != "" && !strings.HasPrefix(tail, "#") {
			return invalid()
		}
		decoder := jsontext.NewDecoder(strings.NewReader(value[:end+1]), jsontext.AllowInvalidUTF8(false))
		token, err := decoder.ReadToken()
		if err != nil || token.Kind() != '"' || !dotenvText(token.String()) {
			return invalid()
		}
		return token.String(), nil
	}
	for index, char := range value {
		if char == '#' && (index == 0 || value[index-1] == ' ' || value[index-1] == '\t') {
			return strings.TrimSpace(value[:index]), nil
		}
		if char == '\'' || char == '"' || char < ' ' && char != '\t' {
			return invalid()
		}
	}
	return value, nil
}

func dotenvKey(key string) bool {
	if len(key) == 0 || len(key) > 256 {
		return false
	}
	for index, char := range key {
		if char != '_' && !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || index > 0 && char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}
func dotenvText(value string) bool {
	return len(value) <= MaxDotenvBytes && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

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
