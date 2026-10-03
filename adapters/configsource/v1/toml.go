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
	"encoding/json"
	"strconv"
	"strings"

	"github.com/pelletier/go-toml/v2"
	"github.com/pelletier/go-toml/v2/unstable"
)

type tomlStatement struct {
	kind  unstable.Kind
	keys  []string
	value any
}

func parseTOML(raw []byte) (map[string]any, error) {
	if err := boundTOMLSyntax(raw); err != nil {
		return nil, err
	}
	var parser unstable.Parser
	parser.Reset(raw)
	budget := newBudget()
	var statements []tomlStatement
	for parser.NextExpression() {
		node := parser.Expression()
		keys, err := tomlKeys(node, &budget, 0)
		if err != nil {
			return nil, err
		}
		statement := tomlStatement{kind: node.Kind, keys: keys}
		if node.Kind == unstable.KeyValue {
			statement.value, err = tomlValue(node.Value(), &budget, len(keys))
			if err != nil {
				return nil, err
			}
		}
		statements = append(statements, statement)
	}
	if err := parser.Error(); err != nil {
		return nil, err
	}
	var checked map[string]any
	if err := toml.Unmarshal(raw, &checked); err != nil {
		return nil, err
	}
	root := map[string]any{}
	current := root
	for _, statement := range statements {
		switch statement.kind {
		case unstable.Table, unstable.ArrayTable:
			parent, err := tomlTable(root, statement.keys[:len(statement.keys)-1])
			if err != nil {
				return nil, err
			}
			name := statement.keys[len(statement.keys)-1]
			if statement.kind == unstable.ArrayTable {
				items, _ := parent[name].([]any)
				current = map[string]any{}
				parent[name] = append(items, current)
			} else {
				current, err = tomlTable(parent, []string{name})
				if err != nil {
					return nil, err
				}
			}
		case unstable.KeyValue:
			parent, err := tomlTable(current, statement.keys[:len(statement.keys)-1])
			if err != nil {
				return nil, err
			}
			parent[statement.keys[len(statement.keys)-1]] = statement.value
		default:
			return nil, fail(ErrDecode, "toml_statement")
		}
	}
	return root, nil
}

func tomlKeys(node *unstable.Node, budget *valueBudget, depth int) ([]string, error) {
	var keys []string
	iterator := node.Key()
	for iterator.Next() {
		part := strings.Clone(string(iterator.Node().Data))
		if err := budget.take(len(part), depth+len(keys)+1); err != nil {
			return nil, err
		}
		keys = append(keys, part)
	}
	if len(keys) == 0 {
		return nil, fail(ErrDecode, "toml_key")
	}
	return keys, nil
}

func tomlTable(root map[string]any, keys []string) (map[string]any, error) {
	for _, key := range keys {
		child, exists := root[key]
		if !exists {
			child = map[string]any{}
			root[key] = child
		}
		if list, ok := child.([]any); ok && len(list) != 0 {
			child = list[len(list)-1]
		}
		next, ok := child.(map[string]any)
		if !ok {
			return nil, fail(ErrDecode, "toml_table")
		}
		root = next
	}
	return root, nil
}

func tomlValue(node *unstable.Node, budget *valueBudget, depth int) (any, error) {
	if node == nil {
		return nil, fail(ErrDecode, "toml_value")
	}
	if err := budget.take(len(node.Data), depth); err != nil {
		return nil, err
	}
	switch node.Kind {
	case unstable.String:
		return strings.Clone(string(node.Data)), nil
	case unstable.Bool:
		return string(node.Data) == "true", nil
	case unstable.Integer:
		token := strings.ReplaceAll(string(node.Data), "_", "")
		base := 10
		if strings.HasPrefix(token, "0x") || strings.HasPrefix(token, "0o") || strings.HasPrefix(token, "0b") {
			base = 0
		}
		number, err := strconv.ParseInt(token, base, 64)
		if err != nil {
			return nil, fail(ErrDecode, "toml_integer", err)
		}
		return json.Number(strconv.FormatInt(number, 10)), nil
	case unstable.Float:
		token := strings.TrimPrefix(strings.ReplaceAll(string(node.Data), "_", ""), "+")
		if !json.Valid([]byte(token)) {
			return nil, fail(ErrDecode, "toml_float")
		}
		return json.Number(token), nil
	case unstable.Array:
		result := []any{}
		children := node.Children()
		for children.Next() {
			value, err := tomlValue(children.Node(), budget, depth+1)
			if err != nil {
				return nil, err
			}
			result = append(result, value)
		}
		return result, nil
	case unstable.InlineTable:
		result := map[string]any{}
		children := node.Children()
		for children.Next() {
			field := children.Node()
			keys, err := tomlKeys(field, budget, depth)
			if err != nil {
				return nil, err
			}
			value, err := tomlValue(field.Value(), budget, depth+len(keys))
			if err != nil {
				return nil, err
			}
			parent, err := tomlTable(result, keys[:len(keys)-1])
			if err != nil {
				return nil, err
			}
			parent[keys[len(keys)-1]] = value
		}
		return result, nil
	default:
		return nil, fail(ErrDecode, "toml_scalar")
	}
}

// The native AST parser recurses through arrays before emitting a node.
func boundTOMLSyntax(raw []byte) error {
	depth := 0
	var quote byte
	multiline := false
	for offset := 0; offset < len(raw); offset++ {
		char := raw[offset]
		if quote != 0 {
			if quote == '"' && char == '\\' {
				offset++
				continue
			}
			if char != quote {
				continue
			}
			if !multiline {
				quote = 0
				continue
			}
			if offset+2 < len(raw) && raw[offset+1] == quote && raw[offset+2] == quote {
				offset += 2
				for offset+1 < len(raw) && raw[offset+1] == quote {
					offset++
				}
				quote = 0
			}
			continue
		}
		switch char {
		case '#':
			for offset+1 < len(raw) && raw[offset+1] != '\n' {
				offset++
			}
		case '"', '\'':
			quote = char
			multiline = offset+2 < len(raw) && raw[offset+1] == char && raw[offset+2] == char
			if multiline {
				offset += 2
			}
		case '[', '{':
			depth++
			if depth > MaxDepth {
				return fail(ErrLimit, "toml_nesting")
			}
		case ']', '}':
			depth--
			if depth < 0 {
				return fail(ErrDecode, "toml_nesting")
			}
		}
	}
	return nil
}
