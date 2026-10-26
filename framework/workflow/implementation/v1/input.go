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

package implementation

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"io"
	"strconv"
	"strings"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

func validateInput(ctx context.Context, raw []byte, schema declaration.DataSchema) error {
	decoder := jsontext.NewDecoder(bytes.NewReader(raw))
	budget := valueBudget{ctx: ctx, phase: "input"}
	if err := readInput(&budget, decoder, schema, 1, "/input"); err != nil {
		return err
	}
	if _, err := decoder.ReadToken(); err != io.EOF {
		return budget.invalid("trailing_value", "/input", err)
	}
	return invocationCanceled(ctx, "input")
}

func readInput(budget *valueBudget, decoder *jsontext.Decoder, schema declaration.DataSchema, depth int, path string) error {
	if err := budget.enter(depth, path); err != nil {
		return err
	}
	token, err := decoder.ReadToken()
	if err != nil {
		return budget.invalid("json", path, err)
	}
	if len(schema.AnyOf) != 0 {
		if token.Kind() == 'n' {
			return nil
		}
		schema = schema.AnyOf[0]
	}
	switch schema.Type {
	case "object":
		if token.Kind() != '{' {
			return budget.invalid("kind", path)
		}
		seen := make(map[string]bool, len(schema.Properties))
		index := 0
		for decoder.PeekKind() != '}' {
			name, err := decoder.ReadToken()
			if err != nil {
				return budget.invalid("json", path, err)
			}
			memberPath := indexed(path, index)
			if name.Kind() != '"' {
				return budget.invalid("member", memberPath)
			}
			key := name.String()
			if err := budget.name(key, memberPath); err != nil {
				return err
			}
			property, ok := schema.Properties[key]
			if !ok {
				return budget.invalid("unknown_member", memberPath)
			}
			if seen[key] {
				return budget.invalid("duplicate_member", memberPath)
			}
			if err := readInput(budget, decoder, property, depth+1, memberPath); err != nil {
				return err
			}
			seen[key] = true
			index++
		}
		if _, err := decoder.ReadToken(); err != nil {
			return budget.invalid("json", path, err)
		}
		if len(seen) != len(schema.Properties) {
			return budget.invalid("missing_member", path)
		}
		return nil
	case "array":
		if token.Kind() != '[' {
			return budget.invalid("kind", path)
		}
		count := int64(0)
		for decoder.PeekKind() != ']' {
			if count >= *schema.MaxItems {
				return budget.invalid("array_length", path)
			}
			if err := readInput(budget, decoder, *schema.Items, depth+1, indexed(path, int(count))); err != nil {
				return err
			}
			count++
		}
		if _, err := decoder.ReadToken(); err != nil {
			return budget.invalid("json", path, err)
		}
		if count < *schema.MinItems {
			return budget.invalid("array_length", path)
		}
		return nil
	case "string":
		if token.Kind() != '"' {
			return budget.invalid("kind", path)
		}
		return budget.stringValue(token.String(), schema, path)
	case "integer":
		if token.Kind() != '0' {
			return budget.invalid("kind", path)
		}
		text := token.String()
		if text == "-0" || strings.ContainsAny(text, ".eE") {
			return budget.invalid("integer_lexeme", path)
		}
		value, err := strconv.ParseInt(text, 10, 64)
		const safeInteger = int64(1<<53 - 1)
		if err != nil || value < -safeInteger || value > safeInteger {
			return budget.invalid("integer_lexeme", path, err)
		}
		return budget.integer(value, schema, path)
	case "boolean":
		if token.Kind() != 't' && token.Kind() != 'f' {
			return budget.invalid("kind", path)
		}
		return nil
	default:
		return budget.invalid("schema", path)
	}
}
