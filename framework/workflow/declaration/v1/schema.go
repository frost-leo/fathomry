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

package declaration

import (
	"slices"
	"unicode/utf16"
	"unicode/utf8"
)

func compareText(left, right string) int {
	return slices.Compare(utf16.Encode([]rune(left)), utf16.Encode([]rune(right)))
}

func (v validator) schema(value any, path string, depth int) error {
	if err := canceled(v.ctx); err != nil {
		return err
	}
	if depth > 16 {
		return problem(ErrLimit, "schema_depth", path)
	}
	record, ok := value.(map[string]any)
	if !ok {
		return invalid("schema", path)
	}
	if raw, ok := record["anyOf"]; ok {
		if _, err := object(record, "anyOf", "", path); err != nil {
			return err
		}
		values, err := array(raw, 2, 2, at(path, "anyOf"))
		if err != nil {
			return err
		}
		first, ok := values[0].(map[string]any)
		if !ok {
			return invalid("nullable", path)
		}
		if _, nested := first["anyOf"]; nested || first["type"] == "null" {
			return invalid("nullable", path)
		}
		if err := v.schema(first, at(path, "anyOf/0"), depth+1); err != nil {
			return err
		}
		last, err := object(values[1], "type", "", at(path, "anyOf/1"))
		if err != nil {
			return err
		}
		return choice(last["type"], "null", at(path, "anyOf/1/type"))
	}
	switch record["type"] {
	case "object":
		if v.authoring {
			if _, ok := record["properties"]; !ok {
				record["properties"] = map[string]any{}
			}
			if _, ok := record["required"]; !ok {
				record["required"] = []any{}
			}
			if _, ok := record["additionalProperties"]; !ok {
				record["additionalProperties"] = false
			}
		}
		if _, err := object(record, "type properties required additionalProperties", "", path); err != nil {
			return err
		}
		if record["additionalProperties"] != false {
			return invalid("closed_schema", at(path, "additionalProperties"))
		}
		properties, ok := record["properties"].(map[string]any)
		if !ok {
			return invalid("properties", path)
		}
		if len(properties) > 256 {
			return problem(ErrLimit, "properties", path)
		}
		for _, key := range keys(properties) {
			if err := identifier(key, 64, "field", at(path, "properties")); err != nil {
				return err
			}
			if err := v.schema(properties[key], at(at(path, "properties"), key), depth+1); err != nil {
				return err
			}
		}
		required, err := array(record["required"], 0, 256, at(path, "required"))
		if err != nil {
			return err
		}
		seen := map[string]bool{}
		for _, value := range required {
			name, ok := value.(string)
			if !ok {
				return invalid("schema_required", path)
			}
			if _, exists := properties[name]; !exists || seen[name] {
				return invalid("schema_required", path)
			}
			seen[name] = true
		}
		slices.SortFunc(required, func(a, b any) int { return compareText(a.(string), b.(string)) })
	case "array":
		if _, err := object(record, "type items minItems maxItems", "", path); err != nil {
			return err
		}
		if err := schemaRange(record, "minItems", "maxItems", 0, 1000000, path); err != nil {
			return err
		}
		return v.schema(record["items"], at(path, "items"), depth+1)
	case "string":
		if _, err := object(record, "type minLength maxLength", "enum", path); err != nil {
			return err
		}
		if err := schemaRange(record, "minLength", "maxLength", 0, 1048576, path); err != nil {
			return err
		}
		if raw, ok := record["enum"]; ok {
			values, err := array(raw, 1, 256, at(path, "enum"))
			if err != nil {
				return err
			}
			seen := map[string]bool{}
			for _, value := range values {
				text, ok := value.(string)
				if !ok {
					return invalid("schema_enum", path)
				}
				length := int64(utf8.RuneCountInString(text))
				if length > 1024 || length < record["minLength"].(int64) || length > record["maxLength"].(int64) || seen[text] {
					return invalid("schema_enum", path)
				}
				seen[text] = true
			}
			slices.SortFunc(values, func(a, b any) int { return compareText(a.(string), b.(string)) })
		}
	case "integer":
		if _, err := object(record, "type minimum maximum", "", path); err != nil {
			return err
		}
		return schemaRange(record, "minimum", "maximum", -safeInteger, safeInteger, path)
	case "boolean", "null":
		_, err := object(record, "type", "", path)
		return err
	default:
		return invalid("schema_kind", path)
	}
	return nil
}

func schemaRange(record map[string]any, low, high string, minimum, maximum int64, path string) error {
	for _, key := range []string{low, high} {
		if err := integerRange(record[key], minimum, maximum, at(path, key)); err != nil {
			return err
		}
	}
	if record[low].(int64) > record[high].(int64) {
		return invalid("schema_range", path)
	}
	return nil
}
