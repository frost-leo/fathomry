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
	"reflect"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

func validateOutput(budget *valueBudget, value reflect.Value, schema declaration.DataSchema, depth int, path string) error {
	if err := budget.enter(depth, path); err != nil {
		return err
	}
	if len(schema.AnyOf) != 0 {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
		schema = schema.AnyOf[0]
	}
	switch schema.Type {
	case "object":
		for index := 0; index < value.NumField(); index++ {
			memberPath := indexed(path, index)
			name, _ := fieldName(value.Type().Field(index))
			if err := budget.name(name, memberPath); err != nil {
				return err
			}
			if err := validateOutput(budget, value.Field(index), schema.Properties[name], depth+1, memberPath); err != nil {
				return err
			}
		}
		return nil
	case "array":
		if value.IsNil() {
			return budget.invalid("null_array", path)
		}
		count := int64(value.Len())
		if count < *schema.MinItems || count > *schema.MaxItems {
			return budget.invalid("array_length", path)
		}
		for index := 0; index < value.Len(); index++ {
			if err := validateOutput(budget, value.Index(index), *schema.Items, depth+1, indexed(path, index)); err != nil {
				return err
			}
		}
		return nil
	case "string":
		return budget.stringValue(value.String(), schema, path)
	case "integer":
		return budget.integer(value.Int(), schema, path)
	case "boolean":
		return nil
	default:
		return budget.invalid("schema", path)
	}
}
