/*
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

package configuration

import (
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"
)

type variableInput struct {
	data []byte
	info []VariableInfo
}

func prepareVariables[T any](bindings []Variable) (variableInput, error) {
	fail := func() (variableInput, error) {
		return variableInput{}, problem(InvalidInput)
	}
	if len(bindings) > MaxVariables {
		return variableInput{}, problem(LimitExceeded)
	}
	paths := make([][]string, len(bindings))
	for index, binding := range bindings {
		if !validVariableName(binding.Name) || binding.Encoding > VariableJSON ||
			!strings.HasPrefix(binding.Field, "/") || len(binding.Field) > 1024 {
			return fail()
		}
		path := strings.Split(binding.Field[1:], "/")
		if len(path) > 16 {
			return fail()
		}
		fieldType := reflect.TypeFor[T]()
		for _, component := range path {
			if !validLabel(component) {
				return fail()
			}
			for depth := 0; fieldType.Kind() == reflect.Pointer; depth++ {
				if depth == 64 {
					return fail()
				}
				fieldType = fieldType.Elem()
			}
			if fieldType.Kind() != reflect.Struct {
				return fail()
			}
			var found reflect.Type
			for fieldIndex := 0; fieldIndex < fieldType.NumField(); fieldIndex++ {
				field := fieldType.Field(fieldIndex)
				if field.IsExported() && !field.Anonymous && field.Tag.Get("json") == component {
					found = field.Type
					break
				}
			}
			if found == nil {
				return fail()
			}
			fieldType = found
		}
		if binding.Encoding == VariableText {
			for depth := 0; fieldType.Kind() == reflect.Pointer; depth++ {
				if depth == 64 {
					return fail()
				}
				fieldType = fieldType.Elem()
			}
			if fieldType.Kind() != reflect.String {
				return fail()
			}
		}
		for _, previous := range bindings[:index] {
			if previous.Field == binding.Field || strings.HasPrefix(previous.Field, binding.Field+"/") ||
				strings.HasPrefix(binding.Field, previous.Field+"/") {
				return fail()
			}
		}
		paths[index] = path
	}
	type captured struct {
		value   string
		present bool
	}
	captures := make(map[string]captured)
	values := make(map[string]any)
	result := variableInput{}
	total := 0
	for index, binding := range bindings {
		item, exists := captures[binding.Name]
		if !exists {
			item.value, item.present = os.LookupEnv(binding.Name)
			captures[binding.Name] = item
		}
		result.info = append(result.info, VariableInfo{Field: strings.Clone(binding.Field), Present: item.present})
		if !item.present {
			if binding.Required {
				return fail()
			}
			continue
		}
		if len(item.value) > MaxDocumentBytes-total {
			return variableInput{}, problem(LimitExceeded)
		}
		total += len(item.value)
		if !utf8.ValidString(item.value) {
			return fail()
		}
		var value any = item.value
		if binding.Encoding == VariableJSON {
			if !json.Valid([]byte(item.value)) {
				return fail()
			}
			value = json.RawMessage(item.value)
		}
		target := values
		path := paths[index]
		for _, component := range path[:len(path)-1] {
			child, _ := target[component].(map[string]any)
			if child == nil {
				child = make(map[string]any)
				target[component] = child
			}
			target = child
		}
		target[path[len(path)-1]] = value
	}
	slices.SortFunc(result.info, func(left, right VariableInfo) int { return strings.Compare(left.Field, right.Field) })
	if len(values) == 0 {
		return result, nil
	}
	data, err := json.Marshal(values)
	if err != nil {
		return fail()
	}
	if len(data) > MaxDocumentBytes {
		return variableInput{}, problem(LimitExceeded)
	}
	result.data = data
	return result, nil
}

func validVariableName(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for index, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char == '_' || index > 0 && char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}
