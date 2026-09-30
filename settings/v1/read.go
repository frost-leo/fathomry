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

package settings

import (
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Bounds apply to one selection, not the project's data schema or snapshot size.
const (
	MaxPathBytes    = 4096
	MaxPathSegments = 64
	MaxDereferences = 64
)

// Read copies only the selected subtree using its non-nil component-owned clone.
// The copy function has the same ownership/concurrency obligations as New and runs
// only after successful lookup and exact type admission. No data methods are invoked.
//
// Path uses JSON Pointer string syntax: empty selects the root, slash separates
// tokens, ~0 means tilde and ~1 means slash. URI fragments are not accepted. Struct
// edges are direct exported fields with explicit nonempty json names; anonymous
// fields are not promoted, untagged/private/"-" fields are hidden. Duplicate
// selected names reject. Maps require string-kind keys. Arrays/slices use canonical
// unsigned decimal indexes, without leading zeros, signs or "-".
//
// Intermediate pointers/interfaces are dereferenced within MaxDereferences; nil
// intermediates and absent keys/fields/indexes return (zero, false, nil). The final
// leaf must have exactly type T: no pointer/interface unwrapping or conversion.
// Typed nil and zero leaves are present. Invalid paths, ambiguous selected tags or
// excess traversal return ErrPath; unsupported containers and leaf mismatches
// return ErrType. Pointer/escape syntax is fully checked before lookup; array
// index syntax is checked only when traversal actually reaches an array/slice.
//
// This is Go data navigation, not an encoding/json field-selection implementation
// or JSON serialization. Custom marshalers and embedded-field dominance do not
// affect it. Callers capturing several related sections must reuse the same View.
func Read[T any](view View, path string, clone func(T) T) (T, bool, error) {
	var zero T
	if view.data == nil {
		return zero, false, reject(ErrSnapshot, "read")
	}
	if clone == nil {
		return zero, false, reject(ErrCopy, "read")
	}
	tokens, err := parsePath(path)
	if err != nil {
		return zero, false, err
	}
	value := view.data.value
	dereferences := 0
	for _, token := range tokens {
		for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
			if value.IsNil() {
				return zero, false, nil
			}
			if dereferences == MaxDereferences {
				return zero, false, reject(ErrPath, "read")
			}
			dereferences++
			value = value.Elem()
		}
		value, err = selectValue(value, token)
		if err != nil {
			return zero, false, err
		}
		if !value.IsValid() {
			return zero, false, nil
		}
	}
	if value.Type() != reflect.TypeFor[T]() {
		return zero, false, reject(ErrType, "read")
	}
	var selected T
	reflect.ValueOf(&selected).Elem().Set(value)
	return clone(selected), true, nil
}

func parsePath(path string) ([]string, error) {
	if len(path) > MaxPathBytes || !utf8.ValidString(path) ||
		path != "" && path[0] != '/' || strings.Count(path, "/") > MaxPathSegments {
		return nil, reject(ErrPath, "read")
	}
	if path == "" {
		return nil, nil
	}
	tokens := strings.Split(path[1:], "/")
	for index, token := range tokens {
		for offset := 0; offset < len(token); offset++ {
			if token[offset] == '~' {
				if offset+1 == len(token) || token[offset+1] != '0' && token[offset+1] != '1' {
					return nil, reject(ErrPath, "read")
				}
				offset++
			}
		}
		tokens[index] = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
	}
	return tokens, nil
}

func selectValue(value reflect.Value, token string) (reflect.Value, error) {
	switch value.Kind() {
	case reflect.Struct:
		kind := value.Type()
		selected := -1
		for index := 0; index < kind.NumField(); index++ {
			field := kind.Field(index)
			if !field.IsExported() {
				continue
			}
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "" || name == "-" || name != token {
				continue
			}
			if selected >= 0 {
				return reflect.Value{}, reject(ErrPath, "read")
			}
			selected = index
		}
		if selected >= 0 {
			return value.Field(selected), nil
		}
		return reflect.Value{}, nil
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return reflect.Value{}, reject(ErrType, "read")
		}
		key := reflect.ValueOf(token).Convert(value.Type().Key())
		return value.MapIndex(key), nil
	case reflect.Array, reflect.Slice:
		if token == "" || len(token) > 1 && token[0] == '0' {
			return reflect.Value{}, reject(ErrPath, "read")
		}
		for _, char := range token {
			if char < '0' || char > '9' {
				return reflect.Value{}, reject(ErrPath, "read")
			}
		}
		index, err := strconv.ParseUint(token, 10, 64)
		if err != nil {
			return reflect.Value{}, reject(ErrPath, "read")
		}
		if index >= uint64(value.Len()) {
			return reflect.Value{}, nil
		}
		return value.Index(int(index)), nil
	default:
		return reflect.Value{}, reject(ErrType, "read")
	}
}
