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
	"encoding"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

type shape struct {
	kind    reflect.Type
	element *shape
	fields  []fieldShape
	names   map[string]int
}
type fieldShape struct {
	name  string
	index int
	shape *shape
}
type typePosition struct {
	kind  reflect.Type
	depth int
}

func compile(root reflect.Type) (*shape, error) {
	active := map[reflect.Type]bool{}
	cache := map[typePosition]*shape{}
	nodes := 0
	var visit func(reflect.Type, int) (*shape, error)
	visit = func(kind reflect.Type, depth int) (*shape, error) {
		if depth > MaxDepth || active[kind] {
			return nil, fail(ErrInput, "schema")
		}
		position := typePosition{kind, depth}
		if cached := cache[position]; cached != nil {
			return cached, nil
		}
		nodes++
		if nodes > MaxNodes {
			return nil, fail(ErrLimit, "schema")
		}
		for _, codec := range []reflect.Type{reflect.TypeFor[json.Marshaler](), reflect.TypeFor[json.Unmarshaler](), reflect.TypeFor[encoding.TextMarshaler](), reflect.TypeFor[encoding.TextUnmarshaler]()} {
			if kind.Implements(codec) || reflect.PointerTo(kind).Implements(codec) {
				return nil, fail(ErrInput, "schema_codec")
			}
		}
		active[kind] = true
		defer delete(active, kind)
		result := &shape{kind: kind}
		switch kind.Kind() {
		case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		case reflect.Pointer, reflect.Slice, reflect.Map:
			if kind.Kind() == reflect.Slice && kind.Elem().Kind() == reflect.Uint8 || kind.Kind() == reflect.Map && kind.Key() != reflect.TypeFor[string]() {
				return nil, fail(ErrInput, "schema")
			}
			element, err := visit(kind.Elem(), depth+1)
			if err != nil {
				return nil, err
			}
			result.element = element
		case reflect.Struct:
			result.names = make(map[string]int)
			for index := 0; index < kind.NumField(); index++ {
				field := kind.Field(index)
				name := field.Tag.Get("json")
				if !field.IsExported() || field.Anonymous || !fieldName(name) {
					return nil, fail(ErrInput, "schema_field")
				}
				if _, exists := result.names[name]; exists {
					return nil, fail(ErrInput, "schema_field")
				}
				child, err := visit(field.Type, depth+1)
				if err != nil {
					return nil, err
				}
				result.names[name] = len(result.fields)
				result.fields = append(result.fields, fieldShape{name, index, child})
			}
		default:
			return nil, fail(ErrInput, "schema_type")
		}
		cache[position] = result
		return result, nil
	}
	return visit(root, 0)
}
func fieldName(name string) bool {
	if name == "" || name == "-" || len(name) > 256 {
		return false
	}
	for _, char := range name {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

type valueBudget struct{ nodes, bytes int }

func newBudget() valueBudget { return valueBudget{MaxNodes, MaxDocumentBytes} }
func (budget *valueBudget) take(bytes, depth int) error {
	budget.nodes--
	budget.bytes -= bytes
	if depth > MaxDepth || budget.nodes < 0 || budget.bytes < 0 {
		return fail(ErrLimit, "structure")
	}
	return nil
}
func fromGo(shape *shape, value reflect.Value, budget *valueBudget, depth int) (any, error) {
	if err := budget.take(0, depth); err != nil {
		return nil, err
	}
	switch shape.kind.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return nil, nil
		}
		return fromGo(shape.element, value.Elem(), budget, depth+1)
	case reflect.Struct:
		result := make(map[string]any, len(shape.fields))
		for _, field := range shape.fields {
			child, err := fromGo(field.shape, value.Field(field.index), budget, depth+1)
			if err != nil {
				return nil, err
			}
			result[field.name] = child
		}
		return result, nil
	case reflect.Map:
		if value.IsNil() {
			return nil, nil
		}
		if value.Len() > budget.nodes {
			return nil, fail(ErrLimit, "defaults")
		}
		result := make(map[string]any, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			key := iterator.Key().String()
			if !utf8.ValidString(key) {
				return nil, fail(ErrInput, "defaults")
			}
			if err := budget.take(len(key), depth+1); err != nil {
				return nil, err
			}
			child, err := fromGo(shape.element, iterator.Value(), budget, depth+1)
			if err != nil {
				return nil, err
			}
			result[key] = child
		}
		return result, nil
	case reflect.Slice:
		if value.IsNil() {
			return nil, nil
		}
		if value.Len() > budget.nodes {
			return nil, fail(ErrLimit, "defaults")
		}
		result := make([]any, value.Len())
		for index := range result {
			child, err := fromGo(shape.element, value.Index(index), budget, depth+1)
			if err != nil {
				return nil, err
			}
			result[index] = child
		}
		return result, nil
	case reflect.String:
		text := value.String()
		if !utf8.ValidString(text) {
			return nil, fail(ErrInput, "defaults")
		}
		if err := budget.take(len(text), depth); err != nil {
			return nil, err
		}
		return text, nil
	case reflect.Bool:
		return value.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return json.Number(strconv.FormatInt(value.Int(), 10)), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return json.Number(strconv.FormatUint(value.Uint(), 10)), nil
	case reflect.Float32, reflect.Float64:
		number := value.Float()
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return nil, fail(ErrInput, "defaults")
		}
		return json.Number(strconv.FormatFloat(number, 'g', -1, shape.kind.Bits())), nil
	}
	return nil, fail(ErrInput, "schema")
}
func toGo(shape *shape, node any, budget *valueBudget, depth int) (reflect.Value, error) {
	invalid := func() (reflect.Value, error) { return reflect.Value{}, fail(ErrDecode, "field_type") }
	if err := budget.take(0, depth); err != nil {
		return reflect.Value{}, err
	}
	result := reflect.New(shape.kind).Elem()
	if node == nil {
		switch shape.kind.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice:
			return result, nil
		}
		return invalid()
	}
	switch shape.kind.Kind() {
	case reflect.Pointer:
		child, err := toGo(shape.element, node, budget, depth+1)
		if err != nil {
			return reflect.Value{}, err
		}
		result.Set(reflect.New(shape.kind.Elem()))
		result.Elem().Set(child)
	case reflect.Struct:
		object, ok := node.(map[string]any)
		if !ok {
			return invalid()
		}
		for key, value := range object {
			index, exists := shape.names[key]
			if !exists {
				return reflect.Value{}, fail(ErrDecode, "unknown_field")
			}
			field := shape.fields[index]
			child, err := toGo(field.shape, value, budget, depth+1)
			if err != nil {
				return reflect.Value{}, err
			}
			result.Field(field.index).Set(child)
		}
	case reflect.Map:
		object, ok := node.(map[string]any)
		if !ok {
			return invalid()
		}
		if len(object) > budget.nodes {
			return reflect.Value{}, fail(ErrLimit, "structure")
		}
		result.Set(reflect.MakeMapWithSize(shape.kind, len(object)))
		for key, value := range object {
			child, err := toGo(shape.element, value, budget, depth+1)
			if err != nil {
				return reflect.Value{}, err
			}
			result.SetMapIndex(reflect.ValueOf(key), child)
		}
	case reflect.Slice:
		items, ok := node.([]any)
		if !ok {
			return invalid()
		}
		if len(items) > budget.nodes {
			return reflect.Value{}, fail(ErrLimit, "structure")
		}
		result.Set(reflect.MakeSlice(shape.kind, len(items), len(items)))
		for index, value := range items {
			child, err := toGo(shape.element, value, budget, depth+1)
			if err != nil {
				return reflect.Value{}, err
			}
			result.Index(index).Set(child)
		}
	case reflect.String:
		text, ok := node.(string)
		if !ok {
			return invalid()
		}
		result.SetString(text)
	case reflect.Bool:
		value, ok := node.(bool)
		if !ok {
			return invalid()
		}
		result.SetBool(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		number, ok := node.(json.Number)
		if !ok || strings.ContainsAny(string(number), ".eE") {
			return invalid()
		}
		value, err := strconv.ParseInt(string(number), 10, shape.kind.Bits())
		if err != nil {
			return invalid()
		}
		result.SetInt(value)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		number, ok := node.(json.Number)
		if !ok || strings.ContainsAny(string(number), ".eE") {
			return invalid()
		}
		value, err := strconv.ParseUint(string(number), 10, shape.kind.Bits())
		if err != nil {
			return invalid()
		}
		result.SetUint(value)
	case reflect.Float32, reflect.Float64:
		number, ok := node.(json.Number)
		if !ok {
			return invalid()
		}
		value, err := strconv.ParseFloat(string(number), shape.kind.Bits())
		if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
			return invalid()
		}
		result.SetFloat(value)
	default:
		return invalid()
	}
	return result, nil
}
