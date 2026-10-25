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
	"context"
	"encoding"
	jsonv1 "encoding/json"
	jsonv2 "encoding/json/v2"
	"reflect"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type modernMessage interface{ ProtoReflect() protoreflect.Message }
type legacyMessage interface {
	Reset()
	String() string
	ProtoMessage()
}

var forbiddenMethods = []reflect.Type{
	reflect.TypeFor[jsonv1.Marshaler](), reflect.TypeFor[jsonv1.Unmarshaler](),
	reflect.TypeFor[jsonv2.MarshalerTo](), reflect.TypeFor[jsonv2.UnmarshalerFrom](),
	reflect.TypeFor[encoding.TextMarshaler](), reflect.TypeFor[encoding.TextUnmarshaler](),
	reflect.TypeFor[encoding.TextAppender](), reflect.TypeFor[modernMessage](), reflect.TypeFor[legacyMessage](),
}

type typeMatcher struct {
	ctx    context.Context
	visits int
	active map[reflect.Type]bool
}

func (matcher *typeMatcher) visit(depth int) error {
	if err := canceled(matcher.ctx); err != nil {
		return err
	}
	matcher.visits++
	if depth > MaxTypeDepth || matcher.visits > MaxTypeVisits {
		return problem(ErrLimit, "type_traversal", "")
	}
	return nil
}

func (matcher *typeMatcher) eligible(value reflect.Type, path string) error {
	if value == reflect.TypeFor[jsonv1.Number]() {
		return problem(ErrUnsupported, "codec_type", path)
	}
	pointer := reflect.PointerTo(value)
	if value.NumMethod() > MaxTypeMethods || pointer.NumMethod() > MaxTypeMethods {
		return problem(ErrLimit, "type_methods", path)
	}
	for _, marker := range forbiddenMethods {
		if value.Implements(marker) || pointer.Implements(marker) {
			return problem(ErrUnsupported, "codec_hook", path)
		}
	}
	return nil
}

func (matcher *typeMatcher) fields(value reflect.Type, names []string, path string) (map[string]reflect.Type, error) {
	if value.Kind() != reflect.Struct {
		return nil, problem(ErrInvalid, "port_struct", path)
	}
	if err := matcher.eligible(value, path); err != nil {
		return nil, err
	}
	if value.NumField() != len(names) {
		return nil, problem(ErrInvalid, "field_count", path)
	}
	result := make(map[string]reflect.Type, len(names))
	allowed := make(map[string]bool, len(names))
	for _, name := range names {
		allowed[name] = true
	}
	for index := 0; index < value.NumField(); index++ {
		if err := matcher.visit(0); err != nil {
			return nil, err
		}
		field := value.Field(index)
		if field.PkgPath != "" || field.Anonymous {
			return nil, problem(ErrUnsupported, "data_field", indexed(path, index))
		}
		name, ok := fieldName(field)
		if !ok {
			return nil, problem(ErrUnsupported, "json_tag", indexed(path, index))
		}
		if !allowed[name] {
			return nil, problem(ErrInvalid, "field_name", indexed(path, index))
		}
		if _, exists := result[name]; exists {
			return nil, problem(ErrInvalid, "duplicate_json_name", indexed(path, index))
		}
		result[name] = field.Type
	}
	return result, nil
}

func (matcher *typeMatcher) ports(value reflect.Type, ports map[string]declaration.Port, contracts map[string]declaration.DataSchema, path string) error {
	fields, err := matcher.fields(value, sortedKeys(ports), path)
	if err != nil {
		return err
	}
	matcher.active[value] = true
	defer delete(matcher.active, value)
	for index, name := range sortedKeys(ports) {
		port := ports[name]
		if !port.Required || port.Delivery != "value" {
			return problem(ErrUnsupported, "port_profile", indexed(path, index))
		}
		if err := matcher.match(fields[name], contracts[port.Contract], 0, indexed(path, index)); err != nil {
			return err
		}
	}
	return nil
}

func (matcher *typeMatcher) schema(value declaration.DataSchema, depth int) error {
	if err := matcher.visit(depth); err != nil {
		return err
	}
	if len(value.AnyOf) != 0 {
		return matcher.schema(value.AnyOf[0], depth+1)
	}
	switch value.Type {
	case "string", "boolean", "integer":
		return nil
	case "array":
		if value.Items == nil {
			return problem(ErrInvalid, "schema", "")
		}
		return matcher.schema(*value.Items, depth+1)
	case "object":
		if len(value.Required) != len(value.Properties) {
			return problem(ErrUnsupported, "optional_property", "")
		}
		for _, name := range sortedKeys(value.Properties) {
			if err := matcher.schema(value.Properties[name], depth+1); err != nil {
				return err
			}
		}
		return nil
	default:
		return problem(ErrUnsupported, "schema_profile", "")
	}
}

func (matcher *typeMatcher) match(value reflect.Type, schema declaration.DataSchema, depth int, path string) error {
	if err := matcher.visit(depth); err != nil {
		return err
	}
	if matcher.active[value] {
		return problem(ErrUnsupported, "recursive_type", path)
	}
	if err := matcher.eligible(value, path); err != nil {
		return err
	}
	switch value.Kind() {
	case reflect.Map, reflect.Interface, reflect.Func, reflect.Chan, reflect.UnsafePointer,
		reflect.Float32, reflect.Float64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Array:
		return problem(ErrUnsupported, "go_kind", path)
	}
	matcher.active[value] = true
	defer delete(matcher.active, value)
	if len(schema.AnyOf) != 0 {
		if value.Kind() != reflect.Pointer {
			return problem(ErrInvalid, "nullable_pointer", path)
		}
		return matcher.match(value.Elem(), schema.AnyOf[0], depth+1, path)
	}
	switch schema.Type {
	case "string":
		if value.Kind() == reflect.String {
			return nil
		}
	case "boolean":
		if value.Kind() == reflect.Bool {
			return nil
		}
	case "integer":
		if value.Kind() == reflect.Int64 {
			return nil
		}
	case "array":
		if value.Kind() != reflect.Slice {
			break
		}
		if value.Elem().Kind() == reflect.Uint8 {
			return problem(ErrUnsupported, "byte_payload", path)
		}
		return matcher.match(value.Elem(), *schema.Items, depth+1, path)
	case "object":
		if len(schema.Required) != len(schema.Properties) {
			return problem(ErrUnsupported, "optional_property", path)
		}
		fields, err := matcher.fields(value, sortedKeys(schema.Properties), path)
		if err != nil {
			return err
		}
		for index, name := range sortedKeys(schema.Properties) {
			if err := matcher.match(fields[name], schema.Properties[name], depth+1, propertyPath(path, index)); err != nil {
				return err
			}
		}
		return nil
	default:
		return problem(ErrUnsupported, "schema_profile", path)
	}
	return problem(ErrInvalid, "shape", path)
}
