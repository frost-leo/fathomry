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

package logging

import (
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// Kind identifies one closed logging representation, not a native SDK field tag.
type Kind uint8

const (
	NullKind Kind = iota
	BoolKind
	Int64Kind
	Uint64Kind
	Float32Kind
	Float64Kind
	StringKind
	BinaryKind
	ByteStringKind
	TimeKind
	DurationKind
	ArrayKind
	GroupKind
)

// Value contains only constructor-selected data. Zero is explicit null.
// Collection constructors borrow until Freeze; accessors copy their own slices.
// Frozen values share only immutable nested values, not caller-owned storage.
type Value struct {
	private
	kind Kind
	data any
}

// Field is a caller-selected key/value, not a source/output routing directive.
type Field struct {
	private
	Key   string
	Value Value
}

func Null() Value                        { return Value{} }
func Bool(value bool) Value              { return Value{kind: BoolKind, data: value} }
func Int64(value int64) Value            { return Value{kind: Int64Kind, data: value} }
func Uint64(value uint64) Value          { return Value{kind: Uint64Kind, data: value} }
func Float32(value float32) Value        { return Value{kind: Float32Kind, data: value} }
func Float64(value float64) Value        { return Value{kind: Float64Kind, data: value} }
func String(value string) Value          { return Value{kind: StringKind, data: value} }
func Binary(value []byte) Value          { return Value{kind: BinaryKind, data: value} }
func ByteString(value []byte) Value      { return Value{kind: ByteStringKind, data: value} }
func Time(value time.Time) Value         { return Value{kind: TimeKind, data: value} }
func Duration(value time.Duration) Value { return Value{kind: DurationKind, data: value} }
func Array(values ...Value) Value        { return Value{kind: ArrayKind, data: values} }
func Group(fields ...Field) Value        { return Value{kind: GroupKind, data: fields} }

// Accessors return zero for a different kind. Test Kind to distinguish a present
// zero/empty value from an inapplicable accessor; no user formatter is called.
func (value Value) Kind() Kind              { return value.kind }
func (value Value) Bool() bool              { result, _ := value.data.(bool); return result }
func (value Value) Int64() int64            { result, _ := value.data.(int64); return result }
func (value Value) Uint64() uint64          { result, _ := value.data.(uint64); return result }
func (value Value) Float32() float32        { result, _ := value.data.(float32); return result }
func (value Value) Float64() float64        { result, _ := value.data.(float64); return result }
func (value Value) StringValue() string     { result, _ := value.data.(string); return result }
func (value Value) BytesCopy() []byte       { result, _ := value.data.([]byte); return slices.Clone(result) }
func (value Value) Time() time.Time         { result, _ := value.data.(time.Time); return result }
func (value Value) Duration() time.Duration { result, _ := value.data.(time.Duration); return result }
func (value Value) ElementsCopy() []Value {
	result, _ := value.data.([]Value)
	return slices.Clone(result)
}
func (value Value) FieldsCopy() []Field {
	result, _ := value.data.([]Field)
	return slices.Clone(result)
}

// Limits bounds one complete field/value tree. All fields are explicit and
// positive; no native provider defaults are copied here. Fields counts keys
// across all groups; Nodes counts values including collection/group containers.
// Bytes is logical copied data plus conservative node/key storage, not hard RSS.
// Keys are nonempty UTF-8 of at most 128 bytes; provider restrictions are additional.
type Limits struct {
	MaxFields int   `json:"max_fields"`
	MaxNodes  int   `json:"max_nodes"`
	MaxDepth  int   `json:"max_depth"`
	MaxBytes  int64 `json:"max_bytes"`
}

func (limits Limits) Validate() error {
	if limits.MaxFields < 1 || limits.MaxFields > 1024 || limits.MaxNodes < 1 || limits.MaxNodes > 65536 ||
		limits.MaxDepth < 1 || limits.MaxDepth > 32 || limits.MaxBytes < 1 || limits.MaxBytes > 16<<20 {
		return fail(ErrInput, "limits")
	}
	return nil
}

// Usage is declared data accounting, not measured allocation or resident memory.
type Usage struct {
	Fields, Nodes int
	Bytes         int64
}
type budget struct {
	limits Limits
	used   Usage
	copy   bool
}

func (budget *budget) charge(bytes int64) error {
	if bytes < 0 || bytes > budget.limits.MaxBytes-budget.used.Bytes {
		return fail(ErrLimit, "value-bytes")
	}
	budget.used.Bytes += bytes
	return nil
}

// Freeze validates and independently owns the complete tree. Cycles consume the
// same bounded depth/node budget and reject without unbounded traversal.
func Freeze(value Value, limits Limits) (Value, error) {
	if err := limits.Validate(); err != nil {
		return Value{}, err
	}
	state := budget{limits: limits, copy: true}
	return state.value(value, 1)
}
func FreezeFields(fields []Field, limits Limits) ([]Field, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	state := budget{limits: limits, copy: true}
	return state.fields(fields, 1)
}

// MeasureFields validates without copying payload storage. Provider-specific
// key/representation rules remain separate; this is not native work admission.
func MeasureFields(fields []Field, limits Limits) (Usage, error) {
	if err := limits.Validate(); err != nil {
		return Usage{}, err
	}
	state := budget{limits: limits}
	_, err := state.fields(fields, 1)
	return state.used, err
}

func (state *budget) fields(fields []Field, depth int) ([]Field, error) {
	if depth > state.limits.MaxDepth || len(fields) > state.limits.MaxFields-state.used.Fields {
		return nil, fail(ErrLimit, "fields")
	}
	names := make(map[string]bool, len(fields))
	var result []Field
	if state.copy {
		result = make([]Field, 0, len(fields))
	}
	for _, field := range fields {
		if field.Key == "" || len(field.Key) > 128 || !utf8.ValidString(field.Key) || names[field.Key] {
			return nil, fail(ErrInput, "field-key")
		}
		names[field.Key] = true
		state.used.Fields++
		if err := state.charge(int64(len(field.Key)) + 32); err != nil {
			return nil, err
		}
		value, err := state.value(field.Value, depth)
		if err != nil {
			return nil, err
		}
		if state.copy {
			result = append(result, Field{Key: strings.Clone(field.Key), Value: value})
		}
	}
	return result, nil
}
func (state *budget) value(value Value, depth int) (Value, error) {
	if depth > state.limits.MaxDepth || state.used.Nodes == state.limits.MaxNodes {
		return Value{}, fail(ErrLimit, "value-nodes")
	}
	state.used.Nodes++
	if err := state.charge(32); err != nil {
		return Value{}, err
	}
	result := value
	switch value.kind {
	case NullKind, BoolKind, Int64Kind, Uint64Kind, DurationKind:
	case Float32Kind:
		number := float64(value.Float32())
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return Value{}, fail(ErrInput, "float32")
		}
	case Float64Kind:
		number := value.Float64()
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return Value{}, fail(ErrInput, "float64")
		}
	case StringKind:
		text := value.StringValue()
		if !utf8.ValidString(text) {
			return Value{}, fail(ErrInput, "string")
		}
		if err := state.charge(int64(len(text))); err != nil {
			return Value{}, err
		}
		if state.copy {
			result.data = strings.Clone(text)
		}
	case BinaryKind, ByteStringKind:
		data := value.data.([]byte)
		if value.kind == ByteStringKind && !utf8.Valid(data) {
			return Value{}, fail(ErrInput, "byte-string")
		}
		if err := state.charge(int64(len(data))); err != nil {
			return Value{}, err
		}
		if state.copy {
			result.data = slices.Clone(data)
		}
	case TimeKind:
		result.data = value.Time().UTC().Round(0)
	case ArrayKind:
		values := value.data.([]Value)
		if len(values) > state.limits.MaxNodes-state.used.Nodes {
			return Value{}, fail(ErrLimit, "array")
		}
		var items []Value
		if state.copy {
			items = make([]Value, 0, len(values))
		}
		for _, item := range values {
			copied, err := state.value(item, depth+1)
			if err != nil {
				return Value{}, err
			}
			if state.copy {
				items = append(items, copied)
			}
		}
		if state.copy {
			result.data = items
		}
	case GroupKind:
		fields, err := state.fields(value.data.([]Field), depth+1)
		if err != nil {
			return Value{}, err
		}
		if state.copy {
			result.data = fields
		}
	default:
		return Value{}, fail(ErrUnsupported, "value-kind")
	}
	return result, nil
}
