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

package zap

import (
	"encoding/base64"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/fault"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	MaxDepth = 8
	MaxNodes = 256
)

// Value is closed logging data, borrowed until Log/With/FreezeFields returns.
// Kind is null (also zero), bool, int64, uint64, float32, float64, string, bytes,
// bytestring, time, duration, array or map. Only its selected member may contain
// data. Nil arrays/maps denote present empty collections; bytes are native
// binary/base64, while bytestring preserves native UTF-8 byte-string identity.
// UTF-8 text may contain NUL, uint64 retains its complete native range, and times
// normalize to UTC without shrinking the native time.Time range. Other targets
// may independently refuse representations unavailable in their own protocol.
type Value struct {
	Kind     string
	Bool     bool
	Int64    int64
	Uint64   uint64
	Float32  float32
	Float64  float64
	String   string
	Bytes    []byte
	Time     time.Time
	Duration time.Duration
	Array    []Value
	Map      []Attribute
}

// Attribute is one ordered map member. Keys are unique within each map.
type Attribute struct {
	Key   string
	Value Value
}

// Field carries closed data into this integration. It is not a native reflected
// field for use with an unrestricted SDK Logger. Validation and freezing replace
// this carrier before any native encoder or structured destination is entered.
func Field(key string, value Value) zapcore.Field {
	return zapcore.Field{Key: key, Type: zapcore.ReflectType, Interface: value}
}

type dataBudget struct{ bytes, nodes int }

func (budget *dataBudget) take(bytes, nodes int) error {
	if bytes < 0 || nodes < 0 || bytes > budget.bytes || nodes > budget.nodes {
		return failure(ErrLimit, "field-data")
	}
	budget.bytes -= bytes
	budget.nodes -= nodes
	return nil
}

func validateValue(value Value, budget *dataBudget, depth int) error {
	if depth > MaxDepth {
		return failure(ErrLimit, "field-depth")
	}
	if value.Kind != "bool" && value.Bool || value.Kind != "int64" && value.Int64 != 0 || value.Kind != "uint64" && value.Uint64 != 0 ||
		value.Kind != "float32" && value.Float32 != 0 || value.Kind != "float64" && value.Float64 != 0 ||
		value.Kind != "string" && value.String != "" || value.Kind != "bytes" && value.Kind != "bytestring" && value.Bytes != nil ||
		value.Kind != "time" && !value.Time.IsZero() || value.Kind != "duration" && value.Duration != 0 ||
		value.Kind != "array" && value.Array != nil || value.Kind != "map" && value.Map != nil {
		return failure(ErrInput, "field-value-members")
	}
	if err := budget.take(192, 1); err != nil {
		return err
	}
	switch value.Kind {
	case "", "null", "bool", "int64", "uint64", "duration", "time":
		return nil
	case "float32":
		if math.IsNaN(float64(value.Float32)) || math.IsInf(float64(value.Float32), 0) {
			return failure(ErrInput, "field-float")
		}
	case "float64":
		if math.IsNaN(value.Float64) || math.IsInf(value.Float64, 0) {
			return failure(ErrInput, "field-float")
		}
	case "string":
		if !utf8.ValidString(value.String) {
			return failure(ErrInput, "field-string")
		}
		return budget.take(len(value.String), 0)
	case "bytes":
		return budget.take(len(value.Bytes), 0)
	case "bytestring":
		if !utf8.Valid(value.Bytes) {
			return failure(ErrInput, "field-bytestring")
		}
		return budget.take(len(value.Bytes), 0)
	case "array":
		if len(value.Array) > budget.nodes {
			return failure(ErrLimit, "field-array")
		}
		for _, item := range value.Array {
			if err := validateValue(item, budget, depth+1); err != nil {
				return err
			}
		}
	case "map":
		if len(value.Map) > MaxFields || len(value.Map) > budget.nodes {
			return failure(ErrLimit, "field-map")
		}
		names := make(map[string]bool, len(value.Map))
		for _, item := range value.Map {
			if !dataKey(item.Key) || names[item.Key] {
				return failure(ErrInput, "field-key")
			}
			names[item.Key] = true
			if err := budget.take(len(item.Key)+32, 1); err != nil {
				return err
			}
			if err := validateValue(item.Value, budget, depth+1); err != nil {
				return err
			}
		}
	default:
		return failure(ErrUnsupported, "field-value-kind")
	}
	return nil
}

func copyValue(value Value) Value {
	value.Kind = strings.Clone(value.Kind)
	value.String = strings.Clone(value.String)
	if value.Bytes != nil {
		value.Bytes = append([]byte{}, value.Bytes...)
	}
	if value.Kind == "time" {
		value.Time = value.Time.UTC().Round(0)
	}
	if value.Array != nil {
		items := make([]Value, len(value.Array))
		for index, item := range value.Array {
			items[index] = copyValue(item)
		}
		value.Array = items
	}
	if value.Map != nil {
		items := make([]Attribute, len(value.Map))
		for index, item := range value.Map {
			items[index] = Attribute{Key: strings.Clone(item.Key), Value: copyValue(item.Value)}
		}
		value.Map = items
	}
	return value
}

type frozenArray []Value
type frozenMap []Attribute

func (values frozenArray) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, value := range values {
		switch value.Kind {
		case "", "null":
			if err := encoder.AppendReflected(nil); err != nil {
				return err
			}
		case "bool":
			encoder.AppendBool(value.Bool)
		case "int64":
			encoder.AppendInt64(value.Int64)
		case "uint64":
			encoder.AppendUint64(value.Uint64)
		case "float32":
			encoder.AppendFloat32(value.Float32)
		case "float64":
			encoder.AppendFloat64(value.Float64)
		case "string":
			encoder.AppendString(value.String)
		case "bytes":
			encoder.AppendString(base64.StdEncoding.EncodeToString(value.Bytes))
		case "bytestring":
			encoder.AppendByteString(value.Bytes)
		case "time":
			encoder.AppendTime(value.Time)
		case "duration":
			encoder.AppendDuration(value.Duration)
		case "array":
			if err := encoder.AppendArray(frozenArray(value.Array)); err != nil {
				return err
			}
		case "map":
			if err := encoder.AppendObject(frozenMap(value.Map)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (values frozenMap) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	for _, item := range values {
		valueField(item.Key, item.Value).AddTo(encoder)
	}
	return nil
}

func valueField(key string, value Value) zapcore.Field {
	switch value.Kind {
	case "", "null":
		return sdk.Reflect(key, nil)
	case "bool":
		return sdk.Bool(key, value.Bool)
	case "int64":
		return sdk.Int64(key, value.Int64)
	case "uint64":
		return sdk.Uint64(key, value.Uint64)
	case "float32":
		return sdk.Float32(key, value.Float32)
	case "float64":
		return sdk.Float64(key, value.Float64)
	case "string":
		return sdk.String(key, value.String)
	case "bytes":
		return sdk.Binary(key, value.Bytes)
	case "bytestring":
		return sdk.ByteString(key, value.Bytes)
	case "time":
		return sdk.Time(key, value.Time)
	case "duration":
		return sdk.Duration(key, value.Duration)
	case "array":
		return zapcore.Field{Key: key, Type: zapcore.ArrayMarshalerType, Interface: frozenArray(value.Array)}
	case "map":
		return zapcore.Field{Key: key, Type: zapcore.ObjectMarshalerType, Interface: frozenMap(value.Map)}
	}
	return sdk.Skip()
}

// FieldsValues produces isolated closed semantic data for a structured bridge.
// It accepts only this integration's supported field families and known fixed
// metadata; no Array/Object/Stringer/reflect callback is inspected or executed.
// Native errors become safe diagnostics, never their cause graphs or formatting.
func FieldsValues(fields []zapcore.Field) ([]Attribute, error) {
	if _, err := validateFieldsBudget(fields, true); err != nil {
		return nil, err
	}
	result := make([]Attribute, 0, len(fields))
	for _, field := range fields {
		if field.Type == zapcore.SkipType {
			continue
		}
		value := fieldValue(field)
		result = append(result, Attribute{Key: strings.Clone(field.Key), Value: copyValue(value)})
	}
	return result, nil
}

func fieldValue(field zapcore.Field) Value {
	switch field.Type {
	case zapcore.StringType:
		return Value{Kind: "string", String: field.String}
	case zapcore.BoolType:
		return Value{Kind: "bool", Bool: field.Integer == 1}
	case zapcore.Int64Type:
		return Value{Kind: "int64", Int64: field.Integer}
	case zapcore.Int32Type:
		return Value{Kind: "int64", Int64: int64(int32(field.Integer))}
	case zapcore.Int16Type:
		return Value{Kind: "int64", Int64: int64(int16(field.Integer))}
	case zapcore.Int8Type:
		return Value{Kind: "int64", Int64: int64(int8(field.Integer))}
	case zapcore.Uint64Type:
		return Value{Kind: "uint64", Uint64: uint64(field.Integer)}
	case zapcore.Uint32Type:
		return Value{Kind: "uint64", Uint64: uint64(uint32(field.Integer))}
	case zapcore.Uint16Type:
		return Value{Kind: "uint64", Uint64: uint64(uint16(field.Integer))}
	case zapcore.Uint8Type:
		return Value{Kind: "uint64", Uint64: uint64(uint8(field.Integer))}
	case zapcore.Float32Type:
		return Value{Kind: "float32", Float32: math.Float32frombits(uint32(field.Integer))}
	case zapcore.Float64Type:
		return Value{Kind: "float64", Float64: math.Float64frombits(uint64(field.Integer))}
	case zapcore.DurationType:
		return Value{Kind: "duration", Duration: time.Duration(field.Integer)}
	case zapcore.TimeType:
		return Value{Kind: "time", Time: time.Unix(0, field.Integer).UTC()}
	case zapcore.TimeFullType:
		return Value{Kind: "time", Time: field.Interface.(time.Time).UTC().Round(0)}
	case zapcore.BinaryType:
		return Value{Kind: "bytes", Bytes: field.Interface.([]byte)}
	case zapcore.ByteStringType:
		return Value{Kind: "bytestring", Bytes: field.Interface.([]byte)}
	case zapcore.ReflectType:
		if field.Interface == nil {
			return Value{}
		}
		return field.Interface.(Value)
	case zapcore.ArrayMarshalerType:
		return Value{Kind: "array", Array: []Value(field.Interface.(frozenArray))}
	case zapcore.ObjectMarshalerType:
		return Value{Kind: "map", Map: []Attribute(field.Interface.(frozenMap))}
	case zapcore.ErrorType:
		diagnostic := field.Interface.(*fault.Error).Diagnostic()
		location := diagnostic.Context
		values := []Attribute{
			{Key: "kind", Value: Value{Kind: "string", String: string(diagnostic.Kind)}},
			{Key: "operation", Value: Value{Kind: "string", String: location.Operation}},
			{Key: "provider", Value: Value{Kind: "string", String: location.Provider}},
			{Key: "scope", Value: Value{Kind: "string", String: location.Scope}},
			{Key: "source", Value: Value{Kind: "string", String: location.Source}},
			{Key: "call", Value: Value{Kind: "string", String: location.Correlation.Call}},
			{Key: "parent", Value: Value{Kind: "string", String: location.Correlation.Parent}},
			{Key: "owner", Value: Value{Kind: "string", String: location.Correlation.Owner}},
			{Key: "has_causes", Value: Value{Kind: "bool", Bool: diagnostic.HasCauses}},
		}
		return Value{Kind: "map", Map: values}
	}
	return Value{}
}
