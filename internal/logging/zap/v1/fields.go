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
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/fault"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Input ceilings apply before native encoding; automatic source fields are
// additional bounded metadata. MaxFieldBytes includes the per-field charge.
const (
	MaxSinks        = 8
	MaxFields       = 64
	MaxFieldBytes   = 64 << 10
	MaxMessageBytes = 64 << 10
)

// FreezeFields accepts native primitives, binary/time/no-op fields, literal null,
// closed Value carriers and nonnil *fault.Error fields. Caller-owned callbacks
// never execute. Returned byte/collection storage is independent; native error
// graphs remain borrowed for deliberate inspection, not copied or heap-capped.
func FreezeFields(fields []zapcore.Field) ([]zapcore.Field, error) {
	if err := validateFields(fields); err != nil {
		return nil, err
	}
	return copyFields(fields), nil
}

func freezeFields(fields []zapcore.Field) ([]zapcore.Field, error) { return FreezeFields(fields) }

func validateFields(fields []zapcore.Field) error {
	_, err := validateFieldsBudget(fields, false)
	return err
}

func validateFieldsBudget(fields []zapcore.Field, metadata bool) (int, error) {
	limit := MaxFields
	if metadata {
		limit += 6
	}
	if len(fields) > limit {
		return 0, failure(ErrLimit, "fields")
	}
	userBudget := dataBudget{bytes: MaxFieldBytes, nodes: MaxNodes}
	metadataBudget := dataBudget{bytes: 6 * 384, nodes: 6}
	names := make(map[string]bool, len(fields))
	userFields := 0
	for _, field := range fields {
		if field.Type == zapcore.SkipType {
			userFields++
			continue
		}
		automatic := metadata && metadataKey(field.Key)
		if !automatic {
			userFields++
		}
		if (!automatic && !fieldKey(field.Key)) || names[field.Key] || automatic && (field.Type != zapcore.StringType || len(field.String) > 128) {
			return 0, failure(ErrInput, "field-key")
		}
		names[field.Key] = true
		budget := &userBudget
		if automatic {
			budget = &metadataBudget
		}
		if err := budget.take(len(field.Key)+128, 1); err != nil {
			return 0, err
		}
		switch field.Type {
		case zapcore.StringType:
			if len(field.String) > MaxFieldBytes {
				return 0, failure(ErrLimit, "field-string")
			}
			if !utf8.ValidString(field.String) {
				return 0, failure(ErrInput, "field-string")
			}
			if err := budget.take(len(field.String), 0); err != nil {
				return 0, err
			}
		case zapcore.BinaryType, zapcore.ByteStringType:
			value, ok := field.Interface.([]byte)
			if !ok {
				return 0, failure(ErrInput, "field-bytes")
			}
			if len(value) > MaxFieldBytes {
				return 0, failure(ErrLimit, "field-bytes")
			}
			if field.Type == zapcore.ByteStringType && !utf8.Valid(value) {
				return 0, failure(ErrInput, "field-bytes")
			}
			if err := budget.take(len(value), 0); err != nil {
				return 0, err
			}
		case zapcore.BoolType, zapcore.Int64Type, zapcore.Int32Type, zapcore.Int16Type, zapcore.Int8Type,
			zapcore.Uint64Type, zapcore.Uint32Type, zapcore.Uint16Type, zapcore.Uint8Type, zapcore.DurationType:
		case zapcore.Float64Type:
			number := math.Float64frombits(uint64(field.Integer))
			if math.IsNaN(number) || math.IsInf(number, 0) {
				return 0, failure(ErrInput, "field-float")
			}
		case zapcore.Float32Type:
			number := float64(math.Float32frombits(uint32(field.Integer)))
			if math.IsNaN(number) || math.IsInf(number, 0) {
				return 0, failure(ErrInput, "field-float")
			}
		case zapcore.TimeType:
			if field.Interface != nil {
				location, ok := field.Interface.(*time.Location)
				if !ok || location == nil {
					return 0, failure(ErrInput, "field-time")
				}
			}
		case zapcore.TimeFullType:
			if _, ok := field.Interface.(time.Time); !ok {
				return 0, failure(ErrInput, "field-time")
			}
		case zapcore.ErrorType:
			original, ok := field.Interface.(*fault.Error)
			if !ok || original == nil {
				return 0, failure(ErrUnsupported, "field-error")
			}
		case zapcore.ReflectType:
			if field.Interface == nil {
				continue
			}
			value, ok := field.Interface.(Value)
			if !ok {
				return 0, failure(ErrUnsupported, "field-reflect")
			}
			if err := validateValue(value, budget, 1); err != nil {
				return 0, err
			}
		case zapcore.ArrayMarshalerType:
			value, ok := field.Interface.(frozenArray)
			if !ok {
				return 0, failure(ErrUnsupported, "field-array-callback")
			}
			if err := validateValue(Value{Kind: "array", Array: []Value(value)}, budget, 1); err != nil {
				return 0, err
			}
		case zapcore.ObjectMarshalerType:
			value, ok := field.Interface.(frozenMap)
			if !ok {
				return 0, failure(ErrUnsupported, "field-object-callback")
			}
			if err := validateValue(Value{Kind: "map", Map: []Attribute(value)}, budget, 1); err != nil {
				return 0, err
			}
		default:
			return 0, failure(ErrUnsupported, "field-type")
		}
	}
	if userFields > MaxFields {
		return 0, failure(ErrLimit, "fields")
	}
	return MaxFieldBytes - userBudget.bytes + 6*384 - metadataBudget.bytes, nil
}

func copyFields(fields []zapcore.Field) []zapcore.Field {
	frozen := make([]zapcore.Field, 0, len(fields))
	for _, field := range fields {
		if field.Type == zapcore.SkipType {
			continue
		}
		key := strings.Clone(field.Key)
		copy := zapcore.Field{Key: key, Type: field.Type, Integer: field.Integer}
		switch field.Type {
		case zapcore.StringType:
			copy.String = strings.Clone(field.String)
		case zapcore.BinaryType, zapcore.ByteStringType:
			copy.Interface = append([]byte(nil), field.Interface.([]byte)...)
		case zapcore.TimeType:
			copy.Interface = time.UTC
		case zapcore.TimeFullType:
			copy = sdk.Time(key, field.Interface.(time.Time).UTC())
		case zapcore.ErrorType:
			copy.Interface = field.Interface
		case zapcore.ReflectType:
			if field.Interface != nil {
				copy = valueField(key, copyValue(field.Interface.(Value)))
			}
		case zapcore.ArrayMarshalerType:
			value := copyValue(Value{Kind: "array", Array: []Value(field.Interface.(frozenArray))})
			copy = valueField(key, value)
		case zapcore.ObjectMarshalerType:
			value := copyValue(Value{Kind: "map", Map: []Attribute(field.Interface.(frozenMap))})
			copy = valueField(key, value)
		}
		frozen = append(frozen, copy)
	}
	return frozen
}

func fieldKey(key string) bool {
	if !dataKey(key) || strings.HasPrefix(key, "fathomry.") {
		return false
	}
	switch key {
	case "ts", "level", "logger", "msg", "caller":
		return false
	}
	return true
}

func dataKey(key string) bool {
	if key == "" || len(key) > 128 {
		return false
	}
	for _, char := range key {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("._-", char)) {
			return false
		}
	}
	return true
}

func metadataKey(key string) bool {
	switch key {
	case "fathomry.call", "fathomry.parent", "fathomry.owner", "fathomry.source", "fathomry.provider", "fathomry.scope":
		return true
	}
	return false
}

// With freezes allowed native fields, including byte storage, before returning
// an independent derived facade. Duplicate/reserved keys are rejected, not shadowed.
// Each successful derivation consumes the physical owner's cumulative count and
// logical retained-byte allowance. Collection does not recycle this allowance.
func (logger *Logger) With(fields ...zapcore.Field) (*Logger, error) {
	if logger == nil || logger.owner == nil {
		return nil, failure(ErrState, "with")
	}
	if len(logger.fields)+len(fields) > MaxFields {
		return nil, failure(ErrLimit, "with")
	}
	combined := make([]zapcore.Field, 0, len(logger.fields)+len(fields))
	combined = append(combined, logger.fields...)
	combined = append(combined, fields...)
	frozen, err := freezeFields(combined)
	if err != nil {
		return nil, err
	}
	charge, err := validateFieldsBudget(frozen, false)
	if err != nil {
		return nil, err
	}
	if err := logger.owner.reserveDerivation(int64(charge + len(logger.name) + 256)); err != nil {
		return nil, err
	}
	derived := *logger
	derived.fields = frozen
	return &derived, nil
}

// Named appends a bounded instrumentation name, not a resource or execution ID.
// No native handle, level mutation or lifecycle authority accompanies derivation.
func (logger *Logger) Named(name string) (*Logger, error) {
	if logger == nil || logger.owner == nil || !fieldKey(name) {
		return nil, failure(ErrInput, "name")
	}
	if logger.name != "" {
		name = logger.name + "." + name
	}
	if len(name) > 128 {
		return nil, failure(ErrLimit, "name")
	}
	if err := logger.owner.reserveDerivation(int64(len(name) + 256)); err != nil {
		return nil, err
	}
	derived := *logger
	derived.name = strings.Clone(name)
	return &derived, nil
}
