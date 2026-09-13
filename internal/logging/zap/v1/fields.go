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

// freezeFields accepts native scalar, binary, time and *fault.Error fields only.
// It never invokes user presentation or marshaling code. Errors retain their
// original cause graphs for deliberate inspection; those graphs are not copied.
func freezeFields(fields []zapcore.Field) ([]zapcore.Field, error) {
	if err := validateFields(fields); err != nil {
		return nil, err
	}
	return copyFields(fields), nil
}

func validateFields(fields []zapcore.Field) error {
	if len(fields) > MaxFields {
		return failure(ErrLimit, "fields")
	}
	total := 0
	names := make(map[string]bool, len(fields))
	for _, field := range fields {
		if field.Type == zapcore.SkipType {
			continue
		}
		if !fieldKey(field.Key) || names[field.Key] {
			return failure(ErrInput, "field-key")
		}
		names[field.Key] = true
		total += len(field.Key) + 128
		switch field.Type {
		case zapcore.StringType:
			if len(field.String) > MaxFieldBytes {
				return failure(ErrLimit, "field-string")
			}
			if !utf8.ValidString(field.String) {
				return failure(ErrInput, "field-string")
			}
			total += len(field.String)
		case zapcore.BinaryType, zapcore.ByteStringType:
			value, ok := field.Interface.([]byte)
			if !ok {
				return failure(ErrInput, "field-bytes")
			}
			if len(value) > MaxFieldBytes {
				return failure(ErrLimit, "field-bytes")
			}
			if field.Type == zapcore.ByteStringType && !utf8.Valid(value) {
				return failure(ErrInput, "field-bytes")
			}
			total += len(value)
		case zapcore.BoolType, zapcore.Int64Type, zapcore.Int32Type, zapcore.Int16Type, zapcore.Int8Type,
			zapcore.Uint64Type, zapcore.Uint32Type, zapcore.Uint16Type, zapcore.Uint8Type, zapcore.DurationType:
		case zapcore.Float64Type:
			number := math.Float64frombits(uint64(field.Integer))
			if math.IsNaN(number) || math.IsInf(number, 0) {
				return failure(ErrInput, "field-float")
			}
		case zapcore.Float32Type:
			number := float64(math.Float32frombits(uint32(field.Integer)))
			if math.IsNaN(number) || math.IsInf(number, 0) {
				return failure(ErrInput, "field-float")
			}
		case zapcore.TimeType:
			if field.Interface != nil {
				location, ok := field.Interface.(*time.Location)
				if !ok || location == nil {
					return failure(ErrInput, "field-time")
				}
			}
		case zapcore.TimeFullType:
			if _, ok := field.Interface.(time.Time); !ok {
				return failure(ErrInput, "field-time")
			}
		case zapcore.ErrorType:
			original, ok := field.Interface.(*fault.Error)
			if !ok || original == nil {
				return failure(ErrUnsupported, "field-error")
			}
		default:
			return failure(ErrUnsupported, "field-type")
		}
		if total > MaxFieldBytes {
			return failure(ErrLimit, "field-bytes")
		}
	}
	return nil
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
		}
		frozen = append(frozen, copy)
	}
	return frozen
}

func fieldKey(key string) bool {
	if key == "" || len(key) > 128 || strings.HasPrefix(key, "fathomry.") {
		return false
	}
	switch key {
	case "ts", "level", "logger", "msg", "caller":
		return false
	}
	for _, char := range key {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("._-", char)) {
			return false
		}
	}
	return true
}

// With freezes allowed native fields, including byte storage, before returning
// an independent derived facade. Duplicate/reserved keys are rejected, not shadowed.
// Callers own derived facade lifetimes/counts and must bound their aggregate memory.
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
	derived := *logger
	derived.name = strings.Clone(name)
	return &derived, nil
}
