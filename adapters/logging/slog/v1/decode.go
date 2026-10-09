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

package slog

import (
	stdslog "log/slog"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
)

type decoder struct {
	limits    logging.Limits
	remaining int
}

func decode(attrs []stdslog.Attr, limits logging.Limits) ([]logging.Field, error) {
	if len(attrs) > limits.MaxFields {
		return nil, fail(logging.ErrLimit, "ingress-attributes")
	}
	state := decoder{limits: limits, remaining: limits.MaxNodes}
	fields, err := state.attrs(attrs, 1)
	if err != nil {
		return nil, err
	}
	return logging.FreezeFields(fields, limits)
}
func (state *decoder) attrs(attrs []stdslog.Attr, depth int) ([]logging.Field, error) {
	if depth > state.limits.MaxDepth || len(attrs) > state.limits.MaxFields {
		return nil, fail(logging.ErrLimit, "ingress-attributes")
	}
	result := make([]logging.Field, 0, len(attrs))
	for _, attr := range attrs {
		if attr.Key == "" && attr.Value.Kind() == stdslog.KindAny && attr.Value.Any() == nil {
			continue
		}
		if attr.Value.Kind() == stdslog.KindGroup {
			if state.remaining == 0 {
				return nil, fail(logging.ErrLimit, "ingress-nodes")
			}
			state.remaining--
			nested, err := state.attrs(attr.Value.Group(), depth+1)
			if err != nil {
				return nil, err
			}
			if len(nested) == 0 {
				continue
			}
			if attr.Key == "" {
				result = append(result, nested...)
			} else {
				result = append(result, logging.Field{Key: attr.Key, Value: logging.Group(nested...)})
			}
			continue
		}
		value, err := state.value(attr.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, logging.Field{Key: attr.Key, Value: value})
	}
	return result, nil
}
func (state *decoder) value(value stdslog.Value) (logging.Value, error) {
	if state.remaining == 0 {
		return logging.Value{}, fail(logging.ErrLimit, "ingress-nodes")
	}
	state.remaining--
	switch value.Kind() {
	case stdslog.KindBool:
		return logging.Bool(value.Bool()), nil
	case stdslog.KindInt64:
		return logging.Int64(value.Int64()), nil
	case stdslog.KindUint64:
		return logging.Uint64(value.Uint64()), nil
	case stdslog.KindFloat64:
		return logging.Float64(value.Float64()), nil
	case stdslog.KindString:
		return logging.String(value.String()), nil
	case stdslog.KindDuration:
		return logging.Duration(value.Duration()), nil
	case stdslog.KindTime:
		return logging.Time(value.Time()), nil
	case stdslog.KindAny, stdslog.KindLogValuer:
		switch raw := value.Any().(type) {
		case nil:
			return logging.Null(), nil
		case logging.Value:
			return state.closed(raw)
		case error:
			projected, err := logging.SafeError(raw)
			if err != nil {
				return logging.Value{}, err
			}
			return state.closed(projected)
		case []byte:
			return logging.Binary(raw), nil
		case []string:
			return decodeArray(state, raw, logging.String)
		case []bool:
			return decodeArray(state, raw, logging.Bool)
		case []int:
			return decodeArray(state, raw, func(value int) logging.Value { return logging.Int64(int64(value)) })
		case []int8:
			return decodeArray(state, raw, func(value int8) logging.Value { return logging.Int64(int64(value)) })
		case []int16:
			return decodeArray(state, raw, func(value int16) logging.Value { return logging.Int64(int64(value)) })
		case []int32:
			return decodeArray(state, raw, func(value int32) logging.Value { return logging.Int64(int64(value)) })
		case []int64:
			return decodeArray(state, raw, logging.Int64)
		case []uint:
			return decodeArray(state, raw, func(value uint) logging.Value { return logging.Uint64(uint64(value)) })
		case []uint16:
			return decodeArray(state, raw, func(value uint16) logging.Value { return logging.Uint64(uint64(value)) })
		case []uint32:
			return decodeArray(state, raw, func(value uint32) logging.Value { return logging.Uint64(uint64(value)) })
		case []uint64:
			return decodeArray(state, raw, logging.Uint64)
		case []float32:
			return decodeArray(state, raw, logging.Float32)
		case []float64:
			return decodeArray(state, raw, logging.Float64)
		case []time.Duration:
			return decodeArray(state, raw, logging.Duration)
		case []time.Time:
			return decodeArray(state, raw, logging.Time)
		case []logging.Value:
			return state.closed(logging.Array(raw...))
		default:
			return logging.Value{}, fail(logging.ErrUnsupported, "ingress-value")
		}
	default:
		return logging.Value{}, fail(logging.ErrUnsupported, "ingress-value")
	}
}
func (state *decoder) closed(value logging.Value) (logging.Value, error) {
	usage, err := logging.MeasureFields([]logging.Field{{Key: "value", Value: value}}, state.limits)
	if err != nil {
		return logging.Value{}, err
	}
	if usage.Nodes-1 > state.remaining {
		return logging.Value{}, fail(logging.ErrLimit, "ingress-nodes")
	}
	state.remaining -= usage.Nodes - 1
	return value, nil
}
func decodeArray[T any](state *decoder, values []T, convert func(T) logging.Value) (logging.Value, error) {
	if len(values) > state.remaining {
		return logging.Value{}, fail(logging.ErrLimit, "ingress-array")
	}
	state.remaining -= len(values)
	result := make([]logging.Value, len(values))
	for index, value := range values {
		result[index] = convert(value)
	}
	return logging.Array(result...), nil
}
