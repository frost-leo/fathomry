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

package otel

import (
	"log/slog"
	"math"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

const (
	MaxAttributes = 64
	MaxNodes      = 256
	MaxDepth      = 8
)

// AttributeMap is an explicit nested attribute map. Use it with slog.Any to
// preserve empty child groups: slog's ordinary []Attr/GroupValue conversion
// removes them. Storage is borrowed until the operation returns, then copied.
// Nil and empty maps both encode as present empty maps, not absent attributes.
type AttributeMap []slog.Attr

func keyValid(key string) bool {
	return key != "" && boundedString(key, 128) && !strings.HasPrefix(key, "fathomry.")
}

type dataBudget struct{ bytes, nodes int }

func (budget *dataBudget) charge(bytes, nodes int) error {
	if bytes < 0 || bytes > budget.bytes || nodes > budget.nodes {
		return failure(ErrLimit, "data")
	}
	budget.bytes -= bytes
	budget.nodes -= nodes
	return nil
}

// freezeAttributes accepts closed slog values, without resolving LogValuers,
// reflection, Stringers or marshalers. All returned SDK storage is independent.
func freezeAttributes(attrs []slog.Attr, budget *dataBudget, depth int) ([]attribute.KeyValue, error) {
	if depth > MaxDepth || len(attrs) > MaxAttributes || len(attrs) > budget.nodes {
		return nil, failure(ErrLimit, "attributes")
	}
	result := make([]attribute.KeyValue, 0, len(attrs))
	names := make(map[string]bool, len(attrs))
	for _, attr := range attrs {
		if !keyValid(attr.Key) || names[attr.Key] {
			return nil, failure(ErrInput, "attribute-key")
		}
		names[attr.Key] = true
		if err := budget.charge(len(attr.Key)+32, 1); err != nil {
			return nil, err
		}
		value, err := freezeValue(attr.Value, budget, depth)
		if err != nil {
			return nil, err
		}
		result = append(result, attribute.KeyValue{Key: attribute.Key(strings.Clone(attr.Key)), Value: value})
	}
	return result, nil
}
func freezeValue(value slog.Value, budget *dataBudget, depth int) (attribute.Value, error) {
	switch value.Kind() {
	case slog.KindString:
		text := value.String()
		if err := budget.charge(len(text), 0); err != nil {
			return attribute.Value{}, err
		}
		if !boundedString(text, len(text)) {
			return attribute.Value{}, failure(ErrInput, "text")
		}
		return attribute.StringValue(strings.Clone(text)), nil
	case slog.KindBool:
		return attribute.BoolValue(value.Bool()), nil
	case slog.KindInt64:
		return attribute.Int64Value(value.Int64()), nil
	case slog.KindUint64:
		if value.Uint64() > math.MaxInt64 {
			return attribute.Value{}, failure(ErrUnsupported, "unsigned-range")
		}
		return attribute.Int64Value(int64(value.Uint64())), nil
	case slog.KindFloat64:
		if !finite(value.Float64()) {
			return attribute.Value{}, failure(ErrInput, "number")
		}
		return attribute.Float64Value(value.Float64()), nil
	case slog.KindDuration:
		return attribute.Int64Value(int64(value.Duration())), nil
	case slog.KindTime:
		timestamp := value.Time().UTC()
		if timestamp.Year() < 1 || timestamp.Year() > 9999 {
			return attribute.Value{}, failure(ErrInput, "time")
		}
		if err := budget.charge(40, 0); err != nil {
			return attribute.Value{}, err
		}
		return attribute.StringValue(timestamp.Format(time.RFC3339Nano)), nil
	case slog.KindGroup:
		attrs, err := freezeAttributes(value.Group(), budget, depth+1)
		if err != nil {
			return attribute.Value{}, err
		}
		return attribute.MapValue(attrs...), nil
	case slog.KindAny:
		switch input := value.Any().(type) {
		case AttributeMap:
			attrs, err := freezeAttributes(input, budget, depth+1)
			if err != nil {
				return attribute.Value{}, err
			}
			return attribute.MapValue(attrs...), nil
		case nil:
			return attribute.Value{}, nil
		case []byte:
			if err := budget.charge(len(input), 0); err != nil {
				return attribute.Value{}, err
			}
			return attribute.ByteSliceValue(input), nil
		case []string:
			if len(input) > budget.nodes {
				return attribute.Value{}, failure(ErrLimit, "array")
			}
			for _, text := range input {
				if err := budget.charge(len(text)+16, 1); err != nil {
					return attribute.Value{}, err
				}
				if !boundedString(text, len(text)) {
					return attribute.Value{}, failure(ErrInput, "text")
				}
			}
			frozen := make([]string, len(input))
			for index, text := range input {
				frozen[index] = strings.Clone(text)
			}
			return attribute.StringSliceValue(frozen), nil
		case []int64:
			if len(input) > budget.nodes {
				return attribute.Value{}, failure(ErrLimit, "array")
			}
			if err := budget.charge(8*len(input), len(input)); err != nil {
				return attribute.Value{}, err
			}
			return attribute.Int64SliceValue(input), nil
		case []bool:
			if len(input) > budget.nodes {
				return attribute.Value{}, failure(ErrLimit, "array")
			}
			if err := budget.charge(len(input), len(input)); err != nil {
				return attribute.Value{}, err
			}
			return attribute.BoolSliceValue(input), nil
		case []float64:
			if len(input) > budget.nodes {
				return attribute.Value{}, failure(ErrLimit, "array")
			}
			if err := budget.charge(8*len(input), len(input)); err != nil {
				return attribute.Value{}, err
			}
			for _, number := range input {
				if !finite(number) {
					return attribute.Value{}, failure(ErrInput, "number")
				}
			}
			return attribute.Float64SliceValue(input), nil
		}
	}
	return attribute.Value{}, failure(ErrUnsupported, "attribute-value")
}
func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }
func timestampValid(value time.Time) bool {
	return value.IsZero() || value.UTC().Year() >= 1970 && value.UTC().Year() <= 2261
}
