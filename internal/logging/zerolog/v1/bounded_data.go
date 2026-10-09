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

package zerolog

import (
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	sdk "github.com/rs/zerolog"
)

// MaxNodes bounds scalar and collection nodes across the entire record.
const MaxNodes = 256

// Value is closed data accepted only as slog.Any's exact Value type. Kind is
// null (also zero), bool, int64, uint64, float32, float64, string, bytes,
// bytestring, time, duration, array or map. Only the selected member may contain
// data. Nil collections mean present empty collections. Binary uses base64;
// bytestring keeps native UTF-8 bytes. No user marshaler or formatter executes.
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

// Attribute is one ordered map member. Duplicate keys are refused per map.
type Attribute struct {
	Key   string
	Value Value
}

type dataBudget struct {
	bytes, nodes, attributes int
}

func (budget *dataBudget) take(bytes, nodes int) error {
	if bytes < 0 || nodes < 0 || bytes > budget.bytes || nodes > budget.nodes {
		return failure(ErrLimit, "attribute-data")
	}
	budget.bytes -= bytes
	budget.nodes -= nodes
	return nil
}

func (budget *dataBudget) key(key string) error {
	if budget.attributes == MaxAttributes {
		return failure(ErrLimit, "attributes")
	}
	if len(key) == 0 || len(key) > 128 || !utf8.ValidString(key) {
		return failure(ErrInput, "attribute-key")
	}
	budget.attributes++
	return budget.take(len(key)+32, 0)
}

func validateValue(value Value, budget *dataBudget, depth int) error {
	if depth > MaxDepth {
		return failure(ErrLimit, "attribute-depth")
	}
	if value.Kind != "bool" && value.Bool || value.Kind != "int64" && value.Int64 != 0 || value.Kind != "uint64" && value.Uint64 != 0 ||
		value.Kind != "float32" && value.Float32 != 0 || value.Kind != "float64" && value.Float64 != 0 ||
		value.Kind != "string" && value.String != "" || value.Kind != "bytes" && value.Kind != "bytestring" && value.Bytes != nil ||
		value.Kind != "time" && value.Time != (time.Time{}) || value.Kind != "duration" && value.Duration != 0 ||
		value.Kind != "array" && value.Array != nil || value.Kind != "map" && value.Map != nil {
		return failure(ErrInput, "attribute-members")
	}
	if err := budget.take(192, 1); err != nil {
		return err
	}
	switch value.Kind {
	case "", "null", "bool", "int64", "uint64", "duration":
	case "time":
		if !validTime(value.Time) {
			return failure(ErrInput, "attribute-time")
		}
	case "float32":
		if !finite(float64(value.Float32)) {
			return failure(ErrInput, "attribute-number")
		}
	case "float64":
		if !finite(value.Float64) {
			return failure(ErrInput, "attribute-number")
		}
	case "string":
		if err := budget.take(len(value.String), 0); err != nil {
			return err
		}
		if !utf8.ValidString(value.String) {
			return failure(ErrInput, "attribute-string")
		}
	case "bytes", "bytestring":
		if err := budget.take(len(value.Bytes), 0); err != nil {
			return err
		}
		if value.Kind == "bytestring" && !utf8.Valid(value.Bytes) {
			return failure(ErrInput, "attribute-bytestring")
		}
	case "array":
		if len(value.Array) > budget.nodes {
			return failure(ErrLimit, "attribute-array")
		}
		for _, item := range value.Array {
			if err := validateValue(item, budget, depth+1); err != nil {
				return err
			}
		}
	case "map":
		if len(value.Map) > MaxAttributes-budget.attributes {
			return failure(ErrLimit, "attributes")
		}
		names := make(map[string]bool, len(value.Map))
		for _, item := range value.Map {
			if names[item.Key] {
				return failure(ErrInput, "attribute-key")
			}
			if err := budget.key(item.Key); err != nil {
				return err
			}
			names[item.Key] = true
			if err := validateValue(item.Value, budget, depth+1); err != nil {
				return err
			}
		}
	default:
		return failure(ErrUnsupported, "attribute-value-kind")
	}
	return nil
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }
func validTime(value time.Time) bool {
	year := value.UTC().Year()
	return year >= 1 && year <= 9999
}

func copyValue(value Value) Value {
	value.Kind, value.String = strings.Clone(value.Kind), strings.Clone(value.String)
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

// FreezeAttributes validates the complete tree under the final record input
// ceiling and returns independent storage. The containing log also charges its
// message against this ceiling. Native errors/callbacks are never admitted here.
func FreezeAttributes(attributes []slog.Attr, limit int) ([]slog.Attr, error) {
	if _, err := validateAttributes(attributes, limit); err != nil {
		return nil, err
	}
	return copyAttributes(attributes), nil
}

// AttributesValues produces a detached closed semantic snapshot without
// invoking user marshaling, reflection or formatting. Original primitive kinds,
// explicit empty maps/arrays and opaque bytes remain distinguishable.
func AttributesValues(attributes []slog.Attr, limit int) ([]Attribute, error) {
	if _, err := validateAttributes(attributes, limit); err != nil {
		return nil, err
	}
	return attributesValues(attributes), nil
}

func attributesValues(attributes []slog.Attr) []Attribute {
	result := make([]Attribute, 0, len(attributes))
	for _, attr := range attributes {
		var value Value
		switch attr.Value.Kind() {
		case slog.KindString:
			value = Value{Kind: "string", String: strings.Clone(attr.Value.String())}
		case slog.KindBool:
			value = Value{Kind: "bool", Bool: attr.Value.Bool()}
		case slog.KindInt64:
			value = Value{Kind: "int64", Int64: attr.Value.Int64()}
		case slog.KindUint64:
			value = Value{Kind: "uint64", Uint64: attr.Value.Uint64()}
		case slog.KindFloat64:
			value = Value{Kind: "float64", Float64: attr.Value.Float64()}
		case slog.KindTime:
			value = Value{Kind: "time", Time: attr.Value.Time().UTC().Round(0)}
		case slog.KindDuration:
			value = Value{Kind: "duration", Duration: attr.Value.Duration()}
		case slog.KindGroup:
			value = Value{Kind: "map", Map: attributesValues(attr.Value.Group())}
		case slog.KindAny:
			if raw, ok := attr.Value.Any().(Value); ok {
				value = copyValue(raw)
			}
		}
		result = append(result, Attribute{Key: strings.Clone(attr.Key), Value: value})
	}
	return result
}

// Legacy input accounting charges 32 bytes per attribute. Retained With views
// additionally reserve the larger slog.Attr header without narrowing that input.
func retainedAttributeHeaders(attributes []slog.Attr) int {
	bytes := 8 * len(attributes)
	for _, attribute := range attributes {
		if attribute.Value.Kind() == slog.KindGroup {
			bytes += retainedAttributeHeaders(attribute.Value.Group())
		}
	}
	return bytes
}

func appendValue(event *sdk.Event, key string, value Value) {
	switch value.Kind {
	case "", "null":
		event.RawJSON(key, []byte("null"))
	case "bool":
		event.Bool(key, value.Bool)
	case "int64":
		event.Int64(key, value.Int64)
	case "uint64":
		event.Uint64(key, value.Uint64)
	case "float32":
		event.RawJSON(key, float32JSON(value.Float32))
	case "float64":
		event.RawJSON(key, strconv.AppendFloat(nil, value.Float64, 'g', -1, 64))
	case "string":
		event.Str(key, value.String)
	case "bytes":
		event.Str(key, base64.StdEncoding.EncodeToString(value.Bytes))
	case "bytestring":
		event.Bytes(key, value.Bytes)
	case "time":
		event.Str(key, value.Time.Format(time.RFC3339Nano))
	case "duration":
		event.Int64(key, int64(value.Duration))
	case "array":
		event.Array(key, valueArray(event, value.Array))
	case "map":
		group := event.CreateDict()
		for _, attr := range value.Map {
			appendValue(group, attr.Key, attr.Value)
		}
		event.Dict(key, group)
	}
}

func valueArray(parent *sdk.Event, values []Value) *sdk.Array {
	array := parent.CreateArray()
	for _, value := range values {
		switch value.Kind {
		case "", "null":
			array.RawJSON([]byte("null"))
		case "bool":
			array.Bool(value.Bool)
		case "int64":
			array.Int64(value.Int64)
		case "uint64":
			array.Uint64(value.Uint64)
		case "float32":
			array.RawJSON(float32JSON(value.Float32))
		case "float64":
			array.RawJSON(strconv.AppendFloat(nil, value.Float64, 'g', -1, 64))
		case "string":
			array.Str(value.String)
		case "bytes":
			array.Str(base64.StdEncoding.EncodeToString(value.Bytes))
		case "bytestring":
			array.Bytes(value.Bytes)
		case "time":
			array.Str(value.Time.Format(time.RFC3339Nano))
		case "duration":
			array.Int64(int64(value.Duration))
		case "array":
			array.RawJSON(appendClosedJSON(nil, value))
		case "map":
			group := parent.CreateDict()
			for _, attr := range value.Map {
				appendValue(group, attr.Key, attr.Value)
			}
			array.Dict(group)
		}
	}
	return array
}

// Only library-produced fragments from validated closed values reach RawJSON.
// Float32 uses the standard finite numeric encoder, matching the selected native
// default precision without consulting zerolog's process-global precision knob.
func float32JSON(value float32) []byte {
	data, _ := json.Marshal(value)
	return data
}

func jsonString(value string) []byte {
	data, _ := json.Marshal(value)
	return data
}

func appendClosedJSON(buffer []byte, value Value) []byte {
	switch value.Kind {
	case "", "null":
		return append(buffer, "null"...)
	case "bool":
		return strconv.AppendBool(buffer, value.Bool)
	case "int64":
		return strconv.AppendInt(buffer, value.Int64, 10)
	case "uint64":
		return strconv.AppendUint(buffer, value.Uint64, 10)
	case "float32":
		return append(buffer, float32JSON(value.Float32)...)
	case "float64":
		return strconv.AppendFloat(buffer, value.Float64, 'g', -1, 64)
	case "string":
		return append(buffer, jsonString(value.String)...)
	case "bytes":
		return append(buffer, jsonString(base64.StdEncoding.EncodeToString(value.Bytes))...)
	case "bytestring":
		return append(buffer, jsonString(string(value.Bytes))...)
	case "time":
		return append(buffer, jsonString(value.Time.Format(time.RFC3339Nano))...)
	case "duration":
		return strconv.AppendInt(buffer, int64(value.Duration), 10)
	case "array":
		buffer = append(buffer, '[')
		for index, item := range value.Array {
			if index > 0 {
				buffer = append(buffer, ',')
			}
			buffer = appendClosedJSON(buffer, item)
		}
		return append(buffer, ']')
	case "map":
		buffer = append(buffer, '{')
		for index, field := range value.Map {
			if index > 0 {
				buffer = append(buffer, ',')
			}
			buffer = append(buffer, jsonString(field.Key)...)
			buffer = append(buffer, ':')
			buffer = appendClosedJSON(buffer, field.Value)
		}
		return append(buffer, '}')
	}
	return buffer
}
