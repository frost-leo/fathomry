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
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func decodedFields(t *testing.T, fields []zapcore.Field) map[string]any {
	t.Helper()
	encoded, err := zapcore.NewJSONEncoder(encoderConfig()).EncodeEntry(zapcore.Entry{}, fields)
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Free()
	decoder := json.NewDecoder(bytes.NewReader(encoded.Bytes()))
	decoder.UseNumber()
	var record map[string]any
	if err := decoder.Decode(&record); err != nil {
		t.Fatal(err)
	}
	return record
}

func TestClosedFieldsPreserveNativeEncodingAndIndependentStorage(t *testing.T) {
	binary := []byte{0, 255}
	value := Value{Kind: "map", Map: []Attribute{
		{Key: "null"}, {Key: "empty_array", Value: Value{Kind: "array"}}, {Key: "empty_map", Value: Value{Kind: "map"}},
		{Key: "min", Value: Value{Kind: "int64", Int64: math.MinInt64}},
		{Key: "max", Value: Value{Kind: "uint64", Uint64: math.MaxUint64}},
		{Key: "float32", Value: Value{Kind: "float32", Float32: 1.2}},
		{Key: "float64", Value: Value{Kind: "float64", Float64: 1.25}},
		{Key: "duration", Value: Value{Kind: "duration", Duration: 3 * time.Nanosecond}},
		{Key: "zero_time", Value: Value{Kind: "time"}},
		{Key: "binary", Value: Value{Kind: "bytes", Bytes: binary}},
		{Key: "array", Value: Value{Kind: "array", Array: []Value{
			{Kind: "bytes", Bytes: binary}, {}, {Kind: "float32", Float32: 1.2}, {Kind: "uint64", Uint64: math.MaxUint64},
			{Kind: "string", String: "nul\x00text"}, {Kind: "map", Map: []Attribute{{Key: "msg", Value: Value{Kind: "string", String: "nested"}}}},
		}}},
	}}
	frozen, err := FreezeFields([]zapcore.Field{Field("payload", value), sdk.Stringp("nil_pointer", nil), sdk.Reflect("literal_nil", nil)})
	if err != nil {
		t.Fatal(err)
	}
	binary[0] = 9
	value.Map[0].Key = "changed"
	value.Map[10].Value.Array[4].String = "changed"
	check := func(fields []zapcore.Field) {
		record := decodedFields(t, fields)
		payload := record["payload"].(map[string]any)
		if payload["null"] != nil || len(payload["empty_array"].([]any)) != 0 || len(payload["empty_map"].(map[string]any)) != 0 ||
			payload["min"] != json.Number("-9223372036854775808") || payload["max"] != json.Number("18446744073709551615") ||
			payload["float32"] != json.Number("1.2") || payload["float64"] != json.Number("1.25") || payload["duration"] != json.Number("3") ||
			payload["zero_time"] != "0001-01-01T00:00:00Z" || payload["binary"] != "AP8=" {
			t.Fatal("closed field changed null/container/scalar/binary/time semantics", payload)
		}
		array := payload["array"].([]any)
		if array[0] != "AP8=" || array[1] != nil || array[2] != json.Number("1.2") || array[3] != json.Number("18446744073709551615") ||
			array[4] != "nul\x00text" || array[5].(map[string]any)["msg"] != "nested" {
			t.Fatal("array binary was coerced to UTF-8 or its typed values changed", array)
		}
		for _, key := range []string{"nil_pointer", "literal_nil"} {
			if actual, exists := record[key]; !exists || actual != nil {
				t.Fatal("native nil helper did not remain present null", key)
			}
		}
	}
	check(frozen)
	detached, err := FieldsValues(frozen)
	if err != nil {
		t.Fatal(err)
	}
	if detached[0].Value.Map[5].Value.Kind != "float32" || detached[0].Value.Map[4].Value.Uint64 != math.MaxUint64 ||
		!bytes.Equal(detached[0].Value.Map[10].Value.Array[0].Bytes, []byte{0, 255}) {
		t.Fatal("semantic snapshot lost native float32/uint64/binary representation")
	}
	detached[0].Value.Map[9].Value.Bytes[0] = 8
	detached[0].Value.Map[10].Value.Array[0].Bytes[0] = 8
	detached[0].Value.Map[0].Key = "changed"
	check(frozen)
}

func TestNativeScalarSemanticSnapshotDoesNotReinterpretWidths(t *testing.T) {
	fields := []zapcore.Field{
		{Key: "bool", Type: zapcore.BoolType, Integer: 2}, {Key: "int8", Type: zapcore.Int8Type, Integer: 255},
		{Key: "int16", Type: zapcore.Int16Type, Integer: 65535}, {Key: "uint8", Type: zapcore.Uint8Type, Integer: -1},
		sdk.Uint64("uint64", math.MaxUint64), sdk.Float32("float32", 1.2), sdk.ByteString("text", []byte("nul\x00text")),
	}
	frozen, err := FreezeFields(fields)
	if err != nil {
		t.Fatal(err)
	}
	for index := range fields {
		if frozen[index].Type != fields[index].Type || frozen[index].Integer != fields[index].Integer {
			t.Fatal("freezing replaced native scalar tags or integer bits")
		}
	}
	values, err := FieldsValues(frozen)
	if err != nil {
		t.Fatal(err)
	}
	if values[0].Value.Bool || values[1].Value.Int64 != -1 || values[2].Value.Int64 != -1 || values[3].Value.Uint64 != 255 ||
		values[4].Value.Uint64 != math.MaxUint64 || values[5].Value.Kind != "float32" || values[6].Value.Kind != "bytestring" || string(values[6].Value.Bytes) != "nul\x00text" {
		t.Fatal("structured semantic conversion differs from native AddTo")
	}
	encoded := decodedFields(t, frozen)
	if encoded["bool"] != false || encoded["int8"] != json.Number("-1") || encoded["uint8"] != json.Number("255") || encoded["float32"] != json.Number("1.2") {
		t.Fatal("native scalar baseline was not preserved", encoded)
	}
}

func TestClosedByteStringIdentityCopyAndArrayEncoding(t *testing.T) {
	data := []byte("text\x00utf8")
	fields, err := FreezeFields([]zapcore.Field{
		Field("text", Value{Kind: "bytestring", Bytes: data}),
		Field("array", Value{Kind: "array", Array: []Value{{Kind: "bytestring", Bytes: data}, {Kind: "bytestring"}}}),
	})
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 'X'
	if fields[0].Type != zapcore.ByteStringType {
		t.Fatal("closed byte-string changed native field tag")
	}
	values, err := FieldsValues(fields)
	if err != nil || values[0].Value.Kind != "bytestring" || values[1].Value.Array[0].Kind != "bytestring" {
		t.Fatal("structured snapshot normalized native byte-string into another kind", err)
	}
	values[0].Value.Bytes[0] = 'X'
	values[1].Value.Array[0].Bytes[0] = 'X'
	encoded := decodedFields(t, fields)
	array := encoded["array"].([]any)
	if encoded["text"] != "text\x00utf8" || array[0] != "text\x00utf8" || array[1] != "" {
		t.Fatal("byte-string data was aliased or encoded as binary/base64", encoded)
	}
}

type forbiddenArray struct{ called *bool }

func (value forbiddenArray) MarshalLogArray(zapcore.ArrayEncoder) error {
	*value.called = true
	panic("must not invoke caller array marshaler")
}

func TestClosedFieldsRefuseCallbacksAmbiguityAndBounds(t *testing.T) {
	called := false
	for _, field := range []zapcore.Field{
		sdk.Array("callback", forbiddenArray{&called}), sdk.Object("callback", hostileValue{&called}),
		sdk.Reflect("typed_nil", (*string)(nil)), sdk.Reflect("object", map[string]int{"value": 1}),
		Field("value", Value{Kind: "bool", String: "ambiguous"}), Field("value", Value{Kind: "string", Array: []Value{}}),
		Field("value", Value{Kind: "float64", Float64: math.NaN()}), Field("value", Value{Kind: "float32", Float32: float32(math.Inf(1))}),
		Field("value", Value{Kind: "string", String: "\xff"}), Field("value", Value{Kind: "bytes", Bytes: make([]byte, MaxFieldBytes)}),
		Field("value", Value{Kind: "bytestring", Bytes: []byte{255}}),
		Field("value", Value{Kind: "array", Array: make([]Value, MaxNodes)}),
		Field("value", Value{Kind: "map", Map: []Attribute{{Key: "duplicate"}, {Key: "duplicate"}}}),
		Field("value", Value{Kind: "map", Map: []Attribute{{Key: ""}}}),
	} {
		if _, err := FreezeFields([]zapcore.Field{field}); err == nil {
			t.Fatal("unsupported or unbounded field was frozen", field.Type)
		}
		if _, err := FieldsValues([]zapcore.Field{field}); err == nil {
			t.Fatal("unsupported or unbounded field entered semantic snapshot", field.Type)
		}
	}
	if called {
		t.Fatal("caller callback executed")
	}
	cycle := Value{Kind: "array", Array: make([]Value, 1)}
	cycle.Array[0] = cycle
	if _, err := FreezeFields([]zapcore.Field{Field("value", cycle)}); !errors.Is(err, ErrLimit) {
		t.Fatal("cycle bypassed depth bound")
	}
	deep := Value{Kind: "bool", Bool: true}
	for range MaxDepth - 1 {
		deep = Value{Kind: "array", Array: []Value{deep}}
	}
	if _, err := FreezeFields([]zapcore.Field{Field("value", deep)}); err != nil {
		t.Fatal("value at depth ceiling refused", err)
	}
	if _, err := FreezeFields([]zapcore.Field{Field("value", Value{Kind: "array", Array: []Value{deep}})}); !errors.Is(err, ErrLimit) {
		t.Fatal("value exceeded depth ceiling")
	}
	if _, err := FreezeFields([]zapcore.Field{sdk.String("same", "one"), Field("same", Value{})}); err == nil {
		t.Fatal("native/closed duplicate key accepted")
	}
	if _, err := FreezeFields([]zapcore.Field{Field("fathomry.call", Value{})}); err == nil {
		t.Fatal("closed field spoofed fixed metadata")
	}
}

func TestStructuredClosedCopiesAndSafeNativeErrorSnapshot(t *testing.T) {
	sink := &recordingSink{}
	fixture := bindFixture(t, OptionsV1{Name: "closed"}, sink, 2)
	view, err := fixture.logger.With(Field("object", Value{Kind: "map", Map: []Attribute{{Key: "bytes", Value: Value{Kind: "bytes", Bytes: []byte{0, 255}}}}}))
	if err != nil {
		t.Fatal(err)
	}
	original := fault.Kind("safe.error").New(fault.Context{Operation: "read"}, errors.New("private-cause-canary"))
	if result := logResult(t, view, "first", sdk.Error(original)); result.Err() != nil {
		t.Fatal(result.Err())
	}
	sink.records[0].fields[0].Interface.(frozenMap)[0].Value.Bytes[0] = 9
	if result := logResult(t, view, "second", sdk.Error(original)); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if value := decodedFields(t, sink.records[1].fields)["object"].(map[string]any)["bytes"]; value != "AP8=" {
		t.Fatal("structured callback mutated retained derived data", value)
	}
	values, err := FieldsValues(sink.records[1].fields)
	if err != nil {
		t.Fatal("snapshot rejected fixed source metadata", err)
	}
	encoded, err := json.Marshal(values)
	if err != nil || strings.Contains(string(encoded), "private-cause-canary") || !strings.Contains(string(encoded), "safe.error") {
		t.Fatal("safe snapshot formatted or lost native error diagnostics", err)
	}
}

func FuzzClosedFieldValues(f *testing.F) {
	f.Add("bytes", "field", []byte{0, 255}, 0)
	f.Add("bytestring", "text", []byte("nul\x00text"), 1)
	f.Add("string", "nested", []byte("valid"), 3)
	f.Fuzz(func(t *testing.T, kind, key string, data []byte, depth int) {
		if len(kind)+len(key)+len(data) > MaxFieldBytes+256 {
			return
		}
		depth = int(uint(depth) % (MaxDepth + 2))
		value := Value{Kind: kind}
		switch kind {
		case "bytes":
			value.Bytes = data
		case "bytestring":
			value.Bytes = data
		case "string":
			value.String = string(data)
		case "array":
			value.Array = []Value{{Kind: "bytes", Bytes: data}}
		}
		for range depth {
			value = Value{Kind: "map", Map: []Attribute{{Key: "child", Value: value}}}
		}
		fields, err := FreezeFields([]zapcore.Field{Field(key, value)})
		if err == nil {
			_ = decodedFields(t, fields)
			if _, err := FieldsValues(fields); err != nil {
				t.Fatal("accepted closed fields cannot be snapshotted", err)
			}
		}
	})
}
