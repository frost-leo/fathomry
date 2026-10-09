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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	sdk "github.com/rs/zerolog"
)

func TestClosedDataActualFileAndDetachedRecord(t *testing.T) {
	opts := fileOptions(t, false, 2)
	opts.MaxRecordBytes, opts.Sinks[0].File.MaxBytes = 16<<10, 16<<10
	collector := &recordCollector{}
	opts.Sinks = append(opts.Sinks, SinkV1{Name: "record", Records: collector})
	fixture := bindFixture(t, opts, 1)
	binary := []byte{0, 255, 128}
	array := []Value{{}, {Kind: "int64", Int64: math.MaxInt64}, {Kind: "uint64", Uint64: math.MaxUint64},
		{Kind: "float32", Float32: 1.2}, {Kind: "float64", Float64: 1.25}, {Kind: "bytes", Bytes: binary},
		{Kind: "bytestring", Bytes: []byte("a\x00文")}, {Kind: "array"}, {Kind: "map"},
		{Kind: "array", Array: []Value{{Kind: "map", Map: []Attribute{{Key: "inside", Value: Value{Kind: "bytes", Bytes: binary}}}}}}}
	attrs := []slog.Attr{slog.Any("values", Value{Kind: "array", Array: array}),
		slog.Any("binary", Value{Kind: "bytes", Bytes: binary}), slog.Any("nil", Value{}),
		slog.Any("map", Value{Kind: "map", Map: []Attribute{{Key: "nul\x00key", Value: Value{Kind: "string", String: "a\x00b"}},
			{Key: "true", Value: Value{Kind: "bool", Bool: true}}, {Key: "duration", Value: Value{Kind: "duration", Duration: -3}},
			{Key: "time", Value: Value{Kind: "time", Time: time.Date(1960, 1, 2, 3, 4, 5, 6, time.FixedZone("offset", 3600))}}}})}
	if result := emit(t, fixture, "closed", Info, "data", attrs...); result.Err() != nil {
		t.Fatal(result.Err())
	}
	wire := readFileRecords(t, filepath.Join(opts.Sinks[0].File.Directory, activeName))
	if len(wire) != 1 || len(collector.records) != 1 {
		t.Fatal("local and structured effects were not independently observed")
	}
	values := wire[0]["attributes"].(map[string]any)
	expected := []any{nil, json.Number("9223372036854775807"), json.Number("18446744073709551615"),
		json.Number("1.2"), json.Number("1.25"), "AP+A", "a\x00文", []any{}, map[string]any{},
		[]any{map[string]any{"inside": "AP+A"}}}
	if !reflect.DeepEqual(values["values"], expected) || values["binary"] != "AP+A" || values["nil"] != nil {
		t.Fatal("closed collection, integer or binary semantics changed", values)
	}
	mapped := values["map"].(map[string]any)
	if mapped["nul\x00key"] != "a\x00b" || mapped["true"] != true || mapped["duration"] != json.Number("-3") || mapped["time"] != "1960-01-02T02:04:05.000000006Z" {
		t.Fatal("map key/scalar/time semantics changed", mapped)
	}
	first, err := AttributesValues(collector.records[0].AttributesCopy(), opts.MaxRecordBytes)
	if err != nil || first[0].Value.Array[3].Kind != "float32" || first[0].Value.Array[5].Kind != "bytes" || first[0].Value.Array[6].Kind != "bytestring" {
		t.Fatal("structured data erased native value kinds", err)
	}
	binary[0], array[1].Int64 = 99, 0
	attrs[0] = slog.Any("changed", nil)
	first[0].Value.Array[5].Bytes[0] = 88
	first[3].Value.Map[0].Key = "changed"
	second, err := AttributesValues(collector.records[0].AttributesCopy(), opts.MaxRecordBytes)
	if err != nil || second[0].Value.Array[1].Int64 != math.MaxInt64 || second[0].Value.Array[5].Bytes[0] != 0 || second[3].Value.Map[0].Key != "nul\x00key" {
		t.Fatal("input or exposed snapshot mutated retained record", err)
	}
	drain(t, fixture.inbox)
}

func TestClosedDataBoundsRefuseBeforeAdmission(t *testing.T) {
	var output bytes.Buffer
	fixture := bindFixture(t, OptionsV1{Name: "bounds", Sinks: []SinkV1{{Name: "out", Writer: &output}}}, 1)
	for name, value := range map[string]Value{
		"unknown": {Kind: "callback"}, "inactive": {Kind: "null", String: "hidden"},
		"inactive-empty": {Kind: "bool", Bytes: []byte{}}, "inactive-nan": {Kind: "bool", Float64: math.NaN()},
		"inactive-zero-zone": {Kind: "bool", Time: time.Time{}.In(time.FixedZone(strings.Repeat("private-zone", 4096), 0))},
		"nan32":              {Kind: "float32", Float32: float32(math.NaN())}, "inf64": {Kind: "float64", Float64: math.Inf(-1)},
		"utf8": {Kind: "string", String: "\xff"}, "bytestring": {Kind: "bytestring", Bytes: []byte{255}},
		"time":      {Kind: "time", Time: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
		"duplicate": {Kind: "map", Map: []Attribute{{Key: "same"}, {Key: "same"}}},
		"badkey":    {Kind: "map", Map: []Attribute{{Key: "\xff"}}},
		"large":     {Kind: "string", String: "\xff" + strings.Repeat("a", 16<<10)},
	} {
		t.Run(name, func(t *testing.T) {
			receipt, err := fixture.logger.Log(context.Background(), correlation(name), Info, "", slog.Any("value", value))
			if receipt != nil || err == nil || output.Len() != 0 || fixture.inbox.Usage().Outstanding != 0 {
				t.Fatal("invalid closed data reached output/admission", err)
			}
			if name == "large" && !errors.Is(err, ErrLimit) {
				t.Fatal("oversized data was scanned before size refusal", err)
			}
		})
	}
	depth := Value{Kind: "bool"}
	for range MaxDepth - 1 {
		depth = Value{Kind: "array", Array: []Value{depth}}
	}
	if _, err := FreezeAttributes([]slog.Attr{slog.Any("depth", depth)}, 1<<20); err != nil {
		t.Fatal("exact depth positive control", err)
	}
	depth = Value{Kind: "array", Array: []Value{depth}}
	if _, err := FreezeAttributes([]slog.Attr{slog.Any("depth", depth)}, 1<<20); !errors.Is(err, ErrLimit) {
		t.Fatal("depth not bounded", err)
	}
	for _, count := range []int{MaxNodes - 1, MaxNodes} {
		_, err := FreezeAttributes([]slog.Attr{slog.Any("nodes", Value{Kind: "array", Array: make([]Value, count)})}, 1<<20)
		if (err == nil) != (count == MaxNodes-1) {
			t.Fatal("cumulative value node boundary changed", count, err)
		}
	}
	fields := make([]Attribute, MaxAttributes)
	for index := range fields {
		fields[index].Key = strconv.Itoa(index)
	}
	if _, err := FreezeAttributes([]slog.Attr{slog.Any("map", Value{Kind: "map", Map: fields[:MaxAttributes-1]})}, 1<<20); err != nil {
		t.Fatal("exact total attribute positive control", err)
	}
	if _, err := FreezeAttributes([]slog.Attr{slog.Any("map", Value{Kind: "map", Map: fields})}, 1<<20); !errors.Is(err, ErrLimit) {
		t.Fatal("nested map escaped total attribute ceiling", err)
	}
	cycle := []Value{{Kind: "array"}}
	cycle[0].Array = cycle
	if _, err := FreezeAttributes([]slog.Attr{slog.Any("cycle", cycle[0])}, 1<<20); !errors.Is(err, ErrLimit) {
		t.Fatal("cyclic caller slices were not bounded", err)
	}
}

func TestClosedFloat32MatchesNativeDefaultWithoutGlobalPrecision(t *testing.T) {
	old := sdk.FloatingPointPrecision
	t.Cleanup(func() { sdk.FloatingPointPrecision = old })
	for _, value := range []float32{1.2, -1.2, 1e-7, 1e-6, 1e20, 1e21, math.SmallestNonzeroFloat32, math.MaxFloat32, float32(math.Copysign(0, -1))} {
		var expected bytes.Buffer
		sdk.FloatingPointPrecision = -1
		logger := sdk.New(&expected)
		logger.Log().Float32("scalar", value).Array("array", sdk.Arr().Float32(value)).Send()
		sdk.FloatingPointPrecision = 0
		data := &recordData{level: Info, attributes: []slog.Attr{slog.Any("scalar", Value{Kind: "float32", Float32: value}),
			slog.Any("array", Value{Kind: "array", Array: []Value{{Kind: "float32", Float32: value}}})}}
		if err := encodeRecord(context.Background(), data, 4096); err != nil {
			t.Fatal(err)
		}
		actual := decodeRecords(t, []byte(data.json))[0]["attributes"]
		if !reflect.DeepEqual(actual, decodeRecords(t, expected.Bytes())[0]) {
			t.Fatal("float32 differed from selected native default or consulted global precision", value, actual)
		}
	}
}

func TestLegacyScalarExactInputBudgetPreserved(t *testing.T) {
	var nativeBytes, actual bytes.Buffer
	baseline := sdk.New(&nativeBytes)
	event := baseline.Log()
	dictionary := event.CreateDict()
	attrs := make([]slog.Attr, MaxAttributes)
	const message = "message"
	limit := len(message)
	for index := range attrs {
		key := strconv.Itoa(index)
		attrs[index] = slog.Bool(key, true)
		limit += len(key) + 32
		dictionary.Bool(key, true)
	}
	event.Dict("attributes", dictionary).Send()
	fixture := bindFixture(t, OptionsV1{Name: "legacy-limit", MaxRecordBytes: limit,
		Sinks: []SinkV1{{Name: "local", Writer: &actual}}}, 1)
	if result := emit(t, fixture, "legacy", Info, message, attrs...); result.Err() != nil {
		t.Fatal("previously admitted exact-limit native scalars were narrowed", result.Err())
	}
	if actual.Len() > limit {
		t.Fatal("positive control exceeded the final encoded-record limit")
	}
	want := decodeRecords(t, nativeBytes.Bytes())[0]["attributes"]
	if got := decodeRecords(t, actual.Bytes())[0]["attributes"]; !reflect.DeepEqual(got, want) {
		t.Fatal("native scalar control changed in actual local JSON")
	}
	drain(t, fixture.inbox)
	view, err := fixture.logger.With(attrs...)
	if err != nil {
		t.Fatal("retained-header accounting narrowed the legacy input contract", err)
	}
	expectedCharge := int64(limit-len(message)+8*MaxAttributes) + 256
	if fixture.logger.owner.derivationBytes != expectedCharge || expectedCharge > fixture.logger.owner.prepared.Metadata().ViewBytes {
		t.Fatal("retained header accounting or declared view envelope was incomplete")
	}
	receipt, err := view.Log(context.Background(), correlation("legacy-derived"), Info, message)
	if result := observed(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if records := decodeRecords(t, actual.Bytes()); len(records) != 2 || !reflect.DeepEqual(records[1]["attributes"], want) {
		t.Fatal("retained legacy fields lost native scalar semantics")
	}
	drain(t, fixture.inbox)
}

func FuzzClosedRecordBoundary(f *testing.F) {
	f.Add("key", "text", []byte{0, 255}, uint64(0), uint8(1))
	f.Add("nul\x00key", "\xff", []byte("valid"), uint64(0x7ff0000000000000), uint8(9))
	f.Fuzz(func(t *testing.T, key, text string, raw []byte, bits uint64, depth uint8) {
		if len(key)+len(text)+len(raw) > 8192 {
			return
		}
		value := Value{Kind: "map", Map: []Attribute{{Key: key, Value: Value{Kind: "string", String: text}},
			{Key: "binary", Value: Value{Kind: "bytes", Bytes: raw}}, {Key: "number", Value: Value{Kind: "float64", Float64: math.Float64frombits(bits)}}}}
		for range int(depth % 10) {
			value = Value{Kind: "array", Array: []Value{value}}
		}
		attrs, err := FreezeAttributes([]slog.Attr{slog.Any("closed", value)}, 8192)
		if err != nil {
			return
		}
		data := &recordData{level: Info, attributes: attrs}
		if err := encodeRecord(context.Background(), data, 8192); err != nil {
			if !errors.Is(err, ErrLimit) {
				t.Fatal("admitted closed value failed encoding", err)
			}
			return
		}
		if !json.Valid([]byte(data.json)) || len(data.json) > 8192 {
			t.Fatal("admitted closed record was invalid or unbounded")
		}
		if _, err := AttributesValues(attrs, 8192); err != nil {
			t.Fatal("frozen data failed semantic snapshot", err)
		}
	})
}
