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
	"errors"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

type badValuer struct{ called *bool }

func (value badValuer) LogValue() slog.Value { *value.called = true; panic("unexpected callback") }

func TestDataTypesCopiesAndRefusals(t *testing.T) {
	bytes := []byte{0, 1, 255}
	integers := []int64{1, 2}
	names := []string{"original"}
	attrs := []slog.Attr{slog.String("string", "text"), slog.Int64("integer", 9), slog.Bool("bool", true), slog.Float64("number", 1.5),
		slog.Uint64("unsigned", 7), slog.Duration("duration", time.Millisecond), slog.Time("time", time.Unix(1, 2)),
		slog.Any("nil", nil), slog.Any("bytes", bytes), slog.Any("integers", integers), slog.Any("strings", names),
		slog.Any("bools", []bool{true, false}), slog.Any("floats", []float64{1.5}), slog.Group("group", slog.String("child", "nested"))}
	budget := dataBudget{bytes: 4096, nodes: MaxNodes}
	frozen, err := freezeAttributes(attrs, &budget, 1)
	if err != nil {
		t.Fatal(err)
	}
	bytes[0] = 9
	integers[0] = 8
	names[0] = "mutated"
	attrs[0] = slog.String("string", "mutated")
	if frozen[0].Value.AsString() != "text" || frozen[8].Value.AsByteSlice()[0] != 0 || frozen[9].Value.AsInt64Slice()[0] != 1 ||
		frozen[10].Value.AsStringSlice()[0] != "original" || frozen[13].Value.AsMap()[0].Value.AsString() != "nested" {
		t.Fatal("input aliases retained")
	}
	expected := []attribute.Type{attribute.STRING, attribute.INT64, attribute.BOOL, attribute.FLOAT64, attribute.INT64, attribute.INT64, attribute.STRING,
		attribute.EMPTY, attribute.BYTESLICE, attribute.INT64SLICE, attribute.STRINGSLICE, attribute.BOOLSLICE, attribute.FLOAT64SLICE, attribute.MAP}
	for index, kind := range expected {
		if frozen[index].Value.Type() != kind {
			t.Errorf("attribute %d changed type", index)
		}
	}
	called := false
	for _, attrs := range [][]slog.Attr{
		{slog.Any("value", badValuer{&called})}, {slog.Any("value", errors.New("private-canary"))},
		{slog.Uint64("value", math.MaxUint64)}, {slog.Float64("value", math.NaN())}, {slog.Any("value", []float64{math.Inf(1)})},
		{slog.String("same", "a"), slog.String("same", "b")}, {slog.String("fathomry.call", "override")}, {slog.String("", "invalid")},
		{slog.String("invalid", "\xff")}, {slog.Any("value", make([]int64, MaxNodes+1))},
	} {
		budget := dataBudget{bytes: 4096, nodes: MaxNodes}
		if _, err := freezeAttributes(attrs, &budget, 1); err == nil {
			t.Fatal("invalid data accepted")
		}
	}
	if called {
		t.Fatal("user diagnostic callback invoked")
	}
	nested := slog.String("leaf", "value")
	for range MaxDepth {
		nested = slog.Group("group", nested)
	}
	budget = dataBudget{bytes: 4096, nodes: MaxNodes}
	if _, err := freezeAttributes([]slog.Attr{nested}, &budget, 1); !errors.Is(err, ErrLimit) {
		t.Fatal("depth limit missing")
	}
	budget = dataBudget{bytes: 32, nodes: MaxNodes}
	if _, err := freezeAttributes([]slog.Attr{slog.String("large", strings.Repeat("x", 64))}, &budget, 1); !errors.Is(err, ErrLimit) {
		t.Fatal("byte limit missing")
	}
}

func TestAttributeMapPreservesEmptyMapsAndCopies(t *testing.T) {
	bytes := []byte{0, 255}
	integers := []int64{1, 2}
	child := AttributeMap{
		slog.Group("empty-group"), slog.Any("empty-map", AttributeMap{}), slog.Any("nil-map", AttributeMap(nil)),
		slog.Any("null", nil), slog.String("text", "original"), slog.Any("bytes", bytes), slog.Any("integers", integers),
	}
	outer := AttributeMap{slog.Any("child", child)}
	input := []slog.Attr{slog.Any("nil", AttributeMap(nil)), slog.Any("empty", AttributeMap{}), slog.Any("outer", outer)}
	if input[2].Value.Kind() != slog.KindAny {
		t.Fatal("explicit map was normalized by slog before freezing")
	}
	ordinary := slog.Any("ordinary", []slog.Attr{slog.Group("empty")})
	if ordinary.Value.Kind() != slog.KindGroup || len(ordinary.Value.Group()) != 0 {
		t.Fatal("native empty-group normalization control changed")
	}
	budget := dataBudget{bytes: 4096, nodes: MaxNodes}
	frozen, err := freezeAttributes(input, &budget, 1)
	if err != nil {
		t.Fatal(err)
	}
	bytes[0] = 9
	integers[0] = 9
	child[0] = slog.String("changed", "value")
	child[4] = slog.String("text", "changed")
	outer[0] = slog.String("changed", "value")
	input[2] = slog.String("outer", "changed")
	if len(frozen) != 3 {
		t.Fatal("explicit maps disappeared")
	}
	for _, index := range []int{0, 1} {
		if frozen[index].Value.Type() != attribute.MAP || len(frozen[index].Value.AsMap()) != 0 {
			t.Fatal("nil or empty explicit map became an absent/null attribute")
		}
	}
	expected := attribute.MapValue(attribute.KeyValue{Key: "child", Value: attribute.MapValue(
		attribute.KeyValue{Key: "empty-group", Value: attribute.MapValue()},
		attribute.KeyValue{Key: "empty-map", Value: attribute.MapValue()},
		attribute.KeyValue{Key: "nil-map", Value: attribute.MapValue()},
		attribute.KeyValue{Key: "null"}, attribute.String("text", "original"),
		attribute.KeyValue{Key: "bytes", Value: attribute.ByteSliceValue([]byte{0, 255})},
		attribute.Int64Slice("integers", []int64{1, 2}),
	)})
	if !reflect.DeepEqual(frozen[2].Value, expected) {
		t.Fatal("nested explicit map lost empty children, value types or independent storage")
	}
}

func TestAttributeMapRejectsCyclesAndRespectsBounds(t *testing.T) {
	self := make(AttributeMap, 1)
	self[0] = slog.Any("self", self)
	for name, nodes := range map[string]int{"depth": MaxNodes, "nodes": 1} {
		t.Run(name, func(t *testing.T) {
			budget := dataBudget{bytes: 64 << 10, nodes: nodes}
			if _, err := freezeAttributes([]slog.Attr{slog.Any("map", self)}, &budget, 1); !errors.Is(err, ErrLimit) {
				t.Fatal("self-referential explicit map did not stop at the input bound")
			}
		})
	}
	deepest := slog.String("leaf", "value")
	for range MaxDepth - 1 {
		deepest = slog.Any("map", AttributeMap{deepest})
	}
	budget := dataBudget{bytes: 4096, nodes: MaxNodes}
	if _, err := freezeAttributes([]slog.Attr{deepest}, &budget, 1); err != nil {
		t.Fatal("explicit map at the depth ceiling was rejected")
	}
	budget = dataBudget{bytes: 4096, nodes: MaxNodes}
	if _, err := freezeAttributes([]slog.Attr{slog.Any("map", AttributeMap{deepest})}, &budget, 1); !errors.Is(err, ErrLimit) {
		t.Fatal("explicit map exceeded the depth ceiling")
	}
	for _, invalid := range []AttributeMap{
		make(AttributeMap, MaxAttributes+1),
		{slog.String("same", "one"), slog.String("same", "two")},
		{slog.String("fathomry.call", "override")},
	} {
		budget := dataBudget{bytes: 4096, nodes: MaxNodes}
		if _, err := freezeAttributes([]slog.Attr{slog.Any("map", invalid)}, &budget, 1); err == nil {
			t.Fatal("explicit map bypassed attribute validation")
		}
	}
}

func FuzzDataBoundaries(f *testing.F) {
	f.Add("key", "value", 0)
	f.Add("fathomry.call", "secret", 8)
	f.Fuzz(func(t *testing.T, key, value string, depth int) {
		if len(key)+len(value) > 1<<16 {
			return
		}
		depth = int(uint(depth) % 12)
		attr := slog.String(key, value)
		for range depth {
			attr = slog.Group("nested", attr)
		}
		budget := dataBudget{bytes: 4096, nodes: MaxNodes}
		_, _ = freezeAttributes([]slog.Attr{attr}, &budget, 1)
	})
}
