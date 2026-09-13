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
	"errors"
	"io"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	sdk "github.com/rs/zerolog"
)

type hostileValue struct{ called *bool }

func (value hostileValue) LogValue() slog.Value {
	*value.called = true
	return slog.StringValue("secret")
}
func (value hostileValue) MarshalJSON() ([]byte, error) {
	*value.called = true
	return []byte("null"), nil
}
func (value hostileValue) Error() string { *value.called = true; return "secret" }

func TestMalformedAndDynamicAttributesRejectBeforeAnyOutput(t *testing.T) {
	called := false
	for _, attrs := range [][]slog.Attr{
		{slog.Any("hostile", hostileValue{&called})}, {slog.Any("map", map[string]string{"key": "value"})},
		{slog.Any("bytes", []byte("secret"))}, {slog.Float64("nan", math.NaN())}, {slog.Float64("inf", math.Inf(1))},
		{slog.String("same", "one"), slog.String("same", "two")}, {slog.String("", "empty")},
		{slog.String("invalid", string([]byte{255}))}, {slog.Time("bad", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC))},
	} {
		if err := validateRecord(Info, "message", attrs, 1024); err == nil {
			t.Fatal("malformed attribute admitted")
		}
	}
	if called {
		t.Fatal("dynamic attribute callback executed")
	}
	group := slog.String("leaf", "value")
	for range MaxDepth {
		group = slog.Group("group", group)
	}
	if err := validateRecord(Info, "", []slog.Attr{group}, 16<<10); !errors.Is(err, ErrLimit) {
		t.Fatal("group depth not bounded")
	}
	var output bytes.Buffer
	f := bindFixture(t, OptionsV1{Name: "bounds", MaxRecordBytes: 1024, Sinks: []SinkV1{{Name: "out", Writer: &output}}}, 1)
	if receipt, err := f.logger.Log(context.Background(), correlation("invalid"), Info, "", slog.Any("value", hostileValue{&called})); receipt != nil || !errors.Is(err, ErrUnsupported) || f.inbox.Usage().Outstanding != 0 {
		t.Fatal("invalid input reached admission")
	}
	result := emit(t, f, "expanded", Info, strings.Repeat("\x00", 300))
	if !errors.Is(result.Err(), ErrLimit) || result.Attempts.Observed != 0 || output.Len() != 0 {
		t.Fatal("encoded expansion reached output")
	}
	drain(t, f.inbox)
	result = emit(t, f, "positive", Info, strings.Repeat("a", 300))
	if result.Err() != nil || len(decodeRecords(t, output.Bytes())) != 1 {
		t.Fatal("positive bounded control rejected")
	}
	drain(t, f.inbox)
}
func TestNativeGlobalsCannotRewriteFieldsOrInvokeCallbacks(t *testing.T) {
	oldLevel, oldMessage, oldError := sdk.LevelFieldName, sdk.MessageFieldName, sdk.ErrorHandler
	oldMarshal, oldTime, oldFloat := sdk.InterfaceMarshalFunc, sdk.TimestampFunc, sdk.FloatingPointPrecision
	oldSeverity := sdk.GlobalLevel()
	t.Cleanup(func() {
		sdk.LevelFieldName, sdk.MessageFieldName, sdk.ErrorHandler = oldLevel, oldMessage, oldError
		sdk.InterfaceMarshalFunc, sdk.TimestampFunc, sdk.FloatingPointPrecision = oldMarshal, oldTime, oldFloat
		sdk.SetGlobalLevel(oldSeverity)
	})
	called := false
	sdk.LevelFieldName, sdk.MessageFieldName = "injected-level", "injected-message"
	sdk.ErrorHandler = func(error) { called = true }
	sdk.InterfaceMarshalFunc = func(any) ([]byte, error) { called = true; return nil, errors.New("global-canary") }
	sdk.TimestampFunc = func() time.Time { called = true; return time.Time{} }
	sdk.FloatingPointPrecision = 0
	sdk.SetGlobalLevel(sdk.ErrorLevel)
	var output bytes.Buffer
	f := bindFixture(t, OptionsV1{Name: "globals", MinLevel: Trace, Sinks: []SinkV1{{Name: "out", Writer: &output}}}, 1)
	result := emit(t, f, "trace", Trace, "unaltered", slog.Float64("float", 1.25))
	record := decodeRecords(t, output.Bytes())[0]
	if result.Err() != nil || called || record["level"] != "trace" || record["message"] != "unaltered" || record["attributes"].(map[string]any)["float"].(interface{ String() string }).String() != "1.25" {
		t.Fatal("native globals changed selected encoding")
	}
	drain(t, f.inbox)
	sdk.SetGlobalLevel(sdk.Disabled)
	result = emit(t, f, "disabled", Info, "not-accepted")
	if !errors.Is(result.Err(), ErrUnsupported) || result.Attempts.Observed != 0 || called {
		t.Fatal("native suppression silently claimed delivery")
	}
	drain(t, f.inbox)
	sdk.SetGlobalLevel(oldSeverity)
}

func TestRecordSizeRefusalPrecedesUTF8Validation(t *testing.T) {
	for _, test := range []struct {
		name, message string
		attrs         []slog.Attr
	}{
		{name: "message", message: "\xff" + strings.Repeat("a", 1024)},
		{name: "attribute", attrs: []slog.Attr{slog.String("key", "\xff"+strings.Repeat("a", 1024))}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			f := bindFixture(t, OptionsV1{Name: "bounds", MaxRecordBytes: 1024, Sinks: []SinkV1{{Name: "out", Writer: &output}}}, 1)
			receipt, err := f.logger.Log(context.Background(), correlation("oversize"), Info, test.message, test.attrs...)
			if receipt != nil || !errors.Is(err, ErrLimit) || errors.Is(err, ErrInput) {
				t.Fatal("oversized input reached semantic scanning before size refusal", err)
			}
			if output.Len() != 0 || f.inbox.Usage().Outstanding != 0 {
				t.Fatal("rejected input acquired output/evidence")
			}
		})
	}
	for _, attrs := range [][]slog.Attr{nil, {slog.String("key", "\xff")}} {
		message := ""
		if attrs == nil {
			message = "\xff"
		}
		if err := validateRecord(Info, message, attrs, 1024); !errors.Is(err, ErrInput) {
			t.Fatal("within-bound invalid UTF-8 was accepted", err)
		}
	}
}

func TestNoncanonicalEmptyGroupRefusedBeforeAdmission(t *testing.T) {
	var output bytes.Buffer
	collector := &recordCollector{}
	f := bindFixture(t, OptionsV1{Name: "groups", Sinks: []SinkV1{{Name: "out", Writer: &output}, {Name: "typed", Records: collector}}}, 1)
	if result := emit(t, f, "original", Info, "message", slog.Group("outer", slog.String("child", "original"))); result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, f.inbox)
	attrs := collector.records[0].AttributesCopy()
	// Inject a noncanonical shape through test-owned storage; ordinary native
	// constructors remove empty children before this boundary.
	attrs[0].Value.Group()[0] = slog.Group("empty")
	size := output.Len()
	receipt, err := f.logger.Log(context.Background(), correlation("noncanonical"), Info, "message", attrs...)
	if receipt != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatal("noncanonical group was silently normalized or admitted", err)
	}
	if output.Len() != size || len(collector.records) != 1 || f.inbox.Usage().Outstanding != 0 {
		t.Fatal("noncanonical group had effects")
	}
	if collector.records[0].AttributesCopy()[0].Value.Group()[0].Value.String() != "original" {
		t.Fatal("independent snapshot was mutated")
	}
	attrs[0] = slog.Group("outer", slog.String("child", "replacement"))
	if result := emit(t, f, "replacement", Info, "message", attrs...); result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, f.inbox)
	if collector.records[1].AttributesCopy()[0].Value.Group()[0].Value.String() != "replacement" {
		t.Fatal("canonical replacement failed")
	}
	if result := emit(t, f, "empty", Info, "message", slog.Group("empty"), slog.Group("outer", slog.Group("removed-before-log"))); result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, f.inbox)
	last := decodeRecords(t, output.Bytes())[2]["attributes"].(map[string]any)
	if len(last) != 2 || len(last["empty"].(map[string]any)) != 0 || len(last["outer"].(map[string]any)) != 0 {
		t.Fatal("canonical top-level empty groups were lost")
	}
}

func BenchmarkOversizeRecordRejection(b *testing.B) {
	for _, size := range []int{1025, 64 << 20} {
		value := strings.Repeat("a", size)
		name := "small"
		if size > 1025 {
			name = "large"
		}
		b.Run(name+"-message", func(b *testing.B) {
			for b.Loop() {
				if err := validateRecord(Info, value, nil, 1024); !errors.Is(err, ErrLimit) {
					b.Fatal(err)
				}
			}
		})
		b.Run(name+"-attribute", func(b *testing.B) {
			attrs := []slog.Attr{slog.String("key", value)}
			for b.Loop() {
				if err := validateRecord(Info, "", attrs, 1024); !errors.Is(err, ErrLimit) {
					b.Fatal(err)
				}
			}
		})
	}
}

func FuzzRecordBoundary(f *testing.F) {
	f.Add("message", "key", "value", uint8(1))
	f.Add("\x00", "key", "\xff", uint8(9))
	f.Fuzz(func(t *testing.T, message, key, value string, depth uint8) {
		if len(message)+len(key)+len(value) > 8192 {
			return
		}
		attr := slog.String(key, value)
		for range int(depth % 12) {
			attr = slog.Group("nested", attr)
		}
		if err := validateRecord(Info, message, []slog.Attr{attr}, 4096); err != nil {
			return
		}
		data := &recordData{time: time.Now().UTC(), level: Info, message: message, correlation: correlation("fuzz"), attributes: copyAttributes([]slog.Attr{attr})}
		err := encodeRecord(context.Background(), data, 4096)
		if err != nil {
			if !errors.Is(err, ErrLimit) {
				t.Fatal("unexpected encoding error", err)
			}
			return
		}
		if len(data.json) > 4096 || len(decodeRecords(t, []byte(data.json))) != 1 {
			t.Fatal("invalid admitted JSON")
		}
	})
}
func BenchmarkControlledTwoSinks(b *testing.B) {
	options := OptionsV1{Name: "benchmark", Sinks: []SinkV1{{Name: "first", Writer: io.Discard}, {Name: "second", Writer: io.Discard}}}
	f := bindFixture(b, options, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result := emit(b, f, "bench", Info, "message", slog.Int("rows", 12), slog.String("component", "node"))
		if result.Err() != nil {
			b.Fatal(result.Err())
		}
		drain(b, f.inbox)
	}
}
