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
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	traceapi "go.opentelemetry.io/otel/trace"
	collog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

func logAttrs(record sdklog.Record) map[string]attribute.Value {
	attrs := map[string]attribute.Value{}
	record.WalkAttributes(func(attr attribute.KeyValue) bool { attrs[string(attr.Key)] = attr.Value; return true })
	return attrs
}
func TestLogsCopyCorrelationSeverityAndFixedIdentity(t *testing.T) {
	fixture := newFixture(t, OptionsV1{}, nil)
	traceID, _ := traceapi.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := traceapi.SpanIDFromHex("0102030405060708")
	contextID := traceapi.NewSpanContext(traceapi.SpanContextConfig{TraceID: traceID, SpanID: spanID})
	ctx := traceapi.ContextWithSpanContext(context.Background(), contextID)
	data := []byte{1, 2, 3}
	input := LogRecord{Message: "message", Severity: 24, EventName: "event", Attributes: []slog.Attr{slog.Any("bytes", data), slog.Group("group", slog.Bool("ok", true))}}
	receipt, err := fixture.client.Emit(ctx, fault.Correlation{Call: "call", Owner: "owner"}, input)
	result := outcomeOf(t, receipt, err)
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	input.Message = "mutated"
	data[0] = 9
	input.Attributes[0] = slog.String("bytes", "changed")
	record := fixture.owner.pendingLogs[0].record
	attrs := logAttrs(record)
	if record.Body().AsString() != "message" || record.Severity() != 24 || record.EventName() != "event" || attrs["bytes"].AsByteSlice()[0] != 1 ||
		attrs["fathomry.call"].AsString() != "call" || attrs["fathomry.owner"].AsString() != "owner" ||
		record.TraceID() != traceID || record.SpanID() != spanID || record.TraceFlags().IsSampled() {
		t.Fatal("log semantics changed")
	}
	copy := result.Outcome.Value.SignalsCopy()
	copy[0].Accepted = 0
	if result.Outcome.Value.SignalsCopy()[0].Accepted != 1 {
		t.Fatal("evidence aliasing")
	}
}
func TestLogQueueSaturationDoesNotOverwrite(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	fixture := newFixture(t, OptionsV1{QueueItems: 2}, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, _ := io.ReadAll(request.Body)
		var logs collog.ExportLogsServiceRequest
		if proto.Unmarshal(data, &logs) != nil {
			writer.WriteHeader(400)
			return
		}
		mu.Lock()
		for _, resource := range logs.ResourceLogs {
			for _, scope := range resource.ScopeLogs {
				for _, record := range scope.LogRecords {
					bodies = append(bodies, record.Body.GetStringValue())
				}
			}
		}
		mu.Unlock()
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	for _, name := range []string{"first", "second"} {
		if result := emitOne(t, fixture, name, name); result.Err() != nil {
			t.Fatal(result.Err())
		}
	}
	third := emitOne(t, fixture, "third", "third")
	if !errors.Is(third.Err(), ErrLimit) {
		t.Fatal("queue did not reject overload")
	}
	if fixture.owner.pendingLogs[0].record.Body().AsString() != "first" || fixture.owner.pendingLogs[1].record.Body().AsString() != "second" {
		t.Fatal("accepted record overwritten")
	}
	if result := flushOne(t, fixture); result.Err() != nil || result.Outcome.Value.SignalsCopy()[0].Acknowledged != 2 {
		t.Fatal("accepted records not exported")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 || bodies[0] != "first" || bodies[1] != "second" {
		t.Fatal("independent receiver observed overwrite or loss")
	}
}
func TestLogLogicalBytesAndDisabledSignal(t *testing.T) {
	fixture := newFixture(t, OptionsV1{MaxRecordBytes: 1024, QueueBytes: 1024}, nil)
	first := emitOne(t, fixture, "first", "first")
	if first.Err() != nil {
		t.Fatal(first.Err())
	}
	second := emitOne(t, fixture, "second", "second")
	if !errors.Is(second.Err(), ErrLimit) {
		t.Fatal("queue-byte limit missing")
	}
	if _, err := fixture.client.Emit(context.Background(), fault.Correlation{Call: "large"}, LogRecord{Message: strings.Repeat("x", 1025)}); err == nil {
		t.Fatal("oversized body accepted")
	}
	if _, _, err := fixture.client.Start(context.Background(), fault.Correlation{Call: "disabled"}, SpanInput{Name: "disabled"}); !errors.Is(err, ErrUnsupported) {
		t.Fatal("disabled trace falsely supported")
	}
	if _, err := fixture.client.MeasureInt64(context.Background(), fault.Correlation{Call: "disabled"}, "count", 1); !errors.Is(err, ErrUnsupported) {
		t.Fatal("disabled metrics falsely supported")
	}
}

func TestTypedLogsAndIdentityPreserveIndependentOTLPTypes(t *testing.T) {
	integer := func(value int64) *commonpb.AnyValue {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: value}}
	}
	text := func(value string) *commonpb.AnyValue {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}
	}
	cases := []struct {
		name  string
		value Value
		wire  *commonpb.AnyValue
	}{
		{"null", Value{}, &commonpb.AnyValue{}},
		{"bool", Value{Kind: "bool", Bool: true}, &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}},
		{"int64-max", Value{Kind: "int64", Int64: math.MaxInt64}, integer(math.MaxInt64)},
		{"int64-min", Value{Kind: "int64", Int64: math.MinInt64}, integer(math.MinInt64)},
		{"double", Value{Kind: "float64", Float64: 1.25}, &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 1.25}}},
		{"string", Value{Kind: "string", String: "original"}, text("original")},
		{"bytes", Value{Kind: "bytes", Bytes: []byte{0, 1, 255}}, &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{0, 1, 255}}}},
		{"array", Value{Kind: "array", Array: []Value{{}, {Kind: "int64", Int64: 9007199254740993}, {Kind: "string", String: "original"}}},
			&commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{{}, integer(9007199254740993), text("original")}}}}},
		{"map", Value{Kind: "map", Map: []TypedAttribute{{Key: "text", Value: Value{Kind: "string", String: "original"}}, {Key: "null"}}},
			&commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: []*commonpb.KeyValue{
				{Key: "null", Value: &commonpb.AnyValue{}}, {Key: "text", Value: text("original")},
			}}}}},
		{"empty-array", Value{Kind: "array"}, &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{}}}},
		{"empty-map", Value{Kind: "map"}, &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{}}}},
	}
	typed := make([]ConfigAttribute, len(cases))
	for index, item := range cases {
		tree, err := Tree(item.value)
		if err != nil {
			t.Fatal(err)
		}
		typed[index] = ConfigAttribute{Key: item.name, Value: tree}
	}
	received := make(chan *collog.ExportLogsServiceRequest, 1)
	fixture := newFixture(t, OptionsV1{TypedResourceAttributes: typed, TypedScopeAttributes: typed,
		ResourceAttributes: map[string]string{"legacy": "resource"}, ScopeAttributes: map[string]string{"legacy": "scope"}},
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			data, err := io.ReadAll(request.Body)
			var logs collog.ExportLogsServiceRequest
			if err != nil || proto.Unmarshal(data, &logs) != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			received <- &logs
			writer.Header().Set("Content-Type", "application/x-protobuf")
		}))
	for index := range typed {
		typed[index].Key = "changed"
		for node := range typed[index].Value.Nodes {
			typed[index].Value.Nodes[node].String = "changed"
		}
	}
	eventTime := time.Date(2026, 10, 1, 2, 3, 4, 123456789, time.FixedZone("offset", 8*60*60))
	for index := range cases {
		item := &cases[index]
		receipt, err := fixture.client.Emit(context.Background(), fault.Correlation{Call: "typed-" + strconv.Itoa(index)},
			LogRecord{Body: &item.value, Time: eventTime, EventName: item.name, Severity: 9, SeverityText: "INFO"})
		if outcome := outcomeOf(t, receipt, err); outcome.Err() != nil {
			t.Fatal(item.name, outcome.Err())
		}
		if len(item.value.Bytes) != 0 {
			item.value.Bytes[0] = 9
		}
		if len(item.value.Array) != 0 {
			item.value.Array[0] = Value{Kind: "string", String: "changed"}
		}
		if len(item.value.Map) != 0 {
			item.value.Map[0].Key = "changed"
		}
		item.value = Value{Kind: "string", String: "changed"}
	}
	for _, message := range []string{"compatibility", ""} {
		if outcome := emitOne(t, fixture, "legacy", message); outcome.Err() != nil {
			t.Fatal(outcome.Err())
		}
	}
	if outcome := flushOne(t, fixture); outcome.Err() != nil {
		t.Fatal(outcome.Err())
	}
	var request *collog.ExportLogsServiceRequest
	select {
	case request = <-received:
	default:
		t.Fatal("successful flush did not reach the independent peer")
	}
	if len(request.ResourceLogs) != 1 || len(request.ResourceLogs[0].ScopeLogs) != 1 {
		t.Fatal("resource/scope identity was not fixed")
	}
	resource := request.ResourceLogs[0]
	scope := resource.ScopeLogs[0]
	for _, fields := range [][]*commonpb.KeyValue{resource.Resource.Attributes, scope.Scope.Attributes} {
		byKey := make(map[string]*commonpb.AnyValue, len(fields))
		for _, field := range fields {
			byKey[field.Key] = field.Value
		}
		for _, item := range cases {
			if !proto.Equal(byKey[item.name], item.wire) {
				t.Errorf("identity attribute %q changed type/value: got %v, want %v", item.name, byKey[item.name], item.wire)
			}
		}
		if byKey["legacy"] == nil {
			t.Error("typed identity replaced the compatible legacy map")
		}
	}
	if len(scope.LogRecords) != len(cases)+2 {
		t.Fatal("typed/legacy records were dropped")
	}
	for index, item := range cases {
		record := scope.LogRecords[index]
		if !proto.Equal(record.Body, item.wire) || record.EventName != item.name || record.SeverityNumber != 9 || record.SeverityText != "INFO" {
			t.Errorf("body %q changed type/value: got %v, want %v", item.name, record.Body, item.wire)
		}
		if record.TimeUnixNano != uint64(eventTime.UnixNano()) || record.ObservedTimeUnixNano <= record.TimeUnixNano {
			t.Error("event time was replaced by observation time")
		}
	}
	for index, message := range []string{"compatibility", ""} {
		if !proto.Equal(scope.LogRecords[len(cases)+index].Body, text(message)) {
			t.Error("absent Body stopped selecting the compatible string Message")
		}
	}
}

func TestTypedLogFailuresDoNotQueuePartialRecords(t *testing.T) {
	fixture := newFixture(t, OptionsV1{MaxRecordBytes: 1024}, nil)
	for _, value := range []Value{
		{Kind: "bool", String: "ambiguous"}, {Kind: "float64", Float64: math.Inf(1)},
		{Kind: "string", String: "\xff"}, {Kind: "bytes", Bytes: make([]byte, 1024)},
		{Kind: "array", Array: make([]Value, MaxNodes)},
	} {
		receipt, err := fixture.client.Emit(context.Background(), fault.Correlation{Call: "refused"},
			LogRecord{Body: &value, Attributes: []slog.Attr{slog.String("valid", "must-not-queue")}})
		if err != nil || outcomeOf(t, receipt, err).Err() == nil || len(fixture.owner.pendingLogs) != 0 || fixture.owner.queueItems != 0 || fixture.owner.queueBytes != 0 {
			t.Fatal("invalid body entered queue or lost its independent refusal")
		}
	}
	for _, input := range []LogRecord{
		{Body: &Value{}, Message: "ambiguous"}, {Time: time.Unix(-1, 0)}, {Time: time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		if receipt, err := fixture.client.Emit(context.Background(), fault.Correlation{Call: "invalid"}, input); !errors.Is(err, ErrInput) || receipt != nil {
			t.Fatal("invalid selector or timestamp reached operation admission")
		}
	}
	if result := emitOne(t, fixture, "next", "healthy"); result.Err() != nil {
		t.Fatal("refused record corrupted subsequent independent admission", result.Err())
	}
}
