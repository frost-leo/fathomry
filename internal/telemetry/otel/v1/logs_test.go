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
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	traceapi "go.opentelemetry.io/otel/trace"
	collog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
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
