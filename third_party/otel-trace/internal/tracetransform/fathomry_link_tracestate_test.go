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

package tracetransform

import (
	"bytes"
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestFathomryInitialAndDynamicLinkTraceState(t *testing.T) {
	for _, stateText := range []string{"", "vendor=original,other=value"} {
		t.Run(stateText, func(t *testing.T) {
			state, err := trace.ParseTraceState(stateText)
			if err != nil {
				t.Fatal(err)
			}
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			defer func() {
				if err := provider.Shutdown(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			linked := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID: trace.TraceID{1, 2, 3, 4}, SpanID: trace.SpanID{5, 6, 7, 8},
				TraceFlags: trace.FlagsSampled, TraceState: state, Remote: true,
			})
			_, span := provider.Tracer("fathomry.link-regression").Start(context.Background(), "links",
				trace.WithLinks(trace.Link{SpanContext: linked, Attributes: []attribute.KeyValue{attribute.String("position", "initial")}}))
			span.AddLink(trace.Link{SpanContext: linked.WithRemote(false), Attributes: []attribute.KeyValue{attribute.String("position", "dynamic")}})
			span.End()
			native := recorder.Ended()
			if len(native) != 1 || len(native[0].Links()) != 2 {
				t.Fatal("native SDK failed to retain initial and dynamic links")
			}
			for _, link := range native[0].Links() {
				if link.SpanContext.TraceState().String() != stateText {
					t.Fatal("native SDK changed input trace-state")
				}
			}
			converted := Spans(native)
			if len(converted) != 1 || len(converted[0].ScopeSpans) != 1 || len(converted[0].ScopeSpans[0].Spans) != 1 {
				t.Fatal("span transform omitted the native span")
			}
			data, err := proto.Marshal(converted[0].ScopeSpans[0].Spans[0])
			if err != nil {
				t.Fatal(err)
			}
			var decoded tracepb.Span
			if err := proto.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			if len(decoded.Links) != 2 {
				t.Fatal("protobuf span dropped native links")
			}
			traceID, spanID := linked.TraceID(), linked.SpanID()
			for index, link := range decoded.Links {
				if link.TraceState != stateText || !bytes.Equal(link.TraceId, traceID[:]) || !bytes.Equal(link.SpanId, spanID[:]) {
					t.Fatalf("link %d changed state/identity on wire: got %q, want %q", index, link.TraceState, stateText)
				}
				if link.Flags&0xff != uint32(trace.FlagsSampled) || link.Flags&0x300 != []uint32{0x300, 0x100}[index] {
					t.Fatal("trace-state correction changed sampled/remote flags")
				}
				if len(link.Attributes) != 1 || link.Attributes[0].Key != "position" ||
					link.Attributes[0].Value.GetStringValue() != []string{"initial", "dynamic"}[index] {
					t.Fatal("trace-state correction changed link attributes")
				}
			}
		})
	}
}
