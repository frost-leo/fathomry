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
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	traceapi "go.opentelemetry.io/otel/trace"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

func TestTraceLifecycleEventsLinksAndSafeError(t *testing.T) {
	fixture := newFixture(t, OptionsV1{TracesEndpoint: "traces", LogsEndpoint: "logs"}, nil)
	parentCtx, parent, err := fixture.client.Start(context.Background(), fault.Correlation{Call: "parent"}, SpanInput{Name: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, child, err := fixture.client.Start(parentCtx, fault.Correlation{Call: "child", Parent: "parent"}, SpanInput{Name: "child", Kind: traceapi.SpanKindClient,
		Attributes: []slog.Attr{slog.String("phase", "start")}, Links: []Link{{Context: traceapi.SpanContextFromContext(parentCtx), Attributes: []slog.Attr{slog.Int("link", 1)}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, escaped := traceapi.SpanFromContext(ctx).TracerProvider().(*sdktrace.TracerProvider); escaped {
		t.Fatal("native provider escaped through context")
	}
	native := errors.New("private-original-canary")
	observed := fault.Kind("test.failure").New(fault.Context{Operation: "read"}, native)
	if err := child.RecordError(observed); err != nil {
		t.Fatal(err)
	}
	if err := child.SetAttributes(slog.Int64("rows", 5)); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEvent("saved", slog.Bool("ok", true)); err != nil {
		t.Fatal(err)
	}
	receipt, err := child.End(context.Background())
	if result := outcomeOf(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	record := fixture.owner.pendingSpans[0]
	if record.Parent().SpanID() != traceapi.SpanContextFromContext(parentCtx).SpanID() || len(record.Links()) != 1 ||
		record.Status().Code != codes.Unset || len(record.Events()) != 2 || record.Events()[0].Name != "exception" {
		t.Fatal("trace facts changed")
	}
	for _, attr := range record.Events()[0].Attributes {
		if attr.Value.AsString() == "private-original-canary" {
			t.Fatal("native cause exported")
		}
	}
	if err := child.SetAttributes(slog.String("late", "no")); !errors.Is(err, ErrState) {
		t.Fatal("ended span mutated")
	}
	_, err = parent.End(context.Background())
	if err != nil {
		t.Fatal(err)
	}
}
func TestSamplingAndExplicitStatus(t *testing.T) {
	zero := 0.0
	fixture := newFixture(t, OptionsV1{TracesEndpoint: "traces", LogsEndpoint: "logs", SampleRatio: &zero}, nil)
	ctx, span, err := fixture.client.Start(context.Background(), fault.Correlation{Call: "root"}, SpanInput{Name: "unsampled"})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := fixture.client.Emit(ctx, fault.Correlation{Call: "log"}, LogRecord{Message: "correlated"})
	if result := outcomeOf(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	_, err = span.End(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(fixture.owner.pendingSpans) != 0 || !fixture.owner.pendingLogs[0].record.TraceID().IsValid() || fixture.owner.pendingLogs[0].record.TraceFlags().IsSampled() {
		t.Fatal("sampling incorrectly suppressed correlated log")
	}
	remote := traceapi.SpanContextFromContext(ctx).WithTraceFlags(traceapi.FlagsSampled).WithRemote(true)
	_, sampled, err := fixture.client.Start(traceapi.ContextWithRemoteSpanContext(context.Background(), remote), fault.Correlation{Call: "sampled"}, SpanInput{Name: "parent-based"})
	if err != nil {
		t.Fatal(err)
	}
	if err := sampled.SetStatus(codes.Error, "failed"); err != nil {
		t.Fatal(err)
	}
	if err := sampled.SetStatus(codes.Ok, "done"); err != nil {
		t.Fatal(err)
	}
	if err := sampled.SetStatus(codes.Error, "later"); err != nil {
		t.Fatal(err)
	}
	_, err = sampled.End(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(fixture.owner.pendingSpans) != 1 || fixture.owner.pendingSpans[0].Status().Code != codes.Ok {
		t.Fatal("native parent sampling/status precedence changed")
	}
	_, root, err := fixture.client.Start(traceapi.ContextWithRemoteSpanContext(context.Background(), remote), fault.Correlation{Call: "new-root"}, SpanInput{Name: "new-root", NewRoot: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = root.End(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(fixture.owner.pendingSpans) != 1 {
		t.Fatal("NewRoot inherited sampled parent")
	}
}
func TestTraceLimitsConcurrentEndAndQueueReservation(t *testing.T) {
	fixture := newFixture(t, OptionsV1{TracesEndpoint: "traces", LogsEndpoint: "logs", QueueItems: 1}, nil)
	_, span, err := fixture.client.Start(context.Background(), fault.Correlation{Call: "span"}, SpanInput{Name: "bounded"})
	if err != nil {
		t.Fatal(err)
	}
	if result := emitOne(t, fixture, "blocked", "cannot replace span"); !errors.Is(result.Err(), ErrLimit) {
		t.Fatal("span did not reserve queue")
	}
	for range 32 {
		if err := span.AddEvent("event"); err != nil {
			t.Fatal(err)
		}
	}
	if err := span.AddEvent("extra"); !errors.Is(err, ErrLimit) {
		t.Fatal("event bound missing")
	}
	var group sync.WaitGroup
	failures := make(chan error, 8)
	for range 8 {
		group.Go(func() {
			_, err := span.End(context.Background())
			if err != nil {
				failures <- err
			}
		})
	}
	group.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if len(fixture.owner.pendingSpans) != 1 {
		t.Fatal("repeated End duplicated span")
	}
	if result := flushOne(t, fixture); result.Err() != nil {
		t.Fatal(result.Err())
	}
}

func testSpanContext(t *testing.T, state string) traceapi.SpanContext {
	t.Helper()
	traceID, err := traceapi.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := traceapi.SpanIDFromHex("0102030405060708")
	if err != nil {
		t.Fatal(err)
	}
	traceState, err := traceapi.ParseTraceState(state)
	if err != nil {
		t.Fatal(err)
	}
	return traceapi.NewSpanContext(traceapi.SpanContextConfig{TraceID: traceID, SpanID: spanID,
		TraceFlags: traceapi.FlagsSampled, TraceState: traceState, Remote: true})
}

func TestSpanExtensionsPreserveIndependentOTLPFactsAndCanceledEnd(t *testing.T) {
	received := make(chan *coltrace.ExportTraceServiceRequest, 1)
	fixture := newFixture(t, OptionsV1{TracesEndpoint: "traces"}, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(request.Body)
		var traces coltrace.ExportTraceServiceRequest
		if err != nil || proto.Unmarshal(data, &traces) != nil || bytes.Contains(data, []byte("private-error-canary")) {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		received <- &traces
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	parent := testSpanContext(t, "vendor=original")
	startTime := time.Date(2020, 1, 2, 3, 4, 5, 123456789, time.FixedZone("offset", 8*60*60))
	eventTime, endTime := startTime.Add(time.Second), startTime.Add(2*time.Second)
	type contextKey struct{}
	parentContext := traceapi.ContextWithRemoteSpanContext(context.WithValue(context.Background(), contextKey{}, "preserved"), parent)
	ctx, span, err := fixture.client.Start(parentContext, fault.Correlation{Call: "span"}, SpanInput{
		Name: "original", Kind: traceapi.SpanKindServer, Time: startTime,
		Links: []Link{{Context: parent}}, Attributes: []slog.Attr{slog.String("phase", "start")},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = span.End(context.Background()) })
	spanContext := span.SpanContext()
	if !span.IsRecording() || !spanContext.IsValid() || !spanContext.IsSampled() || spanContext.IsRemote() ||
		spanContext.TraceID() != parent.TraceID() || spanContext.TraceState().String() != parent.TraceState().String() ||
		!traceapi.SpanContextFromContext(ctx).Equal(spanContext) || ctx.Value(contextKey{}) != "preserved" {
		t.Fatal("read-only span facts or caller context changed")
	}
	if _, escaped := traceapi.SpanFromContext(ctx).TracerProvider().(*sdktrace.TracerProvider); escaped || traceapi.SpanFromContext(ctx).IsRecording() {
		t.Fatal("returned context escaped native mutation/provider authority")
	}
	if err := span.SetName("renamed"); err != nil {
		t.Fatal(err)
	}
	linkData := []byte{0, 255}
	linkAttrs := []slog.Attr{slog.Any("data", linkData)}
	if err := span.AddLink(Link{Context: parent.WithRemote(false), Attributes: linkAttrs}); err != nil {
		t.Fatal(err)
	}
	linkData[0] = 9
	linkAttrs[0] = slog.String("data", "changed")
	if err := span.AddEventAt("observed", eventTime, slog.Int64("count", 9007199254740993)); err != nil {
		t.Fatal(err)
	}
	if err := span.RecordAnnotation(ErrorAnnotation{Type: "public.safe_kind", Message: "Safe technical description.", Time: eventTime}); err != nil {
		t.Fatal(err)
	}
	if err := span.RecordError(fault.Kind("internal.safe_kind").New(fault.Context{}, errors.New("private-error-canary"))); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("canceled-end")
	cancel(cause)
	pending, err := span.EndAt(canceled, endTime.Add(-time.Second))
	if err == nil || !errors.Is(err, cause) || pending == nil || !span.IsRecording() || len(fixture.owner.pendingSpans) != 0 {
		t.Fatalf("canceled End: err=%v, cause=%t, recording=%t, ended_queue=%d", err,
			errors.Is(err, cause), span.IsRecording(), len(fixture.owner.pendingSpans))
	}
	if _, final := pending.Result(); final {
		t.Fatal("canceled End manufactured final evidence")
	}
	receipt, err := span.EndAt(context.Background(), endTime)
	if outcome := outcomeOf(t, receipt, err); outcome.Err() != nil || !outcome.Outcome.Value.SignalsCopy()[0].Sampled {
		t.Fatal("continued End did not complete the original span", outcome.Err())
	}
	if span.IsRecording() || !span.SpanContext().Equal(spanContext) {
		t.Fatal("ended span changed immutable context facts")
	}
	if result, ready := pending.Result(); !ready || !result.Final || result.Context.Correlation.Call != "span" {
		t.Fatal("canceled wait did not retain the original operation receipt")
	}
	if repeated, err := span.EndAt(context.Background(), endTime.Add(time.Hour)); err != nil || repeated == nil || len(fixture.owner.pendingSpans) != 1 {
		t.Fatal("repeated End did not preserve the original operation")
	}
	if err := span.SetName("late"); !errors.Is(err, ErrState) {
		t.Fatal("ended span permitted renaming")
	}
	if err := span.AddLink(Link{Context: parent}); !errors.Is(err, ErrState) {
		t.Fatal("ended span permitted a dynamic link")
	}
	for _, link := range fixture.owner.pendingSpans[0].Links() {
		if link.SpanContext.TraceState().String() != "vendor=original" {
			t.Fatal("native capture lost the link trace-state before export")
		}
	}
	if outcome := flushOne(t, fixture); outcome.Err() != nil {
		t.Fatal(outcome.Err())
	}
	var request *coltrace.ExportTraceServiceRequest
	select {
	case request = <-received:
	default:
		t.Fatal("successful flush did not reach independent trace peer")
	}
	if len(request.ResourceSpans) != 1 || len(request.ResourceSpans[0].ScopeSpans) != 1 || len(request.ResourceSpans[0].ScopeSpans[0].Spans) != 1 {
		t.Fatal("original span missing from independent receiver")
	}
	record := request.ResourceSpans[0].ScopeSpans[0].Spans[0]
	parentID := parent.SpanID()
	if record.Name != "renamed" || record.StartTimeUnixNano != uint64(startTime.UnixNano()) || record.EndTimeUnixNano != uint64(endTime.UnixNano()) ||
		record.TraceState != "vendor=original" || !bytes.Equal(record.ParentSpanId, parentID[:]) || len(record.Links) != 2 || len(record.Events) != 3 {
		t.Fatal("name, timestamps, parent state, links or events changed on wire", record)
	}
	for _, link := range record.Links {
		if !bytes.Equal(link.SpanId, parentID[:]) || link.TraceState != "vendor=original" {
			t.Fatal("dynamic/initial link lost immutable span context", link)
		}
	}
	if len(record.Links[1].Attributes) != 1 || !bytes.Equal(record.Links[1].Attributes[0].Value.GetBytesValue(), []byte{0, 255}) {
		t.Fatal("dynamic link kept mutable input storage")
	}
	if record.Events[0].Name != "observed" || record.Events[0].TimeUnixNano != uint64(eventTime.UnixNano()) ||
		len(record.Events[0].Attributes) != 1 || record.Events[0].Attributes[0].Value.GetIntValue() != 9007199254740993 ||
		record.Events[1].Name != "exception" || record.Events[1].TimeUnixNano != uint64(eventTime.UnixNano()) {
		t.Fatal("explicit event timestamp/type or safe annotation changed on wire")
	}
	expected := []*commonpb.KeyValue{
		{Key: "exception.type", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "public.safe_kind"}}},
		{Key: "exception.message", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "Safe technical description."}}},
	}
	for index, field := range expected {
		if len(record.Events[1].Attributes) != len(expected) || !proto.Equal(record.Events[1].Attributes[index], field) {
			t.Fatal("safe annotation was replaced or reformatted")
		}
	}
	if record.Status.GetCode() != 0 {
		t.Fatal("error annotation changed technical span status")
	}
}

func TestSpanExtensionBudgetsAndRejectedMutations(t *testing.T) {
	fixture := newFixture(t, OptionsV1{TracesEndpoint: "traces"}, nil)
	link := testSpanContext(t, "vendor=value")
	for name, test := range map[string]struct {
		limit  int
		mutate func(*Span) error
	}{
		"name":                {MaxNodes, func(span *Span) error { return span.SetName("renamed") }},
		"empty-attributes":    {MaxNodes, func(span *Span) error { return span.SetAttributes() }},
		"replaced-attributes": {MaxAttributes, func(span *Span) error { return span.SetAttributes(slog.String("same", "value")) }},
		"status":              {MaxNodes, func(span *Span) error { return span.SetStatus(codes.Error, "safe") }},
		"events":              {32, func(span *Span) error { return span.AddEventAt("event", time.Unix(1, 0)) }},
		"links":               {16, func(span *Span) error { return span.AddLink(Link{Context: link}) }},
	} {
		t.Run(name, func(t *testing.T) {
			_, span, err := fixture.client.Start(context.Background(), fault.Correlation{Call: name}, SpanInput{Name: name})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = span.End(context.Background()) })
			for range test.limit {
				if err := test.mutate(span); err != nil {
					t.Fatal("mutation below declared cumulative limit failed", err)
				}
			}
			before := span.budget
			if err := test.mutate(span); !errors.Is(err, ErrLimit) || span.budget != before {
				t.Fatal("repeated mutation bypassed its bound or changed rejected budget", err)
			}
			if _, err := span.End(context.Background()); err != nil {
				t.Fatal("exhausted mutation budget prevented already-reserved End", err)
			}
		})
	}
	_, span, err := fixture.client.Start(context.Background(), fault.Correlation{Call: "invalid"}, SpanInput{Name: "valid", Links: []Link{{Context: link}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = span.End(context.Background()) })
	for _, mutate := range []func() error{
		func() error { return span.SetName("") }, func() error { return span.SetName(strings.Repeat("x", 129)) },
		func() error { return span.AddLink(Link{}) },
		func() error {
			return span.AddLink(Link{Context: link, Attributes: []slog.Attr{slog.Any("bad", errors.New("private"))}})
		},
		func() error { return span.AddEventAt("event", time.Unix(-1, 0)) },
		func() error { return span.AddEventAt("event", time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC)) },
		func() error { return span.RecordAnnotation(ErrorAnnotation{}) },
		func() error { return span.RecordAnnotation(ErrorAnnotation{Type: "safe", Message: "\xff"}) },
		func() error { _, err := span.EndAt(context.Background(), time.Unix(-1, 0)); return err },
	} {
		before := span.budget
		if err := mutate(); err == nil || span.budget != before || !span.IsRecording() {
			t.Fatal("invalid extension mutated the span or its cumulative budget")
		}
	}
	for range 15 {
		if err := span.AddLink(Link{Context: link}); err != nil {
			t.Fatal(err)
		}
	}
	if err := span.AddLink(Link{Context: link}); !errors.Is(err, ErrLimit) {
		t.Fatal("dynamic links ignored initial-link reservation")
	}
}

func TestSpanContextStateConsumesBoundedInputAndSamplingPreservesFacts(t *testing.T) {
	longState := testSpanContext(t, "first="+strings.Repeat("a", 250)+",second="+strings.Repeat("b", 250)+",third="+strings.Repeat("c", 250))
	fixture := newFixture(t, OptionsV1{TracesEndpoint: "traces", MaxRecordBytes: 1024}, nil)
	parent := traceapi.ContextWithRemoteSpanContext(context.Background(), longState)
	if _, span, err := fixture.client.Start(parent, fault.Correlation{Call: "parent-refused"}, SpanInput{Name: "parent"}); !errors.Is(err, ErrLimit) || span != nil {
		t.Fatal("parent trace-state bypassed logical span byte budget", err)
	}
	if _, span, err := fixture.client.Start(context.Background(), fault.Correlation{Call: "link-refused"},
		SpanInput{Name: "linked", Links: []Link{{Context: longState}}}); !errors.Is(err, ErrLimit) || span != nil {
		t.Fatal("initial-link trace-state bypassed logical span byte budget", err)
	}
	_, root, err := fixture.client.Start(parent, fault.Correlation{Call: "new-root"}, SpanInput{Name: "new-root", NewRoot: true})
	if err != nil {
		t.Fatal("NewRoot retained or charged ignored parent state", err)
	}
	t.Cleanup(func() { _, _ = root.End(context.Background()) })
	if root.SpanContext().TraceID() == longState.TraceID() || root.SpanContext().TraceState().Len() != 0 {
		t.Fatal("NewRoot inherited ignored parent identity/state")
	}
	before := root.budget
	if err := root.AddLink(Link{Context: longState}); !errors.Is(err, ErrLimit) || root.budget != before {
		t.Fatal("dynamic-link state bypassed budget or consumed a rejected mutation")
	}
	if err := root.AddLink(Link{Context: testSpanContext(t, "small=valid")}); err != nil {
		t.Fatal("large rejected link corrupted the next valid mutation", err)
	}
	if _, err := root.End(context.Background()); err != nil {
		t.Fatal(err)
	}
	zero := 0.0
	unsampledFixture := newFixture(t, OptionsV1{TracesEndpoint: "traces", SampleRatio: &zero}, nil)
	_, unsampled, err := unsampledFixture.client.Start(context.Background(), fault.Correlation{Call: "unsampled"}, SpanInput{Name: "unsampled"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = unsampled.End(context.Background()) })
	if unsampled.IsRecording() || !unsampled.SpanContext().IsValid() || unsampled.SpanContext().IsSampled() {
		t.Fatal("sampled-out span lost nonrecording or valid identity facts")
	}
	before = unsampled.budget
	if err := unsampled.SetName("renamed"); err != nil || unsampled.budget.nodes != before.nodes-1 {
		t.Fatal("sampled-out mutation bypassed cumulative input accounting", err)
	}
	if _, err := unsampled.EndAt(context.Background(), time.Unix(100, 0)); err != nil || len(unsampledFixture.owner.pendingSpans) != 0 || unsampledFixture.owner.queueItems != 0 {
		t.Fatal("sampled-out End failed to release its actual reservation", err)
	}
}

func TestSpanConcurrentExtensionsAndEnd(t *testing.T) {
	fixture := newFixture(t, OptionsV1{TracesEndpoint: "traces"}, nil)
	_, span, err := fixture.client.Start(context.Background(), fault.Correlation{Call: "concurrent"}, SpanInput{Name: "original"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := span.SpanContext()
	var workers sync.WaitGroup
	failures := make(chan error, 16)
	for range 8 {
		workers.Go(func() {
			if err := span.SetName("renamed"); err != nil && !errors.Is(err, ErrState) {
				failures <- err
			}
			_ = span.IsRecording()
			if !span.SpanContext().Equal(ctx) {
				failures <- errors.New("concurrent access changed immutable span context")
			}
		})
		workers.Go(func() {
			if _, err := span.EndAt(context.Background(), time.Unix(100, 0)); err != nil {
				failures <- err
			}
		})
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if span.IsRecording() || len(fixture.owner.pendingSpans) != 1 || !fixture.owner.pendingSpans[0].EndTime().Equal(time.Unix(100, 0)) {
		t.Fatal("concurrent End duplicated the span or changed its chosen timestamp")
	}
}
