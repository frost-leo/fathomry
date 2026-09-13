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
	"log/slog"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	traceapi "go.opentelemetry.io/otel/trace"
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
