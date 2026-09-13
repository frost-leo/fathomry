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
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	traceapi "go.opentelemetry.io/otel/trace"
)

// Link associates an explicit span context, without borrowing a native span.
type Link struct {
	private
	Context    traceapi.SpanContext
	Attributes []slog.Attr
}

// SpanInput is borrowed until Start returns. Kind zero defaults to Internal.
// Time zero uses the native start time. NewRoot deliberately ignores a parent.
// At most 16 links and 64 attributes fit within the per-span logical byte budget.
type SpanInput struct {
	private
	Name       string
	Kind       traceapi.SpanKind
	Time       time.Time
	NewRoot    bool
	Attributes []slog.Attr
	Links      []Link
}

// Span owns an admitted invocation until End succeeds. Cancellation never ends
// it implicitly. Methods are concurrency-safe; copies must not be made. The
// returned context contains only span IDs/flags, not the SDK span/provider.
// Keeping a span open prevents resource release and consumes an active call.
type Span struct {
	private
	mu                 sync.Mutex
	owner              *owner
	native             traceapi.Span
	call               *invocation.Call[Result]
	budget             dataBudget
	attributes, events int
	ended              bool
}

// Start reserves both independent evidence and a queue slot before native entry.
// Even sampled-out spans must be ended. Queue budget reserves MaxRecordBytes for
// each live/ended sampled span, so End never requires new queue/evidence capacity.
func (client *Client) Start(ctx context.Context, id fault.Correlation, input SpanInput) (context.Context, *Span, error) {
	if client == nil || client.owner == nil {
		return nil, nil, failure(ErrInput, "start")
	}
	if client.owner.tracer == nil {
		return nil, nil, failure(ErrUnsupported, "traces-disabled")
	}
	if !boundedString(input.Name, 128) || input.Name == "" || !timestampValid(input.Time) || len(input.Links) > 16 ||
		input.Kind < 0 || input.Kind > traceapi.SpanKindConsumer {
		return nil, nil, failure(ErrInput, "span")
	}
	call, err := client.begin(ctx, id, "span", invocation.Session)
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (context.Context, *Span, error) {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return nil, nil, err
	}
	work, cancel, err := (invocation.Budget{Limit: client.owner.settings.Timeout}).Context(ctx, invocation.Establish)
	if err != nil {
		return fail(err)
	}
	defer cancel()
	if err := client.owner.enter(work); err != nil {
		return fail(err)
	}
	defer client.owner.leave()
	budget := dataBudget{bytes: client.owner.settings.MaxRecordBytes, nodes: MaxNodes}
	association := client.association(id)
	if err := budget.charge(256+associationBytes(association)+len(input.Name), 0); err != nil {
		return fail(err)
	}
	attrs, err := freezeAttributes(input.Attributes, &budget, 1)
	if err != nil {
		return fail(err)
	}
	links := make([]traceapi.Link, 0, len(input.Links))
	for _, input := range input.Links {
		if !input.Context.IsValid() {
			return fail(failure(ErrInput, "link-context"))
		}
		if err := budget.charge(64, 1); err != nil {
			return fail(err)
		}
		fields, err := freezeAttributes(input.Attributes, &budget, 1)
		if err != nil {
			return fail(err)
		}
		links = append(links, traceapi.Link{SpanContext: input.Context, Attributes: fields})
	}
	if err := client.owner.reserve(client.owner.settings.MaxRecordBytes); err != nil {
		return fail(err)
	}
	kind := input.Kind
	if kind == traceapi.SpanKindUnspecified {
		kind = traceapi.SpanKindInternal
	}
	opts := []traceapi.SpanStartOption{traceapi.WithSpanKind(kind), traceapi.WithAttributes(append(attrs, association...)...), traceapi.WithLinks(links...)}
	if input.NewRoot {
		opts = append(opts, traceapi.WithNewRoot())
	}
	if !input.Time.IsZero() {
		opts = append(opts, traceapi.WithTimestamp(input.Time.UTC().Round(0)))
	}
	if err := work.Err(); err != nil {
		client.owner.unreserve(client.owner.settings.MaxRecordBytes, 1)
		return fail(nativeFailure(ErrState, "start", work, err))
	}
	_, native := client.owner.tracer.Start(work, strings.Clone(input.Name), opts...)
	span := &Span{owner: client.owner, native: native, call: call, budget: budget, attributes: len(attrs)}
	// Do not leak the SDK span's TracerProvider through SpanFromContext.
	return traceapi.ContextWithSpanContext(ctx, native.SpanContext()), span, nil
}

// Receipt can be observed before End; it remains unresolved until actual End.
func (span *Span) Receipt() *invocation.Receipt[Result] {
	if span == nil {
		return nil
	}
	return span.call.Receipt()
}

// SetAttributes adds bounded data without modifying business status. Replacements
// still consume cumulative input budget, bounding repeated mutation work.
func (span *Span) SetAttributes(attrs ...slog.Attr) error {
	if span == nil || span.owner == nil {
		return failure(ErrInput, "span-attributes")
	}
	span.mu.Lock()
	defer span.mu.Unlock()
	if span.ended {
		return failure(ErrState, "ended")
	}
	if span.attributes+len(attrs) > MaxAttributes {
		return failure(ErrLimit, "span-attributes")
	}
	budget := span.budget
	frozen, err := freezeAttributes(attrs, &budget, 1)
	if err != nil {
		return err
	}
	span.native.SetAttributes(frozen...)
	span.budget, span.attributes = budget, span.attributes+len(attrs)
	return nil
}

// AddEvent uses the observation time and accepts at most 32 events per span.
func (span *Span) AddEvent(name string, attrs ...slog.Attr) error {
	if span == nil || span.owner == nil || name == "" || !boundedString(name, 128) {
		return failure(ErrInput, "event")
	}
	span.mu.Lock()
	defer span.mu.Unlock()
	if span.ended {
		return failure(ErrState, "ended")
	}
	if span.events == 32 {
		return failure(ErrLimit, "span-events")
	}
	budget := span.budget
	if err := budget.charge(64+len(name), 1); err != nil {
		return err
	}
	frozen, err := freezeAttributes(attrs, &budget, 1)
	if err != nil {
		return err
	}
	span.native.AddEvent(strings.Clone(name), traceapi.WithAttributes(frozen...))
	span.budget, span.events = budget, span.events+1
	return nil
}

// RecordError records only the safe technical kind as an exception event.
// It neither formats/exports native causes nor sets span status or retry policy.
func (span *Span) RecordError(err *fault.Error) error {
	if err == nil {
		return nil
	}
	kind := string(err.Diagnostic().Kind)
	return span.AddEvent("exception", slog.String("exception.type", kind), slog.String("exception.message", kind))
}

// SetStatus is explicit technical annotation, not an Item/Run outcome. Native
// precedence is preserved: Unset < Error < Ok; Ok cannot later become Error.
func (span *Span) SetStatus(code codes.Code, description string) error {
	if span == nil || span.owner == nil || (code != codes.Unset && code != codes.Error && code != codes.Ok) || !boundedString(description, 1024) {
		return failure(ErrInput, "status")
	}
	span.mu.Lock()
	defer span.mu.Unlock()
	if span.ended {
		return failure(ErrState, "ended")
	}
	budget := span.budget
	if err := budget.charge(len(description)+32, 1); err != nil {
		return err
	}
	span.native.SetStatus(code, strings.Clone(description))
	span.budget = budget
	return nil
}

// End ends once and returns the original receipt on repeated calls. A canceled
// wait leaves the token live: retry End with a separately owned usable context.
// End does not export; Sampled=false is not proof that no correlated logs exist.
func (span *Span) End(ctx context.Context) (*invocation.Receipt[Result], error) {
	if span == nil || span.owner == nil {
		return nil, failure(ErrInput, "end")
	}
	span.mu.Lock()
	ended := span.ended
	span.mu.Unlock()
	if ended {
		return span.Receipt(), nil
	}
	work, cancel, err := (invocation.Budget{Limit: span.owner.settings.Timeout}).Context(ctx, invocation.Cleanup)
	if err != nil {
		return span.Receipt(), err
	}
	defer cancel()
	// Resource ownership outlives End's gate entry and the entire native OnEnd.
	if err := span.owner.enter(work); err != nil {
		return span.Receipt(), err
	}
	span.mu.Lock()
	if span.ended {
		span.mu.Unlock()
		span.owner.leave()
		return span.Receipt(), nil
	}
	if err := work.Err(); err != nil {
		span.mu.Unlock()
		span.owner.leave()
		return span.Receipt(), nativeFailure(ErrState, "end", work, err)
	}
	sampled := span.native.SpanContext().IsSampled()
	span.native.End()
	span.ended = true
	result := SignalResult{Signal: Traces, Sampled: sampled}
	if sampled {
		result.Accepted = 1
	} else {
		span.owner.unreserve(span.owner.settings.MaxRecordBytes, 1)
	}
	span.owner.leave()
	span.call.Complete(successful(result))
	span.mu.Unlock()
	return span.Receipt(), nil
}

type spanCapture struct{ owner *owner }

func (spanCapture) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (capture spanCapture) OnEnd(span sdktrace.ReadOnlySpan) {
	capture.owner.pendingSpans = append(capture.owner.pendingSpans, span)
}
func (spanCapture) Shutdown(context.Context) error   { return nil }
func (spanCapture) ForceFlush(context.Context) error { return nil }
