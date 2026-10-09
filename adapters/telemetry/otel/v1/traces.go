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
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	native "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	"go.opentelemetry.io/otel/codes"
	traceapi "go.opentelemetry.io/otel/trace"
	"log/slog"
	"sync"
	"time"
)

// Link holds non-owning trace identity and copied bounded attributes.
type Link struct {
	private
	Context    traceapi.SpanContext
	Attributes []slog.Attr
}

// SpanInput is borrowed until Start returns. Kind zero selects Internal; Time
// zero selects native observation time. At most 16 links share cumulative bounds.
type SpanInput struct {
	private
	Name       string
	Kind       traceapi.SpanKind
	Time       time.Time
	NewRoot    bool
	Attributes []slog.Attr
	Links      []Link
}

// Span retains its original source generation and admitted operation through
// actual End. Even unsampled spans require End. Cancellation does not end it.
// Methods are concurrent-safe; callers must not overwrite/copy this handle.
type Span struct {
	private
	native    *native.Span
	operation *operation
	mu        sync.Mutex
	ended     bool
}

// Start admits one retained span. The returned context preserves caller lifetime
// and non-owning trace facts; End does not cancel it. Failure after admission
// still publishes independent evidence even when no Span can be returned.
func (client *Client) Start(ctx context.Context, input SpanInput) (context.Context, *Span, error) {
	var span *Span
	var associated context.Context
	var setup error
	receipt, err := client.dispatch(ctx, "span", func(operation *operation) {
		attrs, err := nativeAttributes(input.Attributes)
		if err != nil {
			setup = err
			operation.finish(nil, err)
			return
		}
		if len(input.Links) > 16 {
			setup = fail(ErrLimit, "links")
			operation.finish(nil, setup)
			return
		}
		links := make([]native.Link, len(input.Links))
		for index, link := range input.Links {
			converted, err := nativeAttributes(link.Attributes)
			if err != nil {
				setup = err
				operation.finish(nil, err)
				return
			}
			links[index] = native.Link{Context: link.Context, Attributes: converted}
		}
		var actual *native.Span
		associated, actual, err = operation.native.Start(operation.context, operation.id, native.SpanInput{Name: input.Name, Kind: input.Kind, Time: input.Time, NewRoot: input.NewRoot, Attributes: attrs, Links: links})
		setup = translate(err, "start")
		if actual == nil {
			operation.finish(nil, err)
			return
		}
		span = &Span{native: actual, operation: operation}
		associated = traceapi.ContextWithSpanContext(ctx, actual.SpanContext())
	})
	if span == nil && err == nil && setup == nil {
		if outcome, resolved := receipt.Snapshot(); resolved {
			setup = outcome.Err()
		}
		if setup == nil {
			setup = fail(ErrState, "start")
		}
	}
	return associated, span, joinErrors("start", err, setup)
}

// Receipt observes the original root, unresolved until actual End.
func (span *Span) Receipt() *adapters.Receipt[Result] {
	if span == nil || span.operation == nil {
		return nil
	}
	return span.operation.call.Receipt()
}

// IsRecording reports the native recording state, not queue/export capacity.
func (span *Span) IsRecording() bool {
	return span != nil && span.native != nil && span.native.IsRecording()
}

// SpanContext returns immutable IDs, flags and trace-state, including after End.
func (span *Span) SpanContext() traceapi.SpanContext {
	if span == nil || span.native == nil {
		return traceapi.SpanContext{}
	}
	return span.native.SpanContext()
}

// SetName replaces the name while consuming the cumulative mutation budget.
func (span *Span) SetName(name string) error {
	if span == nil || span.native == nil {
		return fail(ErrInput, "span-name")
	}
	return translate(span.native.SetName(name), "span-name")
}

// SetAttributes copies bounded values; replacing a key still consumes budget.
func (span *Span) SetAttributes(attrs ...slog.Attr) error {
	if span == nil || span.native == nil {
		return fail(ErrInput, "span-attributes")
	}
	converted, err := nativeAttributes(attrs)
	if err != nil {
		return err
	}
	return translate(span.native.SetAttributes(converted...), "span-attributes")
}

// AddLink shares the 16-link and cumulative byte/node bounds with initial links.
func (span *Span) AddLink(link Link) error {
	if span == nil || span.native == nil {
		return fail(ErrInput, "span-link")
	}
	converted, err := nativeAttributes(link.Attributes)
	if err != nil {
		return err
	}
	return translate(span.native.AddLink(native.Link{Context: link.Context, Attributes: converted}), "span-link")
}

// AddEvent uses observation time and shares the 32-event cumulative span bound.
func (span *Span) AddEvent(name string, attrs ...slog.Attr) error {
	return span.AddEventAt(name, time.Time{}, attrs...)
}

// AddEventAt preserves explicit time; zero selects observation time. Nonzero
// instants must fall in UTC years 1970..2261, as for log/start/end timestamps.
func (span *Span) AddEventAt(name string, timestamp time.Time, attrs ...slog.Attr) error {
	if span == nil || span.native == nil {
		return fail(ErrInput, "span-event")
	}
	converted, err := nativeAttributes(attrs)
	if err != nil {
		return err
	}
	return translate(span.native.AddEventAt(name, timestamp, converted...), "span-event")
}

// SetStatus preserves native Unset < Error < Ok precedence, not business status.
func (span *Span) SetStatus(code codes.Code, description string) error {
	if span == nil || span.native == nil {
		return fail(ErrInput, "span-status")
	}
	return translate(span.native.SetStatus(code, description), "span-status")
}

// ErrorAttributes projects only public occurrence metadata and known captured
// i18n text. Native causes and component formatting are never called. Public
// Occurrence accessors retain the existing bounded/cooperative failure contract;
// arbitrary foreign errors are refused. Output remains subject to record bounds.
func ErrorAttributes(original error) ([]slog.Attr, error) {
	if original == nil {
		return nil, nil
	}
	core, ok := failure.Inspect(original)
	if !ok {
		return nil, fail(ErrUnsupported, "error-projection")
	}
	diagnostic := core.Diagnostic()
	definition := diagnostic.Definition
	message := definition.Message
	var localized *i18n.Presentation
	if presented, ok := original.(*i18n.Presented); ok && presented != nil {
		report := presented.Info()
		localized = &report
		message = report.Message.Text
	}
	result := []slog.Attr{slog.String("exception.type", string(definition.Identifier)), slog.String("exception.message", message),
		slog.String("error.code", definition.Code.String()), slog.String("error.module", definition.Module), slog.String("error.component", definition.Component),
		slog.String("error.operation", diagnostic.Location.Operation), slog.String("error.instance", diagnostic.Location.Instance),
		slog.Int("error.cause_count", diagnostic.CauseCount)}
	if localized != nil {
		result = append(result, slog.String("error.locale", localized.Message.Locale), slog.Bool("error.form_fallback", localized.Message.FormFallback))
	}
	return result, nil
}

// RecordError adds one safe exception event without changing span status.
func (span *Span) RecordError(original error) error {
	if span == nil || span.native == nil {
		return fail(ErrInput, "span-error")
	}
	attrs, err := ErrorAttributes(original)
	if err != nil || original == nil {
		return err
	}
	return span.AddEvent("exception", attrs...)
}

// End uses a separate cleanup context, without acquiring new root/evidence
// capacity. Canceled waiting leaves the span live and resumable with another
// context. Repeated successful End returns the same public receipt.
func (span *Span) End(ctx context.Context) (*adapters.Receipt[Result], error) {
	return span.EndAt(ctx, time.Time{})
}

// EndAt selects an explicit final time; zero uses observation time. It retains
// End's independently cancellable waiting and idempotent completion semantics.
func (span *Span) EndAt(ctx context.Context, timestamp time.Time) (*adapters.Receipt[Result], error) {
	if span == nil || span.native == nil || ctx == nil {
		return nil, fail(ErrInput, "span-end")
	}
	span.mu.Lock()
	if span.ended {
		span.mu.Unlock()
		return span.Receipt(), nil
	}
	span.mu.Unlock()
	receipt, err := span.native.EndAt(ctx, timestamp)
	if err != nil {
		return span.Receipt(), translate(err, "span-end")
	}
	span.mu.Lock()
	complete := !span.ended
	span.ended = true
	span.mu.Unlock()
	if complete {
		span.operation.finish(receipt, nil)
	}
	return span.Receipt(), nil
}
