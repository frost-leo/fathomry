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
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	logapi "go.opentelemetry.io/otel/log"
	"log/slog"
	"time"
)

// LogRecord preserves original event time (zero selects observation time),
// severity 0..24, event identity and structured data. Body nil selects Message;
// nonnil Body selects typed data and requires empty Message. Value{} is null.
// No arbitrary LogValuer, Stringer, marshaler or raw error is invoked.
type LogRecord struct {
	private
	Time         time.Time
	Severity     logapi.Severity
	SeverityText string
	EventName    string
	Message      string
	Body         *Value
	Attributes   []slog.Attr
}

// Emit returns accepted operation evidence; local queue acceptance is not export.
func (client *Client) Emit(ctx context.Context, input LogRecord) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "emit", func(operation *operation) (*invocation.Receipt[native.Result], error) {
		attrs, err := nativeAttributes(input.Attributes)
		if err != nil {
			return nil, err
		}
		record := native.LogRecord{Time: input.Time, Severity: input.Severity, SeverityText: input.SeverityText, EventName: input.EventName, Message: input.Message, Attributes: attrs}
		if input.Body != nil {
			nodes := MaxNodes
			value, err := nativeValue(*input.Body, &nodes, 1)
			if err != nil {
				return nil, err
			}
			record.Body = &value
		}
		return operation.native.Emit(operation.context, operation.id, record)
	})
}

// Flush exports queued events once and collects cumulative metric snapshots.
// Submitted event batches are never automatically replayed after unknown effects.
func (client *Client) Flush(ctx context.Context) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "flush", func(operation *operation) (*invocation.Receipt[native.Result], error) {
		return operation.native.Flush(operation.context, operation.id)
	})
}

// MeasureInt64 records into a fixed declared counter/updowncounter/gauge/histogram.
// Dimensions must be declared scalar keys. Native overflow retains totals, not
// every label; arithmetic overflow is distinct from representation precision.
func (client *Client) MeasureInt64(ctx context.Context, name string, value int64, attrs ...slog.Attr) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "measure-int64", func(operation *operation) (*invocation.Receipt[native.Result], error) {
		converted, err := nativeAttributes(attrs)
		if err != nil {
			return nil, err
		}
		return operation.native.MeasureInt64(operation.context, operation.id, name, value, converted...)
	})
}

// MeasureFloat64 preserves the corresponding native synchronous family, refusing
// nonfinite measurements and negative monotonic-counter increments.
func (client *Client) MeasureFloat64(ctx context.Context, name string, value float64, attrs ...slog.Attr) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "measure-float64", func(operation *operation) (*invocation.Receipt[native.Result], error) {
		converted, err := nativeAttributes(attrs)
		if err != nil {
			return nil, err
		}
		return operation.native.MeasureFloat64(operation.context, operation.id, name, value, converted...)
	})
}

// Inject copies bounded W3C propagation; baggage is explicit and never promoted
// into metric dimensions or log attributes. No global propagator is installed.
func Inject(ctx context.Context, includeBaggage bool) (map[string]string, error) {
	value, err := native.Inject(ctx, includeBaggage)
	return value, translate(err, "inject")
}

// Extract preserves native malformed-header ignore semantics with explicit
// baggage permission. Context contains non-owning trace facts, not an SDK span.
func Extract(ctx context.Context, headers map[string]string, includeBaggage bool) (context.Context, error) {
	value, err := native.Extract(ctx, headers, includeBaggage)
	return value, translate(err, "extract")
}
