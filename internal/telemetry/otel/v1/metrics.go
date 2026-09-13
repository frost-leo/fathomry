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
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"go.opentelemetry.io/otel/attribute"
	metricapi "go.opentelemetry.io/otel/metric"
)

type instrument struct {
	config      instrumentSettings
	addInt      func(context.Context, int64, ...metricapi.AddOption)
	addFloat    func(context.Context, float64, ...metricapi.AddOption)
	recordInt   func(context.Context, int64, ...metricapi.RecordOption)
	recordFloat func(context.Context, float64, ...metricapi.RecordOption)
}

func (owner *owner) makeInstruments(attrs []attribute.KeyValue) error {
	meter := owner.metrics.Meter(owner.settings.Scope, metricapi.WithInstrumentationVersion(owner.settings.ScopeVersion),
		metricapi.WithSchemaURL(owner.settings.ScopeSchemaURL), metricapi.WithInstrumentationAttributes(attrs...))
	owner.instruments = make(map[string]instrument, len(owner.settings.Instruments))
	for _, config := range owner.settings.Instruments {
		item := instrument{config: config}
		var err error
		switch config.Kind {
		case "int64-counter":
			native, failure := meter.Int64Counter(config.Name, metricapi.WithUnit(config.Unit), metricapi.WithDescription(config.Description))
			err, item.addInt = failure, native.Add
		case "float64-counter":
			native, failure := meter.Float64Counter(config.Name, metricapi.WithUnit(config.Unit), metricapi.WithDescription(config.Description))
			err, item.addFloat = failure, native.Add
		case "int64-updowncounter":
			native, failure := meter.Int64UpDownCounter(config.Name, metricapi.WithUnit(config.Unit), metricapi.WithDescription(config.Description))
			err, item.addInt = failure, native.Add
		case "float64-updowncounter":
			native, failure := meter.Float64UpDownCounter(config.Name, metricapi.WithUnit(config.Unit), metricapi.WithDescription(config.Description))
			err, item.addFloat = failure, native.Add
		case "int64-gauge":
			native, failure := meter.Int64Gauge(config.Name, metricapi.WithUnit(config.Unit), metricapi.WithDescription(config.Description))
			err, item.recordInt = failure, native.Record
		case "float64-gauge":
			native, failure := meter.Float64Gauge(config.Name, metricapi.WithUnit(config.Unit), metricapi.WithDescription(config.Description))
			err, item.recordFloat = failure, native.Record
		case "int64-histogram":
			native, failure := meter.Int64Histogram(config.Name, metricapi.WithUnit(config.Unit), metricapi.WithDescription(config.Description),
				metricapi.WithExplicitBucketBoundaries(config.Boundaries...))
			err, item.recordInt = failure, native.Record
		case "float64-histogram":
			native, failure := meter.Float64Histogram(config.Name, metricapi.WithUnit(config.Unit), metricapi.WithDescription(config.Description),
				metricapi.WithExplicitBucketBoundaries(config.Boundaries...))
			err, item.recordFloat = failure, native.Record
		}
		if err != nil {
			return failure(ErrInput, "native-instrument", err)
		}
		owner.instruments[config.Name] = item
	}
	return nil
}

// MeasureInt64 records into a declared int64 instrument, never a dynamic meter.
// Attributes must be declared scalar dimensions (maximum 8, strings 256 bytes).
// Correlation remains in independent evidence, never automatically in labels.
func (client *Client) MeasureInt64(ctx context.Context, id fault.Correlation, name string, value int64, attrs ...slog.Attr) (*invocation.Receipt[Result], error) {
	return client.measure(ctx, id, name, value, 0, true, attrs)
}

// MeasureFloat64 is the floating-point counterpart. NaN/Inf and negative
// monotonic-counter increments are rejected before native measurement.
func (client *Client) MeasureFloat64(ctx context.Context, id fault.Correlation, name string, value float64, attrs ...slog.Attr) (*invocation.Receipt[Result], error) {
	if !finite(value) {
		return nil, failure(ErrInput, "measurement")
	}
	return client.measure(ctx, id, name, 0, value, false, attrs)
}
func (client *Client) measure(ctx context.Context, id fault.Correlation, name string, integer int64, floating float64, isInt bool, attrs []slog.Attr) (*invocation.Receipt[Result], error) {
	if client == nil || client.owner == nil {
		return nil, failure(ErrInput, "measure")
	}
	if client.owner.metrics == nil {
		return nil, failure(ErrUnsupported, "metrics-disabled")
	}
	if !nameValid(name) || len(attrs) > 8 {
		return nil, failure(ErrInput, "measurement")
	}
	return client.run(ctx, id, "measure", func(work context.Context) invocation.Outcome[Result] {
		item, ok := client.owner.instruments[name]
		if !ok || isInt != strings.HasPrefix(item.config.Kind, "int64-") {
			return invocation.Outcome[Result]{Primary: failure(ErrUnsupported, "instrument")}
		}
		if strings.HasSuffix(item.config.Kind, "-counter") && (integer < 0 || floating < 0) {
			return invocation.Outcome[Result]{Primary: failure(ErrInput, "counter-increment")}
		}
		for _, attr := range attrs {
			if !slices.Contains(item.config.AttributeKeys, attr.Key) ||
				attr.Value.Kind() != slog.KindString && attr.Value.Kind() != slog.KindBool &&
					attr.Value.Kind() != slog.KindInt64 && attr.Value.Kind() != slog.KindFloat64 ||
				attr.Value.Kind() == slog.KindString && len(attr.Value.String()) > 256 {
				return invocation.Outcome[Result]{Primary: failure(ErrUnsupported, "metric-dimension")}
			}
		}
		budget := dataBudget{bytes: 4096, nodes: 8}
		frozen, err := freezeAttributes(attrs, &budget, 1)
		if err != nil {
			return invocation.Outcome[Result]{Primary: err}
		}
		option := metricapi.WithAttributes(frozen...)
		if err := work.Err(); err != nil {
			return invocation.Outcome[Result]{Primary: nativeFailure(ErrState, "measure", work, err)}
		}
		switch {
		case item.addInt != nil:
			item.addInt(work, integer, option)
		case item.addFloat != nil:
			item.addFloat(work, floating, option)
		case item.recordInt != nil:
			item.recordInt(work, integer, option)
		case item.recordFloat != nil:
			item.recordFloat(work, floating, option)
		}
		return successful(SignalResult{Signal: Metrics, Accepted: 1})
	})
}
