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
	"strings"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
)

const maxPropagationBytes = 8192

// Inject returns independently owned W3C traceparent/tracestate and, only when
// explicitly selected, baggage headers. Baggage is not authentication, resource,
// log attributes or metric labels. No global propagator is consulted.
func Inject(ctx context.Context, includeBaggage bool) (map[string]string, error) {
	if ctx == nil {
		return nil, failure(ErrInput, "inject")
	}
	if includeBaggage && len(baggage.FromContext(ctx).String()) > maxPropagationBytes {
		return nil, failure(ErrLimit, "baggage")
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	if includeBaggage {
		propagation.Baggage{}.Inject(ctx, carrier)
	}
	if err := carrierBound(carrier); err != nil {
		return nil, err
	}
	return carrier, nil
}

// Extract accepts only the three W3C headers, case-insensitively; duplicate
// spellings/oversized carriers reject. Malformed W3C values follow the native
// propagator's ignore semantics, preserving the original context. Unknown headers
// are ignored after the complete carrier is bounded. The caller owns trust policy.
func Extract(ctx context.Context, headers map[string]string, includeBaggage bool) (context.Context, error) {
	if ctx == nil {
		return nil, failure(ErrInput, "extract")
	}
	if err := carrierBound(headers); err != nil {
		return nil, err
	}
	carrier := propagation.MapCarrier{}
	for key, value := range headers {
		key = strings.ToLower(key)
		if key != "traceparent" && key != "tracestate" && key != "baggage" {
			continue
		}
		if _, found := carrier[key]; found {
			return nil, failure(ErrInput, "duplicate-header")
		}
		carrier[key] = strings.Clone(value)
	}
	ctx = propagation.TraceContext{}.Extract(ctx, carrier)
	if includeBaggage {
		ctx = propagation.Baggage{}.Extract(ctx, carrier)
	}
	return ctx, nil
}
func carrierBound(headers map[string]string) error {
	if len(headers) > 32 {
		return failure(ErrLimit, "propagation")
	}
	bytes := 0
	for key, value := range headers {
		if !boundedString(key, 128) || !boundedString(value, maxPropagationBytes) {
			return failure(ErrInput, "propagation")
		}
		bytes += len(key) + len(value)
		if bytes > maxPropagationBytes {
			return failure(ErrLimit, "propagation")
		}
	}
	return nil
}
