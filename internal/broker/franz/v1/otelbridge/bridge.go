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

package otelbridge

import (
	"bytes"
	"context"
	"sort"
	"strings"

	franz "github.com/frost-leo/fathomry/internal/broker/franz/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	otel "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
)

const maxHeaderBytes = 64 << 10

func failure(operation string, causes ...error) error {
	return franz.ErrInput.New(fault.Context{Provider: franz.ProviderID, Operation: operation}, causes...)
}
func recognized(key string) bool {
	return key == "traceparent" || key == "tracestate" || key == "baggage"
}
func validate(headers []franz.Header) error {
	if len(headers) > 64 {
		return failure("trace-headers")
	}
	total := 0
	for _, header := range headers {
		if len(header.Key) > 1024 || len(header.Key) > maxHeaderBytes-total {
			return failure("trace-headers")
		}
		total += len(header.Key)
		if len(header.Value) > maxHeaderBytes-total {
			return failure("trace-headers")
		}
		total += len(header.Value)
	}
	return nil
}

// Inject returns an independently owned header list preserving ordinary byte
// values, duplicates and order. Existing W3C fields reject: this operation does
// not silently overwrite an incoming context. Complete input/output lists are
// bounded to 64 headers/64 KiB; product propagation additionally bounds W3C data
// to 8192 bytes. Input is borrowed only during the call; do not mutate concurrently.
func Inject(ctx context.Context, headers []franz.Header, includeBaggage bool) ([]franz.Header, error) {
	if err := validate(headers); err != nil {
		return nil, err
	}
	for _, header := range headers {
		if recognized(strings.ToLower(header.Key)) {
			return nil, failure("trace-conflict")
		}
	}
	propagation, err := otel.Inject(ctx, includeBaggage)
	if err != nil {
		return nil, failure("trace-inject", err)
	}
	if len(headers)+len(propagation) > 64 {
		return nil, failure("trace-headers")
	}
	result := make([]franz.Header, 0, len(headers)+len(propagation))
	for _, header := range headers {
		result = append(result, franz.Header{Key: strings.Clone(header.Key), Value: bytes.Clone(header.Value)})
	}
	keys := make([]string, 0, len(propagation))
	for key := range propagation {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result = append(result, franz.Header{Key: key, Value: []byte(propagation[key])})
	}
	if err := validate(result); err != nil {
		return nil, err
	}
	return result, nil
}

// Extract reads only the three W3C fields; ordinary duplicate/binary headers
// remain uninterpreted. Reserved names are case-insensitive and duplicates reject.
// The caller decides whether received context/baggage is trusted; this never
// authorizes a source, changes routing, or supplies Item/Run attribution.
func Extract(ctx context.Context, headers []franz.Header, includeBaggage bool) (context.Context, error) {
	if err := validate(headers); err != nil {
		return nil, err
	}
	carrier := make(map[string]string)
	for _, header := range headers {
		key := strings.ToLower(header.Key)
		if !recognized(key) {
			continue
		}
		if _, duplicate := carrier[key]; duplicate {
			return nil, failure("trace-duplicate")
		}
		carrier[key] = string(header.Value)
	}
	extracted, err := otel.Extract(ctx, carrier, includeBaggage)
	if err != nil {
		return nil, failure("trace-extract", err)
	}
	return extracted, nil
}
