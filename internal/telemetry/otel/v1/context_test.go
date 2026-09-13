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
	"testing"

	"go.opentelemetry.io/otel/baggage"
	traceapi "go.opentelemetry.io/otel/trace"
)

func TestExplicitW3CPropagationAndBaggageSeparation(t *testing.T) {
	headers := map[string]string{"TraceParent": "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01", "TraceState": "vendor=value", "Baggage": "region=west"}
	ctx, err := Extract(context.Background(), headers, true)
	if err != nil {
		t.Fatal(err)
	}
	headers["TraceParent"] = "mutated"
	native := traceapi.SpanContextFromContext(ctx)
	if !native.IsRemote() || !native.IsSampled() || baggage.FromContext(ctx).Member("region").Value() != "west" {
		t.Fatal("propagation failed")
	}
	encoded, err := Inject(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if encoded["traceparent"] != "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01" || encoded["tracestate"] != "vendor=value" || encoded["baggage"] != "region=west" {
		t.Fatal("native propagation changed")
	}
	encoded, err = Inject(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := encoded["baggage"]; ok {
		t.Fatal("baggage opt-out ignored")
	}
	clean, err := Extract(context.Background(), map[string]string{"baggage": "region=west"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if baggage.FromContext(clean).Len() != 0 {
		t.Fatal("unselected baggage extracted")
	}
	unchanged, err := Extract(ctx, map[string]string{"traceparent": "malformed"}, false)
	if err != nil || !traceapi.SpanContextFromContext(unchanged).Equal(native) {
		t.Fatal("native invalid-header semantics changed")
	}
}
func TestPropagationBoundsAndAmbiguity(t *testing.T) {
	for _, headers := range []map[string]string{
		{"baggage": strings.Repeat("x", 8193)}, {"traceparent": "x", "TraceParent": "y"}, {"bad": "\x00"},
	} {
		if _, err := Extract(context.Background(), headers, true); err == nil {
			t.Fatal("invalid carrier accepted")
		}
	}
	if _, err := Inject(nil, false); err == nil {
		t.Fatal("nil context accepted")
	}
}
func FuzzPropagation(f *testing.F) {
	f.Add("00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01", "region=west")
	f.Fuzz(func(t *testing.T, parent, baggage string) {
		if len(parent)+len(baggage) > 16384 {
			return
		}
		ctx, err := Extract(context.Background(), map[string]string{"traceparent": parent, "baggage": baggage}, true)
		if err == nil {
			_, _ = Inject(ctx, true)
		}
	})
}
