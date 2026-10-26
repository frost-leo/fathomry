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

package implementation

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"reflect"
	"strings"
	"testing"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

func TestInvocationBudgetEdges(t *testing.T) {
	for _, phase := range []string{"input", "output"} {
		budget := valueBudget{ctx: context.Background(), phase: phase, values: MaxInvocationValues - 1}
		if err := budget.enter(MaxInvocationDepth, ""); err != nil {
			t.Fatal("exact values/depth", err)
		}
		if err := budget.charge(""); !errors.Is(err, ErrLimit) {
			t.Fatal("value over-limit", err)
		}
		budget.values = 0
		if err := budget.enter(MaxInvocationDepth+1, ""); !errors.Is(err, ErrLimit) {
			t.Fatal("depth over-limit", err)
		}
		budget = valueBudget{ctx: context.Background(), phase: phase, stringBytes: MaxInvocationStringBytes - 1}
		if err := budget.name("a", ""); err != nil || budget.values != 1 || budget.stringBytes != MaxInvocationStringBytes {
			t.Fatal("member accounting", err)
		}
		if err := budget.text("a", ""); !errors.Is(err, ErrLimit) {
			t.Fatal("text over-limit", err)
		}
	}
}
func TestInvocationNullableWireAccounting(t *testing.T) {
	schema := declaration.DataSchema{AnyOf: []declaration.DataSchema{scalar("integer"), {Type: "null"}}}
	for _, raw := range []string{"null", "42"} {
		inputBudget := valueBudget{ctx: context.Background(), phase: "input"}
		if err := readInput(&inputBudget, jsontext.NewDecoder(strings.NewReader(raw)), schema, 1, ""); err != nil {
			t.Fatal(err)
		}
		var pointer *int64
		if raw != "null" {
			value := int64(42)
			pointer = &value
		}
		outputBudget := valueBudget{ctx: context.Background(), phase: "output"}
		if err := validateOutput(&outputBudget, reflect.ValueOf(pointer), schema, 1, ""); err != nil {
			t.Fatal(err)
		}
		if inputBudget.values != 1 || outputBudget.values != 1 {
			t.Fatal("pointer inflated wire accounting")
		}
	}
}
func TestInvocationCancellationPreservesObservedFailure(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("caller cause")
	cancel(cause)
	observed := invocationFailure("output", "integer_range", "/output/0", ErrInvalid)
	err := preferInvocationCancellation(ctx, "output", observed)
	if !errors.Is(err, ErrInvocation) || !errors.Is(err, ErrCanceled) || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || !errors.Is(err, observed) || !errors.Is(err, ErrInvalid) {
		t.Fatal("cause lost", err)
	}
	if _, ok := Diagnose(err); ok {
		t.Fatal("runtime error classified as static")
	}
}
func TestInvocationWriterDoesNotPublishOverflow(t *testing.T) {
	buffer := invocationBuffer{ctx: context.Background()}
	if count, err := buffer.Write(make([]byte, MaxInvocationBytes)); err != nil || count != MaxInvocationBytes {
		t.Fatal(err)
	}
	if count, err := buffer.Write([]byte("x")); !errors.Is(err, ErrLimit) || count != 0 || buffer.data.Len() != MaxInvocationBytes {
		t.Fatal("overflow accepted", err)
	}
}
