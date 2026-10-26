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

package implementation_test

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"strings"
	"testing"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	implementation "github.com/frost-leo/fathomry/framework/workflow/implementation/v1"
)

func TestInvokeEncodedByteLimits(t *testing.T) {
	calls := 0
	registry := invokeRegistry(t, stringSchema(1, 8), integerSchema(0, 100), func(context.Context, invocationInput[string]) (invocationOutput[int64], error) {
		calls++
		return invocationOutput[int64]{42}, nil
	})
	base := `{"target":"host"}`
	at := []byte("{" + strings.Repeat(" ", implementation.MaxInvocationBytes-len(base)) + base[1:])
	if result, err := registry.Invoke(context.Background(), "read_cpu", at); err != nil || !result.Invoked || calls != 1 {
		t.Fatal("exact input byte bound", err)
	}
	over := append(append([]byte(nil), at...), ' ')
	if result, err := registry.Invoke(context.Background(), "read_cpu", over); !errors.Is(err, implementation.ErrLimit) || result.Invoked || result.Output != nil || calls != 1 {
		t.Fatal("input byte bound bypassed", err)
	}

	const one = 1 << 20
	remaining := implementation.MaxInvocationBytes - 20 - 2*one*6
	values := []string{strings.Repeat("\x00", one), strings.Repeat("\x00", one), strings.Repeat("\x00", remaining/6) + strings.Repeat("a", remaining%6)}
	metric := arraySchema(stringSchema(0, one), 3, 3)
	registry = invokeRegistry(t, stringSchema(1, 8), metric, func(context.Context, invocationInput[string]) (invocationOutput[[]string], error) {
		return invocationOutput[[]string]{values}, nil
	})
	result, err := registry.Invoke(context.Background(), "read_cpu", []byte(base))
	if err != nil || !result.Invoked || len(result.Output) != implementation.MaxInvocationBytes {
		t.Fatal("exact output byte bound", len(result.Output), err)
	}
	values[2] += "a"
	result, err = registry.Invoke(context.Background(), "read_cpu", []byte(base))
	if !errors.Is(err, implementation.ErrLimit) || !result.Invoked || result.Output != nil {
		t.Fatal("output limit published partial data", err)
	}
}

type budgetLists struct {
	A []bool `json:"a"`
	B []bool `json:"b"`
}

func TestInvokeValueAccountingSymmetry(t *testing.T) {
	// Root/target object and two named arrays charge seven entries before items.
	values := budgetLists{A: make([]bool, 1_000_000), B: make([]bool, implementation.MaxInvocationValues-7-1_000_000)}
	boolean := declaration.DataSchema{Type: "boolean"}
	schema := objectSchema(map[string]declaration.DataSchema{"a": arraySchema(boolean, 0, 1_000_000), "b": arraySchema(boolean, 0, 1_000_000)})
	calls := 0
	registry := invokeRegistry(t, schema, schema, func(_ context.Context, input invocationInput[budgetLists]) (invocationOutput[budgetLists], error) {
		calls++
		return invocationOutput[budgetLists]{input.Target}, nil
	})
	raw, err := json.Marshal(invocationInput[budgetLists]{values})
	if err != nil {
		t.Fatal(err)
	}
	result, err := registry.Invoke(context.Background(), "read_cpu", raw)
	if err != nil || !result.Invoked || calls != 1 {
		t.Fatal("exact input/output visit budget differs", err)
	}
	values.B = append(values.B, false)
	raw, err = json.Marshal(invocationInput[budgetLists]{values})
	if err != nil {
		t.Fatal(err)
	}
	result, err = registry.Invoke(context.Background(), "read_cpu", raw)
	if !errors.Is(err, implementation.ErrLimit) || result.Invoked || result.Output != nil || calls != 1 {
		t.Fatal("input one-over visit budget", err)
	}
	registry = invokeRegistry(t, stringSchema(1, 8), schema, func(context.Context, invocationInput[string]) (invocationOutput[budgetLists], error) {
		return invocationOutput[budgetLists]{values}, nil
	})
	result, err = registry.Invoke(context.Background(), "read_cpu", []byte(`{"target":"host"}`))
	if !errors.Is(err, implementation.ErrLimit) || !result.Invoked || result.Output != nil {
		t.Fatal("output one-over visit budget", err)
	}
}

func TestInvokeOutputUnicodeAndNullableArrays(t *testing.T) {
	for _, text := range []string{"", "invalid", string([]byte{0xff})} {
		schema := stringSchema(1, 1)
		schema.Enum = []string{"é"}
		registry := invokeRegistry(t, stringSchema(1, 8), schema, func(context.Context, invocationInput[string]) (invocationOutput[string], error) {
			return invocationOutput[string]{text}, nil
		})
		if result, err := registry.Invoke(context.Background(), "read_cpu", []byte(`{"target":"host"}`)); err == nil || !result.Invoked || result.Output != nil {
			t.Fatal("bad output escaped", err)
		}
	}
	schema := declaration.DataSchema{AnyOf: []declaration.DataSchema{arraySchema(integerSchema(0, 10), 0, 1), {Type: "null"}}}
	var empty []int64 = []int64{}
	var absent []int64
	for _, value := range []*[]int64{nil, &empty, &absent} {
		registry := invokeRegistry(t, stringSchema(1, 8), schema, func(context.Context, invocationInput[string]) (invocationOutput[*[]int64], error) {
			return invocationOutput[*[]int64]{value}, nil
		})
		result, err := registry.Invoke(context.Background(), "read_cpu", []byte(`{"target":"host"}`))
		if value == &absent {
			if err == nil || result.Output != nil {
				t.Fatal("nonnull pointer hid nil array")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		want := `{"value":null}`
		if value != nil {
			want = `{"value":[]}`
		}
		if string(result.Output) != want {
			t.Fatal("nullable output changed", string(result.Output))
		}
	}
}

func BenchmarkInvokeChecked(b *testing.B) {
	registry := invokeRegistry(b, stringSchema(1, 8), integerSchema(0, 100), func(context.Context, invocationInput[string]) (invocationOutput[int64], error) {
		return invocationOutput[int64]{42}, nil
	})
	input := []byte(`{"target":"host"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := registry.Invoke(context.Background(), "read_cpu", input); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInvokeOversizedStringOutput(b *testing.B) {
	oversized := strings.Repeat("x", implementation.MaxInvocationBytes)
	registry := invokeRegistry(b, stringSchema(1, 8), stringSchema(0, 1<<20), func(context.Context, invocationInput[string]) (invocationOutput[string], error) {
		return invocationOutput[string]{oversized}, nil
	})
	input := []byte(`{"target":"host"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := registry.Invoke(context.Background(), "read_cpu", input)
		if err == nil || !result.Invoked || result.Output != nil {
			b.Fatal("oversized result was not rejected")
		}
	}
}
