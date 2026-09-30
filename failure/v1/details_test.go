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

package failure_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
)

type readDetails struct {
	Document string
	Attempt  uint64
	Fields   map[string][]string
	Offset   *int64
	Bytes    []byte
	Flags    [2]bool
}

func cloneReadDetails(value readDetails) readDetails {
	value.Fields = maps.Clone(value.Fields)
	for name, values := range value.Fields {
		value.Fields[name] = slices.Clone(values)
	}
	if value.Offset != nil {
		offset := *value.Offset
		value.Offset = &offset
	}
	value.Bytes = slices.Clone(value.Bytes)
	return value
}

func detailedDefinition() failure.Definition {
	result := definition()
	result.Details = failure.Contract{ID: "example.source.read_details", Version: 1}
	return result
}

func TestTypedDetails(t *testing.T) {
	t.Run("owner copy isolates input and reads", func(t *testing.T) {
		offset := int64(0)
		input := readDetails{Document: "private-document", Attempt: 0, Fields: map[string][]string{"field": {"private-value"}}, Offset: &offset,
			Bytes: []byte("private-bytes"), Flags: [2]bool{true, false}}
		cause := errors.New("native-cause")
		value, err := failure.NewDetailed(detailedDefinition(), failure.Location{Operation: "read"}, input, cloneReadDetails, cause)
		if err != nil {
			t.Fatal(err)
		}
		input.Fields["field"][0], input.Bytes[0], offset = "changed", 'x', 99
		first, ok := value.Details()
		if !ok || first.Document != "private-document" || first.Fields["field"][0] != "private-value" || string(first.Bytes) != "private-bytes" || first.Offset == nil || *first.Offset != 0 || first.Attempt != 0 {
			t.Fatal("input alias or zero-value semantics changed")
		}
		first.Fields["field"][0], first.Bytes[0], *first.Offset = "mutated", 'z', 12
		delete(first.Fields, "field")
		second, _ := value.Details()
		if second.Fields["field"][0] != "private-value" || string(second.Bytes) != "private-bytes" || *second.Offset != 0 {
			t.Fatal("returned details retained an alias")
		}
		core, ok := failure.Inspect(value)
		if !ok || core != value.Failure() || core.Diagnostic().Definition.Details != detailedDefinition().Details ||
			!errors.Is(value, sampleCode) || !errors.Is(value, cause) {
			t.Fatal("typed/core/native composition lost")
		}
		for _, form := range []any{value, *value, (*failure.Detailed[readDetails])(nil), failure.Detailed[readDetails]{}} {
			for _, canary := range []string{"private-document", "private-value", "private-bytes", "native-cause"} {
				assertPrivate(t, form, canary)
			}
		}
		for _, form := range []any{value, *value} {
			if _, err := json.Marshal(form); !errors.Is(err, failure.ErrSerialization) {
				t.Fatal("typed runtime error serialized", err)
			}
		}
		for _, raw := range []string{"{}", "null", `{"Document":"changed"}`, "[]"} {
			var target failure.Detailed[readDetails]
			if err := json.Unmarshal([]byte(raw), &target); !errors.Is(err, failure.ErrSerialization) {
				t.Fatal("typed runtime reconstructed", err)
			}
		}
	})
	t.Run("nil versus present-empty", func(t *testing.T) {
		for _, input := range []readDetails{{}, {Fields: map[string][]string{}, Bytes: []byte{}}} {
			value, err := failure.NewDetailed(detailedDefinition(), failure.Location{}, input, cloneReadDetails)
			if err != nil {
				t.Fatal(err)
			}
			got, ok := value.Details()
			if !ok || !reflect.DeepEqual(input, got) {
				t.Fatal("nil/empty representation changed")
			}
		}
		var empty *failure.Detailed[readDetails]
		if data, ok := empty.Details(); ok || !reflect.DeepEqual(data, readDetails{}) || empty.Failure() != nil || empty.Unwrap() != nil {
			t.Fatal("nil typed occurrence acquired details")
		}
		if _, ok := failure.Inspect(empty); ok {
			t.Fatal("typed nil became an occurrence")
		}
	})
	t.Run("same occurrence, not a descendant", func(t *testing.T) {
		inner, err := failure.NewDetailed(detailedDefinition(), failure.Location{}, readDetails{Document: "inner"}, cloneReadDetails)
		if err != nil {
			t.Fatal(err)
		}
		outer, err := failure.NewDetailed(detailedDefinition(), failure.Location{}, readDetails{Document: "outer"}, cloneReadDetails, inner)
		if err != nil {
			t.Fatal(err)
		}
		core, _ := failure.Inspect(outer)
		details, _ := outer.Details()
		if core != outer.Failure() || details.Document != "outer" {
			t.Fatal("paired inner details with outer identity")
		}
		var found *failure.Detailed[readDetails]
		if !errors.As(outer, &found) || found != outer {
			t.Fatal("Go traversal order unexpectedly changed")
		}
		wrapped := mustError(t, definition(), failure.Location{}, inner)
		if _, ok := any(wrapped).(interface{ Details() (readDetails, bool) }); ok {
			t.Fatal("untyped outer borrowed descendant details")
		}
		if core, ok := failure.Inspect(fmt.Errorf("wrapped: %w", outer)); core != nil || ok {
			t.Fatal("inferred a primary through arbitrary wrapping")
		}
	})
}

type linkedDetails struct {
	value string
	next  *linkedDetails
}

func cloneLinkedDetails(root *linkedDetails) *linkedDetails {
	copied := make(map[*linkedDetails]*linkedDetails)
	var copyNode func(*linkedDetails) *linkedDetails
	copyNode = func(node *linkedDetails) *linkedDetails {
		if node == nil {
			return nil
		}
		if prior, found := copied[node]; found {
			return prior
		}
		result := &linkedDetails{value: node.value}
		copied[node] = result
		result.next = copyNode(node.next)
		return result
	}
	return copyNode(root)
}

type extendedDetails struct {
	at      time.Time
	payload any
	root    *linkedDetails
}

func TestComponentOwnedTypes(t *testing.T) {
	t.Run("private standard-library fields, interfaces and cycles", func(t *testing.T) {
		first := &linkedDetails{value: "private-first"}
		second := &linkedDetails{value: "private-second", next: first}
		first.next = second
		instant := time.Date(2026, 9, 29, 8, 0, 0, 0, time.FixedZone("display", 8*60*60))
		input := extendedDetails{at: instant, payload: map[int][]string{7: {"private-payload"}}, root: first}
		copyValue := func(value extendedDetails) extendedDetails {
			payload := maps.Clone(value.payload.(map[int][]string))
			for key, values := range payload {
				payload[key] = slices.Clone(values)
			}
			value.payload = payload
			value.root = cloneLinkedDetails(value.root)
			return value
		}
		value, err := failure.NewDetailed(detailedDefinition(), failure.Location{}, input, copyValue)
		if err != nil {
			t.Fatal("component-defined structure refused", err)
		}
		first.value = "changed"
		input.payload.(map[int][]string)[7][0] = "changed"
		got, ok := value.Details()
		if !ok || got.at != instant || got.root.value != "private-first" || got.root.next.next != got.root ||
			got.payload.(map[int][]string)[7][0] != "private-payload" {
			t.Fatal("component-owned fields, cycle or copy semantics changed")
		}
		got.root.next.value = "changed"
		got.payload.(map[int][]string)[7][0] = "changed"
		next, _ := value.Details()
		if next.root.next.value != "private-second" || next.payload.(map[int][]string)[7][0] != "private-payload" {
			t.Fatal("component-owned read copy was not isolated")
		}
		for _, canary := range []string{"private-first", "private-second", "private-payload"} {
			assertPrivate(t, value, canary)
		}
	})
	t.Run("non-struct T and explicit immutable sharing", func(t *testing.T) {
		input := map[int]string{1: "before"}
		value, err := failure.NewDetailed(detailedDefinition(), failure.Location{}, input, maps.Clone[map[int]string])
		if err != nil {
			t.Fatal(err)
		}
		input[1] = "after"
		got, ok := value.Details()
		if !ok || got[1] != "before" {
			t.Fatal("non-struct component data rejected or aliased")
		}
		var calls atomic.Int64
		call := func() string { calls.Add(1); return "deliberately borrowed function" }
		borrowed, err := failure.NewDetailed(detailedDefinition(), failure.Location{}, call, func(value func() string) func() string { return value })
		if err != nil {
			t.Fatal(err)
		}
		assertPrivate(t, borrowed, "deliberately borrowed function")
		if calls.Load() != 0 {
			t.Fatal("failure implicitly executed a detail")
		}
		function, ok := borrowed.Details()
		if !ok || function() != "deliberately borrowed function" || calls.Load() != 1 {
			t.Fatal("declared borrowed data changed")
		}
	})
	t.Run("broken copy policy fails the alias oracle", func(t *testing.T) {
		isolated := func(copyValue func(readDetails) readDetails) bool {
			input := readDetails{Fields: map[string][]string{"field": {"before"}}}
			value, err := failure.NewDetailed(detailedDefinition(), failure.Location{}, input, copyValue)
			if err != nil {
				t.Fatal(err)
			}
			input.Fields["field"][0] = "input mutation"
			first, _ := value.Details()
			if first.Fields["field"][0] != "before" {
				return false
			}
			first.Fields["field"][0] = "output mutation"
			second, _ := value.Details()
			return second.Fields["field"][0] == "before"
		}
		if !isolated(cloneReadDetails) || isolated(func(value readDetails) readDetails { return value }) {
			t.Fatal("alias oracle did not distinguish the broken owner policy")
		}
	})
}

type formatDetail struct{ Value string }

func (formatDetail) String() string               { panic("detail formatter called") }
func (formatDetail) MarshalJSON() ([]byte, error) { panic("detail encoder called") }

func TestDetailContract(t *testing.T) {
	t.Run("admission precedes component copying", func(t *testing.T) {
		var calls int
		clone := func(value readDetails) readDetails { calls++; return cloneReadDetails(value) }
		bad := detailedDefinition()
		bad.Code = 0
		if value, err := failure.NewDetailed(bad, failure.Location{}, readDetails{}, clone); value != nil || !errors.Is(err, failure.ErrCode) {
			t.Fatal(err)
		}
		if value, err := failure.NewDetailed(detailedDefinition(), failure.Location{Operation: "invalid operation"}, readDetails{}, clone); value != nil || !errors.Is(err, failure.ErrLocation) {
			t.Fatal(err)
		}
		if _, err := failure.NewDetailed(detailedDefinition(), failure.Location{}, readDetails{}, clone, make([]error, failure.MaxCauses+1)...); !errors.Is(err, failure.ErrLimit) {
			t.Fatal(err)
		}
		if _, err := failure.NewDetailed(detailedDefinition(), failure.Location{}, readDetails{}, nil); !errors.Is(err, failure.ErrDetails) {
			t.Fatal("implicit copy policy introduced", err)
		}
		if _, err := failure.NewDetailed(definition(), failure.Location{}, readDetails{}, clone); !errors.Is(err, failure.ErrDetails) {
			t.Fatal("undeclared detail contract accepted", err)
		}
		if calls != 0 {
			t.Fatal("component callback ran before shared admission")
		}
	})
	t.Run("diagnostics never invoke copying or detail methods", func(t *testing.T) {
		var copies atomic.Int64
		clone := func(value formatDetail) formatDetail { copies.Add(1); return value }
		value, err := failure.NewDetailed(detailedDefinition(), failure.Location{}, formatDetail{Value: "private"}, clone)
		if err != nil || copies.Load() != 1 {
			t.Fatal(err)
		}
		assertPrivate(t, value, "private")
		_, _ = json.Marshal(value)
		_ = value.Failure().Diagnostic()
		if copies.Load() != 1 {
			t.Fatal("ordinary diagnostics called the component copy function")
		}
		got, ok := value.Details()
		if !ok || got.Value != "private" || copies.Load() != 2 {
			t.Fatal("explicit read did not use the owner copy function")
		}
	})
}

func TestConcurrentDetailReads(t *testing.T) {
	value, err := failure.NewDetailed(detailedDefinition(), failure.Location{}, readDetails{Fields: map[string][]string{"field": {"value"}}}, cloneReadDetails)
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			for range 100 {
				copy, ok := value.Details()
				if !ok || copy.Fields["field"][0] != "value" {
					t.Error("typed read was mutated")
				}
				copy.Fields["field"][0] = "local change"
				_ = value.Error()
				_ = value.LogValue()
			}
		})
	}
	workers.Wait()
}
