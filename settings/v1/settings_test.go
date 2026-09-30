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

package settings

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
)

func identity[T any](value T) T { return value }

func mustSnapshot[T any](t testing.TB, value T, clone func(T) T) Snapshot[T] {
	t.Helper()
	snapshot, err := New(value, clone)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestSnapshot(t *testing.T) {
	t.Run("copy_and_exact_type", func(t *testing.T) {
		type config struct {
			Values  []string
			private time.Time
		}
		input := config{Values: []string{"original"}, private: time.Unix(100, 0)}
		calls := 0
		clone := func(value config) config { calls++; value.Values = slices.Clone(value.Values); return value }
		snapshot := mustSnapshot(t, input, clone)
		input.Values[0] = "input mutation"
		recovered, err := As[config](snapshot.View())
		if err != nil || calls != 1 {
			t.Fatal("As copied or lost root type")
		}
		first, err := recovered.ValueCopy()
		if err != nil || first.Values[0] != "original" || first.private != input.private {
			t.Fatal("root not retained")
		}
		first.Values[0] = "output mutation"
		next, _ := snapshot.ValueCopy()
		if next.Values[0] != "original" || calls != 3 {
			t.Fatal("copy contract violated")
		}
		type other config
		if _, err := As[other](snapshot.View()); !errors.Is(err, ErrType) {
			t.Fatal("named type coerced")
		}
		if _, err := As[*config](snapshot.View()); !errors.Is(err, ErrType) {
			t.Fatal("root silently dereferenced")
		}
	})
	t.Run("nil_and_nonstruct_roots", func(t *testing.T) {
		snapshot := mustSnapshot[any](t, nil, identity[any])
		copy, err := snapshot.ValueCopy()
		if copy != nil || err != nil {
			t.Fatal("nil interface root lost")
		}
		value, found, err := Read[any](snapshot.View(), "", identity[any])
		if err != nil || !found || value != nil {
			t.Fatal("nil interface leaf lost")
		}
		if _, err := As[int](snapshot.View()); !errors.Is(err, ErrType) {
			t.Fatal("interface root type erased")
		}
		list := mustSnapshot(t, []int{1, 2}, slices.Clone[[]int])
		copied, _ := list.ValueCopy()
		copied[0] = 9
		again, _ := list.ValueCopy()
		if again[0] != 1 {
			t.Fatal("nonstruct copy aliased")
		}
	})
	t.Run("recursive_and_owner_defined_data", func(t *testing.T) {
		type node struct {
			Value string
			Next  *node
		}
		input := &node{Value: "original"}
		input.Next = input
		clone := func(value *node) *node {
			if value == nil {
				return nil
			}
			result := &node{Value: value.Value}
			result.Next = result
			return result
		}
		snapshot := mustSnapshot(t, input, clone)
		input.Value = "changed"
		value, _ := snapshot.ValueCopy()
		if value == input || value.Next != value || value.Value != "original" {
			t.Fatal("owner recursive copy lost")
		}
		fn := func() string { return "owner" }
		callable, _ := mustSnapshot(t, fn, identity[func() string]).ValueCopy()
		if callable() != "owner" {
			t.Fatal("function field whitelist reintroduced")
		}
	})
	t.Run("bad_copy_policy_is_not_magically_safe", func(t *testing.T) {
		input := []int{1}
		snapshot := mustSnapshot(t, input, identity[[]int])
		input[0] = 2
		got, _ := snapshot.ValueCopy()
		if got[0] != 2 {
			t.Fatal("oracle no longer exercises a shallow copy")
		}
	})
	t.Run("invalid_handles", func(t *testing.T) {
		if _, err := New(1, nil); !errors.Is(err, ErrCopy) {
			t.Fatal("nil copy accepted")
		}
		var empty Snapshot[int]
		if _, err := empty.ValueCopy(); !errors.Is(err, ErrSnapshot) {
			t.Fatal("zero snapshot accepted")
		}
		if _, err := As[int](empty.View()); !errors.Is(err, ErrSnapshot) {
			t.Fatal("zero view accepted")
		}
	})
}

type privateMethods struct{ Value string }

func (privateMethods) String() string               { panic("String must not run") }
func (privateMethods) LogValue() slog.Value         { panic("LogValue must not run") }
func (privateMethods) MarshalJSON() ([]byte, error) { panic("MarshalJSON must not run") }

func TestOpaqueHandles(t *testing.T) {
	var calls atomic.Int32
	snapshot := mustSnapshot(t, privateMethods{Value: "private-settings-canary"}, func(value privateMethods) privateMethods { calls.Add(1); return value })
	store := NewStore[privateMethods]()
	if err := store.Publish(snapshot); err != nil {
		t.Fatal(err)
	}
	view := snapshot.View()
	reader := store.Reader()
	for _, handle := range []any{snapshot, &snapshot, store, &store, view, &view, reader, &reader} {
		var output bytes.Buffer
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x"} {
			_, _ = fmt.Fprintf(&output, format, handle)
		}
		slog.New(slog.NewTextHandler(&output, nil)).Info("handle", "value", handle)
		slog.New(slog.NewJSONHandler(&output, nil)).Info("handle", "value", handle)
		encoded, err := json.Marshal(handle)
		if !errors.Is(err, ErrSerialization) || len(encoded) != 0 {
			t.Fatal("runtime handle serialized")
		}
		if strings.Contains(output.String(), "private-settings-canary") {
			t.Fatal("private data formatted")
		}
	}
	for _, target := range []any{&snapshot, &store, &view, &reader} {
		for _, document := range []string{"{}", "null"} {
			if err := json.Unmarshal([]byte(document), target); !errors.Is(err, ErrSerialization) {
				t.Fatal("runtime handle reconstructed")
			}
		}
	}
	if calls.Load() != 1 {
		t.Fatal("implicit copy callback invocation")
	}
	if _, err := reader.Capture(); err != nil {
		t.Fatal("rejection changed existing handle")
	}
	recovered, err := snapshot.ValueCopy()
	if err != nil || recovered.Value != "private-settings-canary" {
		t.Fatal("rejection changed original data")
	}
	for _, err := range []error{reject(ErrPath, "read"), reject(ErrType, "as"), reject(ErrSnapshot, "new")} {
		if strings.Contains(fmt.Sprintf("%+v", err), "private-settings-canary") {
			t.Fatal("private error data")
		}
	}
}

func TestDefinitions(t *testing.T) {
	definitions := Definitions()
	catalog, err := failure.Prepare(append(failure.Definitions(), definitions...)...)
	if err != nil {
		t.Fatal(err)
	}
	if len(definitions) != 8 {
		t.Fatal("incomplete definitions")
	}
	for _, definition := range definitions {
		if definition.Code.Domain() != failure.DomainConfiguration || definition.Code.Facility() != 0x041 {
			t.Fatal("settings escaped the configuration capability domain")
		}
		occurrence := reject(definition.Code, "read")
		core, found := failure.Inspect(occurrence)
		if !found || !errors.Is(occurrence, definition.Code) ||
			core.Diagnostic().Definition != definition || core.Diagnostic().Location.Operation != "read" {
			t.Fatal("error not integrated with shared failure")
		}
		looked, found, err := catalog.Lookup(definition.Code)
		if err != nil || !found || looked != definition {
			t.Fatal("numeric atlas lookup failed")
		}
	}
	definitions[0].Message = "changed"
	if Definitions()[0].Message == "changed" {
		t.Fatal("definition alias")
	}
}
