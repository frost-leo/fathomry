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

package errorbridge

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/fault"
)

const query fault.Kind = "test.query"
const limit fault.Kind = "test.limit"
const neutral fault.Kind = "test.neutral"

func classify(kind fault.Kind) failure.Code {
	switch kind {
	case query:
		return failure.ErrDefinition
	case limit:
		return failure.ErrLimit
	}
	return 0
}

type indirectValueError struct{ payload any }

func (indirectValueError) Error() string { return "indirect" }
func TestContainmentChecksDynamicValueComparability(t *testing.T) {
	for _, payload := range []any{[]int{1}, map[string]int{"key": 1}, [1]any{[]byte{1}}} {
		value := indirectValueError{payload: payload}
		if Contains(value, value, 128) {
			t.Fatal("non-comparable dynamic value was matched")
		}
	}
}

func TestAlreadyForwardedAggregatesAreNotReclassified(t *testing.T) {
	first, second := publicError(t), publicError(t)
	translated := errors.Join(first, second)
	original := query.New(fault.Context{}, limit.New(fault.Context{}))
	forwarded := Forward(original, translated)
	for range 32 {
		codes, again := Inspect(forwarded, 1, classify)
		if len(codes) != 0 || again != forwarded {
			t.Fatal("retained native history was reclassified")
		}
		if !errors.Is(again, original) || !errors.Is(again, first) || !errors.Is(again, second) {
			t.Fatal("idempotent forwarding lost history")
		}
		forwarded = Forward(fmt.Errorf("private-wrapper: %w", forwarded), forwarded)
	}
}

type rejectingIs struct{ next error }

func (*rejectingIs) Error() string       { panic("must not format") }
func (*rejectingIs) Is(error) bool       { panic("must not invoke foreign Is") }
func (value *rejectingIs) Unwrap() error { return value.next }

type nonComparableError []int

func (nonComparableError) Error() string { return "non-comparable" }
func TestBoundedContainmentNeverRunsForeignMatching(t *testing.T) {
	target := errors.New("target")
	wrapped := &rejectingIs{next: target}
	if !Contains(wrapped, target, 2) || Contains(wrapped, target, 1) {
		t.Fatal("identity containment ignored its bound")
	}
	cyclic := &cycle{}
	if Contains(cyclic, target, 128) || cyclic.visits != 128 {
		t.Fatal("cycle containment did not terminate")
	}
	if Contains(nonComparableError{1}, nonComparableError{1}, 128) {
		t.Fatal("uncomparable identity guessed")
	}
	if !Contains(nil, nil, 0) || Contains(target, nil, 128) {
		t.Fatal("nil identity changed")
	}
}

func TestDetailsKeepTheirCurrentOccurrenceAndBoundTraversal(t *testing.T) {
	definition := failure.Definitions()[0]
	definition.Details = failure.Contract{ID: "fathomry.failure.detail_data", Version: 1}
	first, err := failure.NewDetailed(definition, failure.Location{Operation: "first"}, 7, func(value int) int { return value })
	if err != nil {
		t.Fatal(err)
	}
	second, err := failure.NewDetailed(definition, failure.Location{Operation: "second"}, 9, func(value int) int { return value }, first)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := Forward(fmt.Errorf("private: %w", first), first)
	if value, present := Details[int](wrapped, first.Failure(), 128); !present || value != 7 {
		t.Fatal("current typed data lost")
	}
	if _, present := Details[int](second, first.Failure(), 128); present {
		t.Fatal("details taken from a different occurrence")
	}
	cyclic := &cycle{}
	if _, present := Details[int](cyclic, first.Failure(), 128); present || cyclic.visits != 128 {
		t.Fatal("detail traversal unbounded")
	}
}

func publicError(t testing.TB) *failure.Error {
	t.Helper()
	value, err := failure.New(failure.Definitions()[0], failure.Location{Operation: "owned"})
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func TestNativePriorityDeduplicationAndPublicBoundary(t *testing.T) {
	native := query.New(fault.Context{}, limit.New(fault.Context{}))
	codes, forwarded := Inspect(errors.Join(native, native), 128, classify)
	if !reflect.DeepEqual(codes, []failure.Code{failure.ErrDefinition, failure.ErrLimit}) || forwarded != nil {
		t.Fatal("native order/classification changed")
	}
	public := publicError(t)
	codes, forwarded = Inspect(query.New(fault.Context{}, public), 128, classify)
	if len(codes) != 1 || codes[0] != failure.ErrDefinition || forwarded != nil {
		t.Fatal("explicit native boundary was bypassed")
	}
	public, err := failure.New(failure.Definitions()[0], failure.Location{Operation: "owned"}, native)
	if err != nil {
		t.Fatal(err)
	}
	codes, forwarded = Inspect(public, 128, classify)
	if len(codes) != 0 || forwarded != public {
		t.Fatal("already-public subtree was reclassified")
	}
}

type opaqueWrapper struct{ cause error }

func (*opaqueWrapper) Error() string       { panic("original presentation must never run") }
func (value *opaqueWrapper) Unwrap() error { return value.cause }
func TestForwardingNeverFormatsWrapperAndRetainsCore(t *testing.T) {
	public := publicError(t)
	original := &opaqueWrapper{cause: public}
	codes, forwarded := Inspect(neutral.New(fault.Context{}, original), 128, classify)
	core, ok := failure.Inspect(forwarded)
	if len(codes) != 0 || !ok || core != public || !errors.Is(forwarded, original) {
		t.Fatal("public forwarding changed identity")
	}
	if text := fmt.Sprintf("%v %+v %#v %s %q", forwarded, forwarded, forwarded, forwarded, forwarded); !strings.Contains(text, string(public.Diagnostic().Definition.Identifier)) {
		t.Fatal("safe identity unavailable")
	}
	var out bytes.Buffer
	slog.New(slog.NewJSONHandler(&out, nil)).Info("safe", "error", forwarded)
	if _, err := json.Marshal(forwarded); !errors.Is(err, failure.ErrSerialization) {
		t.Fatal("runtime graph serialized")
	}
}

type cycle struct{ visits int }

func (*cycle) Error() string       { return "private-cycle" }
func (value *cycle) Unwrap() error { value.visits++; return value }

type wide struct {
	visited  int
	children []error
}

func (*wide) Error() string         { return "private-wide" }
func (value *wide) Unwrap() []error { value.visited++; return value.children }
func TestTraversalBoundsAndIncompletePublicGraph(t *testing.T) {
	cyclic := &cycle{}
	codes, forwarded := Inspect(cyclic, 128, classify)
	if cyclic.visits != 128 || len(codes) != 0 || forwarded != nil {
		t.Fatal("cycle inspection not bounded")
	}
	public := publicError(t)
	expanded := &wide{children: []error{public, public, query.New(fault.Context{})}}
	codes, forwarded = Inspect(expanded, 2, classify)
	if forwarded != nil || len(codes) != 0 || expanded.visited != 1 {
		t.Fatal("unvisited native tail became a public-only graph")
	}
	_, forwarded = Inspect(&wide{}, 128, classify)
	if forwarded != nil {
		t.Fatal("non-nil empty wrapper invented public success")
	}
}
func TestPublicAggregateHasNoInventedPrimaryOwner(t *testing.T) {
	first, second := publicError(t), publicError(t)
	original := fmt.Errorf("private-prefix: %w", errors.Join(first, second))
	codes, forwarded := Inspect(original, 128, classify)
	if len(codes) != 0 || forwarded == nil {
		t.Fatal("aggregate unavailable")
	}
	if _, ok := failure.Inspect(forwarded); ok {
		t.Fatal("aggregate invented one occurrence")
	}
	for _, cause := range []error{original, first, second} {
		if !errors.Is(forwarded, cause) {
			t.Fatal("aggregate cause detached")
		}
	}
	if strings.Contains(forwarded.Error(), "private-") {
		t.Fatal("wrapper text leaked")
	}
}
func FuzzErrorGraphs(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5})
	f.Add([]byte{1, 1, 1, 1})
	f.Add([]byte{3, 3, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 128 {
			return
		}
		public := publicError(t)
		var value error = public
		for _, kind := range data {
			switch kind % 5 {
			case 0:
				value = fmt.Errorf("private-canary: %w", value)
			case 1:
				value = neutral.New(fault.Context{}, value)
			case 2:
				value = errors.Join(value, public)
			case 3:
				value = query.New(fault.Context{}, value)
			case 4:
				value = limit.New(fault.Context{}, value)
			}
		}
		codes, forwarded := Inspect(value, 1024, classify)
		for _, code := range codes {
			if code != failure.ErrDefinition && code != failure.ErrLimit {
				t.Fatal("classification drift")
			}
		}
		if forwarded != nil {
			if !errors.Is(forwarded, public) || strings.Contains(fmt.Sprintf("%v", forwarded), "private-") {
				t.Fatal("forwarded graph lost safety/identity")
			}
		}
	})
}
