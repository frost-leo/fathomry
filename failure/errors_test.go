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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/failure"
)

const invalidInput failure.Code = "example.config.invalid_input"

type nativeError struct{ private string }

func (err *nativeError) Error() string { return err.private }

type hostileError struct{}

func (*hostileError) Error() string                { panic("unexpected native Error") }
func (*hostileError) Is(error) bool                { panic("unexpected native Is") }
func (*hostileError) As(any) bool                  { panic("unexpected native As") }
func (*hostileError) Unwrap() error                { panic("unexpected native Unwrap") }
func (*hostileError) Format(fmt.State, rune)       { panic("unexpected native Format") }
func (*hostileError) LogValue() slog.Value         { panic("unexpected native LogValue") }
func (*hostileError) MarshalJSON() ([]byte, error) { panic("unexpected native JSON") }

type nilMap map[string]string

func (nilMap) Error() string { panic("nil map called") }

type nilSlice []string

func (nilSlice) Error() string { panic("nil slice called") }

type nilFunc func()

func (nilFunc) Error() string { panic("nil func called") }

type nilChan chan bool

func (nilChan) Error() string { panic("nil channel called") }

func TestIdentityAndInvalidConstruction(t *testing.T) {
	for _, test := range []struct {
		code  failure.Code
		valid bool
	}{
		{invalidInput, true}, {"third_party.future.new-meaning_2", true},
		{failure.Code("a." + strings.Repeat("b", failure.MaxCodeBytes-2)), true},
		{"", false}, {"single", false}, {".a", false}, {"a.", false},
		{"a..b", false}, {"a.2b", false}, {"A.b", false}, {"a.中", false},
		{"a.b\n", false}, {"a.b/c", false},
		{failure.Code("a." + strings.Repeat("b", failure.MaxCodeBytes-1)), false},
	} {
		t.Run(fmt.Sprintf("%q", string(test.code)), func(t *testing.T) {
			cause := &hostileError{}
			got := failure.New(test.code, cause)
			want := test.code
			if !test.valid {
				want = failure.Invalid
			}
			if test.code.Valid() != test.valid || got.Code() != want || got.Error() != string(want) ||
				!got.Is(want) || got.Unwrap() != cause {
				t.Fatal("identity validation changed or cause lost")
			}
			described, ok := failure.Inspect(got)
			if !ok || described.Code() != want {
				t.Fatal("direct occurrence not inspectable")
			}
		})
	}
	var zero failure.Error
	if zero.Code() != failure.Invalid || !errors.Is(zero, failure.Invalid) || zero.Unwrap() != nil {
		t.Fatal("zero occurrence is not an explicit invalid failure")
	}
	if got := failure.New("unfamiliar.valid_identity", nil); got.Code() == failure.Invalid || error(got) == nil {
		t.Fatal("unfamiliar identity became invalid or success")
	}
	if errors.Is(failure.New("a.b.c", nil), failure.Code("a.b")) {
		t.Fatal("namespace became classification")
	}
	if errors.Is(failure.New(invalidInput, nil), errors.New(string(invalidInput))) {
		t.Fatal("equal text became identity")
	}
	if zero.Is(failure.Code("")) || zero.Is(nil) {
		t.Fatal("malformed target matched")
	}
}

func TestDirectInspectionIsNotTreeSelection(t *testing.T) {
	first := failure.New("example.first", nil)
	second := failure.New("example.second", nil)
	for _, causes := range [][]error{{first, second}, {second, first}, {errors.Join(second, first), first}} {
		joined := errors.Join(causes...)
		if !errors.Is(joined, first.Code()) || !errors.Is(joined, second.Code()) {
			t.Fatal("joined matching lost an identity")
		}
		if _, ok := failure.Inspect(joined); ok {
			t.Fatal("join silently chose a primary")
		}
		outer := failure.New("example.outer", joined)
		got, ok := failure.Inspect(outer)
		if !ok || got.Code() != "example.outer" {
			t.Fatal("cause replaced described occurrence")
		}
		wrapped := fmt.Errorf("caller: %w", outer)
		if _, ok := failure.Inspect(wrapped); ok {
			t.Fatal("ordinary wrapper was searched implicitly")
		}
		var located failure.Error
		if !errors.As(wrapped, &located) || located.Code() != outer.Code() {
			t.Fatal("intentional Go inspection did not find outer occurrence")
		}
	}
	var forward, reverse failure.Error
	errors.As(errors.Join(first, second), &forward)
	errors.As(errors.Join(second, first), &reverse)
	if forward.Code() == reverse.Code() {
		t.Fatal("rejecting control no longer witnesses ordered first match")
	}
	for _, value := range []error{nil, invalidInput, errors.New("unmapped"), (*failure.Error)(nil)} {
		if _, ok := failure.Inspect(value); ok {
			t.Fatal("absence or unknown object described as occurrence")
		}
	}
}

func TestIntentionalCausesCancellationAndTypedNil(t *testing.T) {
	native := &nativeError{private: "synthetic-private-cause"}
	reason := errors.New("caller supplied reason")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(reason)
	for _, joined := range []error{
		errors.Join(native, ctx.Err(), context.Cause(ctx)),
		errors.Join(context.Cause(ctx), errors.Join(ctx.Err(), native)),
	} {
		got := failure.New("example.wait_ended", joined)
		for _, target := range []error{native, reason, context.Canceled} {
			if !errors.Is(got, target) {
				t.Fatal("intentional cause lost")
			}
		}
		var inspected *nativeError
		if !errors.As(got, &inspected) || inspected != native {
			t.Fatal("intentional typed cause lost")
		}
	}
	hidden := failure.New(invalidInput, nil)
	var nativeView *nativeError
	if errors.Is(hidden, native) || errors.As(hidden, &nativeView) {
		t.Fatal("withheld native error became public")
	}
	if errors.Is(ctx.Err(), reason) {
		t.Fatal("generic cancellation unexpectedly retains separate reason")
	}
	for _, nilCause := range []error{nil, (*nativeError)(nil), nilMap(nil), nilSlice(nil), nilFunc(nil), nilChan(nil), (*failure.Error)(nil)} {
		got := failure.New(invalidInput, nilCause)
		if got.Unwrap() != nil {
			t.Fatal("direct typed nil retained")
		}
		if _, ok := failure.Inspect(nilCause); ok {
			t.Fatal("typed nil was inspected")
		}
	}
	nestedNil := errors.Join((*nativeError)(nil))
	if failure.New(invalidInput, nestedNil).Unwrap() != nestedNil {
		t.Fatal("external graph was traversed or normalized")
	}
}

func TestDiagnosticBoundsCopiesAndOmission(t *testing.T) {
	cause := &hostileError{}
	input := []failure.Attribute{{Name: "z", Value: ""}, {Name: "a", Value: "public"}}
	original := failure.New(invalidInput, cause)
	enriched := failure.New(invalidInput, cause, input...)
	input[0] = failure.Attribute{Name: "changed"}
	snapshot := enriched.Diagnostic()
	want := []failure.Attribute{{Name: "a", Value: "public"}, {Name: "z", Value: ""}}
	if snapshot.Code != invalidInput || snapshot.Omitted || !reflect.DeepEqual(snapshot.Attributes, want) {
		t.Fatal("diagnostics not frozen and sorted")
	}
	snapshot.Attributes[0].Value = "mutated"
	if !reflect.DeepEqual(enriched.Diagnostic().Attributes, want) || len(original.Diagnostic().Attributes) != 0 {
		t.Fatal("snapshot or new construction mutated an existing occurrence")
	}
	cases := [][]failure.Attribute{
		{{Name: ""}}, {{Name: "Upper"}}, {{Name: "a.b"}}, {{Name: "a-b"}},
		{{Name: strings.Repeat("a", failure.MaxAttributeNameBytes+1)}},
		{{Name: "a", Value: strings.Repeat("x", failure.MaxAttributeValueBytes+1)}},
		{{Name: "a", Value: "\xff"}}, {{Name: "a", Value: "\n"}},
		{{Name: "a", Value: "\u0085"}}, {{Name: "a"}, {Name: "a"}},
		make([]failure.Attribute, failure.MaxAttributes+1),
	}
	overBudget := make([]failure.Attribute, 9)
	for index := range overBudget {
		overBudget[index] = failure.Attribute{Name: fmt.Sprintf("a%d", index), Value: strings.Repeat("x", 256)}
	}
	cases = append(cases, overBudget)
	for _, attributes := range cases {
		got := failure.New(invalidInput, cause, attributes...)
		diagnostic := got.Diagnostic()
		if got.Code() != invalidInput || got.Unwrap() != cause || !diagnostic.Omitted || len(diagnostic.Attributes) != 0 {
			t.Fatal("invalid optional diagnostics replaced primary or partially escaped")
		}
	}
	exactBudget := make([]failure.Attribute, 8)
	for index := range exactBudget {
		exactBudget[index] = failure.Attribute{Name: string(rune('a' + index)), Value: strings.Repeat("x", 255)}
	}
	if failure.New(invalidInput, cause, exactBudget...).Diagnostic().Omitted {
		t.Fatal("exact aggregate byte limit refused")
	}
	for _, attributes := range [][]failure.Attribute{
		nil, {}, {{Name: strings.Repeat("a", failure.MaxAttributeNameBytes), Value: strings.Repeat("x", failure.MaxAttributeValueBytes)}},
	} {
		if failure.New(invalidInput, cause, attributes...).Diagnostic().Omitted {
			t.Fatal("valid diagnostics refused")
		}
	}
	maxCount := make([]failure.Attribute, failure.MaxAttributes)
	for index := range maxCount {
		maxCount[index].Name = fmt.Sprintf("a%d", index)
	}
	if failure.New(invalidInput, cause, maxCount...).Diagnostic().Omitted {
		t.Fatal("exact count limit refused")
	}
}

func TestFormattingLoggingAndJSONRefusal(t *testing.T) {
	const private = "synthetic-private-canary"
	cause := &hostileError{}
	err := failure.New(invalidInput, cause, failure.Attribute{Name: "public", Value: private})
	for _, value := range []any{err, &err, failure.Error{}, failure.Code("bad\n" + private)} {
		for _, pattern := range []string{"%v", "%+v", "%#v", "%s", "%q", "%+q", "%#q", "%x", "%#x", "%1000000v", "%.1v"} {
			got := fmt.Sprintf(pattern, value)
			if strings.Contains(got, private) || strings.Contains(got, "PANIC") || len(got) > failure.MaxCodeBytes+2 {
				t.Fatal("formatting escaped its bounded safe projection")
			}
		}
		for _, jsonHandler := range []bool{false, true} {
			var output bytes.Buffer
			var handler slog.Handler = slog.NewTextHandler(&output, nil)
			if jsonHandler {
				handler = slog.NewJSONHandler(&output, nil)
			}
			slog.New(handler).Info("fixture", slog.Any("error", value))
			if strings.Contains(output.String(), private) || strings.Contains(output.String(), "PANIC") {
				t.Fatal("structured logging exposed data or invoked a cause")
			}
		}
	}
	for _, value := range []any{err, &err, failure.Error{}, []failure.Error{err}, struct{ Err error }{err}} {
		data, encodeErr := json.Marshal(value)
		if encodeErr == nil || len(data) != 0 {
			t.Fatal("runtime error silently acquired JSON schema")
		}
	}
	for _, input := range []string{"{}", "null", `{"code":"example.replaced"}`} {
		target := err
		if json.Unmarshal([]byte(input), &target) == nil || target != err {
			t.Fatal("runtime reconstruction accepted or changed the receiver")
		}
	}
	var nilPointer *failure.Error
	if data, encodeErr := json.Marshal(nilPointer); encodeErr != nil || string(data) != "null" {
		t.Fatal("documented native nil-pointer JSON exception changed")
	}
	pointer := &err
	if json.Unmarshal([]byte("null"), &pointer) != nil || pointer != nil {
		t.Fatal("native pointer-container null handling changed")
	}
	var badTarget *failure.Error
	if err.As(badTarget) || err.As(new(*nativeError)) {
		t.Fatal("unrelated As target accepted")
	}
}

func TestRawCompositionIsNotSafePresentation(t *testing.T) {
	raw := &nativeError{private: "synthetic-private-canary"}
	safe := failure.New(invalidInput, raw)
	if strings.Contains(safe.Error(), raw.private) {
		t.Fatal("common fallback leaked cause")
	}
	joined := errors.Join(safe, raw)
	if !strings.Contains(joined.Error(), raw.private) {
		t.Fatal("counterexample no longer demonstrates raw join disclosure")
	}
	protected := failure.New("example.aggregate", joined)
	if strings.Contains(fmt.Sprintf("%#v", protected), raw.private) || !errors.Is(protected, raw) {
		t.Fatal("explicit safe occurrence failed to separate formatting and inspection")
	}
}

func TestFrozenInputsAreDeterministicAndConcurrent(t *testing.T) {
	t.Setenv("LANG", "fixture-one")
	frozen := []failure.Attribute{{Name: "z", Value: "last"}, {Name: "a", Value: "first"}}
	original := failure.New(invalidInput, &hostileError{}, frozen...)
	want := original.Diagnostic()
	t.Setenv("LANG", "fixture-two")
	for range 100 {
		copy := failure.New(invalidInput, original.Unwrap(), frozen...)
		if !reflect.DeepEqual(copy.Diagnostic(), want) || copy.Error() != original.Error() ||
			!copy.LogValue().Equal(original.LogValue()) {
			t.Fatal("explicit frozen inputs produced different owned output")
		}
	}
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			for range 100 {
				copy := original.Diagnostic()
				if !reflect.DeepEqual(copy, want) || !original.Is(invalidInput) {
					t.Error("concurrent read changed immutable occurrence")
				}
				slices.Reverse(copy.Attributes)
				_ = failure.New(original.Code(), original.Unwrap(), copy.Attributes...)
				_ = fmt.Sprintf("%#v", original)
			}
		})
	}
	group.Wait()
}

func FuzzOccurrence(f *testing.F) {
	f.Add("example.identity", "name", "value")
	f.Add("", "", "\xff")
	f.Add("a..b", "same", "\n")
	f.Fuzz(func(t *testing.T, raw, name, value string) {
		code := failure.Code(raw)
		err := failure.New(code, &hostileError{}, failure.Attribute{Name: name, Value: value})
		want := failure.Invalid
		if code.Valid() {
			want = code
		}
		if err.Code() != want || !err.Is(want) || len(err.Error()) > failure.MaxCodeBytes {
			t.Fatal("identity invariant violated")
		}
		copy := err.Diagnostic()
		if copy.Code != want || copy.Omitted && len(copy.Attributes) != 0 {
			t.Fatal("omission invariant violated")
		}
		for _, attribute := range copy.Attributes {
			if len(attribute.Name) > failure.MaxAttributeNameBytes || len(attribute.Value) > failure.MaxAttributeValueBytes {
				t.Fatal("diagnostic bound violated")
			}
		}
		if len(copy.Attributes) != 0 {
			copy.Attributes[0].Value = "changed"
		}
		if fmt.Sprintf("%#v", err) != string(want) {
			t.Fatal("format invoked private state")
		}
		if _, encodeErr := json.Marshal(err); encodeErr == nil {
			t.Fatal("runtime encoding permitted")
		}
	})
}
