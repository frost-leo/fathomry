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
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
)

const primaryCode failure.Condition = "example.source.absent"
const cleanupCode failure.Condition = "another.owner.cleanup"

func newFailure(t *testing.T, condition failure.Condition, causes ...error) *failure.Error {
	t.Helper()
	err, rejected := failure.New(condition, causes...)
	if rejected != nil {
		t.Fatal(rejected)
	}
	return err
}

func TestIdentityAndDirectInspection(t *testing.T) {
	primary := newFailure(t, primaryCode)
	cleanup := newFailure(t, cleanupCode)
	outer := newFailure(t, "example.operation.failed", primary, cleanup, context.Canceled)
	for _, target := range []error{primaryCode, cleanupCode, context.Canceled, primary, cleanup, outer} {
		if !errors.Is(outer, target) {
			t.Fatalf("missing deliberate match %T", target)
		}
	}
	for _, target := range []error{errors.New(primary.Error()), newFailure(t, primaryCode), failure.Condition(""), failure.Condition("owner"), nil} {
		if errors.Is(primary, target) {
			t.Fatalf("unintended match %T", target)
		}
	}
	if current, ok := failure.Inspect(outer); !ok || current != outer || current.Diagnostic().Condition != "example.operation.failed" {
		t.Fatal("outer meaning lost")
	}
	for _, err := range []error{nil, (*failure.Error)(nil), &failure.Error{}, primaryCode, errors.New("foreign"), fmt.Errorf("wrap: %w", primary), errors.Join(primary, cleanup), errors.Join(cleanup, primary)} {
		if current, ok := failure.Inspect(err); current != nil || ok {
			t.Fatalf("inferred direct occurrence %T", err)
		}
	}
	var found *failure.Error
	if !errors.As(fmt.Errorf("wrap: %w", primary), &found) || found != primary {
		t.Fatal("As lost")
	}
	if errors.Unwrap(outer) != nil {
		t.Fatal("unexpected singular unwrap")
	}
	unfamiliar := newFailure(t, "unregistered.partner.new_condition")
	if current, ok := failure.Inspect(unfamiliar); !ok || current != unfamiliar {
		t.Fatal("registry required")
	}
	copyValue := *outer
	if copyValue != *outer || copyValue.Diagnostic() != outer.Diagnostic() {
		t.Fatal("copy identity")
	}
}

func TestRequiredInputBoundsAndOwnership(t *testing.T) {
	for _, code := range []failure.Condition{"", "owner", ".owner", "owner.", "a..b", "a.1b", "A.b", "a.b/c", "a.\x00secret", "a.é", failure.Condition("a." + strings.Repeat("b", failure.MaxConditionBytes-1))} {
		err, rejected := failure.New(code, &hostile{})
		if err != nil || rejected != failure.ErrCondition || strings.Contains(rejected.Error(), "secret") {
			t.Fatalf("invalid input accepted: %q", code)
		}
	}
	atLimit := failure.Condition("a." + strings.Repeat("b", failure.MaxConditionBytes-2))
	if !atLimit.Valid() || newFailure(t, atLimit).Diagnostic().Condition != atLimit {
		t.Fatal("valid limit rejected")
	}
	if !failure.Condition("example.owner-code_1.condition2").Valid() {
		t.Fatal("grammar")
	}
	tooMany := make([]error, failure.MaxCauses+1)
	tooMany[0] = &hostile{}
	err, rejected := failure.New(primaryCode, tooMany...)
	if err != nil || rejected != failure.ErrCauses || tooMany[0] == nil {
		t.Fatal("over-limit accepted or inputs consumed")
	}
	allNil := make([]error, failure.MaxCauses)
	if newFailure(t, primaryCode, allNil...).Diagnostic().CauseCount != 0 {
		t.Fatal("literal nil retained")
	}
	cause := errors.New("caller cause")
	causes := make([]error, failure.MaxCauses)
	for index := range causes {
		causes[index] = cause
	}
	owned := newFailure(t, primaryCode, causes...)
	causes[0] = nil
	exposed := owned.Unwrap()
	exposed[1] = nil
	if owned.Diagnostic().CauseCount != failure.MaxCauses || owned.Unwrap()[0] != cause || owned.Unwrap()[1] != cause {
		t.Fatal("cause alias or truncation")
	}
	projection := owned.Diagnostic()
	projection.Condition, projection.CauseCount = cleanupCode, 0
	if owned.Diagnostic().Condition != primaryCode || owned.Diagnostic().CauseCount != failure.MaxCauses {
		t.Fatal("projection alias")
	}
	var typedNil *hostile
	withNil := newFailure(t, primaryCode, nil, typedNil)
	if got := withNil.Unwrap(); len(got) != 1 || got[0] == nil || got[0] != typedNil {
		t.Fatal("typed nil normalized")
	}
	var matched *hostile
	if !errors.As(withNil, &matched) || matched != nil {
		t.Fatal("typed nil As lost")
	}
}

type hostile struct{}

func (*hostile) Error() string          { panic("secret_panic_canary") }
func (*hostile) Format(fmt.State, rune) { panic("secret_format_canary") }
func (*hostile) LogValue() slog.Value   { panic("secret_log_canary") }
func (*hostile) Is(error) bool          { panic("foreign Is called") }
func (*hostile) As(any) bool            { panic("foreign As called") }
func (err *hostile) Unwrap() error      { return err }

func TestSafeOwnedSurfacesDoNotTraverseForeignObjects(t *testing.T) {
	foreign := &hostile{}
	err := newFailure(t, primaryCode, foreign)
	if _, ok := failure.Inspect(foreign); ok {
		t.Fatal("foreign inspected")
	}
	if current, ok := failure.Inspect(err); !ok || current.Diagnostic().CauseCount != 1 {
		t.Fatal("owned inspection")
	}
	if err.Unwrap()[0] != foreign {
		t.Fatal("foreign object not retained")
	}
	for _, value := range []any{err, *err, &failure.Error{}, failure.Error{}, (*failure.Error)(nil), failure.Condition("bad\nsecret_canary")} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%#q", "%x", "%1000000.1000000v"} {
			output := fmt.Sprintf(format, value)
			if strings.Contains(output, "secret") || strings.Contains(output, "PANIC") || len(output) > failure.MaxConditionBytes+40 {
				t.Fatalf("unsafe format %s: %.200s", format, output)
			}
		}
		for _, jsonLog := range []bool{false, true} {
			var buffer bytes.Buffer
			var handler slog.Handler = slog.NewTextHandler(&buffer, nil)
			if jsonLog {
				handler = slog.NewJSONHandler(&buffer, nil)
			}
			slog.New(handler).Info("safe", "failure", value)
			if strings.Contains(buffer.String(), "secret") || strings.Contains(buffer.String(), "PANIC") {
				t.Fatal("unsafe slog", buffer.String())
			}
		}
	}
	if err.Error() != string(primaryCode) || err.LogValue().String() != string(primaryCode) {
		t.Fatal("not code-only")
	}
	var nilError *failure.Error
	if nilError.Error() != "<nil>" || nilError.LogValue().String() != "<nil>" || nilError.Unwrap() != nil || nilError.Diagnostic() != (failure.Diagnostic{}) || nilError.Is(primaryCode) {
		t.Fatal("nil semantics")
	}
	zero := failure.Error{}
	if zero.Error() != "failure: invalid occurrence" || zero.Is(primaryCode) || zero.Unwrap() != nil || zero.Diagnostic() != (failure.Diagnostic{}) {
		t.Fatal("zero semantics")
	}
	// Deliberate standard traversal invokes foreign hooks; owned surfaces above do not.
	func() {
		defer func() {
			if recover() == nil {
				t.Error("foreign Is was not invoked")
			}
		}()
		_ = errors.Is(err, errors.New("missing"))
	}()
}

func TestRuntimeSerializationRefused(t *testing.T) {
	owned := newFailure(t, primaryCode, &hostile{})
	for _, value := range []any{owned, *owned, failure.Error{}, &failure.Error{}} {
		if output, err := json.Marshal(value); !errors.Is(err, failure.ErrSerialization) || len(output) != 0 {
			t.Fatalf("serialized %T: %s %v", value, output, err)
		}
	}
	before := owned.Diagnostic()
	if err := json.Unmarshal([]byte("{}"), owned); !errors.Is(err, failure.ErrSerialization) || owned.Diagnostic() != before {
		t.Fatal("runtime reconstructed or mutated")
	}
	var nilError *failure.Error
	if output, err := json.Marshal(nilError); err != nil || string(output) != "null" {
		t.Fatal("nil JSON")
	}
	if nilError.UnmarshalJSON(nil) != failure.ErrSerialization {
		t.Fatal("nil reconstruction")
	}
}

func TestConcurrentOwnedInspection(t *testing.T) {
	err := newFailure(t, primaryCode, errors.New("immutable caller"), context.DeadlineExceeded)
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			for range 100 {
				current, ok := failure.Inspect(err)
				if !ok || current != err || !errors.Is(err, primaryCode) || !errors.Is(err, context.DeadlineExceeded) {
					t.Error("concurrent inspection changed")
				}
				current.Unwrap()[0] = nil
				_ = fmt.Sprintf("%#v", *current)
				_ = current.LogValue()
			}
		})
	}
	group.Wait()
}

type nilMapExtension map[string]int

func (nilMapExtension) Error() string           { panic("nil hook") }
func (nilMapExtension) Failure() *failure.Error { panic("nil hook") }

type nilFunctionExtension func()

func (nilFunctionExtension) Error() string           { panic("nil hook") }
func (nilFunctionExtension) Failure() *failure.Error { panic("nil hook") }

type nilSliceExtension []byte

func (nilSliceExtension) Error() string           { panic("nil hook") }
func (nilSliceExtension) Failure() *failure.Error { panic("nil hook") }

type nilChannelExtension chan int

func (nilChannelExtension) Error() string           { panic("nil hook") }
func (nilChannelExtension) Failure() *failure.Error { panic("nil hook") }

type nilPointerExtension struct{}

func (*nilPointerExtension) Error() string           { panic("nil hook") }
func (*nilPointerExtension) Failure() *failure.Error { panic("nil hook") }

func TestTypedNilExtensionsDoNotInvokeAccessors(t *testing.T) {
	for _, err := range []error{nilMapExtension(nil), nilFunctionExtension(nil), nilSliceExtension(nil), nilChannelExtension(nil), (*nilPointerExtension)(nil)} {
		if current, ok := failure.Inspect(err); current != nil || ok {
			t.Fatal("typed nil acquired occurrence")
		}
	}
}

func TestFmtOwnedExceptions(t *testing.T) {
	owned := newFailure(t, primaryCode, &hostile{})
	if got := fmt.Sprintf("%256T", owned); len(got) != 256 || !strings.HasSuffix(got, "*failure.Error") {
		t.Fatal("type formatting no longer bypasses Formatter with width padding")
	}
	if got := fmt.Sprintf("%p", owned); !strings.HasPrefix(got, "0x") {
		t.Fatal("pointer formatting no longer follows fmt inspection")
	}
	invalid := failure.Condition("bad\nformat_boundary_canary")
	for _, format := range []string{"%p", strings.Join([]string{"%", "w"}, "")} {
		if got := fmt.Sprintf(format, invalid); !strings.Contains(got, "format_boundary_canary") {
			t.Fatal("malformed fmt control no longer exposes raw scalar input")
		}
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		if got := fmt.Sprintf(format, invalid); strings.Contains(got, "format_boundary_canary") {
			t.Fatal("supported Formatter-dispatched output exposed invalid text")
		}
	}
}

func FuzzConditionAndConstruction(f *testing.F) {
	for _, input := range []string{"", "a.b", "example.source.absent", "a..b", "a.é", "a.b\nsecret", "a." + strings.Repeat("b", 126), "a." + strings.Repeat("b", 127)} {
		f.Add(input, uint8(2))
	}
	grammar := regexp.MustCompile(`^[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*)+$`)
	f.Fuzz(func(t *testing.T, input string, slots uint8) {
		code := failure.Condition(input)
		valid := len(input) <= failure.MaxConditionBytes && grammar.MatchString(input)
		if code.Valid() != valid {
			t.Fatal("grammar mismatch")
		}
		causes := make([]error, int(slots))
		for index := range causes {
			causes[index] = &hostile{}
		}
		err, rejected := failure.New(code, causes...)
		if !valid || len(causes) > failure.MaxCauses {
			if err != nil || rejected == nil {
				t.Fatal("invalid accepted")
			}
			return
		}
		if rejected != nil || err.Diagnostic().Condition != code || err.Diagnostic().CauseCount != len(causes) || len(err.Unwrap()) != len(causes) || !errors.Is(err, code) {
			t.Fatal("valid input changed")
		}
		if current, ok := failure.Inspect(err); !ok || current != err {
			t.Fatal("direct identity")
		}
		if output := fmt.Sprintf("%#v", err); output != input {
			t.Fatal("projection changed")
		}
	})
}
