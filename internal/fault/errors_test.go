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

package fault_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
)

type nativeError struct{ code, request string }

func (*nativeError) Error() string { panic("native-fault-canary") }

func TestTechnicalContextAndOriginalCauses(t *testing.T) {
	const kind fault.Kind = "example.transfer.unconfirmed"
	native := &nativeError{code: "native-code", request: "request-7"}
	cleanup := errors.New("cleanup-canary")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancelCause := errors.New("cancel-canary")
	cancel(cancelCause)
	location := fault.Context{Operation: "submit", Provider: "fixture", Scope: "worker", Source: "output",
		Correlation: fault.Correlation{Call: "Call-7", Parent: "Call-1", Owner: "owner-A"}}
	causes := []error{native, nil, ctx.Err(), context.Cause(ctx), cleanup}
	value := kind.New(location, causes...)
	causes[0] = nil
	location.Correlation.Owner = "changed"
	value.Unwrap()[0] = nil
	diagnostic := value.Diagnostic()
	diagnostic.Context.Source = "changed"
	var original *nativeError
	if !errors.As(value, &original) || original != native || original.code != "native-code" || original.request != "request-7" {
		t.Fatal("original typed SDK context was lost")
	}
	for _, cause := range []error{kind, native, context.Canceled, cancelCause, cleanup} {
		if !errors.Is(value, cause) {
			t.Fatal("technical classification or original cause lost")
		}
	}
	if len(value.Unwrap()) != 4 || value.Diagnostic().Context.Source != "output" || value.Diagnostic().Context.Correlation.Owner != "owner-A" {
		t.Fatal("wrapper-owned context or cause slices aliased")
	}
	for _, input := range []any{value, *value} {
		conformance.Runtime(t, input, new(fault.Error), "canary")
	}
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			for range 100 {
				value.Unwrap()[0] = nil
				if !errors.Is(value, native) || value.Diagnostic().Context.Source != "output" {
					t.Error("concurrent inspection changed immutable fault state")
				}
			}
		})
	}
	group.Wait()
}

func TestInvalidTechnicalContextPreservesCausesSafely(t *testing.T) {
	native := &nativeError{}
	for _, location := range []fault.Context{
		{Operation: "private/path"}, {Provider: strings.Repeat("a", 65)},
		{Scope: "private\ncanary"}, {Source: "UPPERCASE"},
		{Correlation: fault.Correlation{Call: strings.Repeat("a", 129)}},
		{Correlation: fault.Correlation{Call: "same", Parent: "same"}},
		{Correlation: fault.Correlation{Owner: "private/token"}},
	} {
		value := fault.Kind("example.failure").New(location, native)
		if !errors.Is(value, fault.Invalid) || !errors.Is(value, native) || value.Diagnostic().Context != (fault.Context{}) {
			t.Fatal("invalid technical context retained or original cause discarded")
		}
		conformance.Private(t, value, "private", "canary")
	}
	for _, kind := range []fault.Kind{"", "private/path", "UPPERCASE", fault.Kind(strings.Repeat("a", 129))} {
		value := kind.New(fault.Context{}, native)
		if !errors.Is(value, fault.Invalid) || kind.Error() != string(fault.Invalid) || !errors.Is(value, native) {
			t.Fatal("invalid kind was accepted or lost native context")
		}
	}
}

func TestZeroFaultAndTechnicalOnlyCorrelation(t *testing.T) {
	var absent *fault.Error
	if absent.Error() != "<nil>" || absent.Unwrap() != nil || errors.Is(absent, fault.Invalid) {
		t.Fatal("nil fault invented an occurrence")
	}
	var zero fault.Error
	if zero.Diagnostic().Kind != fault.Invalid || zero.Diagnostic().HasCauses {
		t.Fatal("zero fault invented technical evidence")
	}
	if err := json.Unmarshal([]byte("{}"), &zero); err == nil {
		t.Fatal("technical error reconstructed from JSON")
	}
	if !(fault.Context{}).Valid() || !(fault.Correlation{Call: "Call-A", Owner: "Owner-A"}).Valid() {
		t.Fatal("empty context or bounded opaque correlation rejected")
	}
	for _, kind := range []reflect.Type{reflect.TypeFor[fault.Context](), reflect.TypeFor[fault.Correlation]()} {
		for _, field := range []string{"Run", "Item", "Attempt", "Retryable", "Terminal", "Causes"} {
			if _, exists := kind.FieldByName(field); exists {
				t.Errorf("technical context exposes framework policy or runtime state: %s", field)
			}
		}
	}
}

func FuzzTechnicalFaultContext(f *testing.F) {
	for _, value := range []string{"call-1", "", "private/path", "\xff", "Call-A", strings.Repeat("x", 129)} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 4096 {
			return
		}
		location := fault.Context{Correlation: fault.Correlation{Call: value}}
		original := context.DeadlineExceeded
		result := fault.Kind("example.failure").New(location, original)
		if !errors.Is(result, original) || errors.Is(result, fault.Invalid) == location.Valid() {
			t.Fatal("validation changed native evidence or accepted an invalid context")
		}
		conformance.Runtime(t, result, new(fault.Error))
	})
}
