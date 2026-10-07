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

package duckdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2"
)

func contractWait(t testing.TB, sequence uint64, causes ...error) *failure.Detailed[adapters.Details] {
	t.Helper()
	for _, definition := range adapters.Definitions() {
		if definition.Code == adapters.ErrWait {
			value, err := failure.NewDetailed(definition, failure.Location{Operation: "waiting"},
				adapters.Details{Sequence: sequence, Pending: true}, func(value adapters.Details) adapters.Details { return value }, causes...)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatal("shared wait definition is missing")
	return nil
}

func contractPrivate(t testing.TB, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("non-nil input became success")
	}
	if strings.Contains(err.Error(), "private-") || strings.Contains(fmt.Sprintf("%v %+v %#v %s %q", err, err, err, err, err), "private-") {
		t.Fatal("cause or wrapper canary escaped ordinary presentation")
	}
	var output bytes.Buffer
	slog.New(slog.NewJSONHandler(&output, nil)).Error("failure", "error", err)
	if strings.Contains(output.String(), "private-") || strings.Contains(output.String(), "panicked") {
		t.Fatal("cause or wrapper canary escaped structured presentation")
	}
	if _, err := json.Marshal(err); !errors.Is(err, failure.ErrSerialization) {
		t.Fatal("runtime error was serialized", err)
	}
}

func TestErrorContractWrappedPublicOccurrences(t *testing.T) {
	cause := errors.New("private-original-cause")
	original := contractWait(t, 37, context.DeadlineExceeded, cause)
	wrapper := fmt.Errorf("private-wrapper: %w", original)
	for name, input := range map[string]error{
		"bare": original, "wrapped": wrapper, "nested": fmt.Errorf("private-outer: %w", wrapper),
		"neutral-native":        invocation.ErrFailed.New(fault.Context{}, wrapper),
		"configuration-neutral": source.ErrConfiguration.New(fault.Context{}, wrapper),
		"one-join":              errors.Join(wrapper),
	} {
		t.Run(name, func(t *testing.T) {
			got := translate(input, "query")
			core, ok := failure.Inspect(got)
			if !ok || core != original.Failure() || !errors.Is(got, input) || !errors.Is(got, cause) || !errors.Is(got, context.DeadlineExceeded) {
				t.Fatal("public core or its exact original graph was replaced")
			}
			var detailed *failure.Detailed[adapters.Details]
			if !errors.As(got, &detailed) || detailed != original {
				t.Fatal("typed detail accessor was replaced")
			}
			for range 4 {
				if again := translate(got, "cleanup"); again != got {
					t.Fatal("already-forwarded occurrence was reclassified")
				}
			}
			contractPrivate(t, got)
		})
	}
}

func TestErrorContractAggregatesKeepOwnersAndHistory(t *testing.T) {
	first := contractWait(t, 11)
	second := fail(ErrLimit, "limit", errors.New("private-limit"))
	input := fmt.Errorf("private-aggregate: %w", errors.Join(first, second))
	got := translate(input, "query")
	if _, ok := failure.Inspect(got); ok {
		t.Fatal("heterogeneous public aggregate acquired a DuckDB primary")
	}
	cleanup := fail(ErrCleanup, "cleanup", errors.New("private-cleanup"))
	for range 8 {
		got = combineResultErrors("query", got, cleanup)
		if _, ok := failure.Inspect(got); ok || translate(got, "query") != got {
			t.Fatal("already-classified aggregate was reclassified during phase composition")
		}
	}
	for _, cause := range []error{input, first, second, cleanup, adapters.ErrWait, ErrLimit, ErrCleanup} {
		if !errors.Is(got, cause) {
			t.Fatal("aggregate dropped an original component")
		}
	}
	contractPrivate(t, got)

	history := native.ErrNative.New(fault.Context{}, native.ErrUnsupported)
	opaque := errorbridge.Forward(history, errors.Join(first, second))
	if translated := translate(opaque, "query"); translated != opaque {
		t.Fatal("classified aggregate native history was revisited")
	}
}

func TestErrorContractOpaqueSubtreesAndNativeOuterFrames(t *testing.T) {
	hidden := native.ErrLimit.New(fault.Context{}, errors.New("private-hidden-native"))
	public := contractWait(t, 19, hidden)
	for _, input := range []error{public, fmt.Errorf("private-wrapper: %w", public)} {
		got := translate(input, "query")
		if core, ok := failure.Inspect(got); !ok || core != public.Failure() || errors.Is(got, ErrLimit) {
			t.Fatal("public subtree supplied a new native classification")
		}
	}
	input := native.ErrNative.New(fault.Context{}, public, native.ErrUnsupported.New(fault.Context{}))
	got := translate(input, "query")
	core, ok := failure.Inspect(got)
	if !ok || core.Diagnostic().Definition.Code != ErrNative || !errors.Is(got, ErrUnsupported) || errors.Is(got, ErrLimit) ||
		!errors.Is(got, public) || !errors.Is(got, hidden) || core.Unwrap()[0] != input {
		t.Fatal("native outer frame or opaque public subtree boundary changed")
	}
	contractPrivate(t, got)
}

func TestErrorContractPrimaryDetailsAndCleanup(t *testing.T) {
	nested := contractWait(t, 99)
	primary := contractWait(t, 23, nested, errors.New("private-primary"))
	wrapped := fmt.Errorf("private-wait: %w", primary)
	cleanup := fail(ErrCleanup, "cleanup", errors.New("private-cleanup"))
	for name, input := range map[string]error{"direct": primary, "wrapped": wrapped, "forwarded": translate(wrapped, "query")} {
		t.Run(name, func(t *testing.T) {
			got := combineResultErrors("read", input, cleanup)
			core, ok := failure.Inspect(got)
			detailed, typed := got.(*failure.Detailed[adapters.Details])
			if !ok || !typed || core.Diagnostic().Definition.Code != adapters.ErrWait || core.Diagnostic().Location.Operation != "read" {
				t.Fatal("primary detail contract lost during cleanup composition")
			}
			value, present := detailed.Details()
			if !present || value.Sequence != 23 || !value.Pending || !errors.Is(got, input) || !errors.Is(got, cleanup) {
				t.Fatal("cleanup composition used unrelated nested details or lost a cause")
			}
			contractPrivate(t, got)
		})
	}
}

func TestErrorContractUnknownDetailsRemainOpaque(t *testing.T) {
	code, err := failure.MakeCode(failure.FirstExtensionFacility+failure.FacilityFailure, 1)
	if err != nil {
		t.Fatal(err)
	}
	definition := failure.Definition{Code: code, Identifier: "example.source.failed", Module: "example", Component: "source", Revision: 1,
		Message: "The source failed.", Details: failure.Contract{ID: "example.source.opaque_details", Version: 1}}
	original, err := failure.NewDetailed(definition, failure.Location{Operation: "original"}, "private-detail",
		func(value string) string { return value }, contractWait(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	cleanup := fail(ErrCleanup, "cleanup")
	got := combineResultErrors("query", translate(fmt.Errorf("private-wrapper: %w", original), "query"), cleanup)
	if core, ok := failure.Inspect(got); !ok || core != original.Failure() || !errors.Is(got, cleanup) {
		t.Fatal("unknown primary schema was reclassified")
	}
	var exact *failure.Detailed[string]
	if !errors.As(got, &exact) || exact != original {
		t.Fatal("unknown detail accessor was erased")
	}
	contractPrivate(t, got)
}

func TestErrorContractUnknownSchemaWithKnownGoTypeStaysOpaque(t *testing.T) {
	current := contractWait(t, 31).Failure().Diagnostic().Definition
	for _, schema := range []failure.Contract{
		{ID: "fathomry.operation.details", Version: 2},
		{ID: "fathomry.operation.other_details", Version: 1},
	} {
		definition := current
		definition.Details = schema
		original, err := failure.NewDetailed(definition, failure.Location{Operation: "future"},
			adapters.Details{Sequence: 31, Pending: true}, func(value adapters.Details) adapters.Details { return value })
		if err != nil {
			t.Fatal(err)
		}
		cleanup := fail(ErrCleanup, "cleanup")
		got := combineResultErrors("query", translate(fmt.Errorf("private-future-schema: %w", original), "query"), cleanup)
		if core, ok := failure.Inspect(got); !ok || core != original.Failure() || !errors.Is(got, cleanup) {
			t.Fatal("unknown detail schema was reconstructed from its coincidental Go type")
		}
		var detailed *failure.Detailed[adapters.Details]
		if !errors.As(got, &detailed) || detailed != original {
			t.Fatal("opaque future schema lost its original typed accessor")
		}
		contractPrivate(t, got)
	}
}

func TestErrorContractMissingCurrentDetailsCannotBorrowNestedPayload(t *testing.T) {
	nested := contractWait(t, 43)
	original, err := failure.New(nested.Failure().Diagnostic().Definition, failure.Location{Operation: "missing"}, nested)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := fail(ErrCleanup, "cleanup")
	got := combineResultErrors("query", translate(fmt.Errorf("private-missing-details: %w", original), "query"), cleanup)
	if core, ok := failure.Inspect(got); !ok || core != original || !errors.Is(got, nested) || !errors.Is(got, cleanup) {
		t.Fatal("missing current details were invented from another occurrence")
	}
	contractPrivate(t, got)
}

type contractCycle struct{ visits int }

func (*contractCycle) Error() string       { panic("must not format original graph") }
func (value *contractCycle) Unwrap() error { value.visits++; return value }

type contractLeaf struct{ visits *int }

func (*contractLeaf) Error() string       { panic("must not format original graph") }
func (value *contractLeaf) Unwrap() error { *value.visits++; return nil }

type contractWide struct{ children []error }

func (*contractWide) Error() string         { return "private-wide" }
func (value *contractWide) Unwrap() []error { return value.children }

type contractInvalidOccurrence struct{}

func (contractInvalidOccurrence) Error() string           { return "private-invalid-occurrence" }
func (contractInvalidOccurrence) Failure() *failure.Error { return nil }

func TestErrorContractBoundedIncompleteGraphs(t *testing.T) {
	cyclic := new(contractCycle)
	got := translate(cyclic, "query")
	core, ok := failure.Inspect(got)
	if !ok || core.Unwrap()[0] != cyclic || cyclic.visits != 128 {
		t.Fatal("cyclic native fallback lost its bound or original graph")
	}
	contractPrivate(t, got)
	visits := 0
	wide := &contractWide{children: make([]error, 4096)}
	for index := range wide.children {
		wide.children[index] = &contractLeaf{visits: &visits}
	}
	got = translate(wide, "query")
	if core, ok := failure.Inspect(got); !ok || core.Unwrap()[0] != wide || visits != 127 {
		t.Fatal("wide native inspection exceeded its node bound")
	}
	contractPrivate(t, got)
	for _, original := range []error{&contractWide{}, contractInvalidOccurrence{}} {
		got := translate(original, "query")
		if core, ok := failure.Inspect(got); !ok || core.Diagnostic().Definition.Code != ErrNative || !errors.Is(got, original) {
			t.Fatal("non-nil unclassified wrapper acquired success or unsafe direct authority")
		}
		contractPrivate(t, got)
	}
}

func TestErrorContractConfigurationFallback(t *testing.T) {
	for _, operation := range []string{"open", "query", "validate", "cleanup"} {
		plain := source.ErrConfiguration.New(fault.Context{}, errors.New("private-invalid-settings"))
		got := translate(plain, operation)
		if core, ok := failure.Inspect(got); !ok || core.Diagnostic().Definition.Code != ErrInput || !errors.Is(got, plain) {
			t.Fatal("neutral configuration fallback changed")
		}
		outer := source.ErrConfiguration.New(fault.Context{}, native.ErrUnsupported.New(fault.Context{}))
		got = translate(outer, operation)
		if core, ok := failure.Inspect(got); !ok || core.Diagnostic().Definition.Code != ErrUnsupported {
			t.Fatal("configuration fallback superseded specific native classification")
		}
	}
}

func FuzzErrorContractGraphs(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4})
	f.Add([]byte{1, 1, 1})
	f.Add([]byte{3, 2, 4})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 32 {
			return
		}
		public := contractWait(t, 1)
		var original error = public
		allPublic := true
		for _, shape := range data {
			switch shape % 5 {
			case 0:
				original = fmt.Errorf("private-wrapper: %w", original)
			case 1:
				original = errors.Join(original, public)
			case 2:
				original = invocation.ErrFailed.New(fault.Context{}, original)
			case 3:
				original = native.ErrNative.New(fault.Context{}, original)
				allPublic = false
			case 4:
				original = source.ErrConfiguration.New(fault.Context{}, original)
			}
		}
		got := translate(original, "query")
		if !errors.Is(got, original) || !errors.Is(got, public) || translate(got, "query") != got {
			t.Fatal("graph translation detached causes or ceased being idempotent")
		}
		if allPublic && errors.Is(got, ErrNative) {
			t.Fatal("entirely public graph acquired a native owner")
		}
		contractPrivate(t, got)
	})
}
