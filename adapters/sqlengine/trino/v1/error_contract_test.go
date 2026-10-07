/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package trino

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
	native "github.com/frost-leo/fathomry/internal/sqlengine/trino/v0"
	sdk "github.com/trinodb/trino-go-client/trino"
)

func boundaryWait(t testing.TB, sequence uint64, causes ...error) *failure.Detailed[adapters.Details] {
	t.Helper()
	for _, definition := range adapters.Definitions() {
		if definition.Code == adapters.ErrWait {
			value, err := failure.NewDetailed(definition, failure.Location{Operation: "wait"}, adapters.Details{Sequence: sequence, Pending: true}, func(value adapters.Details) adapters.Details { return value }, causes...)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatal("missing public wait definition")
	return nil
}

func boundaryPrivate(t testing.TB, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("non-nil failure became success")
	}
	if strings.Contains(err.Error(), "private-") || strings.Contains(fmt.Sprintf("%v %+v %#v %s %q", err, err, err, err, err), "private-") {
		t.Fatal("ordinary error presentation exposed original text")
	}
	var output bytes.Buffer
	slog.New(slog.NewJSONHandler(&output, nil)).Error("failure", "error", err)
	if strings.Contains(output.String(), "private-") || strings.Contains(output.String(), "panicked") {
		t.Fatal("structured error presentation exposed original text")
	}
	if _, jsonErr := json.Marshal(err); !errors.Is(jsonErr, failure.ErrSerialization) {
		t.Fatal("runtime error became a JSON payload")
	}
}

func TestErrorBoundaryPublicForwarding(t *testing.T) {
	nativeCause := &sdk.ErrTrino{ErrorName: "TYPE_MISMATCH", ErrorType: "USER_ERROR", Message: "private-native-message"}
	original := boundaryWait(t, 61, context.DeadlineExceeded, nativeCause)
	wrapper := fmt.Errorf("private-wrapper: %w", original)
	for name, input := range map[string]error{
		"bare": original, "wrapper": wrapper, "nested": fmt.Errorf("private-outer: %w", wrapper), "join": errors.Join(wrapper),
		"invocation": invocation.ErrFailed.New(fault.Context{}, wrapper), "configuration": source.ErrConfiguration.New(fault.Context{}, wrapper),
		"assembly": source.ErrAssembly.New(fault.Context{}, wrapper), "initialization": source.ErrInitialization.New(fault.Context{}, wrapper),
	} {
		t.Run(name, func(t *testing.T) {
			got := translate(input, "readiness")
			core, ok := failure.Inspect(got)
			if !ok || core != original.Failure() || !errors.Is(got, input) || !errors.Is(got, context.DeadlineExceeded) {
				t.Fatal("public core or exact wrapper history changed")
			}
			var detailed *failure.Detailed[adapters.Details]
			var nativeError *sdk.ErrTrino
			if !errors.As(got, &detailed) || detailed != original || !errors.As(got, &nativeError) || nativeError != nativeCause {
				t.Fatal("typed public/native cause identity changed")
			}
			if translate(got, "cleanup") != got {
				t.Fatal("already-public occurrence was reclassified")
			}
			boundaryPrivate(t, got)
		})
	}
}

func TestErrorBoundaryAggregatesDoNotInventOwner(t *testing.T) {
	shared := boundaryWait(t, 3)
	provider := fail(ErrAuthority, "routing", errors.New("private-origin"))
	original := fmt.Errorf("private-aggregate: %w", errors.Join(shared, provider))
	got := translate(original, "query")
	if _, ok := failure.Inspect(got); ok {
		t.Fatal("heterogeneous public aggregate acquired a Trino owner")
	}
	cleanup := fail(ErrCleanup, "delete", errors.New("private-cancel"))
	for range 8 {
		got = combineResultErrors("read", got, cleanup)
		if _, ok := failure.Inspect(got); ok || translate(got, "readiness") != got {
			t.Fatal("classified aggregate history was reclassified")
		}
	}
	for _, cause := range []error{original, shared, provider, cleanup, adapters.ErrWait, ErrAuthority, ErrCleanup} {
		if !errors.Is(got, cause) {
			t.Fatal("aggregate lost a component or original graph")
		}
	}
	boundaryPrivate(t, got)
	originalHistory := native.ErrProtocol.New(fault.Context{}, native.ErrInput)
	forwarded := errorbridge.Forward(originalHistory, errors.Join(shared, provider))
	if translate(forwarded, "query") != forwarded {
		t.Fatal("translated aggregate retained history became classification input")
	}
}

func TestErrorBoundaryNativeFrameAndOpaquePublicSubtree(t *testing.T) {
	hidden := native.ErrProtocol.New(fault.Context{}, errors.New("private-protocol"))
	known := boundaryWait(t, 7, hidden)
	for _, input := range []error{known, fmt.Errorf("private-public-wrapper: %w", known)} {
		translated := translate(input, "readiness")
		if core, ok := failure.Inspect(translated); !ok || core != known.Failure() || errors.Is(translated, ErrProtocol) {
			t.Fatal("public subtree was reinterpreted as native protocol failure")
		}
	}
	nativeCause := &sdk.ErrQueryFailed{StatusCode: 503, Reason: errors.New("private-http")}
	original := native.ErrOperation.New(fault.Context{}, known, native.ErrAuthority.New(fault.Context{}, nativeCause))
	translated := translate(source.ErrAssembly.New(fault.Context{}, original), "open")
	core, ok := failure.Inspect(translated)
	var exact *sdk.ErrQueryFailed
	if !ok || core.Diagnostic().Definition.Code != ErrOperation || !errors.Is(translated, ErrAuthority) || errors.Is(translated, ErrProtocol) ||
		!errors.Is(translated, original) || !errors.Is(translated, hidden) || !errors.As(translated, &exact) || exact != nativeCause {
		t.Fatal("Trino outer frame/native graph/opaque subtree boundary changed")
	}
	boundaryPrivate(t, translated)
}

func TestErrorBoundaryTrinoFallbackPriorities(t *testing.T) {
	privateCause := errors.New("private-unclassified")
	for operation, want := range map[string]failure.Code{
		"query": ErrOperation, "next": ErrOperation, "validate": ErrInput, "recommend": ErrInput, "configuration": ErrInput,
		"open": ErrConnect, "readiness": ErrConnect, "cleanup": ErrCleanup, "close": ErrCleanup,
	} {
		translated := translate(privateCause, operation)
		if core, ok := failure.Inspect(translated); !ok || core.Diagnostic().Definition.Code != want || !errors.Is(translated, privateCause) {
			t.Fatalf("phase fallback changed for %s", operation)
		}
	}
	configuration := source.ErrConfiguration.New(fault.Context{}, privateCause)
	assembly := source.ErrAssembly.New(fault.Context{}, privateCause)
	initialization := source.ErrInitialization.New(fault.Context{}, privateCause)
	for _, test := range []struct {
		input error
		want  failure.Code
	}{
		{configuration, ErrInput}, {assembly, ErrConnect}, {initialization, ErrConnect},
		{errors.Join(assembly, configuration), ErrInput}, {errors.Join(configuration, assembly), ErrInput},
		{source.ErrAssembly.New(fault.Context{}, configuration), ErrInput}, {source.ErrConfiguration.New(fault.Context{}, initialization), ErrInput},
		{source.ErrInitialization.New(fault.Context{}, native.ErrProtocol), ErrProtocol},
		{source.ErrConfiguration.New(fault.Context{}, native.ErrUnsupported), ErrUnsupported},
		{source.ErrAssembly.New(fault.Context{}, native.ErrOperation.New(fault.Context{}, native.ErrLimit)), ErrOperation},
		{source.ErrConfiguration.New(fault.Context{}, invocation.ErrAttempts), ErrLimit},
	} {
		for _, operation := range []string{"query", "open", "configuration", "cleanup"} {
			got := translate(test.input, operation)
			if core, ok := failure.Inspect(got); !ok || core.Diagnostic().Definition.Code != test.want || !errors.Is(got, test.input) {
				t.Fatalf("neutral source fallback replaced specific classification during %s", operation)
			}
		}
	}
}

func TestErrorBoundaryKnownPrimaryDetailsAndCleanup(t *testing.T) {
	primary := boundaryWait(t, 17, boundaryWait(t, 999), errors.New("private-primary"))
	wrapped := fmt.Errorf("private-wrapper: %w", primary)
	cleanup := fail(ErrCleanup, "cleanup", context.Canceled)
	for _, input := range []error{primary, wrapped, translate(wrapped, "next")} {
		combined := combineResultErrors("read", input, cleanup)
		core, ok := failure.Inspect(combined)
		details, typed := combined.(*failure.Detailed[adapters.Details])
		if !ok || !typed || core.Diagnostic().Definition.Code != adapters.ErrWait || core.Diagnostic().Location.Operation != "read" {
			t.Fatal("known primary detail identity was lost")
		}
		value, present := details.Details()
		if !present || value.Sequence != 17 || !value.Pending || !errors.Is(combined, input) || !errors.Is(combined, cleanup) || !errors.Is(combined, context.Canceled) {
			t.Fatal("cleanup lost phases or selected nested details")
		}
		boundaryPrivate(t, combined)
	}
}

func TestErrorBoundaryUnknownDetailSchemaStaysOpaque(t *testing.T) {
	base := boundaryWait(t, 5).Failure().Diagnostic().Definition
	for _, contract := range []failure.Contract{{ID: "fathomry.operation.details", Version: 2}, {ID: "fathomry.operation.foreign_details", Version: 1}} {
		definition := base
		definition.Details = contract
		original, err := failure.NewDetailed(definition, failure.Location{Operation: "original"}, adapters.Details{Sequence: 42}, func(value adapters.Details) adapters.Details { return value })
		if err != nil {
			t.Fatal(err)
		}
		cleanup := fail(ErrCleanup, "cleanup")
		got := combineResultErrors("next", translate(fmt.Errorf("private-schema: %w", original), "next"), cleanup)
		if core, ok := failure.Inspect(got); !ok || core != original.Failure() || !errors.Is(got, cleanup) {
			t.Fatal("unknown schema was reconstructed from a familiar Go type")
		}
		var detailed *failure.Detailed[adapters.Details]
		if !errors.As(got, &detailed) || detailed != original {
			t.Fatal("unknown schema lost its original accessor")
		}
		boundaryPrivate(t, got)
	}
	definition := base
	definition.Details = failure.Contract{ID: "fathomry.operation.opaque_details", Version: 1}
	opaque, err := failure.NewDetailed(definition, failure.Location{Operation: "opaque"}, "private-payload", func(value string) string { return value })
	if err != nil {
		t.Fatal(err)
	}
	got := combineResultErrors("query", fmt.Errorf("private-opaque: %w", opaque), fail(ErrCleanup, "cleanup"))
	var exact *failure.Detailed[string]
	if core, ok := failure.Inspect(got); !ok || core != opaque.Failure() || !errors.As(got, &exact) || exact != opaque {
		t.Fatal("unknown Go payload was reclassified")
	}
	boundaryPrivate(t, got)
}

func TestErrorBoundaryMissingCurrentDetailsCannotBorrowNested(t *testing.T) {
	nested := boundaryWait(t, 88)
	current, err := failure.New(nested.Failure().Diagnostic().Definition, failure.Location{Operation: "missing"}, nested)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := fail(ErrCleanup, "cleanup")
	got := combineResultErrors("query", translate(fmt.Errorf("private-missing: %w", current), "query"), cleanup)
	if core, ok := failure.Inspect(got); !ok || core != current || !errors.Is(got, nested) || !errors.Is(got, cleanup) {
		t.Fatal("missing current details were fabricated from a nested occurrence")
	}
	boundaryPrivate(t, got)
}

type boundaryCycle struct{ visits int }

func (*boundaryCycle) Error() string       { panic("original error text must remain unformatted") }
func (value *boundaryCycle) Unwrap() error { value.visits++; return value }

type boundaryLeaf struct{ visits *int }

func (*boundaryLeaf) Error() string       { panic("original error text must remain unformatted") }
func (value *boundaryLeaf) Unwrap() error { *value.visits++; return nil }

type boundaryWide struct{ children []error }

func (*boundaryWide) Error() string         { return "private-wide" }
func (value *boundaryWide) Unwrap() []error { return value.children }

type boundaryInvalidOccurrence struct{}

func (boundaryInvalidOccurrence) Error() string           { return "private-invalid-core" }
func (boundaryInvalidOccurrence) Failure() *failure.Error { return nil }

func TestErrorBoundaryBoundedIncompleteGraphs(t *testing.T) {
	cycle := new(boundaryCycle)
	got := translate(cycle, "readiness")
	if core, ok := failure.Inspect(got); !ok || core.Unwrap()[0] != cycle || core.Diagnostic().Definition.Code != ErrConnect || cycle.visits != 128 {
		t.Fatal("cyclic cause traversal lost its bound/history/default")
	}
	boundaryPrivate(t, got)
	visited := 0
	wide := &boundaryWide{children: make([]error, 4096)}
	for index := range wide.children {
		wide.children[index] = &boundaryLeaf{visits: &visited}
	}
	got = translate(wide, "query")
	if core, ok := failure.Inspect(got); !ok || core.Unwrap()[0] != wide || visited != 127 {
		t.Fatal("wide traversal exceeded its node budget")
	}
	boundaryPrivate(t, got)
	for _, input := range []error{&boundaryWide{}, &boundaryWide{children: []error{nil}}, boundaryInvalidOccurrence{}} {
		got := translate(input, "query")
		if core, ok := failure.Inspect(got); !ok || core.Diagnostic().Definition.Code != ErrOperation || !errors.Is(got, input) {
			t.Fatal("non-nil empty/invalid wrapper gained success or public authority")
		}
		boundaryPrivate(t, got)
	}
}

func FuzzErrorBoundaryGraphs(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6})
	f.Add([]byte{1, 0, 1})
	f.Add([]byte{4, 5, 6})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 32 {
			return
		}
		public := boundaryWait(t, 9, context.Canceled)
		var original error = public
		entirelyPublic := true
		for _, shape := range data {
			switch shape % 7 {
			case 0:
				original = fmt.Errorf("private-wrapper: %w", original)
			case 1:
				original = errors.Join(original, public)
			case 2:
				original = invocation.ErrFailed.New(fault.Context{}, original)
			case 3:
				original = native.ErrProtocol.New(fault.Context{}, original)
				entirelyPublic = false
			case 4:
				original = source.ErrConfiguration.New(fault.Context{}, original)
			case 5:
				original = source.ErrAssembly.New(fault.Context{}, original)
			case 6:
				original = source.ErrInitialization.New(fault.Context{}, original)
			}
		}
		got := translate(original, "readiness")
		if !errors.Is(got, original) || !errors.Is(got, public) || !errors.Is(got, context.Canceled) || translate(got, "cleanup") != got {
			t.Fatal("translation changed original graph or public idempotence")
		}
		if entirelyPublic && (errors.Is(got, ErrOperation) || errors.Is(got, ErrConnect) || errors.Is(got, ErrInput)) {
			t.Fatal("entirely public graph gained a Trino fallback owner")
		}
		boundaryPrivate(t, got)
	})
}
