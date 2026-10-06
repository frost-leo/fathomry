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

package postgres

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Shared occurrences own their semantic/detail contract even through transparent
// callback or phase wrappers. Context text is not a new provider failure.
func TestPublicErrorForwardingContract(t *testing.T) {
	var definition failure.Definition
	for _, candidate := range adapters.Definitions() {
		if candidate.Code == adapters.ErrWait {
			definition = candidate
			break
		}
	}
	cause := errors.New("private-original-cause")
	original, err := failure.NewDetailed(definition, failure.Location{Operation: "waiting"},
		adapters.Details{Sequence: 7, Pending: true}, func(value adapters.Details) adapters.Details { return value }, cause)
	if err != nil {
		t.Fatal(err)
	}
	wrapper := fmt.Errorf("private-wrapper-canary: %w", original)
	cases := map[string]error{
		"bare":     original,
		"single":   wrapper,
		"nested":   fmt.Errorf("private-outer-canary: %w", wrapper),
		"phase":    invocation.ErrFailed.New(fault.Context{}, wrapper),
		"one-join": errors.Join(wrapper),
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			got := translate(input, "forward")
			core, ok := failure.Inspect(got)
			if !ok || core != original.Failure() {
				t.Fatal("transparent wrapper changed public error ownership/core")
			}
			if name == "bare" && got != original {
				t.Fatal("bare public occurrence replaced")
			}
			if !errors.Is(got, original) || !errors.Is(got, cause) {
				t.Fatal("original cause graph lost")
			}
			if name != "phase" && !errors.Is(got, input) {
				t.Fatal("transparent wrapper identity lost")
			}
			var detailed *failure.Detailed[adapters.Details]
			if !errors.As(got, &detailed) || detailed != original {
				t.Fatal("typed detail object replaced")
			}
			details, present := detailed.Details()
			if !present || details.Sequence != 7 || !details.Pending {
				t.Fatal("typed detail meaning changed")
			}
			for _, text := range []string{got.Error(), fmt.Sprintf("%v %+v %#v %s %q", got, got, got, got, got)} {
				if strings.Contains(text, "private-") {
					t.Fatal("private wrapper/cause text leaked")
				}
			}
			var output bytes.Buffer
			slog.New(slog.NewJSONHandler(&output, nil)).Info("safe", "error", got)
			if strings.Contains(output.String(), "private-") {
				t.Fatal("private error text entered diagnostics")
			}
			if _, err := json.Marshal(got); !errors.Is(err, failure.ErrSerialization) {
				t.Fatal("runtime error was serializable")
			}
		})
	}
}

func TestForwardedAggregatePhaseCompositionIsIdempotent(t *testing.T) {
	for _, shape := range []struct {
		name         string
		width, depth int
	}{{"wide", 64, 1}, {"wrapped", 2, 16}} {
		t.Run(shape.name, func(t *testing.T) {
			causes := make([]error, shape.width)
			for index := range causes {
				var err error
				causes[index], err = failure.New(adapters.Definitions()[0], failure.Location{Operation: "shared"})
				if err != nil {
					t.Fatal(err)
				}
			}
			var original error = errors.Join(causes...)
			for range shape.depth {
				original = fmt.Errorf("private-context: %w", original)
			}
			primary := translate(original, "primary")
			if _, ok := failure.Inspect(primary); ok {
				t.Fatal("initial aggregate invented a primary")
			}
			cleanup := fail(ErrCleanup, "cleanup")
			for range 3 {
				primary = combineResultErrors("result", primary, cleanup)
				if _, ok := failure.Inspect(primary); ok {
					t.Fatal("already-classified aggregate was reclassified")
				}
			}
			for _, cause := range append(causes, cleanup) {
				if !errors.Is(primary, cause) {
					t.Fatal("cause graph lost")
				}
			}
			if strings.Contains(fmt.Sprintf("%v %#v", primary, primary), "private-") {
				t.Fatal("private text disclosed")
			}
		})
	}
}

func TestPublicAggregateWithCleanupKeepsOwners(t *testing.T) {
	first, err := failure.New(adapters.Definitions()[0], failure.Location{Operation: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := failure.New(failure.Definitions()[0], failure.Location{Operation: "second"})
	if err != nil {
		t.Fatal(err)
	}
	primary := translate(fmt.Errorf("private-primary: %w", errors.Join(first, second)), "primary")
	cleanup := fail(ErrCleanup, "cleanup", errors.New("private-cleanup"))
	combined := combineResultErrors("result", primary, cleanup)
	if _, ok := failure.Inspect(combined); ok {
		t.Fatal("public aggregate acquired an invented database primary")
	}
	for _, cause := range []error{primary, first, second, cleanup} {
		if !errors.Is(combined, cause) {
			t.Fatal("phase aggregate dropped a cause")
		}
	}
	if strings.Contains(fmt.Sprintf("%v %#v", combined, combined), "private-") {
		t.Fatal("phase aggregate leaked private text")
	}
}

func TestOpaquePrimaryDetailsKeepOwnerWithCleanup(t *testing.T) {
	code, err := failure.MakeCode(failure.FirstExtensionFacility+failure.FacilityFailure, 1)
	if err != nil {
		t.Fatal(err)
	}
	definition := failure.Definition{Code: code, Identifier: "example.source.failed", Module: "example", Component: "source", Revision: 1,
		Message: "The example failed.", Details: failure.Contract{ID: "example.source.opaque_details", Version: 1}}
	original, err := failure.NewDetailed(definition, failure.Location{Operation: "original"}, 17, func(value int) int { return value })
	if err != nil {
		t.Fatal(err)
	}
	cleanup := fail(ErrCleanup, "cleanup")
	combined := combineResultErrors("result", original, cleanup)
	core, ok := failure.Inspect(combined)
	if !ok || core != original.Failure() || !errors.Is(combined, cleanup) {
		t.Fatal("opaque primary was relabeled or cleanup lost")
	}
	var exact *failure.Detailed[int]
	if !errors.As(combined, &exact) || exact != original {
		t.Fatal("opaque detail accessor lost")
	}
}

type emptyContractError struct{}

func (emptyContractError) Error() string   { return "private-empty-wrapper" }
func (emptyContractError) Unwrap() []error { return nil }
func TestNonNilErrorGraphDoesNotBecomeSuccess(t *testing.T) {
	original := emptyContractError{}
	got := translate(original, "forward")
	if got == nil || !errors.Is(got, original) {
		t.Fatal("non-nil original became success or lost its cause")
	}
}

func TestForwardedTypedPrimarySurvivesCleanupJoin(t *testing.T) {
	var definition failure.Definition
	for _, candidate := range adapters.Definitions() {
		if candidate.Code == adapters.ErrWait {
			definition = candidate
			break
		}
	}
	original, err := failure.NewDetailed(definition, failure.Location{Operation: "waiting"},
		adapters.Details{Sequence: 9, Pending: true}, func(value adapters.Details) adapters.Details { return value })
	if err != nil {
		t.Fatal(err)
	}
	primary := translate(fmt.Errorf("private-wrapper: %w", original), "waiting")
	cleanup := fail(ErrCleanup, "cleanup", errors.New("private-cleanup"))
	combined := combineResultErrors("waiting", primary, cleanup)
	core, ok := failure.Inspect(combined)
	if !ok || core.Diagnostic().Definition.Code != adapters.ErrWait {
		t.Fatal("cleanup join relabeled forwarded typed primary")
	}
	detailed, ok := combined.(*failure.Detailed[adapters.Details])
	if !ok {
		t.Fatal("known operation details were erased")
	}
	details, present := detailed.Details()
	if !present || details.Sequence != 9 || !details.Pending || !errors.Is(combined, original) || !errors.Is(combined, cleanup) {
		t.Fatal("phase/detail facts lost")
	}
}

func TestPublicErrorAggregateKeepsOwners(t *testing.T) {
	first, err := failure.New(adapters.Definitions()[0], failure.Location{Operation: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := failure.New(failure.Definitions()[0], failure.Location{Operation: "second"})
	if err != nil {
		t.Fatal(err)
	}
	original := fmt.Errorf("private-aggregate-canary: %w", errors.Join(first, second))
	got := translate(original, "forward")
	if _, ok := failure.Inspect(got); ok {
		t.Fatal("heterogeneous public aggregate invented one owner")
	}
	for _, cause := range []error{original, first, second, first.Diagnostic().Definition.Code, second.Diagnostic().Definition.Code} {
		if !errors.Is(got, cause) {
			t.Fatal("aggregate component lost")
		}
	}
	if strings.Contains(fmt.Sprintf("%v %#v", got, got), "private-") {
		t.Fatal("aggregate wrapper leaked")
	}
}
