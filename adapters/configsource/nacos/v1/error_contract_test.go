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

package nacos

import (
	"errors"
	"fmt"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestPublicErrorForwardingContract(t *testing.T) {
	original, err := failure.NewDetailed(adapters.Definitions()[0], failure.Location{Operation: "shared"},
		adapters.Details{Sequence: 7, Pending: true}, func(value adapters.Details) adapters.Details { return value })
	if err != nil {
		t.Fatal(err)
	}
	for name, input := range map[string]error{
		"bare":    original,
		"wrapped": fmt.Errorf("private-wrapper-canary: %w", original),
		"joined":  errors.Join(original),
		"nested":  fmt.Errorf("private-wrapper-canary: %w", errors.Join(original)),
	} {
		t.Run(name, func(t *testing.T) {
			got := translate(input, "read")
			core, ok := failure.Inspect(got)
			if !ok || core != original.Failure() || !errors.Is(got, input) {
				t.Fatal("public ownership or original graph changed")
			}
			if name == "bare" && got != original {
				t.Fatal("direct public occurrence replaced")
			}
			var details *failure.Detailed[adapters.Details]
			if !errors.As(got, &details) || details != original {
				t.Fatal("typed details detached")
			}
			conformance.Private(t, got, "private-wrapper-canary")
			if translate(got, "close") != got {
				t.Fatal("already classified occurrence was expanded again")
			}
		})
	}
	second, err := failure.New(failure.Definitions()[0], failure.Location{Operation: "other"})
	if err != nil {
		t.Fatal(err)
	}
	joined := errors.Join(original, second)
	got := translate(fmt.Errorf("private-wrapper-canary: %w", joined), "close")
	for range 3 {
		if _, ok := failure.Inspect(got); ok {
			t.Fatal("heterogeneous public aggregate acquired a provider owner")
		}
		if !errors.Is(got, original) || !errors.Is(got, second) {
			t.Fatal("public aggregate lost a cause")
		}
		conformance.Private(t, got, "private-wrapper-canary")
		if next := translate(got, "read"); next != got {
			t.Fatal("classified aggregate was reclassified")
		}
	}
	nativeFrame := native.ErrRead.New(fault.Context{}, original)
	core, ok := failure.Inspect(translate(nativeFrame, "read"))
	if !ok || core.Diagnostic().Definition.Code != ErrRead {
		t.Fatal("explicit native frame lost its own meaning")
	}
}

func TestNativeErrorPrecedenceContract(t *testing.T) {
	for _, test := range []struct {
		input    error
		expected failure.Code
	}{
		{native.ErrWrite.New(fault.Context{}, native.ErrDenied.New(fault.Context{})), ErrWrite},
		{fmt.Errorf("neutral: %w", native.ErrWrite.New(fault.Context{})), ErrRead},
		{errors.Join(native.ErrWrite.New(fault.Context{}), native.ErrDenied.New(fault.Context{})), ErrRead},
	} {
		core, ok := failure.Inspect(translate(test.input, "read"))
		if !ok || core.Diagnostic().Definition.Code != test.expected {
			t.Fatal("existing native classification priority changed")
		}
	}
}

type cyclicCause struct{ visits int }

func (*cyclicCause) Error() string { return "private-wrapper-canary" }
func (value *cyclicCause) Unwrap() error {
	value.visits++
	if value.visits > errorTraversalLimit*2 {
		panic("unbounded internal error traversal")
	}
	return value
}
func TestCyclicTranslationTerminates(t *testing.T) {
	cause := &cyclicCause{}
	result := translate(cause, "read")
	if result == nil || cause.visits > errorTraversalLimit {
		t.Fatal("cyclic input became success or exceeded the traversal budget")
	}
	core, ok := failure.Inspect(result)
	if !ok || core.Diagnostic().Definition.Code != ErrRead {
		t.Fatal("truncated inspection lost its conservative fallback")
	}
	conformance.Private(t, result, "private-wrapper-canary")
}
