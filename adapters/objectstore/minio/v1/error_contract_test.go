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

package minio

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
