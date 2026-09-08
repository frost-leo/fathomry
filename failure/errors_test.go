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
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/failure"
)

func definition() failure.Identity {
	return failure.MustDefine(failure.Definition{Code: "example.resource.failed", Component: "example", Version: 1})
}

type nativeFailure struct{}

func (*nativeFailure) Error() string { panic("native presentation must not be called") }

func TestIdentityCausesAndIsolation(t *testing.T) {
	identity := definition()
	primary := &nativeFailure{}
	cleanup := errors.New("private cleanup")
	causes := []error{primary, nil, cleanup, context.Canceled}
	value := identity.New(failure.Attribution{Operation: "construct", Assembly: "one", Source: "orders"}, causes...)
	causes[0] = errors.New("replacement")
	unwrapped := value.Unwrap()
	unwrapped[0] = nil
	projected := value.Diagnostic()
	projected.Attribution.Source = "changed"
	if !errors.Is(value, identity) || !errors.Is(value, primary) || !errors.Is(value, cleanup) || !errors.Is(value, context.Canceled) {
		t.Fatal("identity or original causes lost")
	}
	var original *nativeFailure
	if !errors.As(value, &original) || original != primary {
		t.Fatal("native cause inspection lost")
	}
	var occurrence *failure.Error
	if !errors.As(value, &occurrence) || occurrence != value {
		t.Fatal("occurrence unavailable")
	}
	if value.Diagnostic().Attribution.Source != "orders" || len(value.Unwrap()) != 3 {
		t.Fatal("mutation escaped")
	}
	for _, changed := range []failure.Definition{
		{Code: "example.resource.failed", Component: "example", Version: 2},
		{Code: "example.resource.failed", Component: "other", Version: 1},
	} {
		if errors.Is(value, failure.MustDefine(changed)) {
			t.Fatal("identity mismatch accepted")
		}
	}
	if errors.Is(value, identity.New(failure.Attribution{})) {
		t.Fatal("distinct occurrences equated")
	}
}

func TestSafePresentationAndNoReconstruction(t *testing.T) {
	value := definition().New(failure.Attribution{}, &nativeFailure{}, errors.New("private-token"))
	for _, input := range []any{value, *value, struct{ Error any }{value}, struct{ hidden any }{value}} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
			if strings.Contains(fmt.Sprintf(format, input), "private-token") {
				t.Fatal("cause disclosed")
			}
		}
	}
	for _, input := range []any{value, *value} {
		if _, err := json.Marshal(input); err == nil || strings.Contains(err.Error(), "private-token") {
			t.Fatal("runtime error serialized without a wire contract")
		}
	}
	var restored failure.Error
	if err := json.Unmarshal([]byte(`{"Definition":{"Code":"example.resource.failed","Version":1}}`), &restored); err == nil {
		t.Fatal("diagnostic masqueraded as reconstruction")
	}
	for _, jsonOutput := range []bool{false, true} {
		var output bytes.Buffer
		var handler slog.Handler = slog.NewTextHandler(&output, nil)
		if jsonOutput {
			handler = slog.NewJSONHandler(&output, nil)
		}
		var absent *failure.Error
		slog.New(handler).Error("failed", "error", value, "copy", *value, "absent", absent)
		if strings.Contains(output.String(), "private-token") || strings.Contains(output.String(), "PANIC") {
			t.Fatalf("unsafe logging: %s", output.String())
		}
	}
}

func TestInvalidAndNilValues(t *testing.T) {
	for _, input := range []failure.Definition{{}, {Code: "invalid code", Component: "example", Version: 1}} {
		if _, err := failure.Define(input); err == nil {
			t.Fatal("invalid declaration accepted")
		}
	}
	var zero failure.Identity
	invalid := zero.New(failure.Attribution{}, context.Canceled)
	if invalid.Diagnostic().Definition.Code != "fathomry.failure.invalid" || !errors.Is(invalid, context.Canceled) {
		t.Fatal("invalid declaration lost its cause")
	}
	value := definition().New(failure.Attribution{Source: "private/path"})
	if value.Diagnostic().Definition.Code != "fathomry.failure.invalid" || value.Diagnostic().Attribution.Source != "" {
		t.Fatal("unsafe attribution retained")
	}
	var absent *failure.Error
	if absent.Unwrap() != nil || absent.Error() != "<nil>" || errors.Is(absent, definition()) {
		t.Fatal("nil occurrence behavior")
	}
	if data, err := json.Marshal(absent); err != nil || string(data) != "null" {
		t.Fatal("nil JSON")
	}
	var blank failure.Error
	if blank.Diagnostic().Definition.Code != "fathomry.failure.invalid" {
		t.Fatal("zero occurrence")
	}
}

func TestConcurrentIndependentOccurrences(t *testing.T) {
	identity := definition()
	value := identity.New(failure.Attribution{Source: "original"}, context.DeadlineExceeded)
	var group sync.WaitGroup
	for index := 0; index < 32; index++ {
		group.Go(func() {
			for iteration := 0; iteration < 100; iteration++ {
				copy := value.Diagnostic()
				copy.Attribution.Source = "changed"
				if !errors.Is(value, context.DeadlineExceeded) || value.Diagnostic().Attribution.Source != "original" {
					t.Error("concurrent mutation")
				}
				other := identity.New(failure.Attribution{Source: "another"})
				if other.Diagnostic().HasCauses {
					t.Error("occurrence state shared")
				}
			}
		})
	}
	group.Wait()
}
