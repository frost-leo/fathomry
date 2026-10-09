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

package otel

import (
	"context"
	"errors"
	"fmt"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	"testing"
)

type admissionCause struct{}

func (admissionCause) Error() string { panic("foreign Error invoked") }
func (admissionCause) Unwrap() error { panic("foreign Unwrap invoked") }

func TestAdmissionCanceledKeepsExactPublicAndNativeBoundary(t *testing.T) {
	for _, phase := range []string{"entry", "emit"} {
		original := native.ErrState.New(fault.Context{Provider: native.ProviderID, Operation: phase}, context.DeadlineExceeded)
		mapped := translate(original, "operation")
		if !AdmissionCanceled(mapped) || !errors.Is(mapped, ErrState) || !errors.Is(mapped, original) {
			t.Fatal("known cancellation lost", phase)
		}
		forwarded := translate(fmt.Errorf("private wrapper: %w", mapped), "forward")
		if !AdmissionCanceled(forwarded) {
			t.Fatal("public forwarding lost current core facts")
		}
		attributed := invocation.ErrFailed.New(fault.Context{Provider: native.ProviderID, Operation: "emit"}, original)
		if !AdmissionCanceled(translate(attributed, "operation")) {
			t.Fatal("actual invocation attribution lost native cancellation boundary")
		}
	}
	for _, original := range []error{
		native.ErrState.New(fault.Context{Operation: "closed"}, context.Canceled),
		native.ErrState.New(fault.Context{Operation: "entry"}, admissionCause{}),
		native.ErrState.New(fault.Context{Operation: "entry"}, errors.Join(context.Canceled, errors.New("unknown"))),
		native.ErrInput.New(fault.Context{Operation: "entry"}, context.Canceled),
		native.ErrState.New(fault.Context{Provider: "different-provider", Operation: "entry"}, context.Canceled),
	} {
		// The foreign cause case uses the directly constructed public occurrence so
		// this test targets inspection, not the separate general translation contract.
		mapped := fail(ErrState, "operation", original)
		if AdmissionCanceled(mapped) {
			t.Fatal("unknown/closed/non-state frame reclassified")
		}
	}
	if AdmissionCanceled(nil) || AdmissionCanceled(errors.Join(fail(ErrState, "state"), fail(ErrInput, "input"))) {
		t.Fatal("aggregate invented a known native boundary")
	}
}
