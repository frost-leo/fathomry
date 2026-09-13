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
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

type nativeCanary struct{}

func (*nativeCanary) Error() string { return "private-error-canary" }

func TestErrorIdentityCausesCancellationAndSafePresentation(t *testing.T) {
	native := &nativeCanary{}
	cause := errors.New("private-cancel-canary")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	primary := nativeFailure(ErrExport, "export", ctx, errors.Join(native, ctx.Err()))
	cleanup := failure(ErrCleanup, "shutdown", errors.New("private-cleanup-canary"))
	outcome := invocation.Outcome[Result]{Primary: primary, Cleanup: cleanup}
	result := invocation.Result[Result]{Outcome: outcome}
	for _, target := range []error{ErrExport, ErrCleanup, native, context.Canceled, cause} {
		if !errors.Is(result.Err(), target) {
			t.Fatal("error identity/cause lost")
		}
	}
	var actual *nativeCanary
	if !errors.As(result.Err(), &actual) || actual != native {
		t.Fatal("native As identity lost")
	}
	var typed *fault.Error
	if !errors.As(primary, &typed) || typed.Diagnostic().Kind != ErrExport {
		t.Fatal("technical projection lost")
	}
	for _, value := range []any{primary, cleanup, result.Err(), OptionsV1{Headers: map[string]string{"Authorization": "private-error-canary"}},
		TLSV1{Key: "private-error-canary"}, LogRecord{Message: "private-error-canary"}, Result{signals: []SignalResult{{Err: primary}}}} {
		conformance.Private(t, value, "private-error-canary", "private-cancel-canary", "private-cleanup-canary")
	}
	if joined(ErrExport, "export", nil, nil) != nil || nativeFailure(ErrExport, "export", ctx, nil) != nil {
		t.Fatal("success manufactured error")
	}
}
func TestFacadesHaveNoNativeOrLifecycleEscape(t *testing.T) {
	fixture := newFixture(t, OptionsV1{TracesEndpoint: "traces"}, nil)
	conformance.Facade(t, fixture.client, "Emit", "Flush", "MeasureInt64", "MeasureFloat64", "Profile", "Start", "String", "GoString", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
	ctx, span, err := fixture.client.Start(context.Background(), fault.Correlation{Call: "span"}, SpanInput{Name: "work"})
	if err != nil {
		t.Fatal(err)
	}
	_ = ctx
	conformance.Facade(t, span, "AddEvent", "End", "Receipt", "RecordError", "SetAttributes", "SetStatus", "String", "GoString", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
	_, err = span.End(context.Background())
	if err != nil {
		t.Fatal(err)
	}
}
