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

// Package conformance supplies internal standard-testing assertions and bounded
// evidence reception for Fathomry's mechanism and Provider integration tests. It
// is not an external Provider-extension SDK or a production dependency. It creates
// no SDK, test service, worker or resource owner.
// Callers supply independent native-effect/lifetime oracles; these checks alone
// cannot prove real-service semantics, durability or native-memory limits.
package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Expected is an independently established operation contract at a synchronized
// checkpoint. Value must assert capability-owned meanings when Present is true;
// derive it from the workload/native fault oracle, not the implementation result.
// Primary/Cleanup require nil or errors.Is identity separately. For additional
// native type inspection use Cause. No raw data/errors are printed by this package.
type Expected[T any] struct {
	Context  fault.Context
	Source   resource.Info
	Limits   resource.Limits
	Shape    invocation.Shape
	Nested   bool
	Present  bool
	Final    bool
	Released bool
	Primary  error
	Cleanup  error
	Attempts invocation.Attempts
	Value    func(testing.TB, T)
}

// Result checks metadata, completion, error identity and typed evidence separately.
// Final technical reporting does not imply release or any external effect.
func Result[T any](t testing.TB, got invocation.Result[T], want Expected[T]) {
	t.Helper()
	if got.Context != want.Context || !reflect.DeepEqual(got.Source, want.Source) || got.Limits != want.Limits {
		t.Errorf("conformance: source, execution attribution or effective limits changed")
	}
	if got.Shape != want.Shape || got.Nested != want.Nested {
		t.Errorf("conformance: execution shape or borrowing relationship changed")
	}
	if got.Outcome.Present != want.Present {
		t.Errorf("conformance: missing and present output were conflated")
	}
	if got.Final != want.Final || got.Released != want.Released {
		t.Errorf("conformance: technical completion and local-use confirmation differ from the native oracle")
	}
	if !matchesError(got.Outcome.Primary, want.Primary) || !matchesError(got.Outcome.Cleanup, want.Cleanup) {
		t.Errorf("conformance: primary or cleanup error identity lost")
	}
	for _, cause := range []error{want.Primary, want.Cleanup} {
		if cause != nil && !errors.Is(got.Err(), cause) {
			t.Errorf("conformance: public error lost an inspectable cause")
		}
	}
	if got.Attempts != want.Attempts {
		t.Errorf("conformance: SDK attempt evidence changed")
	}
	if want.Present && want.Value == nil {
		t.Errorf("conformance: present data needs an independent capability oracle")
	}
	if want.Value != nil {
		want.Value(t, got.Outcome.Value)
	}
}

func matchesError(got, want error) bool {
	if want == nil {
		return got == nil
	}
	return errors.Is(got, want)
}

// Cause checks errors.As without formatting the native error. match can assert
// original pointer identity or other safely inspected native evidence.
func Cause[T error](t testing.TB, err error, match func(T) bool) {
	t.Helper()
	var original T
	if match == nil || !errors.As(err, &original) || !match(original) {
		t.Errorf("conformance: native cause inspection lost")
	}
}

// Receive checks an explicitly bounded batch of independent inbox deliveries,
// keyed by logical call ID rather than arrival order. It checks before relinquishing
// each slot. A canceled/expired context leaves unresolved evidence owned; it does
// not fabricate completion. All expectations must require final released results.
// This is testing support, NOT reliable recording or durable acknowledgement.
// Context must have a deadline; fixtures own any native stop/join cleanup.
func Receive[T any](t testing.TB, ctx context.Context, inbox *invocation.Inbox[T], expected []Expected[T]) {
	t.Helper()
	if ctx == nil || len(expected) == 0 || len(expected) > 1024 {
		t.Errorf("conformance: reception requires a bounded batch and context")
		return
	}
	if _, bounded := ctx.Deadline(); !bounded {
		t.Errorf("conformance: reception requires a deadline")
		return
	}
	wants := make(map[string]Expected[T], len(expected))
	for _, want := range expected {
		id := want.Context.Correlation.Call
		if _, duplicate := wants[id]; duplicate || id == "" || !want.Final || !want.Released {
			t.Errorf("conformance: invalid or duplicate reception expectation")
			return
		}
		wants[id] = want
	}
	for range expected {
		delivery, err := inbox.Next(ctx)
		if err != nil {
			t.Errorf("conformance: required evidence did not arrive within the reception budget")
			return
		}
		got, err := delivery.Receipt().WaitReleased(ctx)
		if err != nil {
			t.Errorf("conformance: evidence remains locally owned at the reception deadline")
			return
		}
		id := got.Context.Correlation.Call
		want, ok := wants[id]
		if !ok {
			t.Errorf("conformance: duplicate or unexpected evidence attribution")
		} else {
			Result(t, got, want)
			delete(wants, id)
		}
		if err := delivery.Release(); err != nil {
			t.Errorf("conformance: confirmed delivery could not release its slot")
		}
		if err := delivery.Release(); err != nil {
			t.Errorf("conformance: delivery release is not idempotent")
		}
	}
	if len(wants) != 0 {
		t.Errorf("conformance: required call evidence is missing")
	}
}

// Accounting checks all declared process-local dimensions at a synchronized
// checkpoint. It does not measure heap/RSS/native allocations, prefetch or SDK
// workers; those require separate native instrumentation and capability tests.
func Accounting(t testing.TB, use resource.Usage, limits resource.Limits, evidence invocation.InboxUsage, count int, bytes int64) {
	t.Helper()
	if use.Active < 0 || use.Active > limits.Active || use.Queued < 0 || use.Queued > limits.Queued ||
		use.ActiveBytes < 0 || use.ActiveBytes > limits.Bytes || use.QueuedBytes < 0 || use.QueuedBytes > limits.QueuedBytes ||
		evidence.Outstanding < 0 || evidence.Outstanding > count || evidence.ReservedBytes < 0 || evidence.ReservedBytes > bytes {
		t.Errorf("conformance: declared count or byte boundary exceeded")
	}
}

// Facade checks a method-only capability's dynamic and addressable-copy method
// surfaces against an exact allowlist, and rejects exported state/callback fields
// in reflect.VisibleFields, including promotion through private embeddings. Field
// hiding/ambiguity follows reflection independently of methods: a method cannot
// excuse exposed structural state. Nil embeddings are inspected by type, not used.
// This catches a narrow interface hiding an owning client. Invoke it also
// for returned dynamic handles/callback values with their own allowed methods.
// It cannot audit arbitrary closure captures or establish an untrusted-code sandbox.
func Facade(t testing.TB, value any, methods ...string) {
	t.Helper()
	kind := reflect.TypeOf(value)
	if kind == nil {
		t.Errorf("conformance: missing public facade")
		return
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan, reflect.Interface:
		if reflected.IsNil() {
			t.Errorf("conformance: nil public facade")
			return
		}
	}
	allowed := make(map[string]bool, len(methods))
	for _, method := range methods {
		if method == "" || allowed[method] {
			t.Errorf("conformance: invalid facade method allowlist")
			return
		}
		allowed[method] = true
	}
	surfaces := []reflect.Type{kind}
	if kind.Kind() != reflect.Pointer {
		surfaces = append(surfaces, reflect.PointerTo(kind))
	}
	for _, surface := range surfaces {
		if surface.NumMethod() != len(allowed) {
			t.Errorf("conformance: dynamic facade exposes an unexpected method surface")
		}
		for index := range surface.NumMethod() {
			if !allowed[surface.Method(index).Name] {
				t.Errorf("conformance: dynamic facade exposes an unapproved method")
			}
		}
	}
	if kind.Kind() == reflect.Pointer {
		kind = kind.Elem()
	}
	if kind.Kind() == reflect.Struct {
		for _, field := range reflect.VisibleFields(kind) {
			if field.IsExported() {
				t.Errorf("conformance: method-only facade exposes public state or callbacks")
			}
		}
	}
}

// Private checks fmt and text/JSON slog against injected secret canaries and probes
// their selected diagnostic hooks for panics. Hooks must be synchronous, bounded
// and repeatable: fmt/marshal probes run in addition to native output checks.
// Supplied values retain their actual method sets; also pass pointers when used.
// It intentionally never echoes the offending value/canary. Capability tests must
// inject secrets into real options, native errors and payload paths, not just pass
// an already-redacted string. It cannot identify failures already suppressed inside
// a hook or previously resolved slog.Value. This is not a general secret detector.
func Private(t testing.TB, value any, forbidden ...string) {
	t.Helper()
	check := func(output string) {
		t.Helper()
		if len(output) > diagnosticLimit {
			t.Errorf("conformance: diagnostic projection exceeded the test bound")
		}
		for _, secret := range forbidden {
			if secret == "" || strings.Contains(output, secret) {
				t.Errorf("conformance: diagnostic projection disclosed a forbidden value")
			}
		}
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		if probeFormat(t, format, value) {
			project(t, func() { check(fmt.Sprintf(format, value)) })
		}
	}
	for _, jsonOutput := range []bool{false, true} {
		remaining := diagnosticLimit
		resolved, ok := probeLog(t, slog.AnyValue(value), jsonOutput, 0, &remaining)
		if !ok {
			continue
		}
		var output bytes.Buffer
		var handler slog.Handler = slog.NewTextHandler(&output, nil)
		if jsonOutput {
			handler = slog.NewJSONHandler(&output, nil)
		}
		project(t, func() {
			slog.New(handler).Info("conformance", slog.Any("value", resolved))
			check(output.String())
		})
	}
}

// Runtime additionally checks JSON refusal in both directions. target must be an
// independently owned non-nil pointer to a zero value of the runtime type, not a
// live handle. It is used only for the reconstruction-rejection test. A callback
// panic fails acceptance; it does not count as an intentional JSON refusal.
func Runtime(t testing.TB, value, target any, forbidden ...string) {
	t.Helper()
	Private(t, value, forbidden...)
	var data []byte
	var err error
	if !project(t, func() { data, err = json.Marshal(value) }) {
		return
	}
	if err == nil {
		t.Errorf("conformance: runtime value accepted unversioned JSON encoding")
	} else {
		Private(t, err, forbidden...)
	}
	for _, secret := range forbidden {
		if secret != "" && bytes.Contains(data, []byte(secret)) {
			t.Errorf("conformance: JSON projection disclosed a forbidden value")
		}
	}
	kind := reflect.ValueOf(target)
	input := reflect.Indirect(reflect.ValueOf(value))
	if !input.IsValid() || !kind.IsValid() || kind.Kind() != reflect.Pointer || kind.IsNil() || kind.Elem().Type() != input.Type() || !kind.Elem().IsZero() {
		t.Errorf("conformance: invalid reconstruction test target")
		return
	}
	project(t, func() {
		if err := json.Unmarshal([]byte("{}"), target); err == nil {
			t.Errorf("conformance: runtime value accepted JSON reconstruction")
		}
	})
}
