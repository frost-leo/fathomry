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

package invocation_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestPhaseBudgetBoundsAndIndependentCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		parent, cancel := context.WithDeadline(context.Background(), now.Add(10*time.Second))
		defer cancel()
		for _, test := range []struct {
			budget invocation.Budget
			want   time.Duration
		}{
			{invocation.Budget{Limit: 20 * time.Second, Reserve: 2 * time.Second}, 8 * time.Second},
			{invocation.Budget{Limit: time.Second, Reserve: 2 * time.Second}, time.Second},
			{invocation.Budget{Reserve: 2 * time.Second}, 8 * time.Second},
		} {
			ctx, end, err := test.budget.Context(parent, invocation.Execute)
			if err != nil {
				t.Fatal(err)
			}
			deadline, ok := ctx.Deadline()
			if !ok || deadline.Sub(now) != test.want {
				t.Fatal("phase broadened parent or lost cleanup reserve")
			}
			end()
		}
		for _, budget := range []invocation.Budget{{Limit: -1}, {Reserve: -1}, {Reserve: 11 * time.Second}} {
			if _, end, err := budget.Context(parent, invocation.Execute); err == nil {
				end()
				t.Fatal("invalid/exhausted phase budget accepted")
			}
		}
		for _, phase := range []invocation.Phase{invocation.Admission, invocation.Execute, invocation.Establish, invocation.Consume,
			invocation.Submit, invocation.Delivery, invocation.Message, invocation.Cleanup, invocation.Waiting, 0, 255} {
			if _, end, err := (invocation.Budget{}).Context(context.Background(), phase); err == nil {
				end()
				t.Fatal("unbounded finite/invalid phase accepted")
			}
		}
		lifetimeOwner, stop := context.WithCancel(context.Background())
		lifetime, end, err := (invocation.Budget{}).Context(lifetimeOwner, invocation.Lifetime)
		if err != nil {
			t.Fatal(err)
		}
		stop()
		if !errors.Is(lifetime.Err(), context.Canceled) {
			t.Fatal("continuous lifetime ignored explicit cancellation")
		}
		end()
		if _, end, err := (invocation.Budget{}).Context(context.Background(), invocation.Lifetime); err == nil {
			end()
			t.Fatal("unowned endless session accepted")
		}
		if _, _, err := (invocation.Budget{Limit: time.Second, Reserve: time.Second}).Context(context.Background(), invocation.Cleanup); err == nil {
			t.Fatal("reserve without an outer deadline invented a guarantee")
		}
		canceled, stopCanceled := context.WithCancelCause(context.Background())
		cause := errors.New("cancellation ownership")
		stopCanceled(cause)
		if _, _, err := (invocation.Budget{Limit: time.Second}).Context(canceled, invocation.Execute); !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
			t.Fatal("phase cause lost")
		}
		cleanup, endCleanup, err := (invocation.Budget{Limit: time.Second}).Context(context.Background(), invocation.Cleanup)
		if err != nil || cleanup.Err() != nil {
			t.Fatal("cleanup has no independent budget")
		}
		endCleanup()
	})
}

func TestEvidenceByteCapacitySurvivesReceivingAndHandledErrors(t *testing.T) {
	limits := policy()
	limits.Active = 3
	_, access, _ := fixture(t, limits)
	evidence, err := invocation.NewInbox[writeEvidence](3, 256)
	if err != nil {
		t.Fatal(err)
	}
	first := begin(t, access, request("first", invocation.Finite), evidence, nil)
	second := begin(t, access, request("second", invocation.Finite), evidence, nil)
	first.Complete(invocation.Outcome[writeEvidence]{Present: true})
	second.Complete(invocation.Outcome[writeEvidence]{Primary: errors.New("business tolerates this")})
	received, err := evidence.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := invocation.Begin(context.Background(), access, request("byte-overload", invocation.Finite), evidence, nil); !errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("receiving/handling discarded necessary evidence")
	}
	if evidence.Usage() != (invocation.InboxUsage{Outstanding: 2, ReservedBytes: 256}) {
		t.Fatal("wrong evidence byte accounting")
	}
	if err := received.Release(); err != nil {
		t.Fatal(err)
	}
	third := begin(t, access, request("third", invocation.Finite), evidence, nil)
	third.Complete(invocation.Outcome[writeEvidence]{})
	partial := releaseDelivery(t, evidence)
	missing := releaseDelivery(t, evidence)
	if partial.Err() == nil || missing.Outcome.Present {
		t.Fatal("failure/missing output became explicit empty output")
	}
	if evidence.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("inbox retained completed history")
	}
}

func TestInvalidRequestsAndAttemptAccounting(t *testing.T) {
	_, access, _ := fixture(t, policy())
	evidence := inbox(t, 1)
	for _, change := range []func(*invocation.Request){
		func(value *invocation.Request) { value.Name = "raw/private-url" },
		func(value *invocation.Request) { value.Correlation.Call = "" },
		func(value *invocation.Request) { value.Correlation.Owner = "private\nowner" },
		func(value *invocation.Request) { value.Correlation.Parent = value.Correlation.Call },
		func(value *invocation.Request) { value.Shape = 0 },
		func(value *invocation.Request) { value.Bytes = -1 },
		func(value *invocation.Request) { value.EvidenceBytes = 0 },
		func(value *invocation.Request) { value.AttemptsKnown = false },
		func(value *invocation.Request) { value.MaxAttempts = 0 },
		func(value *invocation.Request) { value.Admission.Limit = -1 },
	} {
		input := request("invalid", invocation.Finite)
		change(&input)
		if call, err := invocation.Begin(context.Background(), access, input, evidence, nil); call != nil || err == nil {
			t.Fatal("invalid operation accepted")
		}
		if evidence.Usage() != (invocation.InboxUsage{}) {
			t.Fatal("rejection leaked evidence")
		}
	}
	input := request("attempts", invocation.Finite)
	input.MaxAttempts = 2
	call := begin(t, access, input, evidence, nil)
	for want := uint64(1); want <= 2; want++ {
		got, err := call.Attempt()
		if err != nil || got != want {
			t.Fatal("attempt sequence lost")
		}
	}
	if _, err := call.Attempt(); !errors.Is(err, invocation.ErrAttempts) {
		t.Fatal("attempt budget ignored")
	}
	call.Complete(invocation.Outcome[writeEvidence]{Present: true})
	if _, err := call.Attempt(); !errors.Is(err, invocation.ErrState) {
		t.Fatal("post-completion SDK attempt admitted")
	}
	result := releaseDelivery(t, evidence)
	if result.Attempts != (invocation.Attempts{Observed: 2, Exact: true}) {
		t.Fatal("logical and SDK attempts conflated")
	}
}

func TestIndependentNestedOutcomesPreserveParentDuringShutdown(t *testing.T) {
	assembly, access, releases := fixture(t, policy())
	evidence := inbox(t, 2)
	parent := begin(t, access, request("transaction", invocation.Stream), evidence, nil)
	parentDelivery, err := evidence.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	input := request("statement", invocation.Finite)
	input.Bytes, input.Correlation.Parent = 0, "transaction"
	child, err := invocation.BeginNested(context.Background(), parent.Scope(), input, evidence, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("parent use lost")
	}
	parent.Complete(invocation.Outcome[writeEvidence]{Value: writeEvidence{UnknownRows: 1}, Present: true})
	if _, err := parent.Scope().Hold(); err == nil {
		t.Fatal("completed parent authority reused")
	}
	if err := child.Execute(context.Background(), invocation.Budget{Limit: time.Second}, func(context.Context, invocation.Scope) invocation.Outcome[writeEvidence] {
		return invocation.Outcome[writeEvidence]{Primary: errors.New("statement failed"), Cleanup: errors.New("rollback warning")}
	}); err != nil {
		t.Fatal("nested handle cleanup tried new admission", err)
	}
	childResult := releaseDelivery(t, evidence)
	parentResult, err := parentDelivery.Receipt().WaitReleased(context.Background())
	if err != nil || childResult.Err() == nil || parentResult.Err() != nil ||
		parentResult.Outcome.Value.UnknownRows != 1 || childResult.Context.Correlation.Parent != "transaction" {
		t.Fatal("independent facts became a fabricated transaction result")
	}
	if err := parentDelivery.Release(); err != nil {
		t.Fatal(err)
	}
	if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatal("parent completion released twice")
	}
}

func TestObserverDisabledDroppedFailedAndRecursiveAreIsolated(t *testing.T) {
	_, access, _ := fixture(t, policy())
	evidence := inbox(t, 8)
	observer, err := invocation.NewObserver(1)
	if err != nil {
		t.Fatal(err)
	}
	first := begin(t, access, request("first", invocation.Finite), evidence, observer)
	primary := errors.New("private native error")
	first.Complete(invocation.Outcome[writeEvidence]{Primary: primary, Present: true, Value: writeEvidence{UnknownRows: 1}})
	second := begin(t, access, request("second", invocation.Finite), evidence, observer)
	second.Complete(invocation.Outcome[writeEvidence]{Present: true})
	if result, _ := second.Receipt().Result(); result.Observation != invocation.ObservationDropped {
		t.Fatal("saturation not visible")
	}
	disabled := begin(t, access, request("disabled", invocation.Finite), evidence, nil)
	disabled.Complete(invocation.Outcome[writeEvidence]{Present: true})
	if result, _ := disabled.Receipt().Result(); result.Observation != invocation.ObservationDisabled {
		t.Fatal("disabled observation confused with result loss")
	}
	exportFailure := errors.New("private exporter endpoint")
	secondObserver, err := invocation.NewObserver(1)
	if err != nil {
		t.Fatal(err)
	}
	err = observer.ExportOne(context.Background(), func(ctx context.Context, event invocation.Event) error {
		if !event.Failed || event.Shape != invocation.Finite {
			t.Error("wrong diagnostic projection")
		}
		nested, err := invocation.Begin(ctx, access, request("exporter", invocation.Finite), evidence, secondObserver)
		if err != nil {
			return err
		}
		nested.Complete(invocation.Outcome[writeEvidence]{Primary: errors.New("export transport failed")})
		if result, _ := nested.Receipt().Result(); result.Observation != invocation.ObservationSuppressed {
			t.Error("exporter feedback not suppressed")
		}
		return exportFailure
	})
	if !errors.Is(err, exportFailure) || !errors.Is(err, invocation.ErrObservation) {
		t.Fatal("export error hidden")
	}
	result, _ := first.Receipt().Result()
	if !errors.Is(result.Err(), primary) || errors.Is(result.Err(), exportFailure) || result.Outcome.Value.UnknownRows != 1 {
		t.Fatal("exporter replaced primary evidence")
	}
	quiet, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := secondObserver.ExportOne(quiet, func(context.Context, invocation.Event) error { t.Fatal("recursive event enqueued"); return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("diagnostic consumer wait contract")
	}
	for range 4 {
		releaseDelivery(t, evidence)
	}
	if evidence.Usage().Outstanding != 0 {
		t.Fatal("diagnostic failure removed or stranded required evidence")
	}
}

func TestBlockingAndPanickingExporterCannotBlockCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		assembly, access, releases := fixture(t, policy())
		evidence := inbox(t, 2)
		observer, err := invocation.NewObserver(1)
		if err != nil {
			t.Fatal(err)
		}
		call := begin(t, access, request("seed", invocation.Finite), evidence, observer)
		call.Complete(invocation.Outcome[writeEvidence]{Present: true})
		entered, unblock, exported := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			exported <- observer.ExportOne(context.Background(), func(context.Context, invocation.Event) error {
				close(entered)
				<-unblock
				panic("private panic")
			})
		}()
		<-entered
		second := begin(t, access, request("while-blocked", invocation.Finite), evidence, observer)
		second.Complete(invocation.Outcome[writeEvidence]{Present: true})
		releaseDelivery(t, evidence)
		releaseDelivery(t, evidence)
		if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 {
			t.Fatal("blocked exporter held resource or evidence")
		}
		close(unblock)
		err = <-exported
		if !errors.Is(err, invocation.ErrObservation) || strings.Contains(fmt.Sprint(err), "private") {
			t.Fatal("diagnostic panic escaped or leaked")
		}
	})
}

type opaqueError struct{}

func (*opaqueError) Error() string { panic("native Error must not be formatted") }

func TestRuntimeResultsAndHandlesStayOutOfLogsAndJSON(t *testing.T) {
	_, access, _ := fixture(t, policy())
	evidence, err := invocation.NewInbox[string](1, 128)
	if err != nil {
		t.Fatal(err)
	}
	call, err := invocation.Begin(context.Background(), access, request("privacy", invocation.Finite), evidence, nil)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := call.Scope().Hold()
	if err != nil {
		t.Fatal(err)
	}
	native := new(opaqueError)
	outcome := invocation.Outcome[string]{Value: "private-token-payload", Present: true, Primary: native}
	call.Complete(outcome)
	guard.End()
	result, _ := call.Receipt().Result()
	var original *opaqueError
	if !errors.As(result.Err(), &original) || original != native {
		t.Fatal("native inspection lost")
	}
	delivery, err := evidence.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	receiptCopy := reflect.ValueOf(call.Receipt()).Elem().Interface()
	for _, value := range []any{outcome, result, call, *call, call.Receipt(), receiptCopy, call.Scope(), guard, *guard, evidence, delivery} {
		conformance.Private(t, value, "private")
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime serialized as unversioned protocol")
		}
		var output bytes.Buffer
		slog.New(slog.NewJSONHandler(&output, nil)).Info("fixture", "value", value)
		if strings.Contains(output.String(), "private") || strings.Contains(output.String(), "PANIC") {
			t.Fatal("structured logging leaked evidence")
		}
	}
	for _, value := range []any{new(invocation.Outcome[string]), new(invocation.Result[string]), new(invocation.Call[string]),
		new(invocation.Receipt[string]), reflect.New(reflect.TypeOf(receiptCopy)).Interface(),
		new(invocation.Scope), new(invocation.Guard), new(invocation.Inbox[string]), new(invocation.DeliveryRecord[string]), new(invocation.Observer)} {
		if err := json.Unmarshal([]byte("{}"), value); err == nil {
			t.Fatal("runtime reconstructed from JSON")
		}
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestZeroValuesAndInvalidCorrelationLeaveOwnershipExplicit(t *testing.T) {
	var call invocation.Call[writeEvidence]
	if call.Complete(invocation.Outcome[writeEvidence]{}) || call.Resolve(invocation.Outcome[writeEvidence]{}) || call.Finish(nil) {
		t.Fatal("zero completion succeeded")
	}
	if _, err := call.Attempt(); err == nil {
		t.Fatal("zero call attempt")
	}
	if err := call.Execute(context.Background(), invocation.Budget{Limit: time.Second}, nil); err == nil {
		t.Fatal("zero execution")
	}
	if call.Receipt() != nil {
		t.Fatal("zero call supplied a receipt")
	}
	var emptyInbox invocation.Inbox[writeEvidence]
	if _, err := emptyInbox.Next(context.Background()); err == nil {
		t.Fatal("zero inbox blocked")
	}
	var record invocation.DeliveryRecord[writeEvidence]
	if record.Receipt() != nil {
		t.Fatal("zero delivery supplied a receipt")
	}
	if err := record.Release(); err == nil {
		t.Fatal("zero delivery acknowledged")
	}
	var guard *invocation.Guard
	guard.End()
	if _, err := guard.Scope().Hold(); err == nil {
		t.Fatal("zero guard borrowed")
	}
	_, access, _ := fixture(t, policy())
	evidence := inbox(t, 1)
	live := begin(t, access, request("live", invocation.Finite), evidence, nil)
	if live.Finish(nil) {
		t.Fatal("cleanup invented a missing main result")
	}
	if err := live.Execute(context.Background(), invocation.Budget{}, func(context.Context, invocation.Scope) invocation.Outcome[writeEvidence] {
		t.Fatal("unbounded finite SDK entry")
		return invocation.Outcome[writeEvidence]{}
	}); !errors.Is(err, invocation.ErrBudget) {
		t.Fatal("invalid finite budget not rejected")
	}
	result := releaseDelivery(t, evidence)
	if result.Outcome.Present || !errors.Is(result.Err(), invocation.ErrBudget) {
		t.Fatal("pre-execution failure became data success")
	}
}

func TestConcurrentEvidenceReceiverReleaseIsIdempotent(t *testing.T) {
	_, access, _ := fixture(t, policy())
	evidence := inbox(t, 1)
	call := begin(t, access, request("release", invocation.Finite), evidence, nil)
	call.Complete(invocation.Outcome[writeEvidence]{Present: true})
	delivery, err := evidence.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 32 {
		group.Go(func() {
			if err := delivery.Release(); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	if evidence.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("concurrent release double-returned reservation")
	}
}

func TestAlreadyAttributedSemanticOwnerErrorIsNotReplaced(t *testing.T) {
	_, access, _ := fixture(t, policy())
	evidence := inbox(t, 1)
	call := begin(t, access, request("semantic", invocation.Finite), evidence, nil)
	pending, _ := call.Receipt().Result()
	identity := fault.Kind("example.write.visibility_unknown")
	original := identity.New(pending.Context, context.DeadlineExceeded)
	call.Complete(invocation.Outcome[writeEvidence]{Primary: original})
	result := releaseDelivery(t, evidence)
	if result.Outcome.Primary != original || !errors.Is(result.Err(), identity) || !errors.Is(result.Err(), context.DeadlineExceeded) {
		t.Fatal("suitable technical error was replaced with a generic layer identity")
	}
}

func BenchmarkBoundedFiniteCalls(b *testing.B) {
	limits := policy()
	limits.MaxLeases = 1
	_, access, _ := fixture(b, limits)
	evidence := inbox(b, 1)
	input := request("benchmark", invocation.Finite)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		call, err := invocation.Begin(context.Background(), access, input, evidence, nil)
		if err != nil {
			b.Fatal(err)
		}
		call.Complete(invocation.Outcome[writeEvidence]{Present: true})
		delivery, err := evidence.Next(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			b.Fatal(err)
		}
	}
	if evidence.Usage() != (invocation.InboxUsage{}) {
		b.Fatal("completed history retained")
	}
}
