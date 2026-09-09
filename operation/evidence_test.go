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

package operation_test

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
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/operation"
	"github.com/frost-leo/fathomry/source"
)

func TestPhaseBudgetBoundsAndIndependentCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		parent, cancel := context.WithDeadline(context.Background(), now.Add(10*time.Second))
		defer cancel()
		for _, test := range []struct {
			budget operation.Budget
			want   time.Duration
		}{
			{operation.Budget{Limit: 20 * time.Second, Reserve: 2 * time.Second}, 8 * time.Second},
			{operation.Budget{Limit: time.Second, Reserve: 2 * time.Second}, time.Second},
			{operation.Budget{Reserve: 2 * time.Second}, 8 * time.Second},
		} {
			ctx, end, err := test.budget.Context(parent, operation.Execute)
			if err != nil {
				t.Fatal(err)
			}
			deadline, ok := ctx.Deadline()
			if !ok || deadline.Sub(now) != test.want {
				t.Fatal("phase broadened parent or lost cleanup reserve")
			}
			end()
		}
		for _, budget := range []operation.Budget{{Limit: -1}, {Reserve: -1}, {Reserve: 11 * time.Second}} {
			if _, end, err := budget.Context(parent, operation.Execute); err == nil {
				end()
				t.Fatal("invalid/exhausted phase budget accepted")
			}
		}
		for _, phase := range []operation.Phase{operation.Admission, operation.Execute, operation.Establish, operation.Consume,
			operation.Submit, operation.Delivery, operation.Message, operation.Cleanup, operation.Waiting, 0, 255} {
			if _, end, err := (operation.Budget{}).Context(context.Background(), phase); err == nil {
				end()
				t.Fatal("unbounded finite/invalid phase accepted")
			}
		}
		lifetimeOwner, stop := context.WithCancel(context.Background())
		lifetime, end, err := (operation.Budget{}).Context(lifetimeOwner, operation.Lifetime)
		if err != nil {
			t.Fatal(err)
		}
		stop()
		if !errors.Is(lifetime.Err(), context.Canceled) {
			t.Fatal("continuous lifetime ignored explicit cancellation")
		}
		end()
		if _, end, err := (operation.Budget{}).Context(context.Background(), operation.Lifetime); err == nil {
			end()
			t.Fatal("unowned endless session accepted")
		}
		if _, _, err := (operation.Budget{Limit: time.Second, Reserve: time.Second}).Context(context.Background(), operation.Cleanup); err == nil {
			t.Fatal("reserve without an outer deadline invented a guarantee")
		}
		canceled, stopCanceled := context.WithCancelCause(context.Background())
		cause := errors.New("cancellation ownership")
		stopCanceled(cause)
		if _, _, err := (operation.Budget{Limit: time.Second}).Context(canceled, operation.Execute); !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
			t.Fatal("phase cause lost")
		}
		cleanup, endCleanup, err := (operation.Budget{Limit: time.Second}).Context(context.Background(), operation.Cleanup)
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
	evidence, err := operation.NewInbox[writeEvidence](3, 256)
	if err != nil {
		t.Fatal(err)
	}
	first := begin(t, access, request("first", operation.Finite), evidence, nil)
	second := begin(t, access, request("second", operation.Finite), evidence, nil)
	first.Complete(operation.Outcome[writeEvidence]{Present: true})
	second.Complete(operation.Outcome[writeEvidence]{Primary: errors.New("business tolerates this")})
	received, err := evidence.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := operation.Begin(context.Background(), access, request("byte-overload", operation.Finite), evidence, nil); !errors.Is(err, operation.ErrEvidence) {
		t.Fatal("receiving/handling discarded necessary evidence")
	}
	if evidence.Usage() != (operation.InboxUsage{Outstanding: 2, ReservedBytes: 256}) {
		t.Fatal("wrong evidence byte accounting")
	}
	if err := received.Release(); err != nil {
		t.Fatal(err)
	}
	third := begin(t, access, request("third", operation.Finite), evidence, nil)
	third.Complete(operation.Outcome[writeEvidence]{})
	partial := releaseDelivery(t, evidence)
	missing := releaseDelivery(t, evidence)
	if partial.Err() == nil || missing.Outcome.Present {
		t.Fatal("failure/missing output became explicit empty output")
	}
	if evidence.Usage() != (operation.InboxUsage{}) {
		t.Fatal("inbox retained completed history")
	}
}

func TestInvalidRequestsAndAttemptAccounting(t *testing.T) {
	_, access, _ := fixture(t, policy())
	evidence := inbox(t, 1)
	for _, change := range []func(*operation.Request){
		func(value *operation.Request) { value.Name = "raw/private-url" },
		func(value *operation.Request) { value.Execution.Call = "" },
		func(value *operation.Request) { value.Execution.Item = "private\nitem" },
		func(value *operation.Request) { value.Execution.Run = "" },
		func(value *operation.Request) { value.Shape = 0 },
		func(value *operation.Request) { value.Bytes = -1 },
		func(value *operation.Request) { value.EvidenceBytes = 0 },
		func(value *operation.Request) { value.AttemptsKnown = false },
		func(value *operation.Request) { value.MaxAttempts = 0 },
		func(value *operation.Request) { value.Admission.Limit = -1 },
	} {
		input := request("invalid", operation.Finite)
		change(&input)
		if call, err := operation.Begin(context.Background(), access, input, evidence, nil); call != nil || err == nil {
			t.Fatal("invalid operation accepted")
		}
		if evidence.Usage() != (operation.InboxUsage{}) {
			t.Fatal("rejection leaked evidence")
		}
	}
	input := request("attempts", operation.Finite)
	input.MaxAttempts = 2
	call := begin(t, access, input, evidence, nil)
	for want := uint64(1); want <= 2; want++ {
		got, err := call.Attempt()
		if err != nil || got != want {
			t.Fatal("attempt sequence lost")
		}
	}
	if _, err := call.Attempt(); !errors.Is(err, operation.ErrAttempts) {
		t.Fatal("attempt budget ignored")
	}
	call.Complete(operation.Outcome[writeEvidence]{Present: true})
	if _, err := call.Attempt(); !errors.Is(err, operation.ErrState) {
		t.Fatal("post-completion SDK attempt admitted")
	}
	result := releaseDelivery(t, evidence)
	if result.Attempts != (operation.Attempts{Observed: 2, Exact: true}) {
		t.Fatal("logical and SDK attempts conflated")
	}
}

func TestIndependentNestedOutcomesPreserveParentDuringShutdown(t *testing.T) {
	assembly, access, releases := fixture(t, policy())
	evidence := inbox(t, 2)
	parent := begin(t, access, request("transaction", operation.Stream), evidence, nil)
	parentDelivery, err := evidence.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	input := request("statement", operation.Finite)
	input.Bytes, input.Execution.Parent = 0, "transaction"
	child, err := operation.BeginNested(context.Background(), parent.Scope(), input, evidence, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := assembly.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) {
		t.Fatal("parent use lost")
	}
	parent.Complete(operation.Outcome[writeEvidence]{Value: writeEvidence{UnknownRows: 1}, Present: true})
	if _, err := parent.Scope().Hold(); err == nil {
		t.Fatal("completed parent authority reused")
	}
	if err := child.Execute(context.Background(), operation.Budget{Limit: time.Second}, func(context.Context, operation.Scope) operation.Outcome[writeEvidence] {
		return operation.Outcome[writeEvidence]{Primary: errors.New("statement failed"), Cleanup: errors.New("rollback warning")}
	}); err != nil {
		t.Fatal("nested handle cleanup tried new admission", err)
	}
	childResult := releaseDelivery(t, evidence)
	parentResult, err := parentDelivery.Receipt().WaitReleased(context.Background())
	if err != nil || childResult.Err() == nil || parentResult.Err() != nil ||
		parentResult.Outcome.Value.UnknownRows != 1 || childResult.Attribution.Execution.Parent != "transaction" {
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
	observer, err := operation.NewObserver(1)
	if err != nil {
		t.Fatal(err)
	}
	first := begin(t, access, request("first", operation.Finite), evidence, observer)
	primary := errors.New("private native error")
	first.Complete(operation.Outcome[writeEvidence]{Primary: primary, Present: true, Value: writeEvidence{UnknownRows: 1}})
	second := begin(t, access, request("second", operation.Finite), evidence, observer)
	second.Complete(operation.Outcome[writeEvidence]{Present: true})
	if result, _ := second.Receipt().Result(); result.Observation != operation.ObservationDropped {
		t.Fatal("saturation not visible")
	}
	disabled := begin(t, access, request("disabled", operation.Finite), evidence, nil)
	disabled.Complete(operation.Outcome[writeEvidence]{Present: true})
	if result, _ := disabled.Receipt().Result(); result.Observation != operation.ObservationDisabled {
		t.Fatal("disabled observation confused with result loss")
	}
	exportFailure := errors.New("private exporter endpoint")
	secondObserver, err := operation.NewObserver(1)
	if err != nil {
		t.Fatal(err)
	}
	err = observer.ExportOne(context.Background(), func(ctx context.Context, event operation.Event) error {
		if !event.Failed || event.Shape != operation.Finite {
			t.Error("wrong diagnostic projection")
		}
		nested, err := operation.Begin(ctx, access, request("exporter", operation.Finite), evidence, secondObserver)
		if err != nil {
			return err
		}
		nested.Complete(operation.Outcome[writeEvidence]{Primary: errors.New("export transport failed")})
		if result, _ := nested.Receipt().Result(); result.Observation != operation.ObservationSuppressed {
			t.Error("exporter feedback not suppressed")
		}
		return exportFailure
	})
	if !errors.Is(err, exportFailure) || !errors.Is(err, operation.ErrObservation) {
		t.Fatal("export error hidden")
	}
	result, _ := first.Receipt().Result()
	if !errors.Is(result.Err(), primary) || errors.Is(result.Err(), exportFailure) || result.Outcome.Value.UnknownRows != 1 {
		t.Fatal("exporter replaced primary evidence")
	}
	quiet, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := secondObserver.ExportOne(quiet, func(context.Context, operation.Event) error { t.Fatal("recursive event enqueued"); return nil }); !errors.Is(err, context.DeadlineExceeded) {
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
		observer, err := operation.NewObserver(1)
		if err != nil {
			t.Fatal(err)
		}
		call := begin(t, access, request("seed", operation.Finite), evidence, observer)
		call.Complete(operation.Outcome[writeEvidence]{Present: true})
		entered, unblock, exported := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			exported <- observer.ExportOne(context.Background(), func(context.Context, operation.Event) error {
				close(entered)
				<-unblock
				panic("private panic")
			})
		}()
		<-entered
		second := begin(t, access, request("while-blocked", operation.Finite), evidence, observer)
		second.Complete(operation.Outcome[writeEvidence]{Present: true})
		releaseDelivery(t, evidence)
		releaseDelivery(t, evidence)
		if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 {
			t.Fatal("blocked exporter held resource or evidence")
		}
		close(unblock)
		err = <-exported
		if !errors.Is(err, operation.ErrObservation) || strings.Contains(fmt.Sprint(err), "private") {
			t.Fatal("diagnostic panic escaped or leaked")
		}
	})
}

type opaqueError struct{}

func (*opaqueError) Error() string { panic("native Error must not be formatted") }

func TestRuntimeResultsAndHandlesStayOutOfLogsAndJSON(t *testing.T) {
	_, access, _ := fixture(t, policy())
	evidence, err := operation.NewInbox[string](1, 128)
	if err != nil {
		t.Fatal(err)
	}
	call, err := operation.Begin(context.Background(), access, request("privacy", operation.Finite), evidence, nil)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := call.Scope().Hold()
	if err != nil {
		t.Fatal(err)
	}
	native := new(opaqueError)
	outcome := operation.Outcome[string]{Value: "private-token-payload", Present: true, Primary: native}
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
	for _, value := range []any{outcome, result, call, *call, call.Receipt(), *call.Receipt(), call.Scope(), guard, *guard, evidence, delivery} {
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
	for _, value := range []any{new(operation.Outcome[string]), new(operation.Result[string]), new(operation.Call[string]),
		new(operation.Receipt[string]), new(operation.Scope), new(operation.Guard), new(operation.Inbox[string]), new(operation.DeliveryRecord[string]), new(operation.Observer)} {
		if err := json.Unmarshal([]byte("{}"), value); err == nil {
			t.Fatal("runtime reconstructed from JSON")
		}
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestZeroValuesAndInvalidExecutionLeaveOwnershipExplicit(t *testing.T) {
	var call operation.Call[writeEvidence]
	if call.Complete(operation.Outcome[writeEvidence]{}) || call.Resolve(operation.Outcome[writeEvidence]{}) || call.Finish(nil) {
		t.Fatal("zero completion succeeded")
	}
	if _, err := call.Attempt(); err == nil {
		t.Fatal("zero call attempt")
	}
	if err := call.Execute(context.Background(), operation.Budget{Limit: time.Second}, nil); err == nil {
		t.Fatal("zero execution")
	}
	var receipt operation.Receipt[writeEvidence]
	if _, ok := receipt.Result(); ok {
		t.Fatal("zero receipt became known result")
	}
	if _, err := receipt.Wait(context.Background()); err == nil {
		t.Fatal("zero receipt wait")
	}
	if _, err := receipt.WaitReleased(context.Background()); err == nil {
		t.Fatal("zero receipt release")
	}
	var emptyInbox operation.Inbox[writeEvidence]
	if _, err := emptyInbox.Next(context.Background()); err == nil {
		t.Fatal("zero inbox blocked")
	}
	var record operation.DeliveryRecord[writeEvidence]
	if err := record.Release(); err == nil {
		t.Fatal("zero delivery acknowledged")
	}
	var guard *operation.Guard
	guard.End()
	if _, err := guard.Scope().Hold(); err == nil {
		t.Fatal("zero guard borrowed")
	}
	_, access, _ := fixture(t, policy())
	evidence := inbox(t, 1)
	live := begin(t, access, request("live", operation.Finite), evidence, nil)
	if live.Finish(nil) {
		t.Fatal("cleanup invented a missing main result")
	}
	if err := live.Execute(context.Background(), operation.Budget{}, func(context.Context, operation.Scope) operation.Outcome[writeEvidence] {
		t.Fatal("unbounded finite SDK entry")
		return operation.Outcome[writeEvidence]{}
	}); !errors.Is(err, operation.ErrBudget) {
		t.Fatal("invalid finite budget not rejected")
	}
	result := releaseDelivery(t, evidence)
	if result.Outcome.Present || !errors.Is(result.Err(), operation.ErrBudget) {
		t.Fatal("pre-execution failure became data success")
	}
}

func TestConcurrentEvidenceReceiverReleaseIsIdempotent(t *testing.T) {
	_, access, _ := fixture(t, policy())
	evidence := inbox(t, 1)
	call := begin(t, access, request("release", operation.Finite), evidence, nil)
	call.Complete(operation.Outcome[writeEvidence]{Present: true})
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
	if evidence.Usage() != (operation.InboxUsage{}) {
		t.Fatal("concurrent release double-returned reservation")
	}
}

func TestAlreadyAttributedSemanticOwnerErrorIsNotReplaced(t *testing.T) {
	_, access, _ := fixture(t, policy())
	evidence := inbox(t, 1)
	call := begin(t, access, request("semantic", operation.Finite), evidence, nil)
	pending, _ := call.Receipt().Result()
	identity := failure.MustDefine(failure.Definition{Code: "example.write.visibility_unknown", Component: "example.write", Version: 2})
	original := identity.New(pending.Attribution, context.DeadlineExceeded)
	call.Complete(operation.Outcome[writeEvidence]{Primary: original})
	result := releaseDelivery(t, evidence)
	if result.Outcome.Primary != original || !errors.Is(result.Err(), identity) || !errors.Is(result.Err(), context.DeadlineExceeded) {
		t.Fatal("suitable public error was replaced with a generic layer identity")
	}
}

func BenchmarkBoundedFiniteCalls(b *testing.B) {
	limits := policy()
	limits.MaxLeases = 1
	_, access, _ := fixture(b, limits)
	evidence := inbox(b, 1)
	input := request("benchmark", operation.Finite)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		call, err := operation.Begin(context.Background(), access, input, evidence, nil)
		if err != nil {
			b.Fatal(err)
		}
		call.Complete(operation.Outcome[writeEvidence]{Present: true})
		delivery, err := evidence.Next(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			b.Fatal(err)
		}
	}
	if evidence.Usage() != (operation.InboxUsage{}) {
		b.Fatal("completed history retained")
	}
}
