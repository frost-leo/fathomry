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

package resource_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
)

type settings struct {
	Value  string            `json:"value"`
	Labels map[string]string `json:"labels"`
}
type request struct{ Tag string }
type response struct{ Value, Tag string }
type reader interface {
	Read(context.Context, request) (response, error)
}
type readerFacade struct {
	read func(context.Context, request) (response, error)
}

func (value readerFacade) Read(ctx context.Context, input request) (response, error) {
	return value.read(ctx, input)
}

func prepared(t *testing.T, name, provider, value string) resource.Prepared[settings] {
	t.Helper()
	result, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: settings{Value: value, Labels: map[string]string{"key": "original"}}},
		resource.Input{Identity: resource.Identity{Name: name, Provider: provider}, Format: 1})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func complete(context.Context) resource.ReleaseResult {
	return resource.ReleaseResult{Quiescent: true, Released: true}
}
func selection(t *testing.T, name string, release resource.ReleaseFunc) resource.Selection[reader] {
	t.Helper()
	return resource.Select(prepared(t, name, "example.reader", name), func(_ context.Context, config settings) (resource.Resource[reader], error) {
		return resource.Resource[reader]{Acquired: true, Capability: readerFacade{read: func(ctx context.Context, input request) (response, error) {
			return response{Value: config.Value, Tag: input.Tag}, ctx.Err()
		}}, Release: release}, nil
	})
}
func assemble(t *testing.T, scope string, specs ...resource.Spec) *resource.Assembly {
	t.Helper()
	assembly, err := resource.Assemble(context.Background(), context.Background(), scope, specs...)
	if err != nil {
		t.Fatal(err)
	}
	return assembly
}
func assertNoPending(t *testing.T, assembly *resource.Assembly) {
	t.Helper()
	for _, status := range assembly.Snapshot().Sources {
		if status.Pending {
			t.Fatalf("remaining responsibility: %s", status.Name)
		}
	}
}

func TestMultipleProvidersBindingReuseAndConsumerOwnership(t *testing.T) {
	first := selection(t, "first", complete)
	second := selection(t, "second", complete)
	type countSettings struct {
		Count int `json:"count"`
	}
	config, err := resource.Prepare(resource.Schema[countSettings]{Format: 1, Defaults: countSettings{Count: 17}},
		resource.Input{Identity: resource.Identity{Provider: "example.counter", Name: "third"}, Format: 1})
	if err != nil {
		t.Fatal(err)
	}
	third := resource.Select(config, func(_ context.Context, config countSettings) (resource.Resource[func() int], error) {
		return resource.Resource[func() int]{Acquired: true, Capability: func() int { return config.Count }, Release: complete}, nil
	})
	var unusedCalls atomic.Int32
	unused := resource.Select(prepared(t, "unused", "unused.provider", ""), func(context.Context, settings) (resource.Resource[reader], error) {
		unusedCalls.Add(1)
		return resource.Resource[reader]{}, errors.New("must not construct")
	})
	assembly := assemble(t, "application", first, second, third)
	defer assembly.Close(context.Background())
	for _, selected := range []resource.Selection[reader]{first, second} {
		capability, info, err := resource.Bind(assembly, selected)
		if err != nil {
			t.Fatal(err)
		}
		if _, ownsClient := capability.(io.Closer); ownsClient {
			t.Fatal("consumer acquired client ownership")
		}
		conformance.Facade(t, capability, "Read")
		for index := 0; index < 3; index++ {
			result, err := capability.Read(context.Background(), request{Tag: fmt.Sprint(index)})
			if err != nil || result.Value != info.Configuration.Identity.Name || result.Tag != fmt.Sprint(index) {
				t.Fatal("wrong source or call state")
			}
		}
	}
	counter, _, err := resource.Bind(assembly, third)
	if err != nil || counter() != 17 {
		t.Fatal("heterogeneous capability lost")
	}
	if _, _, err := resource.Bind(assembly, unused); !errors.Is(err, resource.ErrSelection) || unusedCalls.Load() != 0 {
		t.Fatal("unselected source used")
	}
	lookalike := selection(t, "first", complete)
	if _, _, err := resource.Bind(assembly, lookalike); !errors.Is(err, resource.ErrSelection) {
		t.Fatal("name fallback allowed")
	}
}

func TestPreflightRejectsAllInvalidSelectionsBeforeConstruction(t *testing.T) {
	var constructed atomic.Int32
	valid := resource.Select(prepared(t, "same", "example.first", ""), func(context.Context, settings) (resource.Resource[reader], error) {
		constructed.Add(1)
		return resource.Resource[reader]{}, errors.New("unexpected")
	})
	otherProvider := resource.Select(prepared(t, "same", "example.second", ""), func(context.Context, settings) (resource.Resource[reader], error) {
		constructed.Add(1)
		return resource.Resource[reader]{}, errors.New("unexpected")
	})
	var nilSelection *resource.Selection[reader]
	var zero resource.Selection[reader]
	unprepared := resource.Select(resource.Prepared[settings]{}, func(context.Context, settings) (resource.Resource[reader], error) {
		constructed.Add(1)
		return resource.Resource[reader]{}, errors.New("unexpected")
	})
	for index, specs := range [][]resource.Spec{{valid, valid}, {valid, otherProvider}, {valid, zero}, {valid, unprepared}, {valid, nil}, {valid, nilSelection}} {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			defer func() {
				if recover() != nil {
					t.Error("malformed selection panicked")
				}
			}()
			assembly, err := resource.Assemble(context.Background(), context.Background(), "test", specs...)
			if assembly != nil || !errors.Is(err, resource.ErrSelection) {
				t.Fatal("invalid selection passed preflight")
			}
		})
	}
	if constructed.Load() != 0 {
		t.Fatal("constructor ran before preflight finished")
	}
}

func TestGenericTypesCannotBeReinterpreted(t *testing.T) {
	type otherSettings struct {
		Value string `json:"value"`
	}
	if reflect.TypeFor[resource.Prepared[settings]]().ConvertibleTo(reflect.TypeFor[resource.Prepared[otherSettings]]()) {
		t.Error("prepared schema can be reinterpreted without validation")
	}
	if reflect.TypeFor[resource.Selection[reader]]().ConvertibleTo(reflect.TypeFor[resource.Selection[any]]()) {
		t.Error("capability token type can be changed")
	}
}

func TestIndependentAssemblyAndPerCallState(t *testing.T) {
	config := prepared(t, "one", "example.reader", "original")
	var constructed atomic.Int32
	selected := resource.Select(config, func(_ context.Context, config settings) (resource.Resource[reader], error) {
		instance := constructed.Add(1)
		if config.Labels["key"] != "original" {
			t.Error("factory received shared configuration")
		}
		config.Labels["key"] = fmt.Sprint(instance)
		return resource.Resource[reader]{Acquired: true, Release: complete, Capability: readerFacade{read: func(ctx context.Context, input request) (response, error) {
			return response{Value: config.Labels["key"], Tag: input.Tag}, ctx.Err()
		}}}, nil
	})
	var group sync.WaitGroup
	for index := 0; index < 16; index++ {
		group.Go(func() {
			assembly, err := resource.Assemble(context.Background(), context.Background(), fmt.Sprintf("isolated-%d", index), selected)
			if err != nil {
				t.Error(err)
				return
			}
			defer assembly.Close(context.Background())
			capability, _, err := resource.Bind(assembly, selected)
			if err != nil {
				t.Error(err)
				return
			}
			baseline, _ := capability.Read(context.Background(), request{})
			var calls sync.WaitGroup
			for call := 0; call < 16; call++ {
				calls.Go(func() {
					tag := fmt.Sprintf("call-%d", call)
					result, err := capability.Read(context.Background(), request{Tag: tag})
					if err != nil || result.Value != baseline.Value || result.Tag != tag {
						t.Error("call state crossed")
					}
				})
			}
			calls.Wait()
		})
	}
	group.Wait()
	if constructed.Load() != 16 {
		t.Fatal("implicit singleton or reconstruction")
	}
}

func TestPartialFailureKeepsPrimaryCleanupAndRemainingResponsibility(t *testing.T) {
	primary := errors.New("private initialization")
	cleanup := errors.New("private cleanup")
	var order []string
	first := selection(t, "dependency", func(context.Context) resource.ReleaseResult {
		order = append(order, "dependency")
		return complete(context.Background())
	})
	second := resource.Select(prepared(t, "partial", "example.reader", ""), func(context.Context, settings) (resource.Resource[reader], error) {
		return resource.Resource[reader]{Acquired: true, Release: func(context.Context) resource.ReleaseResult {
			order = append(order, "partial")
			return resource.ReleaseResult{Err: cleanup, Continue: func(context.Context) resource.ReleaseResult {
				order = append(order, "continue")
				return complete(context.Background())
			}}
		}}, primary
	})
	assembly, err := resource.Assemble(context.Background(), context.Background(), "failed", first, second)
	if assembly == nil || !errors.Is(err, primary) || !errors.Is(err, cleanup) || !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("failure evidence lost")
	}
	report := assembly.Snapshot()
	if report.Ready || !errors.Is(report.Primary, primary) || !report.Sources[0].Pending || !report.Sources[1].Pending ||
		!reflect.DeepEqual(order, []string{"partial"}) {
		t.Fatal("incomplete resource released dependencies")
	}
	if _, _, err := resource.Bind(assembly, first); !errors.Is(err, resource.ErrSelection) {
		t.Fatal("partial assembly exposed")
	}
	if err := assembly.Close(context.Background()); !errors.Is(err, cleanup) || errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("cleanup history or final evidence lost")
	}
	assertNoPending(t, assembly)
	if !errors.Is(assembly.Snapshot().Primary, primary) || !reflect.DeepEqual(order, []string{"partial", "continue", "dependency"}) {
		t.Fatal("primary or order changed")
	}
	report.Sources[1].CleanupErrors[0] = nil
	report.Sources[0].Info.Configuration.Provenance[0].Fields[0] = "changed"
	if assembly.Snapshot().Sources[1].CleanupErrors[0] == nil {
		t.Fatal("snapshot aliased")
	}
	if err := assembly.Close(context.Background()); !errors.Is(err, cleanup) || len(order) != 3 {
		t.Fatal("no-op erased prior cleanup")
	}
}

func TestReadinessFailureAndConfirmedReleaseError(t *testing.T) {
	primary, cleanup := errors.New("readiness"), errors.New("flush failed after release")
	var order []string
	var checks atomic.Int32
	makeSelected := func(name string, readyError error) resource.Selection[reader] {
		return resource.Select(prepared(t, name, "example.reader", ""), func(context.Context, settings) (resource.Resource[reader], error) {
			order = append(order, "construct-"+name)
			return resource.Resource[reader]{Acquired: true, Capability: readerFacade{}, Check: func(context.Context) error {
				checks.Add(1)
				return readyError
			}, Release: func(context.Context) resource.ReleaseResult {
				order = append(order, "release-"+name)
				return resource.ReleaseResult{Quiescent: true, Released: true, Err: cleanup}
			}}, nil
		})
	}
	assembly, err := resource.Assemble(context.Background(), context.Background(), "checks", makeSelected("one", nil), makeSelected("two", primary))
	if !errors.Is(err, primary) || !errors.Is(err, cleanup) || checks.Load() != 2 {
		t.Fatal("readiness/cleanup evidence lost")
	}
	assertNoPending(t, assembly)
	if !reflect.DeepEqual(order, []string{"construct-one", "construct-two", "release-two", "release-one"}) {
		t.Fatal("incorrect ordering")
	}
	var occurrence *fault.Error
	if !errors.As(assembly.Snapshot().Primary, &occurrence) || occurrence.Diagnostic().Context.Operation != "check" {
		t.Fatal("phase attribution lost")
	}
	if err := assembly.Close(context.Background()); !errors.Is(err, cleanup) || len(order) != 4 {
		t.Fatal("released resource retried")
	}
}

func TestIncompleteCloseNeverRetriesOriginalCallback(t *testing.T) {
	var calls atomic.Int32
	selected := selection(t, "one", func(context.Context) resource.ReleaseResult { calls.Add(1); return resource.ReleaseResult{} })
	assembly := assemble(t, "unknown", selected)
	for attempt := 0; attempt < 3; attempt++ {
		if err := assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
			t.Fatal("unknown became success")
		}
	}
	if calls.Load() != 1 || !assembly.Snapshot().Sources[0].Pending || assembly.Snapshot().Sources[0].CanContinue {
		t.Fatal("unapproved retry or lost responsibility")
	}
}

type cleanupCause struct{ attempt int }

func (*cleanupCause) Error() string { return "cleanup-native-canary" }

func TestCleanupHistorySurvivesExplicitContinuationAndCompletion(t *testing.T) {
	const failedAttempts = 4
	causes := make([]*cleanupCause, failedAttempts)
	for index := range causes {
		causes[index] = &cleanupCause{attempt: index}
	}
	originalCalls, continuedCalls := 0, 0
	var continueCleanup resource.ReleaseFunc
	continueCleanup = func(context.Context) resource.ReleaseResult {
		continuedCalls++
		if continuedCalls == failedAttempts {
			return resource.ReleaseResult{Quiescent: true, Released: true}
		}
		return resource.ReleaseResult{Err: causes[continuedCalls], Continue: continueCleanup}
	}
	selected := selection(t, "history", func(context.Context) resource.ReleaseResult {
		originalCalls++
		return resource.ReleaseResult{Err: causes[0], Continue: continueCleanup}
	})
	assembly := assemble(t, "history", selected)
	var snapshots []resource.Report
	for attempt := range failedAttempts + 2 {
		err := assembly.Close(context.Background())
		pending := attempt < failedAttempts
		if errors.Is(err, resource.ErrIncomplete) != pending || originalCalls != 1 || continuedCalls != min(attempt, failedAttempts) {
			t.Fatal("cleanup retried implicitly or lost its completion evidence")
		}
		report := assembly.Snapshot()
		status := report.Sources[0]
		count := min(attempt+1, failedAttempts)
		if status.Pending != pending || status.CanContinue != pending || len(status.CleanupErrors) != count ||
			status.Quiescent == pending || status.Released == pending {
			t.Fatal("cleanup history or positive shutdown facts changed")
		}
		for index, cause := range causes[:count] {
			var original *cleanupCause
			if !errors.Is(err, cause) || !errors.As(status.CleanupErrors[index], &original) || original != cause {
				t.Fatal("native cleanup cause identity lost")
			}
		}
		conformance.Private(t, err, "cleanup-native-canary")
		snapshots = append(snapshots, report)
	}
	for index, report := range snapshots {
		if len(report.Sources[0].CleanupErrors) != min(index+1, failedAttempts) {
			t.Fatal("later continuation mutated a prior snapshot")
		}
		report.Sources[0].CleanupErrors[0] = nil
	}
	if !errors.Is(assembly.Snapshot().Sources[0].CleanupErrors[0], causes[0]) {
		t.Fatal("snapshot mutation erased retained native history")
	}
}

func TestBorrowersDoNotStopOwnersAndPreventPrematureRelease(t *testing.T) {
	var releases atomic.Int32
	selected := selection(t, "original", func(context.Context) resource.ReleaseResult { releases.Add(1); return complete(context.Background()) })
	owner := assemble(t, "owner", selected)
	borrowed := resource.Borrow("alias", owner, selected)
	borrower := assemble(t, "borrower", borrowed)
	nested := resource.Borrow("nested", borrower, borrowed)
	child := assemble(t, "child", nested)
	if err := owner.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) || releases.Load() != 0 {
		t.Fatal("owner closed borrowed client")
	}
	if err := borrower.Close(context.Background()); err != nil || releases.Load() != 0 {
		t.Fatal("borrower closed shared resource")
	}
	capability, info, err := resource.Bind(child, nested)
	if err != nil || info.Scope != "owner" || info.Configuration.Identity.Name != "original" {
		t.Fatal("borrow changed actual identity")
	}
	if result, err := capability.Read(context.Background(), request{Tag: "child"}); err != nil || result.Tag != "child" {
		t.Fatal("other borrowing scope stopped")
	}
	if _, err := resource.Assemble(context.Background(), context.Background(), "late", resource.Borrow("late", child, nested)); !errors.Is(err, resource.ErrSelection) {
		t.Fatal("new lease after owner shutdown")
	}
	if err := child.Close(context.Background()); err != nil || releases.Load() != 0 {
		t.Fatal("nested borrower closed owner")
	}
	if err := owner.Close(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatal("owner release failed")
	}
	assertNoPending(t, owner)
}

func TestPendingConsumerKeepsBorrowedDependency(t *testing.T) {
	var releases atomic.Int32
	selected := selection(t, "connection", func(context.Context) resource.ReleaseResult { releases.Add(1); return complete(context.Background()) })
	owner := assemble(t, "connection-owner", selected)
	dependency := resource.Borrow("connection", owner, selected)
	consumer := selection(t, "consumer", func(context.Context) resource.ReleaseResult {
		return resource.ReleaseResult{Continue: complete}
	})
	borrower := assemble(t, "consumer-scope", dependency, consumer)
	if err := borrower.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("pending consumer ignored")
	}
	if err := owner.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) || releases.Load() != 0 {
		t.Fatal("live dependency released")
	}
	if err := borrower.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatal("dependency not released after consumer")
	}
}

func TestDelegationAcceptanceAndFailureOwnership(t *testing.T) {
	var releases atomic.Int32
	selected := selection(t, "original", func(context.Context) resource.ReleaseResult { releases.Add(1); return complete(context.Background()) })
	donor := assemble(t, "donor", selected)
	delegated := resource.Delegate("received", donor, selected)
	receiver := assemble(t, "receiver", delegated)
	if _, _, err := resource.Bind(donor, selected); !errors.Is(err, resource.ErrSelection) {
		t.Fatal("donor retained binding")
	}
	if err := donor.Close(context.Background()); err != nil || releases.Load() != 0 {
		t.Fatal("donor retained ownership")
	}
	_, info, err := resource.Bind(receiver, delegated)
	if err != nil || info.Scope != "donor" || receiver.Snapshot().Sources[0].Ownership != resource.Delegated {
		t.Fatal("transfer changed identity")
	}
	if err := receiver.Close(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatal("receiver did not release")
	}

	second := selection(t, "second", func(context.Context) resource.ReleaseResult { return resource.ReleaseResult{Continue: complete} })
	origin := assemble(t, "origin", second)
	failed := resource.Select(prepared(t, "failure", "example.reader", ""), func(context.Context, settings) (resource.Resource[reader], error) {
		return resource.Resource[reader]{}, context.Canceled
	})
	target, err := resource.Assemble(context.Background(), context.Background(), "target", resource.Delegate("take", origin, second), failed)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, resource.ErrIncomplete) || !target.Snapshot().Sources[0].Pending {
		t.Fatal("failed recipient lost responsibility")
	}
	if err := origin.Close(context.Background()); err != nil {
		t.Fatal("donor tried cleanup after transfer")
	}
	if err := target.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDelegationPreservesDependencyBoundariesAndRejectsConflicts(t *testing.T) {
	first, second := selection(t, "first", complete), selection(t, "second", complete)
	donor := assemble(t, "ordered", first, second)
	defer donor.Close(context.Background())
	var calls atomic.Int32
	marker := resource.Select(prepared(t, "marker", "example.reader", ""), func(context.Context, settings) (resource.Resource[reader], error) {
		calls.Add(1)
		return resource.Resource[reader]{}, errors.New("must not run")
	})
	for _, selected := range []resource.Selection[reader]{first, second} {
		target, err := resource.Assemble(context.Background(), context.Background(), "target", marker, resource.Delegate("move", donor, selected))
		if target != nil {
			_ = target.Close(context.Background())
		}
		if target != nil || !errors.Is(err, resource.ErrSelection) || calls.Load() != 0 {
			t.Error("unsafe ordered transfer reached construction")
		}
	}
	single := selection(t, "single", complete)
	owner := assemble(t, "single-owner", single)
	defer owner.Close(context.Background())
	for _, overlapping := range []resource.Spec{resource.Delegate("again", owner, single), resource.Borrow("also", owner, single)} {
		target, err := resource.Assemble(context.Background(), context.Background(), "target", marker, resource.Delegate("take", owner, single), overlapping)
		if target != nil {
			_ = target.Close(context.Background())
		}
		if target != nil || !errors.Is(err, resource.ErrSelection) || calls.Load() != 0 {
			t.Error("known exclusive transfer conflict reached construction")
		}
	}
}

func TestCanceledInitializationUsesSeparateCleanupContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var releases atomic.Int32
	selected := resource.Select(prepared(t, "one", "example.reader", ""), func(context.Context, settings) (resource.Resource[reader], error) {
		cancel()
		return resource.Resource[reader]{Acquired: true, Capability: readerFacade{}, Release: func(cleanup context.Context) resource.ReleaseResult {
			if cleanup.Err() != nil {
				t.Error("cleanup inherited expired init context")
			}
			releases.Add(1)
			return complete(cleanup)
		}}, nil
	})
	assembly, err := resource.Assemble(ctx, context.Background(), "cancel", selected)
	if !errors.Is(err, context.Canceled) || releases.Load() != 1 || assembly.Snapshot().Ready {
		t.Fatal("cancellation or cleanup lost")
	}
	assertNoPending(t, assembly)
}

func TestConcurrentCloseWaitCancellationAndNoDoubleRelease(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	selected := selection(t, "one", func(context.Context) resource.ReleaseResult {
		calls.Add(1)
		close(started)
		<-finish
		return complete(context.Background())
	})
	assembly := assemble(t, "closing", selected)
	done := make(chan error, 1)
	go func() { done <- assembly.Close(context.Background()) }()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := assembly.Close(ctx)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, resource.ErrIncomplete) {
		t.Error("canceled waiter claimed settled cleanup")
	}
	if !assembly.Snapshot().Sources[0].Pending {
		t.Error("inflight callback lost ownership")
	}
	close(finish)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for index := 0; index < 32; index++ {
		group.Go(func() {
			if err := assembly.Close(context.Background()); err != nil {
				t.Error(err)
			}
			_ = assembly.Snapshot()
		})
	}
	group.Wait()
	if calls.Load() != 1 {
		t.Fatal("double release")
	}
}

func nilCapability[C any](t *testing.T, value C) {
	t.Helper()
	var releases atomic.Int32
	selected := resource.Select(prepared(t, "nil", "example.reader", ""), func(context.Context, settings) (resource.Resource[C], error) {
		return resource.Resource[C]{Acquired: true, Capability: value, Release: func(context.Context) resource.ReleaseResult {
			releases.Add(1)
			return complete(context.Background())
		}}, nil
	})
	assembly, err := resource.Assemble(context.Background(), context.Background(), "nil", selected)
	if err == nil || !errors.Is(err, resource.ErrInitialization) || assembly == nil || assembly.Snapshot().Ready {
		t.Error("nil capability advertised ready")
	}
	if assembly != nil {
		_ = assembly.Close(context.Background())
	}
	if releases.Load() != 1 {
		t.Error("nil capability leaked acquired resources")
	}
}
func TestNilCapabilitiesAndZeroAssembly(t *testing.T) {
	nilCapability[reader](t, nil)
	nilCapability[*readerFacade](t, nil)
	nilCapability[func()](t, nil)
	var zero resource.Assembly
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := zero.Close(ctx); !errors.Is(err, resource.ErrSelection) {
		t.Fatal("zero assembly did not reject before waiting")
	}
}

func TestQuiescenceReleaseAndErrorAreIndependent(t *testing.T) {
	var dependencyReleased atomic.Int32
	dependency := selection(t, "dependency", func(context.Context) resource.ReleaseResult {
		dependencyReleased.Add(1)
		return complete(context.Background())
	})
	consumer := selection(t, "consumer", func(context.Context) resource.ReleaseResult {
		return resource.ReleaseResult{Released: true, Continue: func(context.Context) resource.ReleaseResult {
			return resource.ReleaseResult{Quiescent: true}
		}}
	})
	assembly := assemble(t, "independent-evidence", dependency, consumer)
	if err := assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("resource release mistaken for quiescence")
	}
	if dependencyReleased.Load() != 0 || !assembly.Snapshot().Sources[1].Released ||
		assembly.Snapshot().Sources[1].Quiescent {
		t.Fatal("shutdown facts conflated")
	}
	if err := assembly.Close(context.Background()); err != nil || dependencyReleased.Load() != 1 {
		t.Fatal("positive completion evidence not retained")
	}
}

func TestExpiredCleanupRetainsUnattemptedResponsibility(t *testing.T) {
	var calls atomic.Int32
	selected := resource.Select(prepared(t, "partial", "example.reader", ""), func(context.Context, settings) (resource.Resource[reader], error) {
		return resource.Resource[reader]{Acquired: true, Release: func(context.Context) resource.ReleaseResult {
			calls.Add(1)
			return complete(context.Background())
		}}, io.ErrUnexpectedEOF
	})
	cleanup, cancel := context.WithCancel(context.Background())
	cancel()
	assembly, err := resource.Assemble(context.Background(), cleanup, "cleanup-budget", selected)
	if !errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(err, context.Canceled) ||
		!errors.Is(err, resource.ErrIncomplete) || calls.Load() != 0 {
		t.Fatal("expired cleanup budget lost responsibility or primary cause")
	}
	if err := assembly.Close(context.Background()); err != nil || calls.Load() != 1 {
		t.Fatal("unattempted cleanup could not resume")
	}
}

func TestMissingCleanupCallbackRemainsVisible(t *testing.T) {
	selected := resource.Select(prepared(t, "partial", "example.reader", ""), func(context.Context, settings) (resource.Resource[reader], error) {
		return resource.Resource[reader]{Acquired: true}, io.ErrUnexpectedEOF
	})
	assembly, err := resource.Assemble(context.Background(), context.Background(), "invalid-provider", selected)
	if !errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(err, resource.ErrIncomplete) ||
		!assembly.Snapshot().Sources[0].Pending || assembly.Snapshot().Sources[0].CanContinue {
		t.Fatal("invalid Provider cleanup contract advertised completion")
	}
}

func TestConcurrentBorrowAndOwnerShutdown(t *testing.T) {
	var releases atomic.Int32
	selected := selection(t, "shared", func(context.Context) resource.ReleaseResult {
		releases.Add(1)
		return complete(context.Background())
	})
	owner := assemble(t, "owner", selected)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := 0; index < 32; index++ {
		group.Go(func() {
			<-start
			borrowed := resource.Borrow("shared", owner, selected)
			scope, err := resource.Assemble(context.Background(), context.Background(), "borrower", borrowed)
			if err != nil {
				if !errors.Is(err, resource.ErrSelection) {
					t.Error(err)
				}
				if scope != nil {
					_ = scope.Close(context.Background())
				}
				return
			}
			capability, _, err := resource.Bind(scope, borrowed)
			if err != nil {
				t.Error(err)
			} else {
				if _, err := capability.Read(context.Background(), request{}); err != nil {
					t.Error(err)
				}
			}
			if err := scope.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	close(start)
	err := owner.Close(context.Background())
	if err != nil && !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal(err)
	}
	group.Wait()
	if err := owner.Close(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatal("sharing race leaked or double-released the resource")
	}
}
