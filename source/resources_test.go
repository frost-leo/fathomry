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

package source_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/source"
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

func prepared(t *testing.T, name, provider, value string) source.Prepared[settings] {
	t.Helper()
	result, err := source.Prepare(source.Schema[settings]{Format: 1, Defaults: settings{Value: value, Labels: map[string]string{"key": "original"}}},
		source.Input{Identity: source.Identity{Name: name, Provider: provider}, Format: 1})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func complete(context.Context) source.ReleaseResult {
	return source.ReleaseResult{Quiescent: true, Released: true}
}
func selection(t *testing.T, name string, release source.ReleaseFunc) source.Selection[reader] {
	t.Helper()
	return source.Select(prepared(t, name, "example.reader", name), func(_ context.Context, config settings) (source.Resource[reader], error) {
		return source.Resource[reader]{Acquired: true, Capability: readerFacade{read: func(ctx context.Context, input request) (response, error) {
			return response{Value: config.Value, Tag: input.Tag}, ctx.Err()
		}}, Release: release}, nil
	})
}
func assemble(t *testing.T, scope string, specs ...source.Spec) *source.Assembly {
	t.Helper()
	assembly, err := source.Assemble(context.Background(), context.Background(), scope, specs...)
	if err != nil {
		t.Fatal(err)
	}
	return assembly
}
func assertNoPending(t *testing.T, assembly *source.Assembly) {
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
	config, err := source.Prepare(source.Schema[countSettings]{Format: 1, Defaults: countSettings{Count: 17}},
		source.Input{Identity: source.Identity{Provider: "example.counter", Name: "third"}, Format: 1})
	if err != nil {
		t.Fatal(err)
	}
	third := source.Select(config, func(_ context.Context, config countSettings) (source.Resource[func() int], error) {
		return source.Resource[func() int]{Acquired: true, Capability: func() int { return config.Count }, Release: complete}, nil
	})
	var unusedCalls atomic.Int32
	unused := source.Select(prepared(t, "unused", "unused.provider", ""), func(context.Context, settings) (source.Resource[reader], error) {
		unusedCalls.Add(1)
		return source.Resource[reader]{}, errors.New("must not construct")
	})
	assembly := assemble(t, "application", first, second, third)
	defer assembly.Close(context.Background())
	for _, selected := range []source.Selection[reader]{first, second} {
		capability, info, err := source.Bind(assembly, selected)
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
	counter, _, err := source.Bind(assembly, third)
	if err != nil || counter() != 17 {
		t.Fatal("heterogeneous capability lost")
	}
	if _, _, err := source.Bind(assembly, unused); !errors.Is(err, source.ErrSelection) || unusedCalls.Load() != 0 {
		t.Fatal("unselected source used")
	}
	lookalike := selection(t, "first", complete)
	if _, _, err := source.Bind(assembly, lookalike); !errors.Is(err, source.ErrSelection) {
		t.Fatal("name fallback allowed")
	}
}

func TestPreflightRejectsAllInvalidSelectionsBeforeConstruction(t *testing.T) {
	var constructed atomic.Int32
	valid := source.Select(prepared(t, "same", "example.first", ""), func(context.Context, settings) (source.Resource[reader], error) {
		constructed.Add(1)
		return source.Resource[reader]{}, errors.New("unexpected")
	})
	otherProvider := source.Select(prepared(t, "same", "example.second", ""), func(context.Context, settings) (source.Resource[reader], error) {
		constructed.Add(1)
		return source.Resource[reader]{}, errors.New("unexpected")
	})
	var nilSelection *source.Selection[reader]
	var zero source.Selection[reader]
	unprepared := source.Select(source.Prepared[settings]{}, func(context.Context, settings) (source.Resource[reader], error) {
		constructed.Add(1)
		return source.Resource[reader]{}, errors.New("unexpected")
	})
	for index, specs := range [][]source.Spec{{valid, valid}, {valid, otherProvider}, {valid, zero}, {valid, unprepared}, {valid, nil}, {valid, nilSelection}} {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			defer func() {
				if recover() != nil {
					t.Error("malformed selection panicked")
				}
			}()
			assembly, err := source.Assemble(context.Background(), context.Background(), "test", specs...)
			if assembly != nil || !errors.Is(err, source.ErrSelection) {
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
	if reflect.TypeFor[source.Prepared[settings]]().ConvertibleTo(reflect.TypeFor[source.Prepared[otherSettings]]()) {
		t.Error("prepared schema can be reinterpreted without validation")
	}
	if reflect.TypeFor[source.Selection[reader]]().ConvertibleTo(reflect.TypeFor[source.Selection[any]]()) {
		t.Error("capability token type can be changed")
	}
}

func TestIndependentAssemblyAndPerCallState(t *testing.T) {
	config := prepared(t, "one", "example.reader", "original")
	var constructed atomic.Int32
	selected := source.Select(config, func(_ context.Context, config settings) (source.Resource[reader], error) {
		instance := constructed.Add(1)
		if config.Labels["key"] != "original" {
			t.Error("factory received shared configuration")
		}
		config.Labels["key"] = fmt.Sprint(instance)
		return source.Resource[reader]{Acquired: true, Release: complete, Capability: readerFacade{read: func(ctx context.Context, input request) (response, error) {
			return response{Value: config.Labels["key"], Tag: input.Tag}, ctx.Err()
		}}}, nil
	})
	var group sync.WaitGroup
	for index := 0; index < 16; index++ {
		group.Go(func() {
			assembly, err := source.Assemble(context.Background(), context.Background(), fmt.Sprintf("isolated-%d", index), selected)
			if err != nil {
				t.Error(err)
				return
			}
			defer assembly.Close(context.Background())
			capability, _, err := source.Bind(assembly, selected)
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
	first := selection(t, "dependency", func(context.Context) source.ReleaseResult {
		order = append(order, "dependency")
		return complete(context.Background())
	})
	second := source.Select(prepared(t, "partial", "example.reader", ""), func(context.Context, settings) (source.Resource[reader], error) {
		return source.Resource[reader]{Acquired: true, Release: func(context.Context) source.ReleaseResult {
			order = append(order, "partial")
			return source.ReleaseResult{Err: cleanup, Continue: func(context.Context) source.ReleaseResult {
				order = append(order, "continue")
				return complete(context.Background())
			}}
		}}, primary
	})
	assembly, err := source.Assemble(context.Background(), context.Background(), "failed", first, second)
	if assembly == nil || !errors.Is(err, primary) || !errors.Is(err, cleanup) || !errors.Is(err, source.ErrIncomplete) {
		t.Fatal("failure evidence lost")
	}
	report := assembly.Snapshot()
	if report.Ready || !errors.Is(report.Primary, primary) || !report.Sources[0].Pending || !report.Sources[1].Pending ||
		!reflect.DeepEqual(order, []string{"partial"}) {
		t.Fatal("incomplete resource released dependencies")
	}
	if _, _, err := source.Bind(assembly, first); !errors.Is(err, source.ErrSelection) {
		t.Fatal("partial assembly exposed")
	}
	if err := assembly.Close(context.Background()); !errors.Is(err, cleanup) || errors.Is(err, source.ErrIncomplete) {
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
	makeSelected := func(name string, readyError error) source.Selection[reader] {
		return source.Select(prepared(t, name, "example.reader", ""), func(context.Context, settings) (source.Resource[reader], error) {
			order = append(order, "construct-"+name)
			return source.Resource[reader]{Acquired: true, Capability: readerFacade{}, Check: func(context.Context) error {
				checks.Add(1)
				return readyError
			}, Release: func(context.Context) source.ReleaseResult {
				order = append(order, "release-"+name)
				return source.ReleaseResult{Quiescent: true, Released: true, Err: cleanup}
			}}, nil
		})
	}
	assembly, err := source.Assemble(context.Background(), context.Background(), "checks", makeSelected("one", nil), makeSelected("two", primary))
	if !errors.Is(err, primary) || !errors.Is(err, cleanup) || checks.Load() != 2 {
		t.Fatal("readiness/cleanup evidence lost")
	}
	assertNoPending(t, assembly)
	if !reflect.DeepEqual(order, []string{"construct-one", "construct-two", "release-two", "release-one"}) {
		t.Fatal("incorrect ordering")
	}
	var occurrence *failure.Error
	if !errors.As(assembly.Snapshot().Primary, &occurrence) || occurrence.Diagnostic().Attribution.Operation != "check" {
		t.Fatal("phase attribution lost")
	}
	if err := assembly.Close(context.Background()); !errors.Is(err, cleanup) || len(order) != 4 {
		t.Fatal("released resource retried")
	}
}

func TestIncompleteCloseNeverRetriesOriginalCallback(t *testing.T) {
	var calls atomic.Int32
	selected := selection(t, "one", func(context.Context) source.ReleaseResult { calls.Add(1); return source.ReleaseResult{} })
	assembly := assemble(t, "unknown", selected)
	for attempt := 0; attempt < 3; attempt++ {
		if err := assembly.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) {
			t.Fatal("unknown became success")
		}
	}
	if calls.Load() != 1 || !assembly.Snapshot().Sources[0].Pending || assembly.Snapshot().Sources[0].CanContinue {
		t.Fatal("unapproved retry or lost responsibility")
	}
}

func TestBorrowersDoNotStopOwnersAndPreventPrematureRelease(t *testing.T) {
	var releases atomic.Int32
	selected := selection(t, "original", func(context.Context) source.ReleaseResult { releases.Add(1); return complete(context.Background()) })
	owner := assemble(t, "owner", selected)
	borrowed := source.Borrow("alias", owner, selected)
	borrower := assemble(t, "borrower", borrowed)
	nested := source.Borrow("nested", borrower, borrowed)
	child := assemble(t, "child", nested)
	if err := owner.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) || releases.Load() != 0 {
		t.Fatal("owner closed borrowed client")
	}
	if err := borrower.Close(context.Background()); err != nil || releases.Load() != 0 {
		t.Fatal("borrower closed shared resource")
	}
	capability, info, err := source.Bind(child, nested)
	if err != nil || info.Scope != "owner" || info.Configuration.Identity.Name != "original" {
		t.Fatal("borrow changed actual identity")
	}
	if result, err := capability.Read(context.Background(), request{Tag: "child"}); err != nil || result.Tag != "child" {
		t.Fatal("other borrowing scope stopped")
	}
	if _, err := source.Assemble(context.Background(), context.Background(), "late", source.Borrow("late", child, nested)); !errors.Is(err, source.ErrSelection) {
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
	selected := selection(t, "connection", func(context.Context) source.ReleaseResult { releases.Add(1); return complete(context.Background()) })
	owner := assemble(t, "connection-owner", selected)
	dependency := source.Borrow("connection", owner, selected)
	consumer := selection(t, "consumer", func(context.Context) source.ReleaseResult {
		return source.ReleaseResult{Continue: complete}
	})
	borrower := assemble(t, "consumer-scope", dependency, consumer)
	if err := borrower.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) {
		t.Fatal("pending consumer ignored")
	}
	if err := owner.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) || releases.Load() != 0 {
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
	selected := selection(t, "original", func(context.Context) source.ReleaseResult { releases.Add(1); return complete(context.Background()) })
	donor := assemble(t, "donor", selected)
	delegated := source.Delegate("received", donor, selected)
	receiver := assemble(t, "receiver", delegated)
	if _, _, err := source.Bind(donor, selected); !errors.Is(err, source.ErrSelection) {
		t.Fatal("donor retained binding")
	}
	if err := donor.Close(context.Background()); err != nil || releases.Load() != 0 {
		t.Fatal("donor retained ownership")
	}
	_, info, err := source.Bind(receiver, delegated)
	if err != nil || info.Scope != "donor" || receiver.Snapshot().Sources[0].Ownership != source.Delegated {
		t.Fatal("transfer changed identity")
	}
	if err := receiver.Close(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatal("receiver did not release")
	}

	second := selection(t, "second", func(context.Context) source.ReleaseResult { return source.ReleaseResult{Continue: complete} })
	origin := assemble(t, "origin", second)
	failed := source.Select(prepared(t, "failure", "example.reader", ""), func(context.Context, settings) (source.Resource[reader], error) {
		return source.Resource[reader]{}, context.Canceled
	})
	target, err := source.Assemble(context.Background(), context.Background(), "target", source.Delegate("take", origin, second), failed)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, source.ErrIncomplete) || !target.Snapshot().Sources[0].Pending {
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
	marker := source.Select(prepared(t, "marker", "example.reader", ""), func(context.Context, settings) (source.Resource[reader], error) {
		calls.Add(1)
		return source.Resource[reader]{}, errors.New("must not run")
	})
	for _, selected := range []source.Selection[reader]{first, second} {
		target, err := source.Assemble(context.Background(), context.Background(), "target", marker, source.Delegate("move", donor, selected))
		if target != nil {
			_ = target.Close(context.Background())
		}
		if target != nil || !errors.Is(err, source.ErrSelection) || calls.Load() != 0 {
			t.Error("unsafe ordered transfer reached construction")
		}
	}
	single := selection(t, "single", complete)
	owner := assemble(t, "single-owner", single)
	defer owner.Close(context.Background())
	for _, overlapping := range []source.Spec{source.Delegate("again", owner, single), source.Borrow("also", owner, single)} {
		target, err := source.Assemble(context.Background(), context.Background(), "target", marker, source.Delegate("take", owner, single), overlapping)
		if target != nil {
			_ = target.Close(context.Background())
		}
		if target != nil || !errors.Is(err, source.ErrSelection) || calls.Load() != 0 {
			t.Error("known exclusive transfer conflict reached construction")
		}
	}
}

func TestCanceledInitializationUsesSeparateCleanupContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var releases atomic.Int32
	selected := source.Select(prepared(t, "one", "example.reader", ""), func(context.Context, settings) (source.Resource[reader], error) {
		cancel()
		return source.Resource[reader]{Acquired: true, Capability: readerFacade{}, Release: func(cleanup context.Context) source.ReleaseResult {
			if cleanup.Err() != nil {
				t.Error("cleanup inherited expired init context")
			}
			releases.Add(1)
			return complete(cleanup)
		}}, nil
	})
	assembly, err := source.Assemble(ctx, context.Background(), "cancel", selected)
	if !errors.Is(err, context.Canceled) || releases.Load() != 1 || assembly.Snapshot().Ready {
		t.Fatal("cancellation or cleanup lost")
	}
	assertNoPending(t, assembly)
}

func TestConcurrentCloseWaitCancellationAndNoDoubleRelease(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	selected := selection(t, "one", func(context.Context) source.ReleaseResult {
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
	if !errors.Is(err, context.Canceled) || !errors.Is(err, source.ErrIncomplete) {
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
	selected := source.Select(prepared(t, "nil", "example.reader", ""), func(context.Context, settings) (source.Resource[C], error) {
		return source.Resource[C]{Acquired: true, Capability: value, Release: func(context.Context) source.ReleaseResult {
			releases.Add(1)
			return complete(context.Background())
		}}, nil
	})
	assembly, err := source.Assemble(context.Background(), context.Background(), "nil", selected)
	if err == nil || !errors.Is(err, source.ErrInitialization) || assembly == nil || assembly.Snapshot().Ready {
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
	var zero source.Assembly
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := zero.Close(ctx); !errors.Is(err, source.ErrSelection) {
		t.Fatal("zero assembly did not reject before waiting")
	}
}

func TestQuiescenceReleaseAndErrorAreIndependent(t *testing.T) {
	var dependencyReleased atomic.Int32
	dependency := selection(t, "dependency", func(context.Context) source.ReleaseResult {
		dependencyReleased.Add(1)
		return complete(context.Background())
	})
	consumer := selection(t, "consumer", func(context.Context) source.ReleaseResult {
		return source.ReleaseResult{Released: true, Continue: func(context.Context) source.ReleaseResult {
			return source.ReleaseResult{Quiescent: true}
		}}
	})
	assembly := assemble(t, "independent-evidence", dependency, consumer)
	if err := assembly.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) {
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
	selected := source.Select(prepared(t, "partial", "example.reader", ""), func(context.Context, settings) (source.Resource[reader], error) {
		return source.Resource[reader]{Acquired: true, Release: func(context.Context) source.ReleaseResult {
			calls.Add(1)
			return complete(context.Background())
		}}, io.ErrUnexpectedEOF
	})
	cleanup, cancel := context.WithCancel(context.Background())
	cancel()
	assembly, err := source.Assemble(context.Background(), cleanup, "cleanup-budget", selected)
	if !errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(err, context.Canceled) ||
		!errors.Is(err, source.ErrIncomplete) || calls.Load() != 0 {
		t.Fatal("expired cleanup budget lost responsibility or primary cause")
	}
	if err := assembly.Close(context.Background()); err != nil || calls.Load() != 1 {
		t.Fatal("unattempted cleanup could not resume")
	}
}

func TestMissingCleanupCallbackRemainsVisible(t *testing.T) {
	selected := source.Select(prepared(t, "partial", "example.reader", ""), func(context.Context, settings) (source.Resource[reader], error) {
		return source.Resource[reader]{Acquired: true}, io.ErrUnexpectedEOF
	})
	assembly, err := source.Assemble(context.Background(), context.Background(), "invalid-provider", selected)
	if !errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(err, source.ErrIncomplete) ||
		!assembly.Snapshot().Sources[0].Pending || assembly.Snapshot().Sources[0].CanContinue {
		t.Fatal("invalid Provider cleanup contract advertised completion")
	}
}

func TestConcurrentBorrowAndOwnerShutdown(t *testing.T) {
	var releases atomic.Int32
	selected := selection(t, "shared", func(context.Context) source.ReleaseResult {
		releases.Add(1)
		return complete(context.Background())
	})
	owner := assemble(t, "owner", selected)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := 0; index < 32; index++ {
		group.Go(func() {
			<-start
			borrowed := source.Borrow("shared", owner, selected)
			scope, err := source.Assemble(context.Background(), context.Background(), "borrower", borrowed)
			if err != nil {
				if !errors.Is(err, source.ErrSelection) {
					t.Error(err)
				}
				if scope != nil {
					_ = scope.Close(context.Background())
				}
				return
			}
			capability, _, err := source.Bind(scope, borrowed)
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
	if err != nil && !errors.Is(err, source.ErrIncomplete) {
		t.Fatal(err)
	}
	group.Wait()
	if err := owner.Close(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatal("sharing race leaked or double-released the resource")
	}
}
