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
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/operation"
	"github.com/frost-leo/fathomry/source"
)

// These are deliberately local fixtures, not SDK/service compatibility tests.
// This capability owns its effect vocabulary; the shared mechanism does not
// translate an aggregate acknowledgement into success for every Item.
type writeEvidence struct {
	Owner           string
	Rows            int
	Committed       bool
	VisibilityKnown bool
	UnknownRows     int
}

func policy() source.Limits {
	return source.Limits{Active: 1, Queued: 2, Bytes: 1024, QueuedBytes: 2048, MaxLeases: 32}
}

func fixture(t testing.TB, limits source.Limits) (*source.Assembly, *source.Access, *atomic.Int32) {
	t.Helper()
	type settings struct {
		Fixed int `json:"fixed"`
	}
	prepared, err := source.Prepare(source.Schema[settings]{Format: 1},
		source.Input{Identity: source.Identity{Provider: "fixture.local", Name: "writes"}, Format: 1})
	if err != nil {
		t.Fatal(err)
	}
	releases := new(atomic.Int32)
	selected := source.WithLimits(source.Select(prepared, func(context.Context, settings) (source.Resource[struct{}], error) {
		return source.Resource[struct{}]{Acquired: true, Capability: struct{}{},
			Release: func(context.Context) source.ReleaseResult {
				releases.Add(1)
				return source.ReleaseResult{Quiescent: true, Released: true}
			}}, nil
	}), limits)
	assembly, err := source.Assemble(context.Background(), context.Background(), "fixture", selected)
	if err != nil {
		t.Fatal(err)
	}
	access, err := source.AccessFor(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = assembly.Close(context.Background()) })
	return assembly, access, releases
}

func request(id string, shape operation.Shape) operation.Request {
	return operation.Request{Name: "write", Shape: shape,
		Execution: failure.Execution{Call: id, Run: "run-A", Item: id, Owner: "account-A", Attempt: 2},
		Bytes:     16, EvidenceBytes: 128, Admission: operation.Budget{Limit: 5 * time.Second},
		AttemptsKnown: true, MaxAttempts: 3}
}

func inbox(t testing.TB, count int) *operation.Inbox[writeEvidence] {
	t.Helper()
	value, err := operation.NewInbox[writeEvidence](count, int64(count)*128)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func begin(t testing.TB, access *source.Access, input operation.Request, evidence *operation.Inbox[writeEvidence], observer *operation.Observer) *operation.Call[writeEvidence] {
	t.Helper()
	call, err := operation.Begin(context.Background(), access, input, evidence, observer)
	if err != nil {
		t.Fatal(err)
	}
	return call
}

func releaseDelivery(t testing.TB, evidence *operation.Inbox[writeEvidence]) operation.Result[writeEvidence] {
	t.Helper()
	delivery, err := evidence.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result, err := delivery.Receipt().WaitReleased(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal("duplicate evidence release", err)
	}
	return result
}

func TestConcurrentCallsKeepAssociationOutcomesAndBounds(t *testing.T) {
	limits := policy()
	limits.Active, limits.Queued = 4, 64
	assembly, access, releases := fixture(t, limits)
	evidence := inbox(t, 64)
	var inSDK, peak atomic.Int32
	primary := errors.New("private-primary")
	cleanup := errors.New("private-cleanup")
	var group sync.WaitGroup
	for index := range 64 {
		group.Go(func() {
			id := fmt.Sprintf("call-%d", index)
			input := request(id, operation.Finite)
			call, err := operation.Begin(context.Background(), access, input, evidence, nil)
			if err != nil {
				t.Error(err)
				return
			}
			input.Execution.Item = "mutated"
			if err := call.Execute(context.Background(), operation.Budget{Limit: time.Second}, func(ctx context.Context, scope operation.Scope) operation.Outcome[writeEvidence] {
				current := inSDK.Add(1)
				defer inSDK.Add(-1)
				for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
				}
				if _, err := call.Attempt(); err != nil {
					t.Error(err)
				}
				runtime.Gosched()
				if _, err := call.Attempt(); err != nil {
					t.Error(err)
				}
				return operation.Outcome[writeEvidence]{Value: writeEvidence{Owner: id, Rows: index, Committed: true, UnknownRows: 1},
					Present: true, Primary: primary, Cleanup: cleanup}
			}); err != nil {
				t.Error(err)
			}
			result, err := call.Receipt().WaitReleased(context.Background())
			if err != nil || !result.Final || !result.Released || result.Attribution.Execution.Item != id ||
				result.Outcome.Value.Owner != id || result.Outcome.Value.Rows != index ||
				result.Attempts != (operation.Attempts{Observed: 2, Exact: true}) {
				t.Error("association, result or attempt accounting crossed calls")
			}
			if !errors.Is(result.Outcome.Primary, primary) || errors.Is(result.Outcome.Primary, cleanup) ||
				!errors.Is(result.Outcome.Cleanup, cleanup) || !errors.Is(result.Err(), primary) || !errors.Is(result.Err(), cleanup) {
				t.Error("primary and cleanup evidence conflated")
			}
			if result.Source.Scope != "fixture" || result.Source.Configuration.Identity.Name != "writes" ||
				result.Attribution.Provider != "fixture.local" || result.Limits != limits {
				t.Error("actual source or policy attribution lost")
			}
			result.Source.Configuration.Provenance[0].Fields[0] = "changed"
			if again, _ := call.Receipt().Result(); again.Source.Configuration.Provenance[0].Fields[0] == "changed" {
				t.Error("metadata snapshot alias")
			}
			if call.Complete(operation.Outcome[writeEvidence]{}) {
				t.Error("duplicate completion accepted")
			}
		})
	}
	group.Wait()
	if peak.Load() > 4 || peak.Load() == 0 {
		t.Fatal("active ceiling not enforced")
	}
	if usage := assembly.Snapshot().Sources[0].Usage; usage != (source.Usage{}) {
		t.Fatalf("resource history retained: %+v", usage)
	}
	if evidence.Usage().Outstanding != 64 {
		t.Fatal("handled errors erased evidence")
	}
	seen := make(map[string]bool)
	for range 64 {
		result := releaseDelivery(t, evidence)
		id := result.Attribution.Execution.Call
		if seen[id] || result.Outcome.Value.Owner != id {
			t.Fatal("duplicate or misattributed evidence")
		}
		seen[id] = true
	}
	if evidence.Usage() != (operation.InboxUsage{}) {
		t.Fatal("evidence capacity leaked")
	}
	if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatal("resource cleanup failed")
	}
}

func TestFiniteTimeoutKeepsRealWorkAndExpiredQueueNeverExecutes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limits := policy()
		limits.MaxLeases = 1
		assembly, access, releases := fixture(t, limits)
		evidence := inbox(t, 3)
		call := begin(t, access, request("slow", operation.Finite), evidence, nil)
		started, finish, executed := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		native := errors.New("response arrived after wait timeout")
		go func() {
			executed <- call.Execute(context.Background(), operation.Budget{Limit: time.Second}, func(ctx context.Context, _ operation.Scope) operation.Outcome[writeEvidence] {
				close(started)
				<-finish
				if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
					t.Error("work budget absent")
				}
				return operation.Outcome[writeEvidence]{Present: true, Value: writeEvidence{UnknownRows: 4}, Primary: native}
			})
		}()
		<-started
		var sdkEntries atomic.Int32
		queued := make(chan error, 1)
		go func() {
			input := request("queued", operation.Finite)
			input.Admission.Limit = time.Second
			next, err := operation.Begin(context.Background(), access, input, evidence, nil)
			if err == nil {
				err = next.Execute(context.Background(), operation.Budget{Limit: time.Second}, func(context.Context, operation.Scope) operation.Outcome[writeEvidence] {
					sdkEntries.Add(1)
					return operation.Outcome[writeEvidence]{}
				})
			}
			queued <- err
		}()
		synctest.Wait()
		waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := call.Receipt().Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("wait did not end")
		}
		if err := <-queued; !errors.Is(err, context.DeadlineExceeded) || sdkEntries.Load() != 0 {
			t.Fatal("expired queue entered SDK")
		}
		if evidence.Usage().Outstanding != 1 {
			t.Fatal("rejected admission leaked evidence reservation")
		}
		result, ready := call.Receipt().Result()
		if ready || result.Released || result.Final {
			t.Fatal("wait timeout completed work")
		}
		if err := assembly.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) || releases.Load() != 0 {
			t.Fatal("live callback lost resource")
		}
		delivery, err := evidence.Next(context.Background())
		if err != nil || !errors.Is(delivery.Release(), operation.ErrPending) {
			t.Fatal("unresolved evidence released")
		}
		close(finish)
		if err := <-executed; err != nil {
			t.Fatal(err)
		}
		result, err = call.Receipt().WaitReleased(context.Background())
		if err != nil || !errors.Is(result.Err(), native) || errors.Is(result.Err(), context.DeadlineExceeded) || result.Outcome.Value.UnknownRows != 4 {
			t.Fatal("wait policy overwrote actual technical outcome")
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
		if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 {
			t.Fatal("finished callback still held resource")
		}
	})
}

func TestStreamPartialReadAndLateCleanupAtEveryLimitOfOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limits := policy()
		limits.Active, limits.MaxLeases, limits.Queued, limits.QueuedBytes = 1, 1, 0, 0
		assembly, access, releases := fixture(t, limits)
		evidence := inbox(t, 1)
		call := begin(t, access, request("stream", operation.Stream), evidence, nil)
		body, writer := io.Pipe()
		written := make(chan struct{})
		go func() {
			defer close(written)
			_, _ = io.WriteString(writer, "partial")
			_ = writer.CloseWithError(io.ErrUnexpectedEOF)
		}()
		data, err := io.ReadAll(body)
		<-written
		if string(data) != "partial" || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal("fixture did not produce partial stream")
		}
		firstCleanup := errors.New("initial cleanup uncertain")
		if !call.Resolve(operation.Outcome[writeEvidence]{Present: true, Primary: err, Cleanup: firstCleanup,
			Value: writeEvidence{Owner: "stream", Rows: len(data), UnknownRows: 2}}) {
			t.Fatal("early result rejected")
		}
		early, err := call.Receipt().Wait(context.Background())
		if err != nil || early.Final || early.Released || early.Observation != operation.ObservationPending {
			t.Fatal("early result implied completed cleanup")
		}
		delivery, err := evidence.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !errors.Is(delivery.Release(), operation.ErrPending) {
			t.Fatal("pending cleanup evidence acknowledged")
		}
		if _, err := operation.Begin(context.Background(), access, request("blocked", operation.Finite), evidence, nil); !errors.Is(err, operation.ErrEvidence) {
			t.Fatal("full evidence not rejected")
		}
		if err := assembly.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) || releases.Load() != 0 {
			t.Fatal("stream cleanup lost owner")
		}
		cleanupCtx, cancel, err := (operation.Budget{Limit: time.Second}).Context(context.Background(), operation.Cleanup)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()
		if cleanupCtx.Err() != nil {
			t.Fatal("cleanup inherited expired wait")
		}
		if _, err := call.Attempt(); err != nil {
			t.Fatal("cleanup could not account for SDK attempt")
		}
		_ = body.Close()
		finalCleanup := errors.New("remote abort acknowledgement missing")
		if !call.Finish(finalCleanup) || call.Finish(errors.New("duplicate")) {
			t.Fatal("cleanup completion not one-shot")
		}
		call.Release()
		result, err := delivery.Receipt().WaitReleased(context.Background())
		if err != nil || !result.Final || !result.Released || !errors.Is(result.Outcome.Primary, io.ErrUnexpectedEOF) ||
			!errors.Is(result.Outcome.Cleanup, firstCleanup) || !errors.Is(result.Outcome.Cleanup, finalCleanup) ||
			result.Outcome.Value.Rows != 7 || result.Outcome.Value.UnknownRows != 2 {
			t.Fatal("primary/partial/uncertain/late-cleanup evidence lost")
		}
		if errors.Is(early.Outcome.Cleanup, finalCleanup) || early.Final {
			t.Fatal("old result projection changed")
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
		if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 {
			t.Fatal("stream resource leaked")
		}
	})
}

func TestAsyncCallbackBeforeSubmitReturnAndDuplicateCompletion(t *testing.T) {
	assembly, access, releases := fixture(t, policy())
	evidence := inbox(t, 1)
	input := request("async", operation.Async)
	input.AttemptsKnown, input.MaxAttempts = false, 0
	call := begin(t, access, input, evidence, nil)
	submitting, err := call.Scope().Hold()
	if err != nil {
		t.Fatal(err)
	}
	if !call.Complete(operation.Outcome[writeEvidence]{Present: true, Value: writeEvidence{Owner: "async", Rows: 3, Committed: true}}) {
		t.Fatal("callback rejected")
	}
	result, err := call.Receipt().Wait(context.Background())
	if err != nil || result.Released || !result.Final || result.Attempts.Exact || result.Attempts.Observed != 0 {
		t.Fatal("callback confused with submit return or invented SDK attempts")
	}
	if err := assembly.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) || releases.Load() != 0 {
		t.Fatal("submit stack lost resource")
	}
	var group sync.WaitGroup
	for range 32 {
		group.Go(func() {
			if call.Complete(operation.Outcome[writeEvidence]{Primary: context.Canceled}) {
				t.Error("duplicate callback accepted")
			}
			_, _ = call.Receipt().Result()
		})
	}
	group.Wait()
	submitting.End()
	submitting.End()
	result = releaseDelivery(t, evidence)
	if result.Err() != nil || result.Outcome.Value.Rows != 3 || !result.Released {
		t.Fatal("duplicate erased callback evidence")
	}
	if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatal("async release not exact once")
	}
}

func TestAsyncWaitCancellationDoesNotCancelDeliveryBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		assembly, access, releases := fixture(t, policy())
		evidence := inbox(t, 1)
		call := begin(t, access, request("receipt", operation.Async), evidence, nil)
		deliveryCtx, cancel, err := (operation.Budget{Limit: 5 * time.Second}).Context(context.Background(), operation.Delivery)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()
		completed := make(chan struct{})
		go func() {
			<-deliveryCtx.Done()
			call.Complete(operation.Outcome[writeEvidence]{Primary: deliveryCtx.Err(), Value: writeEvidence{UnknownRows: 1}, Present: true})
			close(completed)
		}()
		waitCtx, endWait := context.WithTimeout(context.Background(), time.Second)
		defer endWait()
		if _, err := call.Receipt().Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("caller wait budget absent")
		}
		if deliveryCtx.Err() != nil {
			t.Fatal("wait cancelled independent delivery")
		}
		if err := assembly.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) || releases.Load() != 0 {
			t.Fatal("unresolved receipt lost ownership")
		}
		<-completed
		result := releaseDelivery(t, evidence)
		if !errors.Is(result.Outcome.Primary, context.DeadlineExceeded) || result.Outcome.Value.UnknownRows != 1 {
			t.Fatal("delivery unknown result lost")
		}
		if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 {
			t.Fatal("delivery owner leaked")
		}
	})
}

func TestSessionBudgetsNestedMessagesAndBoundedEvidence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		assembly, access, releases := fixture(t, policy())
		evidence := inbox(t, 2)
		parent := begin(t, access, request("subscription", operation.Session), evidence, nil)
		sessionDelivery, err := evidence.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		startCtx, stopStart, err := (operation.Budget{Limit: time.Second}).Context(context.Background(), operation.Establish)
		if err != nil || startCtx.Err() != nil {
			t.Fatal("establishment budget failed")
		}
		ownerCtx, stopOwner := context.WithCancel(context.Background())
		lifetime, stopLifetime, err := (operation.Budget{}).Context(ownerCtx, operation.Lifetime)
		if err != nil {
			t.Fatal(err)
		}
		defer stopLifetime()
		defer stopOwner()
		workerDone := make(chan struct{})
		go func() { <-lifetime.Done(); close(workerDone) }()
		stopStart()
		time.Sleep(2 * time.Second)
		if lifetime.Err() != nil {
			t.Fatal("startup deadline became session lifetime")
		}
		var handled int
		for index := range 128 {
			input := request(fmt.Sprintf("message-%d", index), operation.Finite)
			input.Bytes, input.Execution.Parent = 0, "subscription"
			child, err := operation.BeginNested(context.Background(), parent.Scope(), input, evidence, nil)
			if err != nil {
				t.Fatal("nested operation reacquired source quota", err)
			}
			if err := child.Execute(lifetime, operation.Budget{Limit: time.Second}, func(ctx context.Context, _ operation.Scope) operation.Outcome[writeEvidence] {
				handled++
				if _, err := child.Attempt(); err != nil {
					t.Fatal(err)
				}
				value := writeEvidence{Owner: input.Execution.Item, Rows: 1, Committed: true}
				if index == 64 {
					messageCtx, cancel, err := (operation.Budget{Limit: time.Millisecond}).Context(ctx, operation.Message)
					if err != nil {
						t.Fatal(err)
					}
					defer cancel()
					<-messageCtx.Done()
					value.Committed, value.UnknownRows = false, 1
					return operation.Outcome[writeEvidence]{Present: true, Value: value, Primary: messageCtx.Err()}
				}
				return operation.Outcome[writeEvidence]{Present: true, Value: value}
			}); err != nil {
				t.Fatal(err)
			}
			if evidence.Usage().Outstanding != 2 || assembly.Snapshot().Sources[0].Usage.Active != 1 {
				t.Fatal("session double-counted resources or retained unbounded history")
			}
			if _, err := operation.BeginNested(lifetime, parent.Scope(), input, evidence, nil); !errors.Is(err, operation.ErrEvidence) {
				t.Fatal("saturated nested evidence blocked or bypassed bound")
			}
			result := releaseDelivery(t, evidence)
			if result.Attribution.Execution.Parent != "subscription" || result.Outcome.Value.Owner != input.Execution.Item {
				t.Fatal("message attribution lost")
			}
			if index == 64 && (!errors.Is(result.Err(), context.DeadlineExceeded) || result.Outcome.Value.UnknownRows != 1) {
				t.Fatal("message failure affected wrong lifetime")
			}
		}
		if handled != 128 || evidence.Usage().Outstanding != 1 || lifetime.Err() != nil {
			t.Fatal("message/session lifetime confused")
		}
		if err := assembly.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) || releases.Load() != 0 {
			t.Fatal("live session closed")
		}
		parent.Resolve(operation.Outcome[writeEvidence]{Present: true, Value: writeEvidence{Rows: handled}, Primary: context.Canceled})
		stopOwner()
		<-workerDone
		cleanup := errors.New("session cleanup warning after local termination")
		if !parent.Finish(cleanup) {
			t.Fatal("session cleanup failed")
		}
		parent.Release()
		result, err := sessionDelivery.Receipt().WaitReleased(context.Background())
		if err != nil || !errors.Is(result.Outcome.Cleanup, cleanup) || result.Outcome.Value.Rows != 128 {
			t.Fatal("session terminal evidence lost")
		}
		if err := sessionDelivery.Release(); err != nil {
			t.Fatal(err)
		}
		if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 {
			t.Fatal("session still owns resource")
		}
	})
}

func TestFiniteExecuteOwnsCompletionAndPreservesReturnedFacts(t *testing.T) {
	_, access, _ := fixture(t, policy())
	evidence := inbox(t, 1)
	call := begin(t, access, request("exclusive", operation.Finite), evidence, nil)
	primary, cleanup := errors.New("final primary"), errors.New("late cleanup")
	entered, proceed, executed := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		executed <- call.Execute(context.Background(), operation.Budget{Limit: time.Second}, func(context.Context, operation.Scope) operation.Outcome[writeEvidence] {
			if call.Resolve(operation.Outcome[writeEvidence]{Present: true}) || call.Finish(nil) {
				t.Error("reentrant completion authority accepted")
			}
			close(entered)
			<-proceed
			return operation.Outcome[writeEvidence]{Primary: primary, Cleanup: cleanup, Present: true, Value: writeEvidence{UnknownRows: 1}}
		})
	}()
	<-entered
	if call.Complete(operation.Outcome[writeEvidence]{Present: true}) || call.Resolve(operation.Outcome[writeEvidence]{}) {
		t.Error("concurrent completion discarded finite callback facts")
	}
	close(proceed)
	if err := <-executed; err != nil {
		t.Fatal(err)
	}
	result := releaseDelivery(t, evidence)
	if !result.Final || !result.Released || !errors.Is(result.Outcome.Primary, primary) ||
		!errors.Is(result.Outcome.Cleanup, cleanup) || result.Outcome.Value.UnknownRows != 1 {
		t.Fatal("finite completion lost errors or leaked ownership")
	}
}

type returnedReader struct {
	read  func([]byte) (int, error)
	close func() error
}

func (reader returnedReader) Read(buffer []byte) (int, error) { return reader.read(buffer) }
func (reader returnedReader) Close() error                    { return reader.close() }

func TestReturnedStreamKeepsOwnershipWhileCallerWaitEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		assembly, access, releases := fixture(t, policy())
		evidence := inbox(t, 1)
		body, writer := io.Pipe()
		consumed, written := make(chan struct{}), make(chan struct{})
		cleanup := errors.New("stream cleanup warning")
		var call *operation.Call[writeEvidence]
		var consuming *operation.Guard
		start := func() (io.ReadCloser, *operation.Receipt[writeEvidence]) {
			call = begin(t, access, request("returned", operation.Stream), evidence, nil)
			var err error
			consuming, err = call.Scope().Hold()
			if err != nil {
				t.Fatal(err)
			}
			var once sync.Once
			return returnedReader{read: body.Read, close: func() error {
				once.Do(func() {
					_ = body.Close()
					<-consumed
					call.Finish(cleanup)
					call.Release()
				})
				return cleanup
			}}, call.Receipt()
		}
		handle, receipt := start()
		go func() {
			data, err := io.ReadAll(handle)
			call.Resolve(operation.Outcome[writeEvidence]{Present: true, Primary: err, Value: writeEvidence{Rows: len(data), UnknownRows: 1}})
			consuming.End()
			close(consumed)
		}()
		go func() {
			_, _ = io.WriteString(writer, "abc")
			close(written)
		}()
		<-written
		synctest.Wait()
		wait, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := receipt.Wait(wait); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("still-live returned reader falsely completed")
		}
		if err := assembly.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) || releases.Load() != 0 {
			t.Fatal("initiating return/caller timeout released live stream")
		}
		if !errors.Is(handle.Close(), cleanup) || !errors.Is(handle.Close(), cleanup) {
			t.Fatal("returned cleanup not idempotent")
		}
		_ = writer.Close()
		result := releaseDelivery(t, evidence)
		if result.Outcome.Value.Rows != 3 || result.Outcome.Value.UnknownRows != 1 ||
			!errors.Is(result.Outcome.Primary, io.ErrClosedPipe) || !errors.Is(result.Outcome.Cleanup, cleanup) {
			t.Fatal("returned stream partial or cleanup facts lost")
		}
		if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 {
			t.Fatal("returned stream obligation not discharged")
		}
	})
}

func TestBoundedMixedWorkload(t *testing.T) {
	limits := policy()
	limits.Active, limits.Bytes = 4, 64
	assembly, access, releases := fixture(t, limits)
	evidence := inbox(t, 4)
	observer, err := operation.NewObserver(2)
	if err != nil {
		t.Fatal(err)
	}
	primary, cleanup := errors.New("injected technical failure"), errors.New("injected cleanup failure")
	const batches = 256
	for batch := range batches {
		calls := make([]*operation.Call[writeEvidence], 4)
		for index := range calls {
			id := fmt.Sprintf("batch-%d-call-%d", batch, index)
			calls[index] = begin(t, access, request(id, operation.Shape(index+1)), evidence, observer)
		}
		if usage := assembly.Snapshot().Sources[0].Usage; usage.Active != 4 || usage.ActiveBytes != 64 ||
			evidence.Usage() != (operation.InboxUsage{Outstanding: 4, ReservedBytes: 512}) {
			t.Fatal("declared workload bounds not held")
		}
		start := make(chan struct{})
		var workers sync.WaitGroup
		for index, call := range calls {
			workers.Go(func() {
				<-start
				value := operation.Outcome[writeEvidence]{Present: true,
					Value: writeEvidence{Owner: fmt.Sprintf("batch-%d-call-%d", batch, index), Rows: 1, UnknownRows: 1}, Primary: primary}
				switch index {
				case 0:
					if err := call.Execute(context.Background(), operation.Budget{Limit: time.Second}, func(context.Context, operation.Scope) operation.Outcome[writeEvidence] {
						value.Cleanup = cleanup
						return value
					}); err != nil {
						t.Error(err)
					}
				case 1:
					call.Resolve(value)
					call.Finish(cleanup)
					call.Release()
				case 2:
					guard, err := call.Scope().Hold()
					if err != nil {
						t.Error(err)
						return
					}
					value.Cleanup = cleanup
					call.Complete(value)
					guard.End()
				case 3:
					lifetimeOwner, stop := context.WithCancel(context.Background())
					defer stop()
					lifetime, end, err := (operation.Budget{}).Context(lifetimeOwner, operation.Lifetime)
					if err != nil {
						t.Error(err)
						return
					}
					stop()
					<-lifetime.Done()
					end()
					call.Resolve(value)
					call.Finish(cleanup)
					call.Release()
				}
			})
		}
		close(start)
		seen := make(map[string]bool)
		for range 4 {
			result := releaseDelivery(t, evidence)
			id := result.Attribution.Execution.Call
			if seen[id] || result.Outcome.Value.Owner != id || !result.Final || !result.Released ||
				!errors.Is(result.Err(), primary) || !errors.Is(result.Outcome.Cleanup, cleanup) {
				t.Fatal("mixed workload lost/duplicated evidence")
			}
			seen[id] = true
		}
		workers.Wait()
		if assembly.Snapshot().Sources[0].Usage != (source.Usage{}) || evidence.Usage() != (operation.InboxUsage{}) {
			t.Fatal("batch retained work, bytes or completed history")
		}
	}
	if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatal("bounded workload did not terminate")
	}
	t.Logf("%d logical calls; active/working bytes high-water=4/64; evidence count/bytes=4/512; zero outstanding work after every batch", batches*4)
}

func TestCleanupFailurePublishedWithoutReleasingUnconfirmedUse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limits := policy()
		limits.Active, limits.MaxLeases, limits.Queued, limits.QueuedBytes = 1, 1, 0, 0
		assembly, access, releases := fixture(t, limits)
		evidence := inbox(t, 1)
		call := begin(t, access, request("unconfirmed-cleanup", operation.Async), evidence, nil)
		primary, cleanup := errors.New("partial delivery"), errors.New("shutdown did not confirm worker exit")
		if !call.Resolve(operation.Outcome[writeEvidence]{Primary: primary, Present: true, Value: writeEvidence{UnknownRows: 1}}) {
			t.Fatal("main outcome not published")
		}
		if call.Release() {
			t.Fatal("release before final evidence accepted")
		}
		delivery, err := evidence.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := delivery.Receipt().WaitFinal(canceled); !errors.Is(err, context.Canceled) {
			t.Fatal("unfinished cleanup became a final report")
		}
		stillUsing, requestExit := make(chan struct{}), make(chan struct{})
		go func() { <-requestExit; close(stillUsing) }()
		if !call.Finish(cleanup) {
			t.Fatal("cleanup failure rejected")
		}
		result, err := delivery.Receipt().WaitFinal(context.Background())
		if err != nil || !result.Final || result.Released || !errors.Is(result.Outcome.Primary, primary) ||
			!errors.Is(result.Outcome.Cleanup, cleanup) {
			t.Fatal("cleanup failure cannot coexist with pending ownership")
		}
		if !errors.Is(delivery.Release(), operation.ErrPending) || !errors.Is(assembly.Close(context.Background()), source.ErrIncomplete) ||
			releases.Load() != 0 || assembly.Snapshot().Sources[0].Usage.Active != 1 {
			t.Fatal("reporting cleanup failure released real use")
		}
		close(requestExit)
		<-stillUsing
		if !call.Release() || call.Release() {
			t.Fatal("positive confirmation not exact-once")
		}
		result, err = delivery.Receipt().WaitReleased(context.Background())
		if err != nil || !result.Released || !errors.Is(result.Outcome.Cleanup, cleanup) {
			t.Fatal("late release erased cleanup failure")
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
		if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 {
			t.Fatal("confirmed resource not released")
		}
	})
}
