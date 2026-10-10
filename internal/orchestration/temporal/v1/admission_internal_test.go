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

package temporal

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	adapters "github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	sdktemporal "go.temporal.io/sdk/temporal"
	nativeworker "go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type bridgeScopeKey struct{}

func bridgeFixture(t testing.TB, capacity int) (*Executions, *resource.Assembly, resource.Selection[Source]) {
	t.Helper()
	owner := &connection{settings: settings{Namespace: "bridge", MaxActive: 1, MaxRequestBytes: 1024, MaxResponseBytes: 1024,
		AdmissionTimeout: time.Second, RPCTimeout: time.Second, ConnectTimeout: time.Second,
		RPCs: []string{workflowServicePrefix + "GetSystemInfo"}}}
	prepared, err := resource.Prepare(resource.Schema[struct{}]{Format: 1}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: "bridge"}, Format: 1})
	if err != nil {
		t.Fatal(err)
	}
	selected := resource.WithLimits(resource.Select(prepared, func(context.Context, struct{}) (resource.Resource[Source], error) {
		return resource.Resource[Source]{Acquired: true, Capability: Source{owner: owner}, Release: func(context.Context) resource.ReleaseResult {
			return resource.ReleaseResult{Quiescent: true, Released: true}
		}}, nil
	}), resource.Limits{Active: 1, Bytes: 16 * owner.settings.reservation(), MaxLeases: 16})
	assembly, err := resource.Assemble(context.Background(), context.Background(), "bridge", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assembly.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	inbox, err := invocation.NewInbox[Execution](capacity, int64(capacity)*ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	client, err := BindExecutions(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	return client, assembly, selected
}

func bridgeRuntime(t testing.TB) *adapters.Runtime {
	t.Helper()
	runtime, err := adapters.New(context.Background(), adapters.Options{MaxActive: 1, MaxWorkBytes: 1 << 20, MaxTasks: 16, MaxDepth: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return runtime
}

func bridgeEndpoint[T any](t testing.TB, runtime *adapters.Runtime, capacity int) (Admission[T], *adapters.Inbox[T]) {
	t.Helper()
	inbox, err := adapters.NewInbox[T](adapters.EvidenceOptions{Capacity: capacity, MaxBytes: int64(capacity) * ExecutionEvidenceBytes})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := adapters.Bind(runtime, adapters.Declaration[T]{Evidence: inbox, Copy: func(value T) T { return value }})
	if err != nil {
		t.Fatal(err)
	}
	return func(ctx, lifetime, parent context.Context, request invocation.Request) (context.Context, func(*invocation.DeliveryRecord[T], error), error) {
		var call *adapters.Call[T]
		var guard adapters.Guard
		var held error
		producer := func(value *adapters.Call[T]) { call = value; guard, held = call.Hold() }
		input := adapters.Request{Operation: request.Name, ID: request.Correlation.Call, WorkBytes: request.Bytes, EvidenceBytes: request.EvidenceBytes}
		var receipt *adapters.Receipt[T]
		var err error
		if parent == nil {
			receipt, err = endpoint.RunWithLifetime(ctx, lifetime, input, producer)
		} else {
			scope, ok := parent.Value(bridgeScopeKey{}).(adapters.Scope)
			if !ok {
				return nil, nil, errors.New("test bridge missing parent")
			}
			receipt, err = endpoint.ChildWithLifetime(ctx, lifetime, scope, input, producer)
		}
		if err != nil {
			return nil, nil, err
		}
		if call == nil {
			value, _ := receipt.Snapshot()
			return nil, nil, value.Err()
		}
		if held != nil {
			return nil, nil, held
		}
		return context.WithValue(call.Context(), bridgeScopeKey{}, call.Scope()), func(record *invocation.DeliveryRecord[T], setup error) {
			defer func() {
				if err := guard.Release(); err != nil {
					t.Error(err)
				}
			}()
			outcome := adapters.Outcome[T]{Primary: setup}
			if record != nil {
				result, err := record.Receipt().WaitReleased(context.Background())
				if err != nil {
					t.Error(err)
				}
				outcome = adapters.Outcome[T]{Value: result.Outcome.Value, Present: result.Outcome.Present, Primary: result.Outcome.Primary, Cleanup: result.Outcome.Cleanup}
			}
			if err := call.Resolve(outcome); err != nil {
				t.Error(err)
			}
			if record != nil {
				if err := record.Release(); err != nil {
					t.Error(err)
				}
			}
		}, nil
	}, inbox
}

func bridgeReceive[T any](t testing.TB, inbox *adapters.Inbox[T]) adapters.Snapshot[T] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	delivery, err := inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	value, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestAdmissionBridgeRefusesPublicAndInnerSaturationBeforeNativeEntry(t *testing.T) {
	client, _, _ := bridgeFixture(t, 1)
	runtime := bridgeRuntime(t)
	gate, records := bridgeEndpoint[Execution](t, runtime, 1)
	client = WithAdmissions(client, gate, nil, nil)
	var calls int
	run := func(context.Context, *Execution) (struct{}, error) { calls++; return struct{}{}, nil }
	invoke := func(id string) error {
		_, err := executeNative(context.Background(), client, fault.Correlation{Call: id}, Execution{Operation: "bridge.native"}, run)
		return err
	}
	if err := invoke("first"); err != nil || calls != 1 || client.inbox.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("positive control lost native work or exact custody", err)
	}
	if err := invoke("public-full"); !errors.Is(err, adapters.ErrEvidence) || calls != 1 {
		t.Fatal("public saturation reached native entry", err)
	}
	if value := bridgeReceive(t, records); value.Err() != nil {
		t.Fatal(value.Err())
	}
	reserved, err := invocation.Begin(context.Background(), client.access, invocation.Request{Name: "bridge.held", Shape: invocation.Finite,
		Correlation: fault.Correlation{Call: "held"}, Bytes: client.owner.settings.reservation(), EvidenceBytes: ExecutionEvidenceBytes,
		Admission: invocation.Budget{Limit: time.Second}}, client.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	reserved.Complete(invocation.Outcome[Execution]{Present: true})
	if err := invoke("inner-full"); !errors.Is(err, invocation.ErrEvidence) || calls != 1 {
		t.Fatal("inner saturation reached native entry", err)
	}
	if value := bridgeReceive(t, records); !errors.Is(value.Err(), invocation.ErrEvidence) {
		t.Fatal("failed inner admission lost its already-reserved public result", value.Err())
	}
	stored, err := client.inbox.Next(context.Background())
	if err != nil || stored.Release() != nil {
		t.Fatal("bridge stole another internal record", err)
	}
	if err := invoke("after-release"); err != nil || calls != 2 {
		t.Fatal("explicit record release did not restore admission", err)
	}
	bridgeReceive(t, records)
}

func TestAdmissionBridgeNestedCallbackHighLevelRawAndCleanupNeverReacquireRoot(t *testing.T) {
	client, _, _ := bridgeFixture(t, 2)
	runtime := bridgeRuntime(t)
	executionGate, executionRecords := bridgeEndpoint[Execution](t, runtime, 2)
	workerGate, workerRecords := bridgeEndpoint[WorkerResult](t, runtime, 1)
	taskGate, taskRecords := bridgeEndpoint[TaskResult](t, runtime, 1)
	client = WithAdmissions(client, executionGate, workerGate, taskGate)
	workers, _ := invocation.NewInbox[WorkerResult](1, ExecutionEvidenceBytes)
	tasks, _ := invocation.NewInbox[TaskResult](1, ExecutionEvidenceBytes)
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := invocation.Request{Name: "worker.run", Shape: invocation.Session, Bytes: client.owner.settings.reservation(),
		EvidenceBytes: ExecutionEvidenceBytes, Correlation: fault.Correlation{Call: "worker"}, Admission: invocation.Budget{Limit: time.Second}}
	workerCall, admitted, finishWorker, err := beginAdmitted(context.Background(), lifetime, nil, request, client.access, nil, workers, nil, client.admissions.worker)
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{client: client, call: workerCall, tasks: tasks, handlers: make(chan struct{}, 1),
		correlation: request.Correlation, admissionContext: admitted}
	task, binding, err := worker.beginTask(context.Background(), TaskResult{Kind: "activity"})
	if err != nil {
		t.Fatal("task reentered a saturated root", err)
	}
	callback := &callbackClient{binding: binding}
	var nativeEntries int
	_, err = callbackNative(context.Background(), callback, "", Execution{Operation: "callback.high"}, func(context.Context, *Execution) (struct{}, error) {
		nativeEntries++
		return struct{}{}, activity.ErrResultPending
	})
	if err != activity.ErrResultPending {
		t.Fatal("native callback sentinel was wrapped or root admission repeated", err)
	}
	method := workflowServicePrefix + "GetSystemInfo"
	err = binding.invoke(context.Background(), client.owner, method, &workflowservice.GetSystemInfoRequest{}, &workflowservice.GetSystemInfoResponse{}, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			nativeEntries++
			return nil
		})
	if err != nil || nativeEntries != 2 {
		t.Fatal("raw callback failed to share its task family", err)
	}
	_, err = callbackNative(context.Background(), callback, "", Execution{Operation: "callback.full"}, func(context.Context, *Execution) (struct{}, error) {
		nativeEntries++
		return struct{}{}, nil
	})
	if !errors.Is(err, adapters.ErrEvidence) || nativeEntries != 2 {
		t.Fatal("callback bypassed public evidence saturation", err)
	}
	binding.closed.Store(true)
	task.Complete(invocation.Outcome[TaskResult]{Present: true, Value: TaskResult{HandlerReturned: true, AsyncCompletion: true}})
	binding.finish()
	<-worker.handlers
	_, err = callbackNative(context.Background(), callback, "", Execution{Operation: "callback.expired"}, func(context.Context, *Execution) (struct{}, error) {
		nativeEntries++
		return struct{}{}, nil
	})
	if !errors.Is(err, ErrAuthority) || nativeEntries != 2 {
		t.Fatal("async callback return kept client authority live", err)
	}
	workerCall.Complete(invocation.Outcome[WorkerResult]{Present: true, Value: WorkerResult{Joined: true}})
	finishWorker()
	if status, err := runtime.Inspect(); err != nil || status.Active != 0 || status.Queued != 0 || status.Accepted != 4 {
		t.Fatal("full evidence storage blocked cleanup or duplicated roots", status, err)
	}
	if client.inbox.Usage() != (invocation.InboxUsage{}) || workers.Usage() != (invocation.InboxUsage{}) || tasks.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("exact inner custody was not released")
	}
	first := bridgeReceive(t, executionRecords)
	value, _ := first.ValueCopy()
	if cause, captured := value.NativeCause(); !captured || cause != activity.ErrResultPending {
		t.Fatal("native semantic capture was not separate from attributed error", first.Err())
	}
	bridgeReceive(t, executionRecords)
	bridgeReceive(t, taskRecords)
	bridgeReceive(t, workerRecords)
}

type bridgeDecoder struct {
	converter.DataConverter
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (decoder *bridgeDecoder) FromPayloads(payloads *commonpb.Payloads, values ...any) error {
	decoder.calls.Add(1)
	if decoder.entered != nil {
		close(decoder.entered)
		<-decoder.release
	}
	return decoder.DataConverter.FromPayloads(payloads, values...)
}

func TestAdmissionBridgeNativeErrorAndDelayedDecodeKeepExactScope(t *testing.T) {
	client, assembly, _ := bridgeFixture(t, 1)
	runtime := bridgeRuntime(t)
	gate, records := bridgeEndpoint[Execution](t, runtime, 1)
	client = WithAdmissions(client, gate, nil, nil)
	decoder := &bridgeDecoder{DataConverter: converter.GetDefaultDataConverter()}
	payloads, err := decoder.ToPayloads("private details")
	if err != nil {
		t.Fatal(err)
	}
	wire := &failurepb.Failure{Message: "private native message", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
		ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "native-kind", NonRetryable: true, Details: payloads}}}
	failures := sdktemporal.NewDefaultFailureConverter(sdktemporal.DefaultFailureConverterOptions{DataConverter: decoder})
	original := failures.FailureToError(wire)
	_, err = executeNative(context.Background(), client, fault.Correlation{Call: "failure"}, Execution{Operation: "bridge.failure"},
		func(context.Context, *Execution) (struct{}, error) { return struct{}{}, original })
	native, captured := NativeError(err)
	if !captured || native == nil || strings.Contains(fmt.Sprintf("%+v", err), "private") || !proto.Equal(failures.ErrorToFailure(native), wire) {
		t.Fatal("safe frame lost exact native conversion or leaked presentation")
	}
	if _, ok := NativeError(fmt.Errorf("outer: %w", err)); ok {
		t.Fatal("semantic accessor traversed an arbitrary wrapper")
	}
	if _, ok := NativeError(errors.Join(err, errors.New("other"))); ok {
		t.Fatal("semantic accessor selected an arbitrary joined cause")
	}
	application, ok := native.(*sdktemporal.ApplicationError)
	if !ok {
		t.Fatal("native error shape changed")
	}
	var decoded string
	if err := application.Details(&decoded); !errors.Is(err, adapters.ErrEvidence) || decoder.calls.Load() != 0 {
		t.Fatal("delayed decode bypassed full public evidence", err)
	}
	bridgeReceive(t, records)
	if err := application.Details(&decoded); err != nil || decoded != "private details" || decoder.calls.Load() != 1 {
		t.Fatal("admitted native decoder changed behavior", err)
	}
	bridgeReceive(t, records)
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := application.Details(&decoded); err == nil || decoder.calls.Load() != 1 {
		t.Fatal("late decoder escaped retained Access revocation", err)
	}
	bridgeReceive(t, records)
}

func TestAdmissionBridgeRetainsBlockedDecodeThroughCanceledWaitAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, assembly, _ := bridgeFixture(t, 1)
		runtime := bridgeRuntime(t)
		gate, records := bridgeEndpoint[Execution](t, runtime, 1)
		client = WithAdmissions(client, gate, nil, nil)
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			_, err := executeNative(ctx, client, fault.Correlation{Call: "decode"}, Execution{Operation: "bridge.decode"}, func(context.Context, *Execution) (struct{}, error) {
				close(entered)
				<-release
				return struct{}{}, nil
			})
			done <- err
		}()
		<-entered
		cancel()
		synctest.Wait()
		if err := assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
			t.Fatal("canceled waiting falsely released actual native decode", err)
		}
		if status, _ := runtime.Inspect(); status.Active != 1 || client.inbox.Usage().Outstanding != 1 {
			t.Fatal("blocked native work lost either owner")
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatal("actual result was replaced by waiting cancellation", err)
		}
		bridgeReceive(t, records)
		if err := assembly.Close(context.Background()); err != nil {
			t.Fatal("repeat close did not observe actual completion", err)
		}
	})
}

func TestAdmissionBridgeAbruptNativeExitDrainsExactClaim(t *testing.T) {
	for _, abrupt := range []string{"panic", "goexit"} {
		t.Run(abrupt, func(t *testing.T) {
			client, _, _ := bridgeFixture(t, 1)
			runtimeOwner := bridgeRuntime(t)
			gate, records := bridgeEndpoint[Execution](t, runtimeOwner, 1)
			client = WithAdmissions(client, gate, nil, nil)
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { _ = recover() }()
				_, _ = executeNative(context.Background(), client, fault.Correlation{Call: abrupt}, Execution{Operation: "bridge.abrupt"}, func(context.Context, *Execution) (struct{}, error) {
					if abrupt == "goexit" {
						runtime.Goexit()
					}
					panic("private panic")
				})
			}()
			<-done
			if client.inbox.Usage() != (invocation.InboxUsage{}) {
				t.Fatal("abrupt exit leaked exact claimed record")
			}
			if value := bridgeReceive(t, records); value.Err() == nil || strings.Contains(fmt.Sprint(value.Err()), "private") {
				t.Fatal("abrupt exit lost safe final evidence", value.Err())
			}
		})
	}
}

func TestAdmissionBridgeDirectRawServiceRetainsExactTransportError(t *testing.T) {
	executions, assembly, selected := bridgeFixture(t, 1)
	inner, _ := invocation.NewInbox[RPCResult](1, 1024)
	client, err := Bind(assembly, selected, inner, nil)
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	nativeFailure := errors.New("private transport failure")
	transport, err := grpc.NewClient("passthrough:///bridge.invalid", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(context.Context, string, any, any, *grpc.ClientConn, grpc.UnaryInvoker, ...grpc.CallOption) error {
			calls++
			return nativeFailure
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	executions.owner.transport = transport
	runtime := bridgeRuntime(t)
	gate, records := bridgeEndpoint[RPCResult](t, runtime, 1)
	client = WithRPCAdmission(client, gate)
	_, err = client.WorkflowService(fault.Correlation{Call: "raw"}).GetSystemInfo(context.Background(), &workflowservice.GetSystemInfoRequest{})
	if native, captured := NativeError(err); !captured || native != nativeFailure || calls != 1 || strings.Contains(fmt.Sprint(err), "private") {
		t.Fatal("raw transport native error was replaced or leaked", err)
	}
	_, err = client.WorkflowService(fault.Correlation{Call: "raw-full"}).GetSystemInfo(context.Background(), &workflowservice.GetSystemInfoRequest{})
	if !errors.Is(err, adapters.ErrEvidence) || calls != 1 {
		t.Fatal("raw service bypassed public admission", err)
	}
	value := bridgeReceive(t, records)
	result, _ := value.ValueCopy()
	if native, captured := result.NativeCause(); !captured || native != nativeFailure || inner.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("raw exact receipt lost transport semantics or custody")
	}
}

func TestAdmissionBridgeSameUseReentrySharesRootAndRejectsExpiredOrForeignFrames(t *testing.T) {
	client, assembly, selected := bridgeFixture(t, 8)
	runtime := bridgeRuntime(t)
	gate, records := bridgeEndpoint[Execution](t, runtime, 8)
	client = WithAdmissions(client, gate, nil, nil)
	rawInbox, _ := invocation.NewInbox[RPCResult](2, 2048)
	raw, err := Bind(assembly, selected, rawInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	rawGate, rawRecords := bridgeEndpoint[RPCResult](t, runtime, 2)
	raw = WithRPCAdmission(raw, rawGate)
	var rawCalls int
	transport, err := grpc.NewClient("passthrough:///bridge.invalid", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(context.Context, string, any, any, *grpc.ClientConn, grpc.UnaryInvoker, ...grpc.CallOption) error {
			rawCalls++
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	client.owner.transport = transport
	borrowed := resource.Borrow("other", assembly, selected)
	otherAssembly, err := resource.Assemble(context.Background(), context.Background(), "other", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	defer otherAssembly.Close(context.Background())
	otherInbox, _ := invocation.NewInbox[Execution](1, ExecutionEvidenceBytes)
	other, err := BindExecutions(otherAssembly, borrowed, otherInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	other = WithAdmissions(other, gate, nil, nil)
	var retained context.Context
	var highCalls int
	_, err = executeNative(context.Background(), client, fault.Correlation{Call: "visitor"}, Execution{Operation: "bridge.visitor"}, func(work context.Context, _ *Execution) (struct{}, error) {
		retained = work
		_, err := executeNative(work, client, fault.Correlation{Call: "inner-high"}, Execution{Operation: "bridge.inner"}, func(context.Context, *Execution) (struct{}, error) {
			highCalls++
			return struct{}{}, nil
		})
		if err != nil {
			return struct{}{}, err
		}
		if _, err := raw.WorkflowService(fault.Correlation{Call: "inner-raw"}).GetSystemInfo(work, &workflowservice.GetSystemInfoRequest{}); err != nil {
			return struct{}{}, err
		}
		_, err = executeNative(work, other, fault.Correlation{Call: "foreign"}, Execution{Operation: "bridge.foreign"}, func(context.Context, *Execution) (struct{}, error) {
			highCalls++
			return struct{}{}, nil
		})
		if !errors.Is(err, ErrAuthority) {
			t.Error("same physical source granted another use the live frame", err)
		}
		return struct{}{}, nil
	})
	if err != nil || highCalls != 1 || rawCalls != 1 {
		t.Fatal("same-use visitor reentry blocked behind its own root", err)
	}
	_, err = executeNative(context.WithoutCancel(retained), client, fault.Correlation{Call: "expired"}, Execution{Operation: "bridge.expired"},
		func(context.Context, *Execution) (struct{}, error) { highCalls++; return struct{}{}, nil })
	if !errors.Is(err, ErrAuthority) || highCalls != 1 {
		t.Fatal("ended native frame reopened a retained family", err)
	}
	outer := bridgeReceive(t, records)
	inner := bridgeReceive(t, records)
	if outer.Info().Depth != 1 || inner.Info().Depth != 2 || inner.Info().Parent != outer.Info().Sequence {
		t.Fatal("same-use reentry lost exact public parent")
	}
	if value := bridgeReceive(t, rawRecords); value.Info().Depth != 2 || value.Info().Parent != outer.Info().Sequence {
		t.Fatal("separately bound raw Access did not inherit the same scope")
	}
	if status, _ := runtime.Inspect(); status.Active != 0 || status.Queued != 0 || status.Accepted != 3 {
		t.Fatal("nested reentry duplicated root or accepted refused frames", status)
	}
}

func TestAdmissionBridgeRecursiveFamilyRefusesBeforeNextNativeEntry(t *testing.T) {
	client, _, _ := bridgeFixture(t, 32)
	runtime := bridgeRuntime(t)
	gate, records := bridgeEndpoint[Execution](t, runtime, 32)
	client = WithAdmissions(client, gate, nil, nil)
	var entered int
	var recurse func(context.Context, int) error
	recurse = func(ctx context.Context, depth int) error {
		_, err := executeNative(ctx, client, fault.Correlation{Call: fmt.Sprintf("depth-%d", depth)}, Execution{Operation: "bridge.recurse"}, func(work context.Context, _ *Execution) (struct{}, error) {
			entered++
			return struct{}{}, recurse(work, depth+1)
		})
		return err
	}
	if err := recurse(context.Background(), 0); !errors.Is(err, adapters.ErrLimit) || entered != 8 {
		t.Fatal("recursive native entry escaped the existing family limit", err, entered)
	}
	for range entered {
		bridgeReceive(t, records)
	}
	if status, _ := runtime.Inspect(); status.Active != 0 || status.Queued != 0 || client.inbox.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("recursive refusal stranded actual work or evidence", status)
	}
}

func TestAdmissionBridgeWorkerOwnsConstructionBeyondStartupWait(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(server, &setupPeer{})
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-joined })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	selected, err := Select(OptionsV1{Name: "worker-bridge", Endpoint: listener.Addr().String(), Namespace: "bridge", Plaintext: true,
		MaxActive: 1, MaxRequestBytes: 1024, MaxResponseBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, resource.Limits{Active: 1, Bytes: 18 << 10, MaxLeases: 16})
	assembly, err := resource.Assemble(ctx, ctx, "worker-bridge", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assembly.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	inner, _ := invocation.NewInbox[Execution](1, ExecutionEvidenceBytes)
	client, err := BindExecutions(assembly, selected, inner, nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime := bridgeRuntime(t)
	workerGate, records := bridgeEndpoint[WorkerResult](t, runtime, 1)
	client = WithAdmissions(client, nil, workerGate, nil)
	workers, _ := invocation.NewInbox[WorkerResult](1, ExecutionEvidenceBytes)
	tasks, _ := invocation.NewInbox[TaskResult](1, ExecutionEvidenceBytes)
	entered, release := make(chan struct{}), make(chan struct{})
	var configurations atomic.Int32
	stopBeforePoll := errors.New("intentional start refusal before polling")
	plugin := &reviewWorkerPlugin{
		configure: func(context.Context, nativeworker.PluginConfigureWorkerOptions) error {
			configurations.Add(1)
			close(entered)
			<-release
			return nil
		},
		start: func(context.Context, nativeworker.PluginStartWorkerOptions, func(context.Context, nativeworker.PluginStartWorkerOptions) error) error {
			return stopBeforePoll
		},
	}
	startContext, cancelStart := context.WithCancel(ctx)
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	defer cancelLifetime()
	type started struct {
		worker *Worker
		err    error
	}
	startedWorker := make(chan started, 1)
	spec := WorkerSpec{TaskQueue: "bridge", MaxHandlers: 1, Bytes: 18 << 10, Options: nativeworker.Options{Plugins: []nativeworker.Plugin{plugin}}}
	go func() {
		worker, err := client.StartWorker(startContext, lifetime, fault.Correlation{Call: "worker"}, spec, workers, tasks)
		startedWorker <- started{worker, err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("native construction never entered", ctx.Err())
	}
	cancelStart()
	var result started
	select {
	case result = <-startedWorker:
	case <-ctx.Done():
		t.Fatal("canceled startup observation did not return", ctx.Err())
	}
	if result.worker == nil || !errors.Is(result.err, context.Canceled) || result.worker.lifetime.Err() != nil {
		t.Fatal("startup observation canceled the accepted owning lifetime", result.err)
	}
	if status, _ := runtime.Inspect(); status.Active != 1 || workers.Usage().Outstanding != 1 {
		t.Fatal("blocked plugin construction lost its original reservation")
	}
	close(release)
	if err := result.worker.Stop(ctx); !errors.Is(err, stopBeforePoll) || !result.worker.Status().Joined {
		t.Fatal("partial Worker construction did not preserve cause and actual join", err)
	}
	if workers.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("Worker completion left claimed Inner evidence queued")
	}
	if worker, err := client.StartWorker(ctx, lifetime, fault.Correlation{Call: "worker-full"}, spec, workers, tasks); worker != nil || !errors.Is(err, adapters.ErrEvidence) || configurations.Load() != 1 {
		t.Fatal("full public Worker record admitted constructor/plugin work", err)
	}
	value := bridgeReceive(t, records)
	status, present := value.ValueCopy()
	if !present || !status.Joined || !errors.Is(value.Err(), stopBeforePoll) {
		t.Fatal("Worker receipt did not describe actual native join", value.Err())
	}
}

func TestAdmissionBridgeBoundsOuterQueuedWaitBeforeInnerAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, _, _ := bridgeFixture(t, 1)
		runtime, err := adapters.New(context.Background(), adapters.Options{MaxActive: 1, MaxQueued: 1, MaxWorkBytes: 1 << 20, MaxQueuedBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		gate, records := bridgeEndpoint[Execution](t, runtime, 1)
		client = WithAdmissions(client, gate, nil, nil)
		blockingInbox, _ := adapters.NewInbox[struct{}](adapters.EvidenceOptions{Capacity: 1, MaxBytes: 1})
		blocking, err := adapters.Bind(runtime, adapters.Declaration[struct{}]{Evidence: blockingInbox, Copy: func(value struct{}) struct{} { return value }})
		if err != nil {
			t.Fatal(err)
		}
		var held *adapters.Call[struct{}]
		var guard adapters.Guard
		_, err = blocking.Run(context.Background(), adapters.Request{Operation: "bridge.block", WorkBytes: 1, EvidenceBytes: 1}, func(call *adapters.Call[struct{}]) {
			held = call
			guard, err = call.Hold()
		})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		var nativeEntries int
		_, err = executeNative(context.Background(), client, fault.Correlation{Call: "queued"}, Execution{Operation: "bridge.queued"}, func(context.Context, *Execution) (struct{}, error) {
			nativeEntries++
			return struct{}{}, nil
		})
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != time.Second || nativeEntries != 0 {
			t.Fatal("outer admission ignored configured deadline before native entry", err)
		}
		if client.inbox.Usage() != (invocation.InboxUsage{}) {
			t.Fatal("refused outer admission entered inner reservation")
		}
		if status, _ := records.Inspect(); status.Outstanding != 0 {
			t.Fatal("expired outer waiter leaked evidence reservation", status)
		}
		if err := held.Resolve(adapters.Outcome[struct{}]{Present: true}); err != nil {
			t.Fatal(err)
		}
		if err := guard.Release(); err != nil {
			t.Fatal(err)
		}
		bridgeReceive(t, blockingInbox)
		if err := runtime.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAdmissionBridgeWholeFamilyEnvelopeIsSharedByHighLevelAndRawRoots(t *testing.T) {
	client, assembly, selected := bridgeFixture(t, 1)
	inner, _ := invocation.NewInbox[RPCResult](1, 1024)
	raw, err := Bind(assembly, selected, inner, nil)
	if err != nil {
		t.Fatal(err)
	}
	envelope := 8 * client.owner.settings.reservation()
	client, raw, err = WithWorkEnvelope(client, raw, envelope)
	if err != nil {
		t.Fatal(err)
	}
	runtime := bridgeRuntime(t)
	gate, records := bridgeEndpoint[Execution](t, runtime, 1)
	client = WithAdmissions(client, gate, nil, nil)
	_, err = executeNative(context.Background(), client, fault.Correlation{Call: "family"}, Execution{Operation: "bridge.family"}, func(context.Context, *Execution) (struct{}, error) {
		if usage := assembly.Snapshot().Sources[0].Usage; usage.Active != 1 || usage.ActiveBytes != envelope {
			t.Error("high-level root did not charge whole native family", usage)
		}
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	bridgeReceive(t, records)
	call, _, finish, err := raw.begin(context.Background(), fault.Correlation{Call: "raw-family"}, "rpc.family")
	if err != nil {
		t.Fatal(err)
	}
	if usage := assembly.Snapshot().Sources[0].Usage; usage.Active != 1 || usage.ActiveBytes != envelope {
		t.Fatal("raw root did not share whole native family declaration", usage)
	}
	call.Complete(invocation.Outcome[RPCResult]{Present: true})
	finish()
	delivery, err := inner.Next(context.Background())
	if err != nil || delivery.Release() != nil {
		t.Fatal("raw test failed to retain its ordinary record", err)
	}
	for _, invalid := range []int64{0, client.owner.settings.reservation() - 1, client.access.Limits().Bytes + 1} {
		if executions, rpc, err := WithWorkEnvelope(client, raw, invalid); executions != nil || rpc != nil || !errors.Is(err, ErrInput) {
			t.Fatal("invalid whole-family envelope was accepted", invalid, err)
		}
	}
	borrowed := resource.Borrow("foreign", assembly, selected)
	other, err := resource.Assemble(context.Background(), context.Background(), "foreign", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close(context.Background())
	foreign, err := Bind(other, borrowed, inner, nil)
	if err != nil {
		t.Fatal(err)
	}
	if executions, rpc, err := WithWorkEnvelope(client, foreign, envelope); executions != nil || rpc != nil || !errors.Is(err, ErrInput) {
		t.Fatal("whole-family declaration crossed retained-use origin", err)
	}
}

func TestResetConvenienceIdentityValidationPreservesRawBlankRequest(t *testing.T) {
	options := settings{Namespace: "test", MaxRequestBytes: 1 << 20}
	for _, operation := range []string{"", "callback.raw", "workflow.reset", "callback.resetworkflowexecution"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			if operation != "" {
				ctx = context.WithValue(ctx, executionEvidenceKey{}, &Execution{Operation: operation})
			}
			nativeConvenience := operation == "workflow.reset" || operation == "callback.resetworkflowexecution"
			for _, id := range []string{"", "bounded-id", strings.Repeat("a", 1025)} {
				err := validateNativeRequest(ctx, options, &workflowservice.ResetWorkflowExecutionRequest{Namespace: "test", RequestId: id})
				switch {
				case nativeConvenience && id == "":
					if !errors.Is(err, ErrInput) {
						t.Fatal("native Reset lost mandatory effective identity", err)
					}
				case nativeConvenience && len(id) > 1024:
					if !errors.Is(err, ErrLimit) {
						t.Fatal("native Reset exceeded its retained identity envelope", err)
					}
				default:
					if err != nil {
						t.Fatal("convenience identity rule changed raw protocol authority", err)
					}
				}
			}
		})
	}
}
