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

package temporal_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type retainedClientInterceptor struct {
	interceptor.ClientInterceptorBase
	next    interceptor.ClientOutboundInterceptor
	context context.Context
	input   interceptor.ClientExecuteWorkflowInput
	calls   atomic.Int32
}

type retainedClientOutbound struct {
	interceptor.ClientOutboundInterceptorBase
	owner *retainedClientInterceptor
}

func (extension *retainedClientInterceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	extension.next = next
	return &retainedClientOutbound{ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next}, owner: extension}
}

func (outbound *retainedClientOutbound) ExecuteWorkflow(ctx context.Context, input *interceptor.ClientExecuteWorkflowInput) (sdk.WorkflowRun, error) {
	outbound.owner.calls.Add(1)
	outbound.owner.context = context.WithoutCancel(ctx)
	outbound.owner.input = *input
	return outbound.Next.ExecuteWorkflow(ctx, input)
}

func TestNativeMetricsPropagationAndRetainedClientBoundary(t *testing.T) {
	metrics := newNativeTestMetrics()
	tracer := &nativeTestTracer{}
	retained := &retainedClientInterceptor{}
	native := temporal.RuntimeOptions{MetricsHandler: metrics, Interceptors: []interceptor.ClientInterceptor{retained, interceptor.NewTracingInterceptor(tracer)},
		ContextPropagators: []workflow.ContextPropagator{&nativeTestPropagator{}}}
	fixture := newRuntimeFixture(t, 1, native, nil, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.start = func(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
			incoming, _ := metadata.FromIncomingContext(ctx)
			if len(incoming.Get("authorization")) != 0 || !slices.Equal(incoming.Get("temporal-namespace"), []string{"test"}) {
				return nil, errors.New("caller transport metadata escaped the source boundary")
			}
			if string(request.GetHeader().GetFields()[nativeHeaderKey].GetData()) != "unit-context" || request.GetHeader().GetFields()["gh61-trace"] == nil {
				t.Logf("header control: context_match=%t trace_present=%t", string(request.GetHeader().GetFields()[nativeHeaderKey].GetData()) == "unit-context", request.GetHeader().GetFields()["gh61-trace"] != nil)
				return nil, errors.New("native headers were not injected")
			}
			return &workflowservice.StartWorkflowExecutionResponse{RunId: "observed-run"}, nil
		}
	})
	native.Interceptors[0] = nil
	native.ContextPropagators[0] = nil
	executions, inbox := executionBinding(t, fixture)
	ctx := context.WithValue(context.Background(), nativeContextKey{}, "unit-context")
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "must-not-send", "temporal-namespace", "not-test"))
	_, err := executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "extended-start"}, sdk.StartWorkflowOptions{ID: "extended", TaskQueue: "unit"}, "definition")
	if err != nil {
		t.Fatal(err)
	}
	if retained.calls.Load() != 1 || fixture.server.starts.Load() != 1 {
		t.Fatal("client interception did not run exactly once")
	}
	if result := receiveExecution(t, inbox); result.Err() != nil || !result.Outcome.Value.Accepted {
		t.Fatal("native extension lost independent execution evidence")
	}
	spans := tracer.observations()
	if len(spans) != 1 || spans[0].operation != "StartWorkflow" || !spans[0].finished || spans[0].err != nil {
		t.Fatal("native tracing lifetime changed")
	}
	if err := retained.next.CancelWorkflow(context.Background(), &interceptor.ClientCancelWorkflowInput{WorkflowID: "extended"}); err == nil || fixture.server.cancels.Load() != 0 {
		t.Fatal("unscoped interceptor continuation escaped source admission")
	}
	if _, err := retained.next.ExecuteWorkflow(retained.context, &retained.input); !errors.Is(err, temporal.ErrAuthority) || fixture.server.starts.Load() != 1 {
		t.Fatal("expired interceptor continuation escaped source admission")
	}
	found := false
	for _, reading := range metrics.readings() {
		if reading.name == "temporal_request" && reading.tags["namespace"] == "test" && reading.tags["operation"] == "StartWorkflowExecution" {
			found = true
		}
	}
	if !found {
		t.Fatal("native RPC metrics/tagging were not delivered")
	}
}

type faultyClientFactory struct {
	interceptor.ClientInterceptorBase
	cause error
	mode  string
}

func (factory *faultyClientFactory) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	switch factory.mode {
	case "panic":
		panic(factory.cause)
	case "nil":
		return nil
	case "mutation":
		factory.cause = next.CancelWorkflow(context.Background(), &interceptor.ClientCancelWorkflowInput{WorkflowID: "forbidden"})
	}
	return next
}

func TestClientExtensionInitializationFailureKeepsCleanup(t *testing.T) {
	fixture := newFixture(t, 1)
	original := errors.New("synthetic factory panic")
	for _, mode := range []string{"panic", "nil", "mutation"} {
		t.Run(mode, func(t *testing.T) {
			factory := &faultyClientFactory{cause: original, mode: mode}
			selected, err := temporal.SelectWithRuntime(temporal.OptionsV1{Name: "failed-extension", Endpoint: fixture.endpoint, Namespace: "test", Plaintext: true},
				temporal.RuntimeOptions{Interceptors: []interceptor.ClientInterceptor{factory}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			assembly, err := resource.Assemble(ctx, ctx, "failed-extension", selected)
			if err == nil || assembly == nil {
				t.Fatalf("extension initialization failure was not retained (synthetic factory error: %v)", factory.cause)
			}
			if mode == "panic" && !errors.Is(err, original) {
				t.Fatal("factory panic identity lost")
			}
			if mode == "mutation" && !errors.Is(err, temporal.ErrAuthority) {
				t.Fatal("handled setup mutation did not fail construction")
			}
			for _, source := range assembly.Snapshot().Sources {
				if !source.Quiescent || !source.Released {
					t.Fatal("partially constructed native client was abandoned")
				}
			}
			if fixture.server.starts.Load() != 0 || fixture.server.cancels.Load() != 0 {
				t.Fatal("extension factory performed an unadmitted mutation")
			}
		})
	}
}

type shortCircuitInterceptor struct {
	interceptor.InterceptorBase
	clientFactories atomic.Int32
	body            func(context.Context) (any, error)
	unsafeFactory   atomic.Bool
}

type shortCircuitInbound struct {
	interceptor.ActivityInboundInterceptorBase
	owner *shortCircuitInterceptor
}

func (extension *shortCircuitInterceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	extension.clientFactories.Add(1)
	return next
}

func (extension *shortCircuitInterceptor) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	if _, raw := next.(interceptor.ActivityOutboundInterceptor); raw {
		extension.unsafeFactory.Store(true)
	}
	var refusal any
	func() { defer func() { refusal = recover() }(); activity.GetClient(ctx).Close() }()
	if err, ok := refusal.(error); !ok || !errors.Is(err, temporal.ErrAuthority) {
		extension.unsafeFactory.Store(true)
	}
	if activity.GetInfo(ctx).Namespace != "test" {
		extension.unsafeFactory.Store(true)
	}
	return &shortCircuitInbound{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, owner: extension}
}

func (inbound *shortCircuitInbound) ExecuteActivity(ctx context.Context, _ *interceptor.ExecuteActivityInput) (any, error) {
	return inbound.owner.body(ctx)
}

func TestCombinedInterceptorCannotBypassTaskAdmissionAndLifetime(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var originalBody atomic.Int32
	var borrower sdk.Client
	var callbackContext context.Context
	extension := &shortCircuitInterceptor{body: func(ctx context.Context) (any, error) {
		borrower = activity.GetClient(ctx)
		callbackContext = context.WithoutCancel(ctx)
		count, err := borrower.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
		if err != nil || count.GetCount() != 7 {
			return nil, errors.New("combined interceptor is outside the admitted task scope")
		}
		close(entered)
		<-release
		return "short-circuit-result", nil
	}}
	runtime := temporal.RuntimeOptions{Interceptors: []interceptor.ClientInterceptor{extension}}
	fixture := newRuntimeFixture(t, 2, runtime, nil, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) { peer.activityName = "intercepted" })
	runtime.Interceptors[0] = nil
	executions, _ := executionBinding(t, fixture)
	workers, tasks := workerInboxes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "intercepted-worker"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1, WorkerStopTimeout: time.Millisecond},
		Activities: []temporal.ActivityRegistration{{Definition: func(context.Context) error { originalBody.Add(1); return nil }, Options: activity.RegisterOptions{Name: "intercepted"}}},
	}, workers, tasks)
	if managed != nil {
		t.Cleanup(func() {
			once.Do(func() { close(release) })
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := managed.Stop(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("combined worker interceptor did not execute")
	}
	if extension.clientFactories.Load() != 1 {
		t.Fatal("private polling client leaked into client interceptor factories")
	}
	if extension.unsafeFactory.Load() {
		t.Fatal("interceptor factory exposed a raw client or lost native Activity metadata")
	}
	if tasks.Usage().Outstanding != 1 {
		t.Fatal("short circuit bypassed mandatory task evidence admission")
	}
	short, endWait := context.WithTimeout(ctx, 20*time.Millisecond)
	err = managed.Stop(short)
	endWait()
	if !errors.Is(err, context.DeadlineExceeded) || managed.Status().Joined {
		t.Fatal("live extension was released")
	}
	once.Do(func() { close(release) })
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if originalBody.Load() != 0 {
		t.Fatal("native short-circuit semantics were changed")
	}
	delivery, err := tasks.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := delivery.Receipt().WaitReleased(ctx)
	if err != nil || result.Err() != nil || !result.Outcome.Value.HandlerReturned {
		t.Fatal("interceptor completion evidence missing")
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	_, err = borrower.CountWorkflow(callbackContext, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
	if !errors.Is(err, temporal.ErrAuthority) {
		t.Fatal("retained extension callback outlived its task scope")
	}
}

func TestContextPropagationFailureAndRuntimeValidation(t *testing.T) {
	original := errors.New("synthetic propagation failure")
	fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{ContextPropagators: []workflow.ContextPropagator{&nativeTestPropagator{injectError: original}}}, nil)
	executions, inbox := executionBinding(t, fixture)
	_, err := executions.ExecuteWorkflow(context.Background(), fault.Correlation{Call: "bad-header"}, sdk.StartWorkflowOptions{ID: "headers", TaskQueue: "unit"}, "definition")
	if !errors.Is(err, original) || fixture.server.starts.Load() != 0 {
		t.Fatal("propagation failure entered transport or lost its cause")
	}
	if result := receiveExecution(t, inbox); !errors.Is(result.Err(), original) || result.Attempts.Observed != 0 {
		t.Fatal("handled propagation error lost independent evidence")
	}
	var metrics *nativeTestMetrics
	var extension *retainedClientInterceptor
	var propagator *nativeTestPropagator
	for _, runtime := range []temporal.RuntimeOptions{
		{MetricsHandler: metrics}, {Interceptors: []interceptor.ClientInterceptor{extension}},
		{ContextPropagators: []workflow.ContextPropagator{propagator}}, {Interceptors: make([]interceptor.ClientInterceptor, 33)},
		{ContextPropagators: make([]workflow.ContextPropagator, 33)},
	} {
		if _, err := temporal.SelectWithRuntime(temporal.OptionsV1{}, runtime); !errors.Is(err, temporal.ErrInput) {
			t.Fatal("invalid runtime extension accepted")
		}
	}
}

func TestNativeTracingFailureRemainsAnExecutionFailure(t *testing.T) {
	original := errors.New("synthetic tracer start failure")
	tracer := &nativeTestTracer{startError: original}
	fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{Interceptors: []interceptor.ClientInterceptor{interceptor.NewTracingInterceptor(tracer)}}, nil)
	executions, inbox := executionBinding(t, fixture)
	_, err := executions.ExecuteWorkflow(context.Background(), fault.Correlation{Call: "trace-failed"}, sdk.StartWorkflowOptions{ID: "trace-failed", TaskQueue: "unit"}, "definition")
	if !errors.Is(err, original) || fixture.server.starts.Load() != 0 {
		t.Fatal("native tracing failure was suppressed or sent a request")
	}
	result := receiveExecution(t, inbox)
	if !errors.Is(result.Err(), original) || result.Outcome.Value.Accepted || result.Attempts.Observed != 0 {
		t.Fatal("handled tracing failure lost independent evidence")
	}
}

func TestAuditExpiredGetClientSignalBeforeTransport(t *testing.T) {
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.activityName = "retained-client"
	})
	executions, inbox := executionBinding(t, fixture)
	workers, tasks := workerInboxes(t)
	borrowed := make(chan sdk.Client, 1)
	body := func(ctx context.Context) error { borrowed <- activity.GetClient(ctx); return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "retained"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1},
		Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "retained-client"}}},
	}, workers, tasks)
	if managed != nil {
		t.Cleanup(func() {
			if !managed.Status().Joined {
				if err := managed.Stop(ctx); err != nil {
					t.Error(err)
				}
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	var client sdk.Client
	select {
	case client = <-borrowed:
	case <-ctx.Done():
		t.Fatal("activity did not run")
	}
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	releaseServiceEvidence(t, inbox)
	releaseServiceEvidence(t, workers)
	releaseServiceEvidence(t, tasks)
	if err := fixture.assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	err = client.SignalWorkflow(context.Background(), "workflow", "run", "signal", activityMarshalProbe{called: &calls})
	if !errors.Is(err, temporal.ErrAuthority) || calls.Load() != 0 {
		t.Fatalf("expired Signal entered converter before transport: refused=%v conversions=%d", errors.Is(err, temporal.ErrAuthority), calls.Load())
	}
}

func TestAuditRetainedClientContinuationBeforeTransport(t *testing.T) {
	extension := &retainedClientInterceptor{}
	fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{Interceptors: []interceptor.ClientInterceptor{extension}}, nil)
	executions, inbox := executionBinding(t, fixture)
	var calls atomic.Int32
	_, err := executions.ExecuteWorkflow(context.Background(), fault.Correlation{Call: "retained-next"}, sdk.StartWorkflowOptions{ID: "workflow", TaskQueue: "unit"}, "definition", activityMarshalProbe{called: &calls})
	if err != nil || calls.Load() != 1 {
		t.Fatal("normal ExecuteWorkflow control failed", err)
	}
	releaseServiceEvidence(t, inbox)
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err = extension.next.ExecuteWorkflow(extension.context, &extension.input)
	if !errors.Is(err, temporal.ErrAuthority) || calls.Load() != 1 {
		t.Fatalf("expired continuation entered converter after source release: refused=%v conversions=%d", errors.Is(err, temporal.ErrAuthority), calls.Load())
	}
}

type auditRetainedWorkerInterceptor struct {
	interceptor.InterceptorBase
	next  interceptor.ActivityInboundInterceptor
	ctx   context.Context
	input *interceptor.ExecuteActivityInput
}

type auditRetainedWorkerInbound struct {
	interceptor.ActivityInboundInterceptorBase
	owner *auditRetainedWorkerInterceptor
}

func (extension *auditRetainedWorkerInterceptor) InterceptActivity(_ context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	extension.next = next
	return &auditRetainedWorkerInbound{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, owner: extension}
}

func (inbound *auditRetainedWorkerInbound) ExecuteActivity(ctx context.Context, input *interceptor.ExecuteActivityInput) (any, error) {
	inbound.owner.ctx = context.WithoutCancel(ctx)
	inbound.owner.input = input
	return inbound.Next.ExecuteActivity(ctx, input)
}

func TestAuditCallbackRawMetadataIsStripped(t *testing.T) {
	var leaked atomic.Bool
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.activityName = "raw-metadata"
		peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			if _, ok := request.(*workflowservice.CountWorkflowExecutionsRequest); ok {
				md, _ := metadata.FromIncomingContext(ctx)
				leaked.Store(len(md.Get("authorization")) != 0)
			}
			return next(ctx, request)
		}
	})
	executions, inbox := executionBinding(t, fixture)
	workers, tasks := workerInboxes(t)
	done := make(chan error, 1)
	body := func(ctx context.Context) error {
		ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "synthetic-untrusted-credential"))
		response, err := activity.GetClient(ctx).WorkflowService().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
		if err == nil && response.Count != 7 {
			err = errors.New("wrong count")
		}
		done <- err
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "raw-metadata"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1},
		Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "raw-metadata"}}},
	}, workers, tasks)
	if managed != nil {
		t.Cleanup(func() {
			if !managed.Status().Joined {
				if err := managed.Stop(ctx); err != nil {
					t.Error(err)
				}
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("normal raw RPC failed", err)
		}
	case <-ctx.Done():
		t.Fatal("activity did not run")
	}
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	releaseServiceEvidence(t, inbox)
	releaseServiceEvidence(t, workers)
	releaseServiceEvidence(t, tasks)
	if leaked.Load() {
		t.Fatal("callback raw service transmitted caller authorization metadata")
	}
}

type decodeCounter struct{ calls *atomic.Int32 }

func (value *decodeCounter) UnmarshalJSON([]byte) error {
	value.calls.Add(1)
	return nil
}

func TestCompletedCallbackUpdateCannotDecodeAfterScope(t *testing.T) {
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.activityName = "update-borrower"
		peer.update = func(context.Context, *workflowservice.UpdateWorkflowExecutionRequest) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
			return &workflowservice.UpdateWorkflowExecutionResponse{Stage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
				UpdateRef: &updatepb.UpdateRef{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "target", RunId: "run"}, UpdateId: "update"},
				Outcome:   &updatepb.Outcome{Value: &updatepb.Outcome_Success{Success: &commonpb.Payloads{Payloads: []*commonpb.Payload{activityPayload("\"result\"")}}}}}, nil
		}
	})
	executions, inbox := executionBinding(t, fixture)
	workers, tasks := workerInboxes(t)
	var borrowed sdk.WorkflowUpdateHandle
	var calls atomic.Int32
	done := make(chan error, 1)
	body := func(ctx context.Context) error {
		var err error
		borrowed, err = activity.GetClient(ctx).UpdateWorkflow(ctx, sdk.UpdateWorkflowOptions{WorkflowID: "target", UpdateID: "update", UpdateName: "echo", WaitForStage: sdk.WorkflowUpdateStageCompleted})
		if err == nil {
			err = borrowed.Get(ctx, &decodeCounter{calls: &calls})
		}
		done <- err
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "borrowed-update"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1},
		Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "update-borrower"}}},
	}, workers, tasks)
	if managed != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := managed.Stop(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil || calls.Load() != 1 {
			t.Fatal("normal completed Update result control failed", err)
		}
	case <-ctx.Done():
		t.Fatal("Update borrower did not run")
	}
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	releaseServiceEvidence(t, inbox)
	releaseServiceEvidence(t, workers)
	releaseServiceEvidence(t, tasks)
	err = borrowed.Get(context.Background(), &decodeCounter{calls: &calls})
	if !errors.Is(err, temporal.ErrAuthority) || calls.Load() != 1 {
		t.Fatalf("expired native Update entered user decoding: calls=%d refused=%v", calls.Load(), errors.Is(err, temporal.ErrAuthority))
	}
}

func TestReviewCallbackInheritedMetadataBoundary(t *testing.T) {
	var rawCalls, inheritedCalls atomic.Int32
	var rawLeak, inheritedLeak atomic.Bool
	reviewReleasedCallbackFixture(t, "GetSearchAttributes", temporal.RuntimeOptions{}, func(ctx context.Context, client sdk.Client) error {
		ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "synthetic-review-credential"))
		if _, err := client.WorkflowService().GetSearchAttributes(ctx, &workflowservice.GetSearchAttributesRequest{}); err != nil {
			return err
		}
		if _, err := client.GetSearchAttributes(ctx); err != nil {
			return err
		}
		return nil
	}, func(ctx context.Context, request any, next grpc.UnaryHandler) (any, error) {
		if _, ok := request.(*workflowservice.GetSearchAttributesRequest); ok {
			md, _ := metadata.FromIncomingContext(ctx)
			if rawCalls.Load() == 0 {
				rawCalls.Add(1)
				rawLeak.Store(len(md.Get("authorization")) != 0)
			} else {
				inheritedCalls.Add(1)
				inheritedLeak.Store(len(md.Get("authorization")) != 0)
			}
			return &workflowservice.GetSearchAttributesResponse{}, nil
		}
		return next(ctx, request)
	})
	if rawCalls.Load() != 1 || inheritedCalls.Load() != 1 || rawLeak.Load() {
		t.Fatalf("control did not exercise matching live calls: raw=%d inherited=%d raw leak=%v", rawCalls.Load(), inheritedCalls.Load(), rawLeak.Load())
	}
	if inheritedLeak.Load() {
		t.Fatal("native inherited Client.GetSearchAttributes transmitted caller authorization metadata; generated callback raw-service control stripped it")
	}
}

type reviewNestedDescriptionFailure struct {
	interceptor.ClientInterceptorBase
	retained *sdktemporal.ApplicationError
	decodes  atomic.Int32
}

type reviewNestedDescriptionOutbound struct {
	interceptor.ClientOutboundInterceptorBase
	owner *reviewNestedDescriptionFailure
}

func (extension *reviewNestedDescriptionFailure) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return &reviewNestedDescriptionOutbound{ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next}, owner: extension}
}

func (extension *reviewNestedDescriptionOutbound) DescribeActivity(ctx context.Context, input *interceptor.ClientDescribeActivityInput) (*interceptor.ClientDescribeActivityOutput, error) {
	output, err := extension.Next.DescribeActivity(ctx, input)
	if err != nil {
		return output, err
	}
	if !errors.As(output.Description.GetLastFailure(), &extension.owner.retained) {
		return output, errors.New("native last-failure type was not preserved")
	}
	if err := extension.owner.retained.Details(&decodeCounter{calls: &extension.owner.decodes}); err != nil {
		return output, err
	}
	return output, nil
}
