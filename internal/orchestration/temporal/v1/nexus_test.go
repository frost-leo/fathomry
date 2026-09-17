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
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/nexus-rpc/sdk-go/nexus"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type controlledNexusOperation struct {
	nexus.UnimplementedOperation[string, string]
	start func(context.Context, string, nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[string], error)
}

func (*controlledNexusOperation) Name() string { return "async" }

func (operation *controlledNexusOperation) Start(ctx context.Context, input string, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[string], error) {
	return operation.start(ctx, input, options)
}

func TestNexusAsyncHandlerLifetimeIdentityAndScopedStart(t *testing.T) {
	var issued atomic.Bool
	var backendRequests atomic.Int32
	responses := make(chan *workflowservice.RespondNexusTaskCompletedRequest, 1)
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			switch request.(type) {
			case *workflowservice.StartWorkflowExecutionRequest, *workflowservice.StartActivityExecutionRequest, *workflowservice.UpdateWorkflowExecutionRequest,
				*workflowservice.QueryWorkflowRequest, *workflowservice.RequestCancelWorkflowExecutionRequest:
				backendRequests.Add(1)
			}
			switch request := request.(type) {
			case *workflowservice.PollNexusTaskQueueRequest:
				if issued.CompareAndSwap(false, true) {
					return &workflowservice.PollNexusTaskQueueResponse{TaskToken: []byte("private-task-token"), Request: &nexuspb.Request{
						Header: map[string]string{nexus.HeaderRequestTimeout: "5s"},
						Variant: &nexuspb.Request_StartOperation{StartOperation: &nexuspb.StartOperationRequest{
							Service: "unit-service", Operation: "async", Payload: activityPayload("\"input\""), RequestId: "request-42",
							Callback: "http://callback.invalid/owned", CallbackHeader: map[string]string{"private": "secret"},
							Links: []*nexuspb.Link{{Url: "https://example.invalid/input", Type: "fixture"}},
						}},
					}}, nil
				}
				<-ctx.Done()
				return nil, status.FromContextError(ctx.Err()).Err()
			case *workflowservice.RespondNexusTaskCompletedRequest:
				responses <- request
				return &workflowservice.RespondNexusTaskCompletedResponse{}, nil
			}
			return next(ctx, request)
		}
	})
	inbox, err := invocation.NewInbox[temporal.Execution](1, temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	executions, err := temporal.BindExecutions(fixture.assembly, fixture.selection, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	workers, tasks := workerInboxes(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var borrowed sdk.Client
	var borrowedContext context.Context
	var encoded atomic.Int32
	operations := func(ctx context.Context) []func() error {
		return []func() error{
			func() error {
				_, err := borrowed.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{ID: "refused", TaskQueue: "unit"}, "definition", activityMarshalProbe{called: &encoded})
				return err
			},
			func() error {
				_, err := borrowed.ExecuteActivity(ctx, sdk.StartActivityOptions{ID: "refused", TaskQueue: "unit", StartToCloseTimeout: time.Second}, "definition", activityMarshalProbe{called: &encoded})
				return err
			},
			func() error {
				_, err := borrowed.UpdateWorkflow(ctx, sdk.UpdateWorkflowOptions{WorkflowID: "backend", UpdateID: "refused", UpdateName: "update",
					WaitForStage: sdk.WorkflowUpdateStageAccepted, Args: []any{activityMarshalProbe{called: &encoded}}})
				return err
			},
			func() error {
				_, err := borrowed.QueryWorkflow(ctx, "backend", "", "query", activityMarshalProbe{called: &encoded})
				return err
			},
			func() error {
				_, err := borrowed.QueryWorkflowWithOptions(ctx, &sdk.QueryWorkflowWithOptionsRequest{WorkflowID: "backend", QueryType: "query", Args: []any{activityMarshalProbe{called: &encoded}}})
				return err
			},
			func() error { return borrowed.CancelWorkflow(ctx, "backend", "") },
			func() error {
				return borrowed.CancelWorkflowWithOptions(ctx, sdk.CancelWorkflowOptions{WorkflowID: "backend"})
			},
		}
	}
	service := nexus.NewService("unit-service")
	linkURL, _ := url.Parse("https://example.invalid/output")
	err = service.Register(&controlledNexusOperation{start: func(ctx context.Context, input string, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[string], error) {
		if input != "input" || options.RequestID != "request-42" || options.CallbackURL != "http://callback.invalid/owned" || len(options.Links) != 1 {
			return nil, errors.New("native Nexus request options changed")
		}
		borrowed = temporalnexus.GetClient(ctx)
		borrowedContext = context.WithoutCancel(ctx)
		run, err := borrowed.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{ID: "backend", TaskQueue: "unit"}, "definition", activityMarshalProbe{called: &encoded})
		if err != nil || run.GetRunID() != "observed-run" {
			return nil, errors.New("scoped native backend start failed")
		}
		for _, operation := range operations(ctx) {
			if err := operation(); !errors.Is(err, invocation.ErrEvidence) {
				return nil, errors.New("Nexus backend entered without evidence capacity")
			}
		}
		if encoded.Load() != 1 || backendRequests.Load() != 1 {
			return nil, errors.New("Nexus evidence saturation reached conversion or transport")
		}
		nexus.AddHandlerLinks(ctx, nexus.Link{URL: linkURL, Type: "fixture"})
		close(entered)
		<-release
		return &nexus.HandlerStartOperationResultAsync{OperationToken: "private-operation-token"}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "nexus-owner"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options:       worker.Options{DisableWorkflowWorker: true, MaxConcurrentNexusTaskExecutionSize: 1, MaxConcurrentNexusTaskPollers: 1, WorkerStopTimeout: time.Millisecond},
		NexusServices: []*nexus.Service{service},
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
		t.Fatal("Nexus handler did not start")
	}
	short, endWait := context.WithTimeout(ctx, 20*time.Millisecond)
	err = managed.Stop(short)
	endWait()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Nexus handler outlived its claimed owner")
	}
	if err := fixture.assembly.Close(ctx); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("Nexus dependency was released early")
	}
	once.Do(func() { close(release) })
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-responses:
		async := response.GetResponse().GetStartOperation().GetAsyncSuccess()
		if async.GetOperationToken() != "private-operation-token" || len(async.GetLinks()) != 1 {
			t.Fatal("native asynchronous result/link changed")
		}
	case <-ctx.Done():
		t.Fatal("native Nexus response was not observed")
	}
	delivery, err := tasks.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	record, err := delivery.Receipt().WaitReleased(ctx)
	if err != nil || record.Err() != nil {
		t.Fatal("Nexus evidence not complete")
	}
	value := record.Outcome.Value
	if !value.AsyncCompletion || !value.HandlerReturned || value.NexusService != "unit-service" || value.NexusOperation != "async" ||
		value.NexusRequestID != "request-42" || !value.NexusCallbackRequested || value.NexusRequestLinks != 1 {
		t.Fatal("Nexus evidence lost native identity/options")
	}
	conformance.Private(t, value, "private-task-token", "private-operation-token", "callback.invalid", "secret")
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	backend := receiveExecution(t, inbox)
	if !backend.Nested || backend.Outcome.Value.Operation != "callback.workflow.start" || !backend.Outcome.Value.Accepted {
		t.Fatal("backend start escaped nested evidence")
	}
	for index, operation := range operations(borrowedContext) {
		if err := operation(); !errors.Is(err, temporal.ErrAuthority) {
			t.Fatalf("expired Nexus operation %d was not refused", index)
		}
	}
	if encoded.Load() != 1 || backendRequests.Load() != 1 {
		t.Fatal("expired Nexus client entered a converter or sent another request")
	}
}

func TestNexusCancelDoesNotWaitForResultCache(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var pollOnce, cancelOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	fixture := newFixture(t, 1, func(options *temporal.OptionsV1, limits *resource.Limits, peer *rpcServer) {
		options.MaxActive = 2
		limits.Active = 2
		limits.Bytes *= 2
		peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			switch request.(type) {
			case *workflowservice.PollNexusOperationExecutionRequest:
				pollOnce.Do(func() { close(entered) })
				select {
				case <-release:
					return &workflowservice.PollNexusOperationExecutionResponse{Outcome: &workflowservice.PollNexusOperationExecutionResponse_Result{Result: activityPayload("\"result\"")}}, nil
				case <-ctx.Done():
					return nil, status.FromContextError(ctx.Err()).Err()
				}
			case *workflowservice.RequestCancelNexusOperationExecutionRequest:
				cancelOnce.Do(func() { close(canceled) })
				return &workflowservice.RequestCancelNexusOperationExecutionResponse{}, nil
			}
			return next(ctx, request)
		}
	})
	executions, inbox := executionBinding(t, fixture)
	run, err := executions.GetNexusOperationHandle(sdk.GetNexusOperationHandleOptions{OperationID: "operation", RunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		var result string
		err := run.Get(ctx, fault.Correlation{Call: "result"}, &result)
		if err == nil && result != "result" {
			err = errors.New("wrong result")
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("result wait never entered")
	}
	short, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	err = run.Cancel(short, fault.Correlation{Call: "cancel"}, sdk.CancelNexusOperationOptions{Reason: "fixture"})
	stop()
	if err != nil {
		t.Fatal("control was queued behind result waiting", err)
	}
	select {
	case <-canceled:
	default:
		t.Fatal("cancel RPC was not independently observed")
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	receiveExecution(t, inbox)
	receiveExecution(t, inbox)
}

func TestReviewCallbackCachedNexusResultAfterRelease(t *testing.T) {
	for _, withInterceptor := range []bool{false, true} {
		name := "without-interceptor"
		runtime := temporal.RuntimeOptions{}
		if withInterceptor {
			name = "with-interceptor"
			runtime.Interceptors = []interceptor.ClientInterceptor{&interceptor.ClientInterceptorBase{}}
		}
		t.Run(name, func(t *testing.T) {
			var calls, polls atomic.Int32
			var borrowed sdk.NexusOperationHandle
			reviewReleasedCallbackFixture(t, "PollNexusOperationExecution", runtime, func(ctx context.Context, client sdk.Client) error {
				borrowed = client.GetNexusOperationHandle(sdk.GetNexusOperationHandleOptions{OperationID: "operation", RunID: "run"})
				return borrowed.Get(ctx, &decodeCounter{calls: &calls})
			}, func(ctx context.Context, request any, next grpc.UnaryHandler) (any, error) {
				if _, ok := request.(*workflowservice.PollNexusOperationExecutionRequest); ok {
					polls.Add(1)
					return &workflowservice.PollNexusOperationExecutionResponse{Outcome: &workflowservice.PollNexusOperationExecutionResponse_Result{Result: activityPayload("\"result\"")}}, nil
				}
				return next(ctx, request)
			})
			if calls.Load() != 1 || polls.Load() != 1 {
				t.Fatalf("live control: decodes=%d polls=%d, want one each", calls.Load(), polls.Load())
			}
			err := borrowed.Get(context.Background(), &decodeCounter{calls: &calls})
			if !errors.Is(err, temporal.ErrAuthority) || calls.Load() != 1 || polls.Load() != 1 {
				t.Fatalf("released cached Nexus result: authority refusal=%v decodes=%d polls=%d; want refusal, one decode, one poll", errors.Is(err, temporal.ErrAuthority), calls.Load(), polls.Load())
			}
		})
	}
}
