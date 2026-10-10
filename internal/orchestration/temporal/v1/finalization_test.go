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
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/testsuite"
	nativeworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type finalizationRead struct {
	native error
	detail string
	err    error
}

type finalizationDetailsConverter struct {
	converter.FailureConverter
	check func(error)
	mu    sync.Mutex
	reads []finalizationRead
}

func (conversion *finalizationDetailsConverter) ErrorToFailure(err error) *failurepb.Failure {
	if conversion.check != nil {
		conversion.check(err)
	}
	if application := finalizationApplicationDetails(err); application != nil {
		var value string
		readErr := application.Details(&value)
		conversion.mu.Lock()
		conversion.reads = append(conversion.reads, finalizationRead{err, value, readErr})
		conversion.mu.Unlock()
	}
	return conversion.FailureConverter.ErrorToFailure(err)
}

func finalizationApplicationDetails(value error) *sdktemporal.ApplicationError {
	for current := value; current != nil; current = errors.Unwrap(current) {
		if application, ok := current.(*sdktemporal.ApplicationError); ok && application.HasDetails() {
			return application
		}
	}
	return nil
}

func finalizationWorkerFixture(t testing.TB, taskCapacity int) (*Worker, *Executions) {
	t.Helper()
	client, _, _ := bridgeFixture(t, 2)
	payloads, err := converter.GetDefaultDataConverter().ToPayloads("detail")
	if err != nil {
		t.Fatal(err)
	}
	wire := &failurepb.Failure{Message: "fixture failure", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
		ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "fixture", NonRetryable: true, Details: payloads}}}
	peer := func(_ context.Context, _ string, _, reply any, _ *grpc.ClientConn, _ grpc.UnaryInvoker, _ ...grpc.CallOption) error {
		switch reply := reply.(type) {
		case *workflowservice.GetSystemInfoResponse:
			reply.ServerVersion = "1.32.0"
		case *workflowservice.GetWorkflowExecutionHistoryResponse:
			reply.History = &historypb.History{Events: []*historypb.HistoryEvent{{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{Failure: wire}}}}}
		}
		return nil
	}
	native, err := sdk.NewLazyClient(sdk.Options{Namespace: "bridge", HostPort: "127.0.0.1:1", Logger: disabledLogger{},
		ConnectionOptions: sdk.ConnectionOptions{DialOptions: []grpc.DialOption{grpc.WithNoProxy(), grpc.WithUnaryInterceptor(client.owner.capture), grpc.WithChainUnaryInterceptor(peer)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(native.Close)
	client.owner.native = native
	client.owner.ready.Store(true)
	workers, _ := invocation.NewInbox[WorkerResult](1, ExecutionEvidenceBytes)
	tasks, _ := invocation.NewInbox[TaskResult](taskCapacity, int64(taskCapacity)*ExecutionEvidenceBytes)
	call, err := invocation.Begin(context.Background(), client.access, invocation.Request{Name: "worker.run", Shape: invocation.Session,
		Correlation: fault.Correlation{Call: "worker"}, Bytes: client.owner.settings.reservation(), EvidenceBytes: ExecutionEvidenceBytes,
		Admission: invocation.Budget{Limit: time.Second}}, workers, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { call.Complete(invocation.Outcome[WorkerResult]{Present: true}) })
	return &Worker{client: client, call: call, tasks: tasks, handlers: make(chan struct{}, 1), correlation: fault.Correlation{Call: "worker"}}, client
}

type finalizationCancelOperation struct {
	nexus.UnimplementedOperation[string, string]
	cancel func(context.Context) error
}

func (*finalizationCancelOperation) Name() string { return "operation" }
func (*finalizationCancelOperation) Start(context.Context, string, nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[string], error) {
	return &nexus.HandlerStartOperationResultAsync{OperationToken: "fixture-token"}, nil
}
func (operation *finalizationCancelOperation) Cancel(ctx context.Context, _ string, _ nexus.CancelOperationOptions) error {
	return &nexus.HandlerError{Type: nexus.HandlerErrorTypeInternal, Cause: operation.cancel(ctx)}
}

func TestNativeFinalizationReadsCallbackErrorWithoutRevivingAuthority(t *testing.T) {
	for _, kind := range []string{"activity", "local-activity", "nexus-start", "nexus-cancel"} {
		for _, mode := range []string{"plain-user-error", "callback-client-error"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				taskCount := 1
				if kind == "nexus-cancel" {
					taskCount = 2
				}
				worker, client := finalizationWorkerFixture(t, taskCount)
				conversion := &finalizationDetailsConverter{FailureConverter: sdktemporal.NewDefaultFailureConverter(sdktemporal.DefaultFailureConverterOptions{})}
				var retained sdk.Client
				var returned error
				definition := func(ctx context.Context) error {
					if mode == "plain-user-error" {
						returned = sdktemporal.NewNonRetryableApplicationError("fixture failure", "fixture", nil, "detail")
						return returned
					}
					if kind == "nexus-start" || kind == "nexus-cancel" {
						retained = temporalnexus.GetClient(ctx)
					} else {
						retained = activity.GetClient(ctx)
					}
					returned = retained.GetWorkflow(ctx, "workflow", "run").Get(ctx, nil)
					var application *sdktemporal.ApplicationError
					if !errors.As(returned, &application) {
						t.Error("callback result lacked native ApplicationError", returned)
						return returned
					}
					var value string
					if err := application.Details(&value); err != nil || value != "detail" {
						t.Error("live handler detail control failed", err)
					}
					if client.inbox.Usage().Outstanding != 2 {
						t.Error("fixture did not saturate both callback operation records")
					}
					return returned
				}
				conversion.check = func(error) {
					if retained != nil {
						if err := retained.SignalWorkflow(context.Background(), "workflow", "run", "late", nil); !errors.Is(err, ErrAuthority) {
							t.Error("native finalization retained callback RPC authority", err)
						}
						if client.inbox.Usage().Outstanding != 2 || worker.tasks.Usage().Outstanding != taskCount || len(worker.handlers) != 0 {
							t.Error("native finalization changed callback/evidence scope")
						}
					}
				}
				var suite testsuite.WorkflowTestSuite
				options := nativeworker.Options{Interceptors: []interceptor.WorkerInterceptor{&taskInterceptor{worker: worker}}}
				var result error
				if kind == "activity" {
					environment := suite.NewTestActivityEnvironment()
					environment.SetFailureConverter(finalizationConverter(conversion))
					environment.SetWorkerOptions(options)
					environment.RegisterActivity(definition)
					_, result = environment.ExecuteActivity(definition)
				} else {
					environment := suite.NewTestWorkflowEnvironment()
					environment.SetFailureConverter(finalizationConverter(conversion))
					environment.SetWorkerOptions(options)
					if kind == "local-activity" {
						environment.RegisterActivity(definition)
						environment.ExecuteWorkflow(func(ctx workflow.Context) error {
							ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
							return workflow.ExecuteLocalActivity(ctx, definition).Get(ctx, nil)
						})
					} else {
						service := nexus.NewService("fixture")
						var operation nexus.Operation[string, string]
						if kind == "nexus-start" {
							operation = nexus.NewSyncOperation("operation", func(ctx context.Context, _ string, _ nexus.StartOperationOptions) (string, error) {
								return "", &nexus.OperationError{State: nexus.OperationStateFailed, Message: "operation failed", Cause: definition(ctx)}
							})
						} else {
							operation = &finalizationCancelOperation{cancel: definition}
							environment.SetOnNexusOperationCanceledListener(func(string, string) { environment.SignalWorkflow("cancel-finished", nil) })
						}
						if err := service.Register(operation); err != nil {
							t.Fatal(err)
						}
						environment.RegisterNexusService(service)
						environment.ExecuteWorkflow(func(ctx workflow.Context) error {
							child, cancel := workflow.WithCancel(ctx)
							future := workflow.NewNexusClient("endpoint", "fixture").ExecuteOperation(child, "operation", "input", workflow.NexusOperationOptions{})
							if kind == "nexus-cancel" {
								if err := future.GetNexusOperationExecution().Get(ctx, nil); err != nil {
									return err
								}
								cancel()
								workflow.GetSignalChannel(ctx, "cancel-finished").Receive(ctx, nil)
							}
							return future.Get(ctx, nil)
						})
					}
					result = environment.GetWorkflowError()
				}
				conversion.mu.Lock()
				reads := append([]finalizationRead(nil), conversion.reads...)
				conversion.mu.Unlock()
				if result == nil || len(reads) == 0 {
					t.Fatal("native task failure finalization did not execute")
				}
				for _, read := range reads {
					if read.err != nil || read.detail != "detail" {
						t.Errorf("Worker-owned native finalization lost returned error details: refusal=%t detail=%q", errors.Is(read.err, ErrAuthority), read.detail)
					}
				}
				if retained != nil {
					var application *sdktemporal.ApplicationError
					if !errors.As(returned, &application) || !errors.Is(application.Details(new(string)), ErrAuthority) {
						t.Error("finalization revived the retained original error")
					}
					application = finalizationApplicationDetails(reads[0].native)
					if application == nil || application.Details(new(string)) == nil {
						t.Error("retained converter input outlived its conversion window")
					}
					if client.inbox.Usage().Outstanding != 2 {
						t.Error("finalization allocated fresh operation evidence")
					}
				}
			})
		}
	}
}

type finalizationContextConverter struct {
	converter.FailureConverter
	context  converter.SerializationContext
	observed *converter.SerializationContext
	cause    error
	wire     *failurepb.Failure
}

func (conversion *finalizationContextConverter) WithSerializationContext(ctx converter.SerializationContext) converter.FailureConverter {
	copy := *conversion
	copy.context = ctx
	return &copy
}

func (conversion *finalizationContextConverter) ErrorToFailure(err error) *failurepb.Failure {
	*conversion.observed = conversion.context
	if err != conversion.cause {
		return &failurepb.Failure{Message: "ordinary custom error changed"}
	}
	return conversion.wire
}

func (conversion *finalizationContextConverter) FailureToError(value *failurepb.Failure) error {
	*conversion.observed = conversion.context
	if !proto.Equal(value, conversion.wire) {
		return errors.New("ordinary custom failure changed")
	}
	return conversion.cause
}

func TestNativeFinalizationConverterPreservesSerializationContextAndCustomIdentity(t *testing.T) {
	want := converter.ActivitySerializationContext{Namespace: "namespace", WorkflowID: "workflow", WorkflowType: "definition", ActivityType: "activity", TaskQueue: "queue"}
	var observed converter.SerializationContext
	cause := errors.New("custom error")
	wire := &failurepb.Failure{Message: "custom failure"}
	original := &finalizationContextConverter{observed: &observed, cause: cause, wire: wire}
	bound := converter.WithFailureConverterSerializationContext(finalizationConverter(original), want)
	if result := bound.ErrorToFailure(cause); result != wire || observed != want || original.context != nil {
		t.Fatal("serialization context or exact custom ErrorToFailure changed")
	}
	observed = nil
	if bound.FailureToError(wire) != cause || observed != want || original.context != nil {
		t.Fatal("serialization context or exact custom FailureToError changed")
	}
	plain := sdktemporal.NewApplicationError("plain", "fixture")
	if !proto.Equal(finalizationConverter(nil).ErrorToFailure(plain), sdktemporal.GetDefaultFailureConverter().ErrorToFailure(plain)) {
		t.Fatal("native default failure conversion changed")
	}
}
