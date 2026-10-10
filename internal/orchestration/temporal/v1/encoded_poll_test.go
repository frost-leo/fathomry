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
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"google.golang.org/grpc"
)

type encodedPollInterceptor struct {
	interceptor.ClientInterceptorBase
	captured converter.EncodedValue
	stale    bool
	polls    int
}

func (extension *encodedPollInterceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return newEncodedPollOutbound(extension, next, next.PollWorkflowUpdate)
}

type encodedPollOutbound[Input any, Output ~nativePollOutput] struct {
	interceptor.ClientOutboundInterceptorBase
	extension *encodedPollInterceptor
	poll      func(context.Context, Input) (*Output, error)
}

func newEncodedPollOutbound[Input any, Output ~nativePollOutput](extension *encodedPollInterceptor, next interceptor.ClientOutboundInterceptor, poll func(context.Context, Input) (*Output, error)) *encodedPollOutbound[Input, Output] {
	return &encodedPollOutbound[Input, Output]{ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next}, extension: extension, poll: poll}
}

func (extension *encodedPollInterceptor) wrap(value converter.EncodedValue) converter.EncodedValue {
	extension.polls++
	if extension.stale {
		return extension.captured
	}
	extension.captured = &encodedMappingValue{child: value}
	return extension.captured
}

func (extension *encodedPollOutbound[Input, Output]) PollWorkflowUpdate(ctx context.Context, input Input) (*Output, error) {
	value, err := extension.poll(ctx, input)
	if value != nil {
		copy := nativePollOutput(*value)
		copy.Result = extension.extension.wrap(copy.Result)
		mapped := Output(copy)
		value = &mapped
	}
	return value, err
}

func (extension *encodedPollOutbound[Input, Output]) PollActivityResult(ctx context.Context, input *interceptor.ClientPollActivityResultInput) (*interceptor.ClientPollActivityResultOutput, error) {
	value, err := extension.Next.PollActivityResult(ctx, input)
	if value != nil {
		copy := *value
		copy.Result = extension.extension.wrap(copy.Result)
		value = &copy
	}
	return value, err
}

func (extension *encodedPollOutbound[Input, Output]) PollNexusOperationResult(ctx context.Context, input *interceptor.ClientPollNexusOperationResultInput) (*interceptor.ClientPollNexusOperationResultOutput, error) {
	value, err := extension.Next.PollNexusOperationResult(ctx, input)
	if value != nil {
		copy := *value
		copy.Result = extension.extension.wrap(copy.Result)
		value = &copy
	}
	return value, err
}

func encodedPollFixture(t *testing.T) (*Executions, *encodedPollInterceptor, *resource.Assembly, resource.Selection[Source]) {
	t.Helper()
	client, assembly, selected := bridgeFixture(t, 32)
	payloads, err := converter.GetDefaultDataConverter().ToPayloads("poll-result")
	if err != nil {
		t.Fatal(err)
	}
	ref := &updatepb.UpdateRef{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow", RunId: "run"}, UpdateId: "update"}
	peer := func(_ context.Context, _ string, _, reply any, _ *grpc.ClientConn, _ grpc.UnaryInvoker, _ ...grpc.CallOption) error {
		switch reply := reply.(type) {
		case *workflowservice.GetSystemInfoResponse:
			reply.ServerVersion = "1.32.0"
			reply.Capabilities = &workflowservice.GetSystemInfoResponse_Capabilities{}
		case *workflowservice.PollWorkflowExecutionUpdateResponse:
			reply.Outcome = &updatepb.Outcome{Value: &updatepb.Outcome_Success{Success: payloads}}
		case *workflowservice.UpdateWorkflowExecutionResponse:
			reply.Stage = enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED
			reply.UpdateRef = ref
		case *workflowservice.PollActivityExecutionResponse:
			reply.Outcome = &activitypb.ActivityExecutionOutcome{Value: &activitypb.ActivityExecutionOutcome_Result{Result: payloads}}
		case *workflowservice.PollNexusOperationExecutionResponse:
			reply.Outcome = &workflowservice.PollNexusOperationExecutionResponse_Result{Result: payloads.Payloads[0]}
		}
		return nil
	}
	extension := &encodedPollInterceptor{}
	native, err := sdk.NewLazyClient(sdk.Options{Namespace: "bridge", HostPort: "127.0.0.1:1", Logger: disabledLogger{}, DisableWorkerEnvironmentInfo: true,
		Interceptors:      []interceptor.ClientInterceptor{&clientFactory{native: extension, owner: client.owner, setup: &client.owner.setup}},
		ConnectionOptions: sdk.ConnectionOptions{DialOptions: []grpc.DialOption{grpc.WithNoProxy(), grpc.WithUnaryInterceptor(client.owner.capture), grpc.WithChainUnaryInterceptor(peer)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(native.Close)
	client.owner.native = native
	client.owner.ready.Store(true)
	return client, extension, assembly, selected
}

func TestEncodedPollResultsPreserveFreshAndCachedNativeConsumption(t *testing.T) {
	for _, kind := range []string{"lazy-update", "fallback-completed-update", "activity", "nexus"} {
		t.Run(kind, func(t *testing.T) {
			client, extension, assembly, selected := encodedPollFixture(t)
			makeHandle := func() (func(context.Context, fault.Correlation, any) error, func(context.Context, any) error) {
				switch kind {
				case "lazy-update", "fallback-completed-update":
					var handle *WorkflowUpdate
					var err error
					if kind == "lazy-update" {
						handle, err = client.GetWorkflowUpdateHandle(sdk.GetWorkflowUpdateHandleOptions{WorkflowID: "workflow", RunID: "run", UpdateID: "update"})
					} else {
						handle, err = client.UpdateWorkflow(context.Background(), fault.Correlation{Call: "create-update"}, sdk.UpdateWorkflowOptions{WorkflowID: "workflow", RunID: "run", UpdateID: "update", UpdateName: "update", WaitForStage: sdk.WorkflowUpdateStageCompleted})
					}
					if err != nil {
						t.Fatal(err)
					}
					return handle.Get, handle.native.Get
				case "activity":
					handle, err := client.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "activity", RunID: "run"})
					if err != nil {
						t.Fatal(err)
					}
					return handle.Get, func(ctx context.Context, output any) error { return handle.native.Get(ctx, output) }
				default:
					handle, err := client.GetNexusOperationHandle(sdk.GetNexusOperationHandleOptions{OperationID: "operation", RunID: "run"})
					if err != nil {
						t.Fatal(err)
					}
					return handle.Get, func(ctx context.Context, output any) error { return handle.native.Get(ctx, output) }
				}
			}
			get, nativeGet := makeHandle()
			for _, id := range []string{"first", "cached"} {
				var output string
				if err := get(context.Background(), fault.Correlation{Call: id}, &output); err != nil || output != "poll-result" {
					t.Fatal("managed native encoded consumption failed", id, err)
				}
				if err := extension.captured.Get(&output); !errors.Is(err, ErrAuthority) {
					t.Fatal("native consumption revived retained interceptor alias", err)
				}
			}
			wantPolls := 1
			if kind == "lazy-update" {
				wantPolls = 2
			}
			if extension.polls != wantPolls {
				t.Fatal("native cache/poll semantics changed", extension.polls)
			}
			if kind != "lazy-update" {
				borrowed := resource.Borrow("other-use", assembly, selected)
				otherAssembly, err := resource.Assemble(context.Background(), context.Background(), "other-use", borrowed)
				if err != nil {
					t.Fatal(err)
				}
				defer otherAssembly.Close(context.Background())
				inbox, _ := invocation.NewInbox[Execution](2, 2*ExecutionEvidenceBytes)
				other, err := BindExecutions(otherAssembly, borrowed, inbox, nil)
				if err != nil {
					t.Fatal(err)
				}
				_, err = executeNative(context.Background(), other, fault.Correlation{Call: "foreign-use"}, Execution{Operation: "fixture.decode"}, func(work context.Context, _ *Execution) (struct{}, error) {
					return struct{}{}, nativeGet(work, new(string))
				})
				if !errors.Is(err, ErrAuthority) {
					t.Fatal("cached result transferred to a different retained Access", err)
				}
			}
			extension.stale = true
			staleGet, _ := makeHandle()
			if err := staleGet(context.Background(), fault.Correlation{Call: "stale-injection"}, new(string)); !errors.Is(err, ErrAuthority) {
				t.Fatal("fresh native poll promoted an expired same-use interceptor value", err)
			}
		})
	}
}

func TestEncodedPollCachedCallbackResultRequiresSameTaskButJoinsAcceptedChild(t *testing.T) {
	client, _, _, _ := encodedPollFixture(t)
	workers, _ := invocation.NewInbox[WorkerResult](1, ExecutionEvidenceBytes)
	tasks, _ := invocation.NewInbox[TaskResult](2, 2*ExecutionEvidenceBytes)
	call, err := invocation.Begin(context.Background(), client.access, invocation.Request{Name: "worker", Shape: invocation.Session,
		Correlation: fault.Correlation{Call: "worker"}, Bytes: client.owner.settings.reservation(), EvidenceBytes: ExecutionEvidenceBytes,
		Admission: invocation.Budget{Limit: time.Second}}, workers, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer call.Complete(invocation.Outcome[WorkerResult]{Present: true})
	worker := &Worker{client: client, call: call, tasks: tasks, handlers: make(chan struct{}, 1), correlation: fault.Correlation{Call: "worker"}}
	native := client.owner.native.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "activity", RunID: "run"})
	for index := range 2 {
		task, binding, err := worker.beginTask(context.Background(), TaskResult{Kind: "activity"})
		if err != nil {
			t.Fatal(err)
		}
		borrower := &callbackClient{binding: binding, nativeClient: client.owner.native}
		_, err = callbackNative(context.Background(), borrower, "", Execution{Operation: "callback.fixture.get"}, func(work context.Context, _ *Execution) (struct{}, error) {
			binding.closed.Store(true)
			var output string
			err := native.Get(work, &output)
			if index == 0 && (err != nil || output != "poll-result") {
				t.Error("accepted callback child was discarded when parent returned", err)
			}
			return struct{}{}, err
		})
		task.Complete(invocation.Outcome[TaskResult]{Present: true})
		binding.finish()
		<-worker.handlers
		if index == 0 && err != nil || index == 1 && !errors.Is(err, ErrAuthority) {
			t.Fatal("cached callback result task identity changed", index, err)
		}
	}
}
