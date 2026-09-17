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
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
)

type returnedReviewDecoder struct {
	converter.DataConverter
	calls atomic.Int32
}

type joinedResultValue struct {
	entered, release chan struct{}
	reads            int
}

func (*joinedResultValue) HasValue() bool { return true }

func (value *joinedResultValue) Get(output any) error {
	value.reads++
	close(value.entered)
	<-value.release
	*output.(*int) = 73
	return nil
}

func TestInterceptorReturnedDecodeIsJoinedBeforeInvocationEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		owner := &connection{}
		factory := &clientFactory{owner: owner, setup: &owner.setup}
		authority := &nativeCall{owner: owner, scopeOwner: sdk.NewFathomryScopeOwnerV1()}
		ctx := context.WithValue(context.Background(), nativeCallKey{}, authority)
		outer := newClientContinuation[any, nativePollOutput](factory, &interceptor.ClientOutboundInterceptorBase{}, nil, false)
		inner := newClientContinuation[any, nativePollOutput](factory, &interceptor.ClientOutboundInterceptorBase{}, nil, true)
		value := &joinedResultValue{entered: make(chan struct{}), release: make(chan struct{})}
		invocationDone, decodeDone, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var borrowed converter.EncodedValue
		var decoded int
		var decodeError error
		go func() {
			_, _ = clientContinue(ctx, outer, func(work context.Context) (struct{}, error) {
				borrowed = inner.resultValue(work, value)
				go func() { defer close(decodeDone); decodeError = borrowed.Get(&decoded) }()
				<-value.entered
				close(returned)
				return struct{}{}, nil
			})
			authority.closed.Store(true)
			close(invocationDone)
		}()
		<-returned
		synctest.Wait()
		select {
		case <-invocationDone:
			t.Fatal("invocation ended before entered decoder exited")
		default:
		}
		close(value.release)
		<-decodeDone
		<-invocationDone
		if decodeError != nil || decoded != 73 || value.reads != 1 {
			t.Fatal("admitted decoder control failed", decodeError)
		}
		if err := borrowed.Get(&decoded); !errors.Is(err, ErrAuthority) || value.reads != 1 {
			t.Fatal("retained decoder reopened ended invocation", err)
		}
	})
}

func (decoder *returnedReviewDecoder) FromPayloads(payloads *commonpb.Payloads, values ...any) error {
	decoder.calls.Add(1)
	return decoder.DataConverter.FromPayloads(payloads, values...)
}

func (decoder *returnedReviewDecoder) FromPayload(payload *commonpb.Payload, value any) error {
	decoder.calls.Add(1)
	return decoder.DataConverter.FromPayload(payload, value)
}

type returnedReviewInterceptor struct {
	interceptor.ClientInterceptorBase
	query       converter.EncodedValue
	update      sdk.WorkflowUpdateHandle
	poll        *interceptor.ClientPollActivityResultOutput
	description *interceptor.ClientDescribeWorkflowOutput
	live        func(func(any) error)
}

func (extension *returnedReviewInterceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return &returnedReviewOutbound{ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next}, saved: extension}
}

type returnedReviewOutbound struct {
	interceptor.ClientOutboundInterceptorBase
	saved *returnedReviewInterceptor
}

func (extension *returnedReviewOutbound) QueryWorkflow(ctx context.Context, input *interceptor.ClientQueryWorkflowInput) (converter.EncodedValue, error) {
	result, err := extension.Next.QueryWorkflow(ctx, input)
	extension.saved.query = result
	if result != nil {
		extension.saved.live(result.Get)
	}
	return result, err
}

func (extension *returnedReviewOutbound) UpdateWorkflow(ctx context.Context, input *interceptor.ClientUpdateWorkflowInput) (sdk.WorkflowUpdateHandle, error) {
	result, err := extension.Next.UpdateWorkflow(ctx, input)
	extension.saved.update = result
	if result != nil {
		extension.saved.live(func(output any) error { return result.Get(ctx, output) })
	}
	return result, err
}

func (extension *returnedReviewOutbound) PollActivityResult(ctx context.Context, input *interceptor.ClientPollActivityResultInput) (*interceptor.ClientPollActivityResultOutput, error) {
	result, err := extension.Next.PollActivityResult(ctx, input)
	extension.saved.poll = result
	if result != nil {
		if result.Result != nil {
			extension.saved.live(result.Result.Get)
		}
		var application *sdktemporal.ApplicationError
		if errors.As(result.Error, &application) {
			extension.saved.live(func(output any) error { return application.Details(output) })
		}
	}
	return result, err
}

func (extension *returnedReviewOutbound) DescribeWorkflow(ctx context.Context, input *interceptor.ClientDescribeWorkflowInput) (*interceptor.ClientDescribeWorkflowOutput, error) {
	result, err := extension.Next.DescribeWorkflow(ctx, input)
	extension.saved.description = result
	if result != nil {
		extension.saved.live(func(output any) error { return result.Response.GetMemoValue("field", output) })
	}
	return result, err
}

func TestReviewInterceptorReturnedNativeValuesExpireBeforeDecode(t *testing.T) {
	for _, mode := range []string{"query-value", "completed-update", "poll-result", "poll-error-details", "workflow-description-memo"} {
		t.Run(mode, func(t *testing.T) {
			decoder := &returnedReviewDecoder{DataConverter: converter.GetDefaultDataConverter()}
			payloads, err := converter.GetDefaultDataConverter().ToPayloads(73)
			if err != nil {
				t.Fatal(err)
			}
			originalFailure := &failurepb.Failure{Message: "controlled", Source: "review",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					Type: "review", Details: payloads,
				}},
			}
			var rpcCalls atomic.Int32
			peer := func(_ context.Context, _ string, _, reply any, _ *grpc.ClientConn, _ grpc.UnaryInvoker, _ ...grpc.CallOption) error {
				rpcCalls.Add(1)
				switch output := reply.(type) {
				case *workflowservice.GetSystemInfoResponse:
					output.Capabilities = &workflowservice.GetSystemInfoResponse_Capabilities{}
				case *workflowservice.QueryWorkflowResponse:
					output.QueryResult = payloads
				case *workflowservice.UpdateWorkflowExecutionResponse:
					output.Stage = enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
					output.UpdateRef = &updatepb.UpdateRef{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow", RunId: "run"}, UpdateId: "update"}
					output.Outcome = &updatepb.Outcome{Value: &updatepb.Outcome_Success{Success: payloads}}
				case *workflowservice.PollActivityExecutionResponse:
					if mode == "poll-error-details" {
						output.Outcome = &activitypb.ActivityExecutionOutcome{Value: &activitypb.ActivityExecutionOutcome_Failure{Failure: originalFailure}}
					} else {
						output.Outcome = &activitypb.ActivityExecutionOutcome{Value: &activitypb.ActivityExecutionOutcome_Result{Result: payloads}}
					}
				case *workflowservice.DescribeWorkflowExecutionResponse:
					output.WorkflowExecutionInfo = &workflowpb.WorkflowExecutionInfo{
						Execution: &commonpb.WorkflowExecution{WorkflowId: "workflow", RunId: "run"},
						Type:      &commonpb.WorkflowType{Name: "review"}, TaskQueue: "review",
						SearchAttributes: &commonpb.SearchAttributes{},
						Memo:             &commonpb.Memo{Fields: map[string]*commonpb.Payload{"field": payloads.Payloads[0]}},
					}
				default:
					return fmt.Errorf("unexpected fake peer response type %T", reply)
				}
				return nil
			}
			owner := &connection{settings: settings{Namespace: "review", MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20}}
			saved := &returnedReviewInterceptor{}
			var liveError error
			var liveCount int
			saved.live = func(decode func(any) error) {
				var value int
				if err := decode(&value); err != nil || value != 73 {
					liveError = fmt.Errorf("live control: value=%d err=%v", value, err)
				}
				liveCount++
			}
			factory := &clientFactory{native: saved, owner: owner, setup: &owner.setup}
			native, err := sdk.NewLazyClient(sdk.Options{
				Namespace: "review", HostPort: "127.0.0.1:1", Logger: disabledLogger{},
				WorkerHeartbeatInterval: -1, DisableWorkerEnvironmentInfo: true,
				DataConverter: decoder, FailureConverter: sdktemporal.NewDefaultFailureConverter(sdktemporal.DefaultFailureConverterOptions{DataConverter: decoder}),
				Interceptors: []interceptor.ClientInterceptor{factory},
				ConnectionOptions: sdk.ConnectionOptions{DialOptions: []grpc.DialOption{grpc.WithNoProxy(),
					grpc.WithUnaryInterceptor(owner.capture), grpc.WithChainUnaryInterceptor(peer)}},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer native.Close()
			owner.native = native
			owner.ready.Store(true)
			authority := &nativeCall{owner: owner, scopeOwner: sdk.NewFathomryScopeOwnerV1()}
			ctx := context.WithValue(context.Background(), nativeCallKey{}, authority)
			var decode func(any) error
			switch mode {
			case "query-value":
				var result converter.EncodedValue
				result, err = native.QueryWorkflow(ctx, "workflow", "run", "query")
				if err == nil {
					saved.live(result.Get)
					decode = saved.query.Get
				}
			case "completed-update":
				var result sdk.WorkflowUpdateHandle
				result, err = native.UpdateWorkflow(ctx, sdk.UpdateWorkflowOptions{WorkflowID: "workflow", RunID: "run", UpdateID: "update", UpdateName: "review", WaitForStage: sdk.WorkflowUpdateStageCompleted})
				if err == nil {
					saved.live(func(output any) error { return result.Get(ctx, output) })
					decode = func(output any) error { return saved.update.Get(context.Background(), output) }
				}
			case "poll-result", "poll-error-details":
				handle := native.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "activity", RunID: "run"})
				var ignored int
				err = handle.Get(ctx, &ignored)
				if mode == "poll-error-details" {
					var application *sdktemporal.ApplicationError
					if saved.poll == nil || !errors.As(saved.poll.Error, &application) || application.Type() != "review" {
						t.Fatal("native failure control did not reach captured output")
					}
					decode = func(output any) error { return application.Details(output) }
					err = nil
				} else if err == nil {
					beforePoll := rpcCalls.Load()
					saved.live(func(output any) error { return handle.Get(ctx, output) })
					if rpcCalls.Load() != beforePoll {
						t.Fatal("cached native Get unexpectedly repeated RPC")
					}
					decode = saved.poll.Result.Get
				}
			case "workflow-description-memo":
				_, err = native.DescribeWorkflow(ctx, "workflow", "run")
				if err == nil {
					decode = func(output any) error { return saved.description.Response.GetMemoValue("field", output) }
				}
			}
			if err != nil || decode == nil {
				t.Fatalf("native admitted setup failed: %v", err)
			}
			var value int
			if liveCount == 0 || liveError != nil {
				t.Fatalf("live native decode control failed: count=%d error=%v", liveCount, liveError)
			}
			before := decoder.calls.Load()
			if before < 1 {
				t.Fatal("normal control never used configured converter")
			}
			beforeRPC := rpcCalls.Load()
			authority.closed.Store(true)
			native.Close()
			value = 0
			late := decode(&value)
			if !errors.Is(late, ErrAuthority) || decoder.calls.Load() != before || value != 0 {
				t.Errorf("returned interceptor %s invoked cached decoder after scope/client close: value=%d reads=%d->%d error=%v", mode, value, before, decoder.calls.Load(), late)
			}
			if rpcCalls.Load() != beforeRPC {
				t.Fatal("counterexample was not a cached no-RPC decode")
			}
		})
	}
}

func TestNativeIdentitySnapshotProtocolFieldsOrderAndBounds(t *testing.T) {
	for _, test := range []struct {
		name              string
		request, response any
		want              Execution
	}{
		{"workflow", &workflowservice.SignalWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow", RunId: "run"}}, nil, Execution{WorkflowID: "workflow", RunID: "run"}},
		{"activity", &workflowservice.StartActivityExecutionRequest{ActivityId: "activity"}, &workflowservice.StartActivityExecutionResponse{RunId: "run"}, Execution{ActivityID: "activity", RunID: "run"}},
		{"standalone-completion", &workflowservice.RespondActivityTaskCompletedByIdRequest{ResourceId: "standalone", RunId: "run"}, nil, Execution{ActivityID: "standalone", RunID: "run"}},
		{"nexus", &workflowservice.StartNexusOperationExecutionRequest{OperationId: "operation"}, &workflowservice.StartNexusOperationExecutionResponse{RunId: "run"}, Execution{NexusOperationID: "operation", RunID: "run"}},
		{"schedule", &workflowservice.CreateScheduleRequest{ScheduleId: "schedule"}, nil, Execution{ScheduleID: "schedule"}},
		{"deployment", &workflowservice.UpdateWorkerDeploymentVersionMetadataRequest{DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build"}}, nil, Execution{DeploymentName: "deployment"}},
		{"opaque-token", &workflowservice.RespondActivityTaskCompletedRequest{TaskToken: []byte("opaque-fixture")}, nil, Execution{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var observed observedNativeIdentity
			sequence := observed.begin(test.request)
			observed.complete(sequence, test.response)
			var got Execution
			if observed.apply(&got) || got != test.want {
				t.Fatal("native protocol identity differs from independent fixture", got)
			}
		})
	}
	t.Run("late-response", func(t *testing.T) {
		var observed observedNativeIdentity
		first := observed.begin(&workflowservice.StartWorkflowExecutionRequest{WorkflowId: "first"})
		second := observed.begin(&workflowservice.StartWorkflowExecutionRequest{WorkflowId: "second"})
		observed.complete(second, &workflowservice.StartWorkflowExecutionResponse{RunId: "second-run"})
		observed.complete(first, &workflowservice.StartWorkflowExecutionResponse{RunId: "late-first-run"})
		var got Execution
		if observed.apply(&got) || got.WorkflowID != "second" || got.RunID != "second-run" {
			t.Fatal("late response overwrote latest observed attempt")
		}
	})
	t.Run("empty-latest-run", func(t *testing.T) {
		var observed observedNativeIdentity
		observed.begin(&workflowservice.SignalWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow", RunId: "old-run"}})
		observed.begin(&workflowservice.SignalWorkflowExecutionRequest{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow"}})
		got := Execution{RunID: "stale-wrapper-run"}
		if observed.apply(&got) || got.RunID != "" {
			t.Fatal("latest-run selection retained an obsolete run ID")
		}
	})
	t.Run("bounded-retention", func(t *testing.T) {
		var observed observedNativeIdentity
		observed.begin(&workflowservice.StartWorkflowExecutionRequest{WorkflowId: "bounded"})
		observed.begin(&workflowservice.StartWorkflowExecutionRequest{WorkflowId: strings.Repeat("x", 1025)})
		if observed.identity.values[identityWorkflow] != "bounded" {
			t.Fatal("oversized ID was retained in the evidence snapshot")
		}
		var got Execution
		if !observed.apply(&got) {
			t.Fatal("oversized observed identity did not require intention fallback")
		}
	})
}
