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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNativeWorkflowHandlesRetainEvidenceAndRejectClosedSource(t *testing.T) {
	fixture := newFixture(t, 1)
	executions, inbox := executionBinding(t, fixture)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	run, err := executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "start"}, sdk.StartWorkflowOptions{ID: "requested", TaskQueue: "unit"}, "definition")
	if err != nil {
		t.Fatal(err)
	}
	if run.GetID() != "requested" || run.GetRunID() != "observed-run" {
		t.Fatal("native start identities changed")
	}
	var result string
	if err := run.Get(ctx, fault.Correlation{Call: "result"}, &result); err != nil {
		t.Fatal(err)
	}
	if result != "expected-result" {
		t.Fatal("native execution result was not decoded")
	}
	for _, want := range []string{"workflow.start", "workflow.result"} {
		delivery, err := inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		evidence, err := delivery.Receipt().WaitReleased(ctx)
		if err != nil || evidence.Err() != nil {
			t.Fatal("execution evidence not complete")
		}
		if evidence.Outcome.Value.Operation != want || evidence.Outcome.Value.WorkflowID != "requested" || evidence.Outcome.Value.RunID != "observed-run" {
			t.Fatal("execution identity/evidence mismatch")
		}
		if want == "workflow.start" && !evidence.Outcome.Value.Accepted || want == "workflow.result" && !evidence.Outcome.Value.ResultObtained {
			t.Fatal("start and result evidence conflated")
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	err = run.Get(ctx, fault.Correlation{Call: "closed"}, &result)
	if err == nil || errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("retained native handle bypassed source lifetime")
	}
	var empty *temporal.WorkflowRun
	if err := empty.Get(ctx, fault.Correlation{Call: "nil"}, &result); !errors.Is(err, temporal.ErrInput) {
		t.Fatal("nil handle was accepted")
	}
}

func receiveExecution(t *testing.T, inbox *invocation.Inbox[temporal.Execution]) invocation.Result[temporal.Execution] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	delivery, err := inbox.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := delivery.Receipt().WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestDetachedWorkflowIdentityDoesNotDial(t *testing.T) {
	var descriptions, histories atomic.Int32
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.describe = func(_ context.Context, request *workflowservice.DescribeWorkflowExecutionRequest) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
			descriptions.Add(1)
			if request.Namespace != "test" || request.Execution.WorkflowId != "detached" || request.Execution.RunId != "" {
				return nil, errors.New("wrong lazy execution target")
			}
			return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: "detached", RunId: "latest-run"}}}, nil
		}
		peer.history = func(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
			histories.Add(1)
			if request.Execution.RunId != "latest-run" {
				return nil, errors.New("lazy execution identity was resolved outside admission")
			}
			return (&rpcServer{}).GetWorkflowExecutionHistory(ctx, request)
		}
	})
	executions, inbox := executionBinding(t, fixture)
	run, err := executions.GetWorkflow("detached", "")
	if err != nil {
		t.Fatal(err)
	}
	if run.GetID() != "detached" || run.GetRunID() != "" || run.GetFirstExecutionRunID() != "" || descriptions.Load() != 0 || histories.Load() != 0 {
		t.Fatal("an identity getter performed I/O or invented identity")
	}
	var result string
	if err := run.Get(context.Background(), fault.Correlation{Call: "resolve"}, &result); err != nil {
		t.Fatalf("controlled result failed (descriptions=%d histories=%d): %v", descriptions.Load(), histories.Load(), err)
	}
	if result != "expected-result" || run.GetRunID() != "latest-run" || descriptions.Load() != 1 || histories.Load() != 1 {
		t.Fatal("detached handle did not resolve under the controlled execution")
	}
	record := receiveExecution(t, inbox)
	if record.Outcome.Value.RunID != "latest-run" || !record.Outcome.Value.ResultObtained {
		t.Fatal("resolved execution identity was absent from evidence")
	}
}

func TestWorkflowFollowingSerializesNativeHandleAndCancelsWaiter(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var first sync.Once
	var reads atomic.Int32
	fixture := newFixture(t, 1, func(options *temporal.OptionsV1, limits *resource.Limits, peer *rpcServer) {
		options.MaxActive, limits.Active, limits.Bytes = 2, 2, 2*limits.Bytes
		peer.history = func(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
			reads.Add(1)
			if request.Execution.RunId == "initial" {
				first.Do(func() { close(entered) })
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{EventId: 5,
					EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
					Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{NewExecutionRunId: "successor"}},
				}}}}, nil
			}
			if request.Execution.RunId != "successor" {
				return nil, errors.New("unexpected execution-chain target")
			}
			return (&rpcServer{}).GetWorkflowExecutionHistory(ctx, request)
		}
	})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	executions, inbox := executionBinding(t, fixture)
	run, err := executions.GetWorkflow("chain", "initial")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	var result string
	go func() { done <- run.Get(ctx, fault.Correlation{Call: "follow"}, &result) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("result observation did not start")
	}
	short, stopWaiting := context.WithTimeout(ctx, 20*time.Millisecond)
	err = run.Get(short, fault.Correlation{Call: "waiting"}, nil)
	stopWaiting()
	if !errors.Is(err, context.DeadlineExceeded) || reads.Load() != 1 {
		t.Fatal("same-handle waiter escaped serialization or ignored cancellation")
	}
	if run.GetRunID() != "initial" {
		t.Fatal("getter blocked behind observation or invented a successor")
	}
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for range 10000 {
			_ = run.GetRunID()
			_ = run.GetFirstExecutionRunID()
		}
	}()
	releaseOnce.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-readDone
	if result != "expected-result" || run.GetRunID() != "successor" || reads.Load() != 2 {
		t.Fatal("native continuation following changed")
	}
	strict, err := executions.GetWorkflow("chain", "initial")
	if err != nil {
		t.Fatal(err)
	}
	err = strict.GetWithOptions(ctx, fault.Correlation{Call: "strict"}, nil, sdk.WorkflowRunGetOptions{DisableFollowingRuns: true})
	var continued *workflow.ContinueAsNewError
	if !errors.As(err, &continued) || strict.GetRunID() != "initial" || reads.Load() != 3 {
		t.Fatal("strict result observation lost native Continue-As-New semantics")
	}
	for range 3 {
		record := receiveExecution(t, inbox)
		value := record.Outcome.Value
		switch record.Context.Correlation.Call {
		case "follow":
			if record.Err() != nil || value.RunID != "successor" || !value.ResultObtained {
				t.Fatal("chain completion evidence missing")
			}
		case "waiting":
			if value.NativeCalled || !errors.Is(record.Err(), context.DeadlineExceeded) {
				t.Fatal("canceled serialized wait claimed native entry")
			}
		case "strict":
			if !errors.As(record.Err(), &continued) || value.ResultObtained {
				t.Fatal("handled continuation error disappeared from evidence")
			}
		default:
			t.Fatal("unexpected workflow observation")
		}
	}
}

func TestExecutionEvidenceBoundsReturnedIdentity(t *testing.T) {
	fixture := newFixture(t, 1, func(options *temporal.OptionsV1, limits *resource.Limits, peer *rpcServer) {
		options.MaxResponseBytes = 4096
		limits.Bytes += 3072
		peer.describe = func(context.Context, *workflowservice.DescribeWorkflowExecutionRequest) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
			return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{RunId: strings.Repeat("x", 1025)}}}, nil
		}
	})
	executions, inbox := executionBinding(t, fixture)
	run, err := executions.GetWorkflow("bounded", "")
	if err != nil {
		t.Fatal(err)
	}
	err = run.Get(context.Background(), fault.Correlation{Call: "oversized-identity"}, nil)
	if !errors.Is(err, temporal.ErrLimit) {
		t.Fatal("oversized observed identity was retained without a limit error")
	}
	record := receiveExecution(t, inbox)
	if !record.Outcome.Value.IdentityOmitted || record.Outcome.Value.RunID != "" || record.Outcome.Value.WorkflowID != "bounded" || !errors.Is(record.Err(), temporal.ErrLimit) {
		t.Fatal("identity overflow erased the original intention or exceeded evidence capacity")
	}
}

type interactionInput struct{ Value, Phase int }

func interactionWorkflow(ctx workflow.Context, input interactionInput) (int, error) {
	if input.Phase == 1 {
		var marker int
		if err := workflow.SideEffect(ctx, func(workflow.Context) any { return 7 }).Get(&marker); err != nil {
			return 0, err
		}
		version := workflow.GetVersion(ctx, "interaction-child-v1", workflow.DefaultVersion, 1)
		if version != 1 {
			return 0, errors.New("unexpected version marker")
		}
		childContext := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID + "-child"})
		var child int
		if err := workflow.ExecuteChildWorkflow(childContext, "interaction-child", input.Value).Get(ctx, &child); err != nil {
			return 0, err
		}
		return child + marker, nil
	}
	value := input.Value
	if err := workflow.SetQueryHandler(ctx, "value", func() (int, error) { return value, nil }); err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, "add", func(_ workflow.Context, delta int) (int, error) {
		value += delta
		return value, nil
	}, workflow.UpdateHandlerOptions{Validator: func(delta int) error {
		if delta < 1 {
			return errors.New("delta must be positive")
		}
		return nil
	}}); err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandler(ctx, "deferred", func(updateContext workflow.Context, delta int) (int, error) {
		var release bool
		workflow.GetSignalChannel(updateContext, "release-update").Receive(updateContext, &release)
		value += delta
		return value, nil
	}); err != nil {
		return 0, err
	}
	var finish bool
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, &finish)
	if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return 0, err
	}
	return 0, workflow.NewContinueAsNewError(ctx, "interaction-flow", interactionInput{Value: value, Phase: 1})
}

func interactionChild(ctx workflow.Context, value int) (int, error) {
	if err := workflow.Sleep(ctx, 5*time.Millisecond); err != nil {
		return 0, err
	}
	return value * 2, nil
}

func incompatibleInteraction(ctx workflow.Context, input interactionInput) (int, error) {
	if input.Phase == 1 {
		if err := workflow.Sleep(ctx, time.Hour); err != nil {
			return 0, err
		}
	}
	return interactionWorkflow(ctx, input)
}

func signalStartWorkflow(ctx workflow.Context) (string, error) {
	var value string
	workflow.GetSignalChannel(ctx, "value").Receive(ctx, &value)
	return value, nil
}

func TestDeterministicInteractionChildAndMarkers(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	suite.SetLogger(executionLogger{})
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflowWithOptions(interactionWorkflow, workflow.RegisterOptions{Name: "interaction-flow"})
	environment.RegisterWorkflowWithOptions(interactionChild, workflow.RegisterOptions{Name: "interaction-child"})
	environment.ExecuteWorkflow("interaction-flow", interactionInput{Value: 7, Phase: 1})
	if !environment.IsWorkflowCompleted() || environment.GetWorkflowError() != nil {
		t.Fatal("native deterministic environment did not complete")
	}
	var result int
	if err := environment.GetWorkflowResult(&result); err != nil || result != 21 {
		t.Fatal("child/timer/version/side-effect result disagreed with the independent oracle")
	}
}

func updateResult(value string) *updatepb.Outcome {
	return &updatepb.Outcome{Value: &updatepb.Outcome_Success{Success: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
		Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(value),
	}}}}}
}

func combinedReply(workflowID, updateID string) *workflowservice.ExecuteMultiOperationResponse {
	return &workflowservice.ExecuteMultiOperationResponse{Responses: []*workflowservice.ExecuteMultiOperationResponse_Response{
		{Response: &workflowservice.ExecuteMultiOperationResponse_Response_StartWorkflow{StartWorkflow: &workflowservice.StartWorkflowExecutionResponse{RunId: "combined-run", FirstExecutionRunId: "combined-run"}}},
		{Response: &workflowservice.ExecuteMultiOperationResponse_Response_UpdateWorkflow{UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionResponse{
			Stage:     enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
			UpdateRef: &updatepb.UpdateRef{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: "combined-run"}, UpdateId: updateID},
		}}},
	}}
}

func TestUpdateWithStartRetainsAcknowledgementAfterWaitCancellation(t *testing.T) {
	var starts, polls atomic.Int32
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.multi = func(_ context.Context, request *workflowservice.ExecuteMultiOperationRequest) (*workflowservice.ExecuteMultiOperationResponse, error) {
			starts.Add(1)
			if request.Namespace != "test" || len(request.Operations) != 2 ||
				request.Operations[0].GetStartWorkflow().GetWorkflowId() != "combined" ||
				request.Operations[1].GetUpdateWorkflow().GetRequest().GetMeta().GetUpdateId() != "update" {
				return nil, status.Error(codes.InvalidArgument, "incorrect combined request")
			}
			return combinedReply("combined", "update"), nil
		}
		peer.pollUpdate = func(ctx context.Context, _ *workflowservice.PollWorkflowExecutionUpdateRequest) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
			polls.Add(1)
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	})
	executions, inbox := executionBinding(t, fixture)
	operation, err := executions.NewWithStartWorkflowOperation(sdk.StartWorkflowOptions{ID: "combined", TaskQueue: "unit",
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING}, "definition")
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = executions.UpdateWithStartWorkflow(short, fault.Correlation{Call: "combined"}, operation,
		sdk.UpdateWorkflowOptions{UpdateID: "update", UpdateName: "change", WaitForStage: sdk.WorkflowUpdateStageCompleted})
	cancel()
	var nativeTimeout *sdk.WorkflowUpdateServiceTimeoutOrCanceledError
	if !errors.As(err, &nativeTimeout) || !errors.Is(err, context.DeadlineExceeded) || starts.Load() != 1 || polls.Load() != 1 {
		t.Fatalf("combined update did not preserve native wait cancellation: %v", err)
	}
	record := receiveExecution(t, inbox)
	value := record.Outcome.Value
	if !value.StartAccepted || value.Accepted || value.ResultObtained || value.RunID != "combined-run" ||
		value.UpdateStage != enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED || !errors.As(record.Err(), &nativeTimeout) {
		t.Fatal("acknowledged start or accepted Update stage was erased by the later wait failure")
	}
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	run, err := operation.Get(ctx, fault.Correlation{Call: "start-result"})
	if err != nil || run.GetID() != "combined" || run.GetRunID() != "combined-run" || run.GetFirstExecutionRunID() != "combined-run" {
		t.Fatal("independent native start result was lost")
	}
	again, err := operation.Get(ctx, fault.Correlation{Call: "start-result-again"})
	if err != nil || again != run {
		t.Fatal("one native run acquired competing mutable handle owners")
	}
	_, err = executions.UpdateWithStartWorkflow(ctx, fault.Correlation{Call: "reuse"}, operation,
		sdk.UpdateWorkflowOptions{UpdateID: "update", UpdateName: "change", WaitForStage: sdk.WorkflowUpdateStageAccepted})
	if !errors.Is(err, temporal.ErrInput) || starts.Load() != 1 {
		t.Fatal("a consumed combined start intention was sent again")
	}
	for range 3 {
		record := receiveExecution(t, inbox)
		if record.Context.Correlation.Call == "reuse" && (record.Outcome.Value.NativeCalled || record.Attempts.Observed != 0) {
			t.Fatal("local duplicate rejection claimed a native call")
		}
	}
}

func TestUpdateResponseLossCanBeObservedWithoutResubmission(t *testing.T) {
	requests := make(chan *workflowservice.UpdateWorkflowExecutionRequest, 1)
	var sends, reads atomic.Int32
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.update = func(_ context.Context, request *workflowservice.UpdateWorkflowExecutionRequest) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
			sends.Add(1)
			requests <- request
			return nil, status.Error(codes.DeadlineExceeded, "accepted but response lost")
		}
		peer.pollUpdate = func(_ context.Context, request *workflowservice.PollWorkflowExecutionUpdateRequest) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
			reads.Add(1)
			if request.Namespace != "test" || request.UpdateRef.WorkflowExecution.WorkflowId != "target" || request.UpdateRef.WorkflowExecution.RunId != "run" || request.UpdateRef.UpdateId == "" {
				return nil, status.Error(codes.InvalidArgument, "wrong recovered update")
			}
			return &workflowservice.PollWorkflowExecutionUpdateResponse{Outcome: updateResult("\"updated\"")}, nil
		}
	})
	executions, inbox := executionBinding(t, fixture)
	_, err := executions.UpdateWorkflow(context.Background(), fault.Correlation{Call: "unknown"},
		sdk.UpdateWorkflowOptions{WorkflowID: "target", RunID: "run", UpdateName: "change", WaitForStage: sdk.WorkflowUpdateStageAccepted})
	var nativeTimeout *sdk.WorkflowUpdateServiceTimeoutOrCanceledError
	if !errors.As(err, &nativeTimeout) || sends.Load() != 1 {
		t.Fatal("response-loss control did not preserve the native uncertainty")
	}
	request := <-requests
	record := receiveExecution(t, inbox)
	if !record.Outcome.Value.NativeCalled || record.Outcome.Value.Accepted || record.Outcome.Value.UpdateID == "" ||
		record.Outcome.Value.UpdateID != request.Request.Meta.UpdateId || !errors.As(record.Err(), &nativeTimeout) {
		t.Fatal("handled response loss lost the generated update intention")
	}
	handle, err := executions.GetWorkflowUpdateHandle(sdk.GetWorkflowUpdateHandleOptions{WorkflowID: "target", RunID: "run", UpdateID: record.Outcome.Value.UpdateID})
	if err != nil {
		t.Fatal(err)
	}
	var result string
	if err := handle.Get(context.Background(), fault.Correlation{Call: "recovered"}, &result); err != nil {
		t.Fatal(err)
	}
	if result != "updated" || sends.Load() != 1 || reads.Load() != 1 {
		t.Fatal("result recovery resubmitted or changed the Update")
	}
	recovered := receiveExecution(t, inbox)
	if !recovered.Outcome.Value.ResultObtained || recovered.Outcome.Value.UpdateStage != enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED {
		t.Fatal("recovered Update completion fact missing")
	}
}

func TestUpdateFailureRemainsNativeAndIndependentlyRetained(t *testing.T) {
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.update = func(context.Context, *workflowservice.UpdateWorkflowExecutionRequest) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
			return &workflowservice.UpdateWorkflowExecutionResponse{
				Stage:     enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
				UpdateRef: &updatepb.UpdateRef{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "target", RunId: "run"}, UpdateId: "rejected"},
				Outcome: &updatepb.Outcome{Value: &updatepb.Outcome_Failure{Failure: &failurepb.Failure{Message: "synthetic rejection",
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "denied-update", NonRetryable: true}}}}},
			}, nil
		}
	})
	executions, inbox := executionBinding(t, fixture)
	update, err := executions.UpdateWorkflow(context.Background(), fault.Correlation{Call: "request"},
		sdk.UpdateWorkflowOptions{WorkflowID: "target", RunID: "run", UpdateID: "rejected", UpdateName: "change", WaitForStage: sdk.WorkflowUpdateStageCompleted})
	if err != nil {
		t.Fatal(err)
	}
	request := receiveExecution(t, inbox)
	if !request.Outcome.Value.Accepted || request.Outcome.Value.ResultObtained || request.Outcome.Value.UpdateStage != enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED {
		t.Fatal("request acknowledgement was confused with the application result")
	}
	err = update.Get(context.Background(), fault.Correlation{Call: "rejection-result"}, nil)
	var application *sdktemporal.ApplicationError
	if !errors.As(err, &application) || application.Type() != "denied-update" || !application.NonRetryable() {
		t.Fatal("native failure semantics changed")
	}
	record := receiveExecution(t, inbox)
	if !errors.As(record.Err(), &application) || record.Outcome.Value.ResultObtained {
		t.Fatal("handled application rejection disappeared")
	}
}

func TestUpdateWithStartAdmissionRefusalDoesNotConsumeIntention(t *testing.T) {
	var starts atomic.Int32
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.multi = func(context.Context, *workflowservice.ExecuteMultiOperationRequest) (*workflowservice.ExecuteMultiOperationResponse, error) {
			starts.Add(1)
			return combinedReply("bounded", "update"), nil
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
	ctx := context.Background()
	_, err = executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "fill"}, sdk.StartWorkflowOptions{ID: "fill", TaskQueue: "unit"}, "definition")
	if err != nil {
		t.Fatal(err)
	}
	operation, err := executions.NewWithStartWorkflowOperation(sdk.StartWorkflowOptions{ID: "bounded", TaskQueue: "unit",
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING}, "definition")
	if err != nil {
		t.Fatal(err)
	}
	options := sdk.UpdateWorkflowOptions{UpdateID: "update", UpdateName: "change", WaitForStage: sdk.WorkflowUpdateStageAccepted}
	_, err = executions.UpdateWithStartWorkflow(ctx, fault.Correlation{Call: "full"}, operation, options)
	if !errors.Is(err, invocation.ErrEvidence) || starts.Load() != 0 {
		t.Fatal("saturated evidence allowed native combined work")
	}
	receiveExecution(t, inbox)
	update, err := executions.UpdateWithStartWorkflow(ctx, fault.Correlation{Call: "admitted"}, operation, options)
	if err != nil || update.RunID() != "combined-run" || starts.Load() != 1 {
		t.Fatal("pre-invocation refusal consumed a reusable start intention")
	}
	receiveExecution(t, inbox)
	other, _ := executionBinding(t, newFixture(t, 1))
	_, err = other.UpdateWithStartWorkflow(ctx, fault.Correlation{Call: "other-source"}, operation, options)
	if !errors.Is(err, temporal.ErrAuthority) {
		t.Fatal("combined intention was retargeted to another source")
	}
}

func TestUpdateWithStartPreservesNativeValidationAndCapabilityRefusal(t *testing.T) {
	for _, test := range []struct {
		name          string
		conflict      enumspb.WorkflowIdConflictPolicy
		stage         sdk.WorkflowUpdateStage
		unimplemented bool
	}{
		{name: "missing-conflict", stage: sdk.WorkflowUpdateStageAccepted},
		{name: "admitted-stage", conflict: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, stage: sdk.WorkflowUpdateStageAdmitted},
		{name: "service-unimplemented", conflict: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, stage: sdk.WorkflowUpdateStageAccepted, unimplemented: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, 1)
			executions, inbox := executionBinding(t, fixture)
			operation, err := executions.NewWithStartWorkflowOperation(sdk.StartWorkflowOptions{ID: "validation", TaskQueue: "unit", WorkflowIDConflictPolicy: test.conflict}, "definition")
			if err != nil {
				t.Fatal(err)
			}
			_, err = executions.UpdateWithStartWorkflow(context.Background(), fault.Correlation{Call: test.name}, operation,
				sdk.UpdateWorkflowOptions{UpdateName: "change", WaitForStage: test.stage})
			if err == nil {
				t.Fatal("native refusal became a successful Update")
			}
			record := receiveExecution(t, inbox)
			var unimplemented *serviceerror.Unimplemented
			if errors.As(err, &unimplemented) != test.unimplemented || errors.As(record.Err(), &unimplemented) != test.unimplemented || record.Outcome.Value.Accepted || record.Outcome.Value.StartAccepted {
				t.Fatal("validation and service capability errors were conflated")
			}
			if !test.unimplemented && record.Attempts.Observed != 0 || test.unimplemented && record.Attempts.Observed != 1 {
				t.Fatal("native refusal reached the wrong execution boundary")
			}
			short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			_, err = operation.Get(short, fault.Correlation{Call: "unobserved-start"})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("missing native start acknowledgement invented a completed start")
			}
		})
	}
}

func TestReviewCallbackNativeCombinedStartAndHistoryControls(t *testing.T) {
	for _, extensions := range []int{0, 2} {
		name := "native"
		runtime := temporal.RuntimeOptions{}
		if extensions != 0 {
			name = "two-noop-interceptors"
			for range extensions {
				runtime.Interceptors = append(runtime.Interceptors, &interceptor.ClientInterceptorBase{})
			}
		}
		t.Run(name, func(t *testing.T) {
			reviewReleasedCallbackFixture(t, "ExecuteMultiOperation", runtime, func(ctx context.Context, client sdk.Client) error {
				intention := client.NewWithStartWorkflowOperation(sdk.StartWorkflowOptions{ID: "combined", TaskQueue: "unit", WorkflowExecutionTimeout: time.Minute,
					WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING}, "definition")
				update, err := client.UpdateWithStartWorkflow(ctx, sdk.UpdateWithStartWorkflowOptions{StartWorkflowOperation: intention,
					UpdateOptions: sdk.UpdateWorkflowOptions{UpdateID: "update", UpdateName: "echo", WaitForStage: sdk.WorkflowUpdateStageCompleted}})
				if err != nil {
					return err
				}
				var value string
				if err := update.Get(ctx, &value); err != nil {
					return err
				}
				if value != "updated" {
					return errors.New("completed combined result changed")
				}
				run, err := intention.Get(ctx)
				if err != nil {
					return err
				}
				if run.GetID() != "combined" || run.GetRunID() != "combined-run" {
					return errors.New("combined native identity changed")
				}
				iterator := client.GetWorkflowHistory(ctx, "combined", "combined-run", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
				for _, expected := range []int64{1, 2} {
					if !iterator.HasNext() {
						return errors.New("native iterator ended before both events")
					}
					event, err := iterator.Next()
					if err != nil {
						return err
					}
					if event.EventId != expected {
						return errors.New("native history event identity changed")
					}
				}
				if iterator.HasNext() {
					return errors.New("native iterator did not end")
				}
				return nil
			}, func(ctx context.Context, request any, next grpc.UnaryHandler) (any, error) {
				switch request.(type) {
				case *workflowservice.ExecuteMultiOperationRequest:
					response := combinedReply("combined", "update")
					response.Responses[1].GetUpdateWorkflow().Stage = enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
					response.Responses[1].GetUpdateWorkflow().Outcome = updateResult("\"updated\"")
					return response, nil
				case *workflowservice.GetWorkflowExecutionHistoryRequest:
					return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{EventId: 1}, {EventId: 2}}}}, nil
				}
				return next(ctx, request)
			})
		})
	}
}

func TestCallbackStartIdentityRetainsEvidenceAfterResponseLoss(t *testing.T) {
	type identity struct{ workflowID, updateID string }
	for _, mode := range []string{"signal-start", "update-start"} {
		for _, explicit := range []bool{false, true} {
			for _, lost := range []bool{false, true} {
				name := mode + "/generated"
				if explicit {
					name = mode + "/explicit"
				}
				if lost {
					name += "/response-lost"
				} else {
					name += "/acknowledged"
				}
				t.Run(name, func(t *testing.T) {
					var seenOnce sync.Once
					seen := make(chan identity, 1)
					fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
						peer.activityName = "callback-start-identity"
						peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
							var target identity
							switch input := request.(type) {
							case *workflowservice.SignalWithStartWorkflowExecutionRequest:
								target.workflowID = input.WorkflowId
							case *workflowservice.ExecuteMultiOperationRequest:
								target.workflowID = input.Operations[0].GetStartWorkflow().WorkflowId
								target.updateID = input.Operations[1].GetUpdateWorkflow().GetRequest().GetMeta().GetUpdateId()
							default:
								return next(ctx, request)
							}
							seenOnce.Do(func() { seen <- target })
							if lost {
								return nil, status.Error(codes.Unavailable, "synthetic response loss after request observation")
							}
							if mode == "signal-start" {
								return &workflowservice.SignalWithStartWorkflowExecutionResponse{RunId: "run"}, nil
							}
							return combinedReply(target.workflowID, target.updateID), nil
						}
					})
					executions, inbox := executionBinding(t, fixture)
					workers, tasks := workerInboxes(t)
					observed := make(chan error, 1)
					body := func(ctx context.Context) error {
						client := activity.GetClient(ctx)
						limit := time.Second
						if lost {
							limit = 80 * time.Millisecond
						}
						ctx, cancel := context.WithTimeout(ctx, limit)
						defer cancel()
						start := sdk.StartWorkflowOptions{TaskQueue: "unit"}
						update := sdk.UpdateWorkflowOptions{UpdateName: "echo", WaitForStage: sdk.WorkflowUpdateStageAccepted}
						if explicit {
							start.ID, update.UpdateID = "explicit-workflow", "explicit-update"
						}
						var err error
						if mode == "signal-start" {
							_, err = client.SignalWithStartWorkflow(ctx, start.ID, "signal", "value", start, "definition")
						} else {
							start.WorkflowIDConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
							operation := client.NewWithStartWorkflowOperation(start, "definition")
							_, err = client.UpdateWithStartWorkflow(ctx, sdk.UpdateWithStartWorkflowOptions{StartWorkflowOperation: operation, UpdateOptions: update})
						}
						observed <- err
						return nil
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					lifetime, stop := context.WithCancel(context.Background())
					defer stop()
					managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "callback-identity"}, temporal.WorkerSpec{
						TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
						Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1},
						Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "callback-start-identity"}}},
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
					case err = <-observed:
					case <-ctx.Done():
						t.Fatal("callback did not finish")
					}
					if err := managed.Stop(ctx); err != nil {
						t.Fatal(err)
					}
					record := receiveExecution(t, inbox)
					releaseServiceEvidence(t, workers)
					releaseServiceEvidence(t, tasks)
					if (err != nil) != lost || (record.Err() != nil) != lost || record.Outcome.Value.Accepted == lost {
						t.Fatal("normal/lost response control or independent error evidence differs", err)
					}
					var requested identity
					select {
					case requested = <-seen:
					default:
						t.Fatal("peer never observed a start request")
					}
					if requested.workflowID == "" || mode == "update-start" && requested.updateID == "" {
						t.Fatal("native request is missing an execution identity")
					}
					if explicit && (requested.workflowID != "explicit-workflow" || mode == "update-start" && requested.updateID != "explicit-update") {
						t.Fatal("explicit native identity changed")
					}
					if record.Outcome.Value.WorkflowID != requested.workflowID || record.Outcome.Value.UpdateID != requested.updateID {
						t.Fatal("submitted callback start identity lost from independent evidence")
					}
				})
			}
		}
	}
}

type updateIdentityPrefix struct {
	interceptor.ClientInterceptorBase
	enabled bool
}
type updateIdentityOutbound struct {
	interceptor.ClientOutboundInterceptorBase
	enabled bool
}

func (extension *updateIdentityPrefix) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return &updateIdentityOutbound{ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next}, enabled: extension.enabled}
}
func (extension *updateIdentityOutbound) UpdateWorkflow(ctx context.Context, input *interceptor.ClientUpdateWorkflowInput) (sdk.WorkflowUpdateHandle, error) {
	copy := *input
	if extension.enabled {
		copy.WorkflowID = "routed-" + copy.WorkflowID
		copy.UpdateID = "routed-" + copy.UpdateID
	}
	return extension.Next.UpdateWorkflow(ctx, &copy)
}

func TestUpdateResponseLossAfterInterceptorRewriteKeepsRecoveryIdentity(t *testing.T) {
	for _, prefixed := range []bool{false, true} {
		name := "unchanged-control"
		wantedWorkflow, wantedUpdate := "target", "update"
		if prefixed {
			name = "native-id-prefix"
			wantedWorkflow, wantedUpdate = "routed-target", "routed-update"
		}
		t.Run(name, func(t *testing.T) {
			requests := make(chan *workflowservice.UpdateWorkflowExecutionRequest, 1)
			var sends, reads atomic.Int32
			fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{Interceptors: []interceptor.ClientInterceptor{&updateIdentityPrefix{enabled: prefixed}}}, nil,
				func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
					peer.update = func(_ context.Context, request *workflowservice.UpdateWorkflowExecutionRequest) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
						sends.Add(1)
						requests <- request
						return nil, status.Error(codes.DeadlineExceeded, "synthetic response loss after request observation")
					}
					peer.pollUpdate = func(_ context.Context, request *workflowservice.PollWorkflowExecutionUpdateRequest) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
						reads.Add(1)
						if request.UpdateRef.WorkflowExecution.WorkflowId != wantedWorkflow || request.UpdateRef.UpdateId != wantedUpdate {
							return nil, status.Error(codes.NotFound, "not the submitted update")
						}
						return &workflowservice.PollWorkflowExecutionUpdateResponse{Outcome: updateResult("\"updated\"")}, nil
					}
				})
			executions, inbox := executionBinding(t, fixture)
			_, err := executions.UpdateWorkflow(context.Background(), fault.Correlation{Call: "unknown"}, sdk.UpdateWorkflowOptions{
				WorkflowID: "target", RunID: "run", UpdateID: "update", UpdateName: "change", WaitForStage: sdk.WorkflowUpdateStageAccepted})
			var nativeTimeout *sdk.WorkflowUpdateServiceTimeoutOrCanceledError
			if !errors.As(err, &nativeTimeout) || sends.Load() != 1 {
				t.Fatal("native lost-response control failed", err)
			}
			request := <-requests
			if request.WorkflowExecution.WorkflowId != wantedWorkflow || request.Request.Meta.UpdateId != wantedUpdate {
				t.Fatal("native interceptor did not submit independently expected prefixed IDs")
			}
			record := receiveExecution(t, inbox)
			handle, err := executions.GetWorkflowUpdateHandle(sdk.GetWorkflowUpdateHandleOptions{
				WorkflowID: record.Outcome.Value.WorkflowID, RunID: record.Outcome.Value.RunID, UpdateID: record.Outcome.Value.UpdateID})
			if err != nil {
				t.Fatal(err)
			}
			var result string
			recoveryErr := handle.Get(context.Background(), fault.Correlation{Call: "evidence-recovery"}, &result)
			receiveExecution(t, inbox)
			if recoveryErr != nil {
				correct, err := executions.GetWorkflowUpdateHandle(sdk.GetWorkflowUpdateHandleOptions{WorkflowID: wantedWorkflow, RunID: "run", UpdateID: wantedUpdate})
				if err != nil {
					t.Fatal(err)
				}
				if err := correct.Get(context.Background(), fault.Correlation{Call: "wire-identity-control"}, &result); err != nil || result != "updated" {
					t.Fatal("submitted-identity recovery control failed", err)
				}
				receiveExecution(t, inbox)
				t.Fatalf("evidence recovery targets wrong Update after native ID rewrite: recorded=%q/%q submitted=%q/%q error=%v", record.Outcome.Value.WorkflowID, record.Outcome.Value.UpdateID, wantedWorkflow, wantedUpdate, recoveryErr)
			}
			if result != "updated" || sends.Load() != 1 || reads.Load() != 1 {
				t.Fatal("recovery changed or resubmitted update")
			}
		})
	}
}

type submittedIdentityTraffic struct{}

func (*submittedIdentityTraffic) CheckCallAllowed(_ context.Context, _ string, request, _ any) error {
	switch request := request.(type) {
	case *workflowservice.StartWorkflowExecutionRequest:
		request.WorkflowId = "submitted-workflow"
	case *workflowservice.SignalWithStartWorkflowExecutionRequest:
		request.WorkflowId = "submitted-workflow"
	case *workflowservice.UpdateWorkflowExecutionRequest:
		request.WorkflowExecution.WorkflowId = "submitted-workflow"
		request.Request.Meta.UpdateId = "submitted-update"
	case *workflowservice.ExecuteMultiOperationRequest:
		request.Operations[0].GetStartWorkflow().WorkflowId = "submitted-workflow"
		request.Operations[1].GetUpdateWorkflow().WorkflowExecution.WorkflowId = "submitted-workflow"
		request.Operations[1].GetUpdateWorkflow().Request.Meta.UpdateId = "submitted-update"
	}
	return nil
}

func TestWorkflowEvidenceUsesInnermostSubmittedIdentity(t *testing.T) {
	for _, mode := range []string{"start", "signal-start", "update", "update-start"} {
		for _, lost := range []bool{false, true} {
			name := mode + "/acknowledged"
			if lost {
				name = mode + "/response-lost"
			}
			t.Run(name, func(t *testing.T) {
				var requests atomic.Int32
				fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{TrafficController: &submittedIdentityTraffic{}}, nil,
					func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
						peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
							var workflowID, updateID string
							var response any
							switch input := request.(type) {
							case *workflowservice.StartWorkflowExecutionRequest:
								workflowID = input.WorkflowId
								response = &workflowservice.StartWorkflowExecutionResponse{RunId: "observed-run"}
							case *workflowservice.SignalWithStartWorkflowExecutionRequest:
								workflowID = input.WorkflowId
								response = &workflowservice.SignalWithStartWorkflowExecutionResponse{RunId: "observed-run"}
							case *workflowservice.UpdateWorkflowExecutionRequest:
								workflowID, updateID = input.WorkflowExecution.WorkflowId, input.Request.Meta.UpdateId
								response = &workflowservice.UpdateWorkflowExecutionResponse{Stage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
									UpdateRef: &updatepb.UpdateRef{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "submitted-workflow", RunId: "observed-run"}, UpdateId: "submitted-update"}}
							case *workflowservice.ExecuteMultiOperationRequest:
								workflowID = input.Operations[0].GetStartWorkflow().WorkflowId
								updateID = input.Operations[1].GetUpdateWorkflow().Request.Meta.UpdateId
								response = combinedReply("submitted-workflow", "submitted-update")
							default:
								return next(ctx, request)
							}
							if workflowID != "submitted-workflow" || strings.Contains(mode, "update") && updateID != "submitted-update" {
								return nil, status.Error(codes.InvalidArgument, "traffic rewrite did not reach peer")
							}
							requests.Add(1)
							if lost {
								return nil, status.Error(codes.DeadlineExceeded, "synthetic lost response")
							}
							return response, nil
						}
					})
				executions, inbox := executionBinding(t, fixture)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				start := sdk.StartWorkflowOptions{ID: "caller-workflow", TaskQueue: "unit"}
				update := sdk.UpdateWorkflowOptions{WorkflowID: "caller-workflow", UpdateID: "caller-update", UpdateName: "echo", WaitForStage: sdk.WorkflowUpdateStageAccepted}
				correlation := fault.Correlation{Call: "submitted-identity"}
				var err error
				switch mode {
				case "start":
					_, err = executions.ExecuteWorkflow(ctx, correlation, start, "definition")
				case "signal-start":
					_, err = executions.SignalWithStartWorkflow(ctx, correlation, start.ID, "signal", "value", start, "definition")
				case "update":
					_, err = executions.UpdateWorkflow(ctx, correlation, update)
				case "update-start":
					start.WorkflowIDConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
					operation, createErr := executions.NewWithStartWorkflowOperation(start, "definition")
					if createErr != nil {
						t.Fatal(createErr)
					}
					_, err = executions.UpdateWithStartWorkflow(ctx, correlation, operation, update)
				}
				record := receiveExecution(t, inbox)
				if (err != nil) != lost || requests.Load() < 1 || record.Outcome.Value.Accepted == lost {
					t.Fatal("native acknowledgement/response-loss control changed", err)
				}
				value := record.Outcome.Value
				if value.WorkflowID != "submitted-workflow" || strings.Contains(mode, "update") && value.UpdateID != "submitted-update" {
					t.Fatal("caller defaults or stale native handle overwrote submitted identities")
				}
				if !lost {
					wantedRun := "observed-run"
					if mode == "update-start" {
						wantedRun = "combined-run"
					}
					if value.RunID != wantedRun {
						t.Fatal("request snapshot erased the acknowledged run identity")
					}
				}
			})
		}
	}
}
