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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type workflowBoundaryServer struct {
	*testServer
	requests       chan proto.Message
	unknownReset   atomic.Bool
	historyEntered chan struct{}
	historyOnce    sync.Once
}

func newWorkflowBoundaryServer() *workflowBoundaryServer {
	return &workflowBoundaryServer{testServer: &testServer{}, requests: make(chan proto.Message, 64)}
}
func (peer *workflowBoundaryServer) capture(message proto.Message) {
	peer.requests <- proto.Clone(message)
}
func workflowRequest(t *testing.T, peer *workflowBoundaryServer) proto.Message {
	t.Helper()
	select {
	case request := <-peer.requests:
		return request
	case <-time.After(3 * time.Second):
		t.Fatal("expected native Workflow request was not sent")
		return nil
	}
}
func (peer *workflowBoundaryServer) StartWorkflowExecution(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
	peer.capture(request)
	return &workflowservice.StartWorkflowExecutionResponse{RunId: "started-run", FirstExecutionRunId: "first-run"}, nil
}
func (peer *workflowBoundaryServer) SignalWithStartWorkflowExecution(_ context.Context, request *workflowservice.SignalWithStartWorkflowExecutionRequest) (*workflowservice.SignalWithStartWorkflowExecutionResponse, error) {
	peer.capture(request)
	return &workflowservice.SignalWithStartWorkflowExecutionResponse{RunId: "signal-run", FirstExecutionRunId: "signal-first"}, nil
}
func (peer *workflowBoundaryServer) RequestCancelWorkflowExecution(_ context.Context, request *workflowservice.RequestCancelWorkflowExecutionRequest) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
	peer.capture(request)
	return &workflowservice.RequestCancelWorkflowExecutionResponse{}, nil
}
func (peer *workflowBoundaryServer) TerminateWorkflowExecution(_ context.Context, request *workflowservice.TerminateWorkflowExecutionRequest) (*workflowservice.TerminateWorkflowExecutionResponse, error) {
	peer.capture(request)
	return &workflowservice.TerminateWorkflowExecutionResponse{}, nil
}
func workflowUpdateReply(workflowID, runID, updateID string) *workflowservice.UpdateWorkflowExecutionResponse {
	payloads, _ := converter.GetDefaultDataConverter().ToPayloads("updated")
	return &workflowservice.UpdateWorkflowExecutionResponse{
		UpdateRef: &updatepb.UpdateRef{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID}, UpdateId: updateID},
		Stage:     enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
		Outcome:   &updatepb.Outcome{Value: &updatepb.Outcome_Success{Success: payloads}},
	}
}
func (peer *workflowBoundaryServer) UpdateWorkflowExecution(_ context.Context, request *workflowservice.UpdateWorkflowExecutionRequest) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
	peer.capture(request)
	return workflowUpdateReply(request.WorkflowExecution.WorkflowId, "updated-run", request.Request.Meta.UpdateId), nil
}
func (peer *workflowBoundaryServer) PollWorkflowExecutionUpdate(_ context.Context, request *workflowservice.PollWorkflowExecutionUpdateRequest) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
	peer.capture(request)
	return &workflowservice.PollWorkflowExecutionUpdateResponse{Stage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
		Outcome: workflowUpdateReply("workflow", "run", "update").Outcome}, nil
}
func (peer *workflowBoundaryServer) ExecuteMultiOperation(_ context.Context, request *workflowservice.ExecuteMultiOperationRequest) (*workflowservice.ExecuteMultiOperationResponse, error) {
	peer.capture(request)
	start := request.Operations[0].GetStartWorkflow()
	update := request.Operations[1].GetUpdateWorkflow()
	return &workflowservice.ExecuteMultiOperationResponse{Responses: []*workflowservice.ExecuteMultiOperationResponse_Response{
		{Response: &workflowservice.ExecuteMultiOperationResponse_Response_StartWorkflow{StartWorkflow: &workflowservice.StartWorkflowExecutionResponse{RunId: "combined-run", FirstExecutionRunId: "combined-first"}}},
		{Response: &workflowservice.ExecuteMultiOperationResponse_Response_UpdateWorkflow{UpdateWorkflow: workflowUpdateReply(start.WorkflowId, "combined-run", update.Request.Meta.UpdateId)}},
	}}, nil
}
func (peer *workflowBoundaryServer) ResetWorkflowExecution(_ context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	peer.capture(request)
	if peer.unknownReset.Load() {
		return nil, status.Error(codes.Unknown, "private-reset-reply-canary")
	}
	return &workflowservice.ResetWorkflowExecutionResponse{RunId: "reset-run"}, nil
}
func (peer *workflowBoundaryServer) UpdateWorkflowExecutionOptions(_ context.Context, request *workflowservice.UpdateWorkflowExecutionOptionsRequest) (*workflowservice.UpdateWorkflowExecutionOptionsResponse, error) {
	peer.capture(request)
	return &workflowservice.UpdateWorkflowExecutionOptionsResponse{WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE},
	}}, nil
}
func (peer *workflowBoundaryServer) DescribeWorkflowExecution(_ context.Context, request *workflowservice.DescribeWorkflowExecutionRequest) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	peer.capture(request)
	dc := converter.GetDefaultDataConverter()
	memo, _ := dc.ToPayload("memo")
	summary, _ := dc.ToPayload("summary")
	details, _ := dc.ToPayload("details")
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: proto.Clone(request.Execution).(*commonpb.WorkflowExecution),
			Type:      &commonpb.WorkflowType{Name: "definition"}, TaskQueue: "queue",
			Memo:             &commonpb.Memo{Fields: map[string]*commonpb.Payload{"key": memo}},
			SearchAttributes: &commonpb.SearchAttributes{},
		},
		ExecutionConfig: &workflowpb.WorkflowExecutionConfig{UserMetadata: &sdkpb.UserMetadata{Summary: summary, Details: details}},
	}, nil
}

func (peer *workflowBoundaryServer) QueryWorkflow(_ context.Context, request *workflowservice.QueryWorkflowRequest) (*workflowservice.QueryWorkflowResponse, error) {
	peer.capture(request)
	if request.Query.QueryType == "reject" {
		return &workflowservice.QueryWorkflowResponse{QueryRejected: &querypb.QueryRejected{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}}, nil
	}
	payloads, _ := converter.GetDefaultDataConverter().ToPayloads("queried")
	return &workflowservice.QueryWorkflowResponse{QueryResult: payloads}, nil
}
func (peer *workflowBoundaryServer) GetWorkflowExecutionHistory(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	if peer.historyEntered != nil {
		peer.historyOnce.Do(func() { close(peer.historyEntered) })
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return peer.testServer.GetWorkflowExecutionHistory(ctx, request)
}

func TestPublicWorkflowStartSignalCancelTerminateAndResultOptions(t *testing.T) {
	peer := newWorkflowBoundaryServer()
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	client := fixture.owner.Client()
	options := sdk.StartWorkflowOptions{ID: "workflow", TaskQueue: "queue", WorkflowExecutionTimeout: time.Minute,
		WorkflowRunTimeout: 30 * time.Second, WorkflowTaskTimeout: 5 * time.Second, StaticSummary: "start summary", StaticDetails: "start details"}
	run, err := client.ExecuteWorkflow(fixture.ctx, options, "definition", 17)
	if err != nil {
		t.Fatal(err)
	}
	request := workflowRequest(t, peer).(*workflowservice.StartWorkflowExecutionRequest)
	var input int
	if err := converter.GetDefaultDataConverter().FromPayloads(request.Input, &input); err != nil {
		t.Fatal(err)
	}
	if request.Namespace != "test" || request.WorkflowId != "workflow" || request.TaskQueue.Name != "queue" ||
		request.WorkflowType.Name != "definition" || input != 17 || request.WorkflowExecutionTimeout.AsDuration() != time.Minute ||
		request.WorkflowRunTimeout.AsDuration() != 30*time.Second || request.WorkflowTaskTimeout.AsDuration() != 5*time.Second ||
		request.UserMetadata == nil || run.GetID() != "workflow" || run.GetRunID() != "started-run" || run.GetFirstExecutionRunID() != "first-run" {
		t.Fatal("public start changed native options or returned identity")
	}
	if record := capabilityEvidence(t, fixture, "workflow.start"); !record.Execution.Accepted || record.Execution.RunID != run.GetRunID() {
		t.Fatal("start evidence changed")
	}
	var output string
	if err := run.GetWithOptions(fixture.ctx, &output, sdk.WorkflowRunGetOptions{DisableFollowingRuns: true}); err != nil || output != "result" {
		t.Fatal("native Workflow result changed", err)
	}
	if record := capabilityEvidence(t, fixture, "workflow.result"); !record.Execution.ResultObtained {
		t.Fatal("result evidence missing")
	}
	signaled, err := client.SignalWithStartWorkflow(fixture.ctx, "signal-workflow", "signal", "argument", sdk.StartWorkflowOptions{TaskQueue: "queue"}, "definition", 23)
	if err != nil {
		t.Fatal(err)
	}
	signal := workflowRequest(t, peer).(*workflowservice.SignalWithStartWorkflowExecutionRequest)
	if signal.Namespace != "test" || signal.WorkflowId != "signal-workflow" || signal.SignalName != "signal" || signaled.GetRunID() != "signal-run" {
		t.Fatal("signal-start forwarding changed")
	}
	if err := converter.GetDefaultDataConverter().FromPayloads(signal.SignalInput, &output); err != nil || output != "argument" {
		t.Fatal("signal input changed", err)
	}
	capabilityEvidence(t, fixture, "workflow.signal-start")
	cancel := sdk.CancelWorkflowOptions{WorkflowID: "workflow", RunID: "run", FirstExecutionRunID: "first", Reason: "cancel reason"}
	if err := client.CancelWorkflow(fixture.ctx, cancel); err != nil {
		t.Fatal(err)
	}
	canceled := workflowRequest(t, peer).(*workflowservice.RequestCancelWorkflowExecutionRequest)
	if canceled.Namespace != "test" || canceled.WorkflowExecution.RunId != "run" || canceled.FirstExecutionRunId != "first" || canceled.Reason != "cancel reason" || canceled.RequestId == "" {
		t.Fatal("native cancellation targeting/default identity changed")
	}
	capabilityEvidence(t, fixture, "workflow.cancel")
	if err := client.TerminateWorkflow(fixture.ctx, sdk.TerminateWorkflowOptions{WorkflowID: "workflow", RunID: "run", FirstExecutionRunID: "first", Reason: "terminate reason", Details: []any{"detail"}}); err != nil {
		t.Fatal(err)
	}
	terminated := workflowRequest(t, peer).(*workflowservice.TerminateWorkflowExecutionRequest)
	if err := converter.GetDefaultDataConverter().FromPayloads(terminated.Details, &output); err != nil || output != "detail" || terminated.FirstExecutionRunId != "first" || terminated.Reason != "terminate reason" {
		t.Fatal("termination detail/target forwarding changed", err)
	}
	capabilityEvidence(t, fixture, "workflow.terminate")
}

func TestPublicWorkflowUpdateAndCombinedStartKeepOriginalUse(t *testing.T) {
	peer := newWorkflowBoundaryServer()
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	client := fixture.owner.Client()
	update, err := client.UpdateWorkflow(fixture.ctx, sdk.UpdateWorkflowOptions{WorkflowID: "workflow", RunID: "run", UpdateID: "update", UpdateName: "change", Args: []any{7}, WaitForStage: sdk.WorkflowUpdateStageCompleted})
	if err != nil {
		t.Fatal(err)
	}
	request := workflowRequest(t, peer).(*workflowservice.UpdateWorkflowExecutionRequest)
	if request.Namespace != "test" || request.WorkflowExecution.WorkflowId != "workflow" || request.Request.Input.Name != "change" || request.Request.Meta.UpdateId != "update" ||
		update.WorkflowID() != "workflow" || update.RunID() != "updated-run" || update.UpdateID() != "update" {
		t.Fatal("update request/handle identity changed")
	}
	capabilityEvidence(t, fixture, "workflow.update")
	var output string
	if err := update.Get(fixture.ctx, &output); err != nil || output != "updated" {
		t.Fatal("completed update result changed", err)
	}
	capabilityEvidence(t, fixture, "workflow.update-result")
	detached, err := client.GetWorkflowUpdateHandle(sdk.GetWorkflowUpdateHandleOptions{WorkflowID: "workflow", RunID: "run", UpdateID: "detached"})
	if err != nil {
		t.Fatal(err)
	}
	if err := detached.Get(fixture.ctx, &output); err != nil || output != "updated" {
		t.Fatal("detached Update polling changed", err)
	}
	poll := workflowRequest(t, peer).(*workflowservice.PollWorkflowExecutionUpdateRequest)
	if poll.UpdateRef.UpdateId != "detached" || poll.UpdateRef.WorkflowExecution.RunId != "run" {
		t.Fatal("detached Update target changed")
	}
	capabilityEvidence(t, fixture, "workflow.update-result")
	intention, err := client.NewWithStartWorkflowOperation(sdk.StartWorkflowOptions{ID: "combined", TaskQueue: "queue", WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING}, "definition")
	if err != nil {
		t.Fatal(err)
	}
	peerUse, err := client.Borrow(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peerUse.UpdateWithStartWorkflow(fixture.ctx, intention, sdk.UpdateWorkflowOptions{UpdateName: "change", WaitForStage: sdk.WorkflowUpdateStageCompleted}); !errors.Is(err, ErrAuthority) || len(peer.requests) != 0 {
		t.Fatal("peer consumed foreign WithStart intention", err)
	}
	copied, err := client.WithID("combined-copy")
	if err != nil {
		t.Fatal(err)
	}
	combined, err := copied.UpdateWithStartWorkflow(fixture.ctx, intention, sdk.UpdateWorkflowOptions{UpdateID: "combined-update", UpdateName: "change", WaitForStage: sdk.WorkflowUpdateStageCompleted})
	if err != nil {
		t.Fatal(err)
	}
	multi := workflowRequest(t, peer).(*workflowservice.ExecuteMultiOperationRequest)
	if len(multi.Operations) != 2 || multi.Namespace != "test" || multi.Operations[0].GetStartWorkflow().WorkflowId != "combined" || combined.RunID() != "combined-run" {
		t.Fatal("combined request/handle changed")
	}
	record := capabilityEvidence(t, fixture, "workflow.update-start")
	if !record.Execution.Accepted || !record.Execution.StartAccepted || record.Execution.UpdateID != "combined-update" {
		t.Fatal("combined acknowledgements lost")
	}
	if err := combined.Get(fixture.ctx, &output); err != nil || output != "updated" {
		t.Fatal("combined result changed", err)
	}
	capabilityEvidence(t, fixture, "workflow.update-result")
	started, err := intention.Get(fixture.ctx)
	if err != nil || started.GetRunID() != "combined-run" || intention.WorkflowID() != "combined" {
		t.Fatal("combined start observation changed", err)
	}
	capabilityEvidence(t, fixture, "workflow.start-result")
	if _, err := client.UpdateWithStartWorkflow(fixture.ctx, intention, sdk.UpdateWorkflowOptions{UpdateName: "change", WaitForStage: sdk.WorkflowUpdateStageCompleted}); !errors.Is(err, ErrInput) || len(peer.requests) != 0 {
		t.Fatal("used intention resent native start", err)
	}
	capabilityEvidence(t, fixture, "workflow.update-start")
	if err := client.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if err := combined.Get(fixture.ctx, &output); err == nil {
		t.Fatal("closed originating use decoded cached Update")
	}
	if err := peerUse.SignalWorkflow(fixture.ctx, "peer", "run", "signal", nil); err != nil {
		t.Fatal("closing original use closed peer", err)
	}
	capabilityEvidence(t, fixture, "workflow.signal")
}

func TestPublicWorkflowResetAndExecutionOptionsRequireExactGrants(t *testing.T) {
	peer := newWorkflowBoundaryServer()
	denied := newCapabilityFixture(t, peer, NativeOptions{})
	request := &workflowservice.ResetWorkflowExecutionRequest{Namespace: "test", WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow", RunId: "run"}, WorkflowTaskFinishEventId: 8}
	if _, err := denied.owner.Client().ResetWorkflowExecution(denied.ctx, request); !errors.Is(err, ErrAuthority) || len(peer.requests) != 0 {
		t.Fatal("reset acquired implicit authority", err)
	}
	change := sdk.UpdateWorkflowExecutionOptionsRequest{WorkflowId: "workflow", WorkflowExecutionOptionsChanges: sdk.WorkflowExecutionOptionsChanges{VersioningOverride: &sdk.VersioningOverrideChange{Value: &sdk.AutoUpgradeVersioningOverride{}}}}
	if _, err := denied.owner.Client().UpdateWorkflowExecutionOptions(denied.ctx, change); !errors.Is(err, ErrAuthority) || len(peer.requests) != 0 {
		t.Fatal("execution options acquired implicit authority", err)
	}
	allowed := newCapabilityFixture(t, peer, NativeOptions{}, "ResetWorkflowExecution", "UpdateWorkflowExecutionOptions")
	client := allowed.owner.Client()
	before := proto.Clone(request)
	reset, err := client.ResetWorkflowExecution(allowed.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	wire := workflowRequest(t, peer).(*workflowservice.ResetWorkflowExecutionRequest)
	if reset.RequestID == "" || reset.RequestID != wire.RequestId || reset.Response.GetRunId() != "reset-run" || !proto.Equal(before, request) {
		t.Fatal("reset default identity/result/caller copy changed")
	}
	if record := capabilityEvidence(t, allowed, "workflow.reset"); record.Execution.RequestID != reset.RequestID || !record.Execution.Accepted {
		t.Fatal("reset evidence identity changed")
	}
	request.RequestId = reset.RequestID
	peer.unknownReset.Store(true)
	wait, cancel := context.WithTimeout(allowed.ctx, 200*time.Millisecond)
	defer cancel()
	uncertain, err := client.ResetWorkflowExecution(wait, request)
	if err == nil || uncertain.Response != nil || uncertain.RequestID != reset.RequestID || strings.Contains(err.Error(), "private-reset-reply-canary") {
		t.Fatal("unknown reset became success, lost identity or leaked cause", err)
	}
	unknown := workflowRequest(t, peer).(*workflowservice.ResetWorkflowExecutionRequest)
	if unknown.RequestId != reset.RequestID {
		t.Fatal("explicit retry changed native identity")
	}
	for len(peer.requests) > 0 {
		if retry := workflowRequest(t, peer).(*workflowservice.ResetWorkflowExecutionRequest); retry.RequestId != reset.RequestID {
			t.Fatal("native retry changed the uncertain reset identity")
		}
	}
	nativeError, present := NativeError(err)
	if !present || nativeError == nil {
		t.Fatal("unknown native error capture missing")
	}
	record := capabilityEvidence(t, allowed, "workflow.reset")
	if !record.Execution.NativeCalled || record.Execution.Accepted || record.Execution.RequestID != reset.RequestID {
		t.Fatal("unknown reset evidence guessed an effect")
	}
	options, err := client.UpdateWorkflowExecutionOptions(allowed.ctx, change)
	if err != nil {
		t.Fatal(err)
	}
	wireOptions := workflowRequest(t, peer).(*workflowservice.UpdateWorkflowExecutionOptionsRequest)
	if wireOptions.Namespace != "test" || wireOptions.WorkflowExecution.WorkflowId != "workflow" || len(wireOptions.UpdateMask.Paths) != 1 || wireOptions.UpdateMask.Paths[0] != "versioning_override" {
		t.Fatal("native execution options patch changed")
	}
	if _, ok := options.VersioningOverride.(*sdk.AutoUpgradeVersioningOverride); !ok {
		t.Fatal("native versioning result conversion lost")
	}
	capabilityEvidence(t, allowed, "workflow.execution-options")
	if _, err := client.UpdateWorkflowExecutionOptions(allowed.ctx, sdk.UpdateWorkflowExecutionOptionsRequest{}); err == nil || len(peer.requests) != 0 {
		t.Fatal("native execution options validation bypassed")
	}
	capabilityEvidence(t, allowed, "workflow.execution-options")
}

func TestPublicWorkflowDescriptionKeepsDetachedMetadataAndAdmittedDecoders(t *testing.T) {
	peer := newWorkflowBoundaryServer()
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	client := fixture.owner.Client()
	raw, err := client.DescribeWorkflowExecution(fixture.ctx, "workflow", "run")
	if err != nil || raw.GetWorkflowExecutionInfo().GetExecution().GetWorkflowId() != "workflow" {
		t.Fatal("raw description changed", err)
	}
	workflowRequest(t, peer)
	capabilityEvidence(t, fixture, "workflow.describe")
	description, err := client.DescribeWorkflow(fixture.ctx, "workflow", "run")
	if err != nil {
		t.Fatal(err)
	}
	workflowRequest(t, peer)
	capabilityEvidence(t, fixture, "workflow.describe-metadata")
	if description.Metadata().WorkflowExecution.ID != "workflow" {
		t.Fatal("metadata identity changed")
	}
	metadata := description.Metadata()
	metadata.Memo.Fields["key"].Data[0] ^= 1
	var value string
	if err := description.GetMemoValue(fixture.ctx, "key", &value); err != nil || value != "memo" {
		t.Fatal("metadata alias changed lazy memo", err)
	}
	capabilityEvidence(t, fixture, "workflow.memo-value")
	if value, err := description.GetStaticSummary(fixture.ctx); err != nil || value != "summary" {
		t.Fatal("summary decoder changed", err)
	}
	capabilityEvidence(t, fixture, "workflow.static-summary")
	if value, err := description.GetStaticDetails(fixture.ctx); err != nil || value != "details" {
		t.Fatal("details decoder changed", err)
	}
	capabilityEvidence(t, fixture, "workflow.static-details")
	if err := client.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := description.GetStaticSummary(fixture.ctx); err == nil {
		t.Fatal("description decoder outlived originating use")
	}
	if description.Metadata().WorkflowExecution.ID != "workflow" {
		t.Fatal("detached metadata required live authority")
	}
}

func TestPublicWorkflowCanceledResultWaitDoesNotCancelRemoteExecution(t *testing.T) {
	peer := newWorkflowBoundaryServer()
	peer.historyEntered = make(chan struct{})
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	run, err := fixture.owner.Client().GetWorkflow("workflow", "run")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(fixture.ctx)
	finished := make(chan error, 1)
	go func() { finished <- run.Get(ctx, new(string)) }()
	select {
	case <-peer.historyEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("native result polling did not enter")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("result wait cancellation lost", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("native result wait did not return")
	}
	if len(peer.requests) != 0 {
		t.Fatal("canceling result observation sent remote cancellation/termination")
	}
	record := capabilityEvidence(t, fixture, "workflow.result")
	if record.Execution.Accepted || record.Execution.ResultObtained {
		t.Fatal("canceled observation invented remote completion")
	}
}

func TestPublicWorkflowZeroValuesRefuseWithoutPanicking(t *testing.T) {
	ctx := context.Background()
	for _, run := range []*WorkflowRun{nil, {}} {
		if run.GetID() != "" || run.GetRunID() != "" || run.GetFirstExecutionRunID() != "" || !errors.Is(run.Get(ctx, new(string)), ErrInput) {
			t.Fatal("zero Workflow run did not refuse safely")
		}
	}
	for _, update := range []*WorkflowUpdate{nil, {}} {
		if update.WorkflowID() != "" || update.RunID() != "" || update.UpdateID() != "" || !errors.Is(update.Get(ctx, new(string)), ErrInput) {
			t.Fatal("zero Update handle did not refuse safely")
		}
	}
	for _, operation := range []*WithStartWorkflowOperation{nil, {}} {
		if operation.WorkflowID() != "" {
			t.Fatal("zero WithStart identity changed")
		}
		if _, err := operation.Get(ctx); !errors.Is(err, ErrInput) {
			t.Fatal("zero WithStart did not refuse safely", err)
		}
	}
	var client *Client
	if _, err := client.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{}, "definition"); !errors.Is(err, ErrInput) {
		t.Fatal("nil Client start did not refuse", err)
	}
}

type workflowContinuationServer struct {
	*testServer
	runs chan string
}

func (peer *workflowContinuationServer) GetWorkflowExecutionHistory(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	peer.runs <- request.Execution.RunId
	if request.Namespace != "test" || request.Execution.WorkflowId != "chain" {
		return nil, status.Error(codes.InvalidArgument, "unexpected chain target")
	}
	switch request.Execution.RunId {
	case "initial":
		return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{
			EventId: 8, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{NewExecutionRunId: "successor"}},
		}}}}, nil
	case "successor":
		return peer.testServer.GetWorkflowExecutionHistory(ctx, request)
	default:
		return nil, status.Error(codes.InvalidArgument, "unexpected continuation run")
	}
}

func TestPublicWorkflowRunOptionsDistinguishStrictAndFollowingResults(t *testing.T) {
	peer := &workflowContinuationServer{testServer: &testServer{}, runs: make(chan string, 4)}
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	client := fixture.owner.Client()
	strict, err := client.GetWorkflow("chain", "initial")
	if err != nil {
		t.Fatal(err)
	}
	var result string
	err = strict.GetWithOptions(fixture.ctx, &result, sdk.WorkflowRunGetOptions{DisableFollowingRuns: true})
	semantic, captured := NativeError(err)
	if _, ok := semantic.(*workflow.ContinueAsNewError); !ok || !captured || strict.GetRunID() != "initial" || len(peer.runs) != 1 {
		t.Fatal("strict observation followed or lost the exact native ContinueAsNewError", err)
	}
	if <-peer.runs != "initial" {
		t.Fatal("strict observation changed its target")
	}
	if record := capabilityEvidence(t, fixture, "workflow.result"); record.Execution.ResultObtained || record.Execution.RunID != "initial" {
		t.Fatal("strict observation claimed successor completion")
	}
	following, err := client.GetWorkflow("chain", "initial")
	if err != nil {
		t.Fatal(err)
	}
	if err := following.Get(fixture.ctx, &result); err != nil || result != "result" || following.GetRunID() != "successor" {
		t.Fatal("default observation did not follow native continuation", err)
	}
	if len(peer.runs) != 2 || <-peer.runs != "initial" || <-peer.runs != "successor" {
		t.Fatal("following observation did not request the exact native run chain")
	}
	if record := capabilityEvidence(t, fixture, "workflow.result"); !record.Execution.ResultObtained || record.Execution.RunID != "successor" {
		t.Fatal("following observation lost effective successor evidence")
	}
}

func TestPublicWorkflowEagerQueryOptionsPreserveRejectionWithoutDecoding(t *testing.T) {
	peer := newWorkflowBoundaryServer()
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	request := &sdk.QueryWorkflowWithOptionsRequest{WorkflowID: "workflow", RunID: "run", QueryType: "query", Args: []any{17},
		QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN}
	var value string
	rejected, err := fixture.owner.Client().QueryWorkflowWithOptions(fixture.ctx, request, &value)
	if err != nil || rejected != nil || value != "queried" {
		t.Fatal("eager query result changed", err)
	}
	wire := workflowRequest(t, peer).(*workflowservice.QueryWorkflowRequest)
	var input int
	if err := converter.GetDefaultDataConverter().FromPayloads(wire.Query.QueryArgs, &input); err != nil ||
		input != 17 || wire.Namespace != "test" || wire.Execution.RunId != "run" || wire.QueryRejectCondition != request.QueryRejectCondition {
		t.Fatal("eager query native targeting/options changed", err)
	}
	if record := capabilityEvidence(t, fixture, "workflow.query"); !record.Execution.ResultObtained {
		t.Fatal("eager query result evidence missing")
	}
	request.QueryType = "reject"
	value = "untouched"
	rejected, err = fixture.owner.Client().QueryWorkflowWithOptions(fixture.ctx, request, &value)
	if err != nil || rejected.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED || value != "untouched" {
		t.Fatal("native rejection became decoded output", err)
	}
	workflowRequest(t, peer)
	if record := capabilityEvidence(t, fixture, "workflow.query"); record.Execution.ResultObtained {
		t.Fatal("rejected query claimed a decoded result")
	}
	if _, err := fixture.owner.Client().QueryWorkflowWithOptions(fixture.ctx, nil, &value); !errors.Is(err, ErrInput) || len(peer.requests) != 0 {
		t.Fatal("nil query options entered native path", err)
	}
}
