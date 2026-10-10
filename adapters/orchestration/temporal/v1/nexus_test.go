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
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type publicNexusPeer struct {
	testServer
	requests    chan proto.Message
	result      *commonpb.Payload
	poll        func(context.Context, *workflowservice.PollNexusOperationExecutionRequest) (*workflowservice.PollNexusOperationExecutionResponse, error)
	description *workflowservice.DescribeNexusOperationExecutionResponse
	list        func(*workflowservice.ListNexusOperationExecutionsRequest) *workflowservice.ListNexusOperationExecutionsResponse
	count       *workflowservice.CountNexusOperationExecutionsResponse
	unavailable error
	polls       atomic.Int32
	cancels     atomic.Int32
	terminates  atomic.Int32
}

func newPublicNexusPeer(t testing.TB) *publicNexusPeer {
	return &publicNexusPeer{requests: make(chan proto.Message, 128), result: semanticPayloads(t, "nexus-result").Payloads[0]}
}

func (peer *publicNexusPeer) record(request proto.Message) { peer.requests <- proto.Clone(request) }
func (peer *publicNexusPeer) StartNexusOperationExecution(_ context.Context, request *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	peer.record(request)
	return &workflowservice.StartNexusOperationExecutionResponse{RunId: "operation-run", Started: true}, peer.unavailable
}
func (peer *publicNexusPeer) PollNexusOperationExecution(ctx context.Context, request *workflowservice.PollNexusOperationExecutionRequest) (*workflowservice.PollNexusOperationExecutionResponse, error) {
	peer.record(request)
	peer.polls.Add(1)
	if peer.unavailable != nil {
		return nil, peer.unavailable
	}
	if peer.poll != nil {
		return peer.poll(ctx, request)
	}
	return &workflowservice.PollNexusOperationExecutionResponse{Outcome: &workflowservice.PollNexusOperationExecutionResponse_Result{Result: peer.result}}, nil
}
func (peer *publicNexusPeer) RequestCancelNexusOperationExecution(_ context.Context, request *workflowservice.RequestCancelNexusOperationExecutionRequest) (*workflowservice.RequestCancelNexusOperationExecutionResponse, error) {
	peer.record(request)
	peer.cancels.Add(1)
	return &workflowservice.RequestCancelNexusOperationExecutionResponse{}, peer.unavailable
}
func (peer *publicNexusPeer) TerminateNexusOperationExecution(_ context.Context, request *workflowservice.TerminateNexusOperationExecutionRequest) (*workflowservice.TerminateNexusOperationExecutionResponse, error) {
	peer.record(request)
	peer.terminates.Add(1)
	return &workflowservice.TerminateNexusOperationExecutionResponse{}, peer.unavailable
}
func (peer *publicNexusPeer) DescribeNexusOperationExecution(_ context.Context, request *workflowservice.DescribeNexusOperationExecutionRequest) (*workflowservice.DescribeNexusOperationExecutionResponse, error) {
	peer.record(request)
	if peer.unavailable != nil {
		return nil, peer.unavailable
	}
	value := proto.Clone(peer.description).(*workflowservice.DescribeNexusOperationExecutionResponse)
	if request.OperationId == "no-cancellation" {
		value.Info.CancellationInfo = nil
	}
	return value, nil
}
func (peer *publicNexusPeer) ListNexusOperationExecutions(_ context.Context, request *workflowservice.ListNexusOperationExecutionsRequest) (*workflowservice.ListNexusOperationExecutionsResponse, error) {
	peer.record(request)
	if peer.unavailable != nil {
		return nil, peer.unavailable
	}
	return peer.list(request), nil
}
func (peer *publicNexusPeer) CountNexusOperationExecutions(_ context.Context, request *workflowservice.CountNexusOperationExecutionsRequest) (*workflowservice.CountNexusOperationExecutionsResponse, error) {
	peer.record(request)
	return peer.count, peer.unavailable
}

type publicNexusHeaderHook struct {
	interceptor.ClientInterceptorBase
	calls atomic.Int32
}

func (hook *publicNexusHeaderHook) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return &publicNexusHeaderOutbound{ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next}, hook: hook}
}

type publicNexusHeaderOutbound struct {
	interceptor.ClientOutboundInterceptorBase
	hook *publicNexusHeaderHook
}

func (outbound *publicNexusHeaderOutbound) ExecuteNexusOperation(ctx context.Context, input *interceptor.ClientExecuteNexusOperationInput) (sdk.NexusOperationHandle, error) {
	outbound.hook.calls.Add(1)
	input.NexusHeader["trace-fixture"] = "native-header"
	return outbound.Next.ExecuteNexusOperation(ctx, input)
}

func TestPublicNexusStartOptionsTypedReferenceAndCachedResult(t *testing.T) {
	peer := newPublicNexusPeer(t)
	hook := &publicNexusHeaderHook{}
	fixture := newCapabilityFixture(t, peer, NativeOptions{Interceptors: []interceptor.ClientInterceptor{hook}})
	client, err := fixture.owner.Client().Borrow(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := sdktemporal.NewSearchAttributeKeyKeyword("Category")
	target := sdk.NexusClientOptions{Endpoint: "endpoint", Service: "service"}
	definition := nexus.NewOperationReference[string, string]("definition")
	options := sdk.StartNexusOperationOptions{ID: "operation", ScheduleToCloseTimeout: 25 * time.Second, ScheduleToStartTimeout: 2 * time.Second,
		StartToCloseTimeout: 12 * time.Second, IDConflictPolicy: enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
		IDReusePolicy:    enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE,
		SearchAttributes: sdktemporal.NewSearchAttributes(key.ValueSet("blue")), Summary: "nexus-summary"}
	run, err := client.ExecuteNexusOperation(fixture.ctx, target, definition, "input", options)
	if err != nil || run.GetID() != "operation" || run.GetRunID() != "operation-run" || hook.calls.Load() != 1 {
		t.Fatal("public Nexus start lost native handle/interceptor semantics", err)
	}
	request := semanticRequest[*workflowservice.StartNexusOperationExecutionRequest](t, peer.requests)
	if request.Namespace != "test" || request.Endpoint != "endpoint" || request.Service != "service" || request.Operation != "definition" || request.OperationId != "operation" || request.RequestId == "" ||
		request.GetScheduleToCloseTimeout().AsDuration() != 25*time.Second || request.GetScheduleToStartTimeout().AsDuration() != 2*time.Second || request.GetStartToCloseTimeout().AsDuration() != 12*time.Second ||
		request.IdConflictPolicy != options.IDConflictPolicy || request.IdReusePolicy != options.IDReusePolicy || request.NexusHeader["trace-fixture"] != "native-header" {
		t.Fatal("Nexus target, native header or option conversion changed")
	}
	var input, summary, category string
	dc := converter.GetDefaultDataConverter()
	if dc.FromPayload(request.Input, &input) != nil || input != "input" || dc.FromPayload(request.GetUserMetadata().GetSummary(), &summary) != nil || summary != "nexus-summary" ||
		dc.FromPayload(request.GetSearchAttributes().GetIndexedFields()["Category"], &category) != nil || category != "blue" {
		t.Fatal("Nexus input/summary/search attributes were not natively encoded")
	}
	started := capabilityEvidence(t, fixture, "nexus.start")
	if !started.Execution.Accepted || started.Execution.NexusOperationID != "operation" || started.Execution.RunID != "operation-run" || started.Source != client.Attribution() || started.Execution.WorkflowID != "" {
		t.Fatal("Nexus start evidence lost native target/use or invented Workflow identity")
	}
	for range 2 {
		var result string
		if err := run.Get(fixture.ctx, &result); err != nil || result != "nexus-result" {
			t.Fatal("Nexus result or cached decode changed", err)
		}
		if !capabilityEvidence(t, fixture, "nexus.result").Execution.ResultObtained {
			t.Fatal("Nexus result evidence missing")
		}
	}
	poll := semanticRequest[*workflowservice.PollNexusOperationExecutionRequest](t, peer.requests)
	if poll.Namespace != "test" || poll.OperationId != "operation" || poll.RunId != "operation-run" || poll.WaitStage != enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED || peer.polls.Load() != 1 || len(peer.requests) != 0 {
		t.Fatal("native Nexus cache/wait-stage/target semantics changed")
	}
	if value, err := client.ExecuteNexusOperation(fixture.ctx, target, definition, 17, options); err == nil || value != nil || hook.calls.Load() != 1 || len(peer.requests) != 0 {
		t.Fatal("typed operation-reference mismatch reached native serialization/wire")
	}
	capabilityEvidence(t, fixture, "nexus.start")
	if err := client.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if err := run.Get(fixture.ctx, new(string)); !errors.Is(err, ErrState) || peer.polls.Load() != 1 {
		t.Fatal("closed Nexus use revived a cached decoder", err)
	}
	other, err := fixture.owner.Client().GetNexusOperationHandle(sdk.GetNexusOperationHandleOptions{OperationID: "operation", RunID: "operation-run"})
	if err != nil || other.Get(fixture.ctx, new(string)) != nil || peer.polls.Load() != 2 {
		t.Fatal("closing one Nexus use revoked its live peer", err)
	}
	semanticRequest[*workflowservice.PollNexusOperationExecutionRequest](t, peer.requests)
	capabilityEvidence(t, fixture, "nexus.result")
}

func TestPublicNexusCanceledWaitDoesNotCancelOrTerminateOperation(t *testing.T) {
	peer := newPublicNexusPeer(t)
	entered := make(chan struct{})
	peer.poll = func(ctx context.Context, _ *workflowservice.PollNexusOperationExecutionRequest) (*workflowservice.PollNexusOperationExecutionResponse, error) {
		if peer.polls.Load() == 1 {
			close(entered)
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return &workflowservice.PollNexusOperationExecutionResponse{Outcome: &workflowservice.PollNexusOperationExecutionResponse_Result{Result: peer.result}}, nil
	}
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	run, err := fixture.owner.Client().GetNexusOperationHandle(sdk.GetNexusOperationHandleOptions{OperationID: "operation"})
	if err != nil || run.GetID() != "operation" || run.GetRunID() != "" || len(peer.requests) != 0 {
		t.Fatal("latest Nexus handle issued native work during construction", err)
	}
	wait, cancel := context.WithCancel(fixture.ctx)
	done := make(chan error, 1)
	go func() { done <- run.Get(wait, new(string)) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Nexus result poll did not enter")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("Nexus wait cancellation was lost", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled Nexus wait did not return")
	}
	semanticRequest[*workflowservice.PollNexusOperationExecutionRequest](t, peer.requests)
	if value := capabilityEvidence(t, fixture, "nexus.result"); value.Execution.ResultObtained || peer.cancels.Load() != 0 || peer.terminates.Load() != 0 {
		t.Fatal("canceling observation changed the remote Nexus execution")
	}
	var result string
	if err := run.Get(fixture.ctx, &result); err != nil || result != "nexus-result" || peer.polls.Load() != 2 {
		t.Fatal("fresh Nexus wait could not observe the result", err)
	}
	semanticRequest[*workflowservice.PollNexusOperationExecutionRequest](t, peer.requests)
	capabilityEvidence(t, fixture, "nexus.result")
	if err := run.Cancel(fixture.ctx, sdk.CancelNexusOperationOptions{Reason: "cancel"}); err != nil {
		t.Fatal(err)
	}
	if err := run.Terminate(fixture.ctx, sdk.TerminateNexusOperationOptions{Reason: "terminate"}); err != nil {
		t.Fatal(err)
	}
	canceled := semanticRequest[*workflowservice.RequestCancelNexusOperationExecutionRequest](t, peer.requests)
	terminated := semanticRequest[*workflowservice.TerminateNexusOperationExecutionRequest](t, peer.requests)
	if canceled.Namespace != "test" || canceled.OperationId != "operation" || canceled.RunId != "" || canceled.Reason != "cancel" || canceled.RequestId == "" ||
		terminated.OperationId != "operation" || terminated.RunId != "" || terminated.Reason != "terminate" || terminated.RequestId == canceled.RequestId {
		t.Fatal("explicit Nexus cancellation/termination request changed")
	}
	capabilityEvidence(t, fixture, "nexus.cancel")
	capabilityEvidence(t, fixture, "nexus.terminate")
}

func TestPublicNexusDescriptionLinksNestedFailureAndClosedUse(t *testing.T) {
	peer := newPublicNexusPeer(t)
	failure := &failurepb.Failure{Message: "attempt failure", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "nexus-attempt", NonRetryable: true, Details: semanticPayloads(t, "detail")}}}
	link := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "test", WorkflowId: "linked", RunId: "linked-run",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{EventRef: &commonpb.Link_WorkflowEvent_EventReference{EventId: 9, EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED}}}}}
	peer.description = &workflowservice.DescribeNexusOperationExecutionResponse{RunId: "operation-run", Info: &nexuspb.NexusOperationExecutionInfo{
		OperationId: "operation", RunId: "operation-run", Endpoint: "endpoint", Service: "service", Operation: "definition", Status: enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
		Attempt: 3, ScheduleTime: timestamppb.New(time.Unix(123, 0)), ScheduleToCloseTimeout: durationpb.New(time.Minute), StateTransitionCount: 7,
		OperationToken: "native-token", Links: []*commonpb.Link{link}, LastAttemptFailure: failure, UserMetadata: &sdkpb.UserMetadata{Summary: semanticPayloads(t, "summary").Payloads[0]},
		CancellationInfo: &nexuspb.NexusOperationExecutionCancellationInfo{Reason: "cancel reason", Attempt: 2, LastAttemptFailure: failure}}}
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	client, err := fixture.owner.Client().Borrow(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := client.GetNexusOperationHandle(sdk.GetNexusOperationHandleOptions{OperationID: "operation", RunID: "operation-run"})
	if err != nil {
		t.Fatal(err)
	}
	description, err := run.Describe(fixture.ctx, sdk.DescribeNexusOperationOptions{})
	if err != nil || description.Cancellation() == nil {
		t.Fatal("Nexus description lost nested cancellation", err)
	}
	request := semanticRequest[*workflowservice.DescribeNexusOperationExecutionRequest](t, peer.requests)
	if request.Namespace != "test" || request.OperationId != "operation" || request.RunId != "operation-run" {
		t.Fatal("Nexus description target changed")
	}
	capabilityEvidence(t, fixture, "nexus.describe")
	metadata := description.Metadata()
	if metadata.OperationID != "operation" || metadata.OperationRunID != "operation-run" || metadata.Attempt != 3 || metadata.Cancellation.Attempt != 2 || metadata.Cancellation.Reason != "cancel reason" ||
		metadata.ScheduleToCloseTimeout != time.Minute || metadata.StateTransitionCount != 7 || len(metadata.RawInfo.Links) != 1 || !proto.Equal(metadata.RawInfo.Links[0], link) {
		t.Fatal("Nexus description metadata/links conversion changed")
	}
	metadata.RawInfo.Links[0].GetWorkflowEvent().WorkflowId = "mutation"
	metadata.Cancellation.RawInfo.LastAttemptFailure.Message = "mutation"
	if !proto.Equal(description.Metadata().RawInfo.Links[0], link) || description.Metadata().Cancellation.RawInfo.LastAttemptFailure.Message != "attempt failure" {
		t.Fatal("Nexus returned links/cancellation metadata aliased retained data")
	}
	if summary, err := description.GetSummary(fixture.ctx); err != nil || summary != "summary" {
		t.Fatal("Nexus summary decoder changed", err)
	}
	capabilityEvidence(t, fixture, "nexus.describe-summary")
	var retained *sdktemporal.ApplicationError
	for _, test := range []struct {
		operation string
		read      func() error
	}{
		{"nexus.describe-failure", func() error { return description.GetLastAttemptFailure(fixture.ctx) }},
		{"nexus.cancel-failure", func() error { return description.Cancellation().GetLastAttemptFailure(fixture.ctx) }},
	} {
		err := test.read()
		native, captured := NativeError(err)
		if !captured || !errors.As(native, &retained) || retained.Type() != "nexus-attempt" || !retained.NonRetryable() || !proto.Equal(sdktemporal.GetDefaultFailureConverter().ErrorToFailure(native), failure) {
			t.Fatal("Nexus nested failure lost exact native semantics", test.operation, err)
		}
		capabilityEvidence(t, fixture, test.operation)
		var detail string
		if err := retained.Details(&detail); err != nil || detail != "detail" {
			t.Fatal("Nexus nested lazy error lost its originating use", err)
		}
		capabilityEvidence(t, fixture, "execution.error-details")
	}
	absentRun, _ := client.GetNexusOperationHandle(sdk.GetNexusOperationHandleOptions{OperationID: "no-cancellation"})
	absent, err := absentRun.Describe(fixture.ctx, sdk.DescribeNexusOperationOptions{})
	if err != nil || absent.Cancellation() != nil {
		t.Fatal("absent native cancellation was invented", err)
	}
	semanticRequest[*workflowservice.DescribeNexusOperationExecutionRequest](t, peer.requests)
	capabilityEvidence(t, fixture, "nexus.describe")
	if err := client.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := description.GetSummary(fixture.ctx); !errors.Is(err, ErrState) || retained.Details(new(string)) == nil || len(peer.requests) != 0 {
		t.Fatal("closed Nexus description/error retained decode authority", err)
	}
	if description.Metadata().OperationToken != "native-token" {
		t.Fatal("closing use erased caller-owned metadata")
	}
}

func TestPublicNexusPollingFailureKeepsNativeShapeAndCachedOutcome(t *testing.T) {
	peer := newPublicNexusPeer(t)
	application := &failurepb.Failure{Message: "native application", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "operation-failure", NonRetryable: true, Details: semanticPayloads(t, "detail")}}}
	wire := &failurepb.Failure{Message: "native operation", Cause: application, FailureInfo: &failurepb.Failure_NexusOperationExecutionFailureInfo{NexusOperationExecutionFailureInfo: &failurepb.NexusOperationFailureInfo{Endpoint: "endpoint", Service: "service", Operation: "definition", OperationToken: "private-token-canary"}}}
	peer.poll = func(context.Context, *workflowservice.PollNexusOperationExecutionRequest) (*workflowservice.PollNexusOperationExecutionResponse, error) {
		return &workflowservice.PollNexusOperationExecutionResponse{Outcome: &workflowservice.PollNexusOperationExecutionResponse_Failure{Failure: wire}}, nil
	}
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	run, err := fixture.owner.Client().GetNexusOperationHandle(sdk.GetNexusOperationHandleOptions{OperationID: "operation", RunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		err := run.Get(fixture.ctx, new(string))
		native, captured := NativeError(err)
		operation, exact := native.(*sdktemporal.NexusOperationError)
		var cause *sdktemporal.ApplicationError
		if !captured || !exact || !errors.As(native, &cause) || operation.OperationToken != "private-token-canary" || !cause.NonRetryable() ||
			!proto.Equal(sdktemporal.GetDefaultFailureConverter().ErrorToFailure(native), wire) || strings.Contains(err.Error(), "private-token-canary") {
			t.Fatal("Nexus native error/cause/wire or safe presentation changed", err)
		}
		value := capabilityEvidence(t, fixture, "nexus.result")
		if value.Execution.ResultObtained {
			t.Fatal("failed Nexus poll was reported as obtained success")
		}
		var detail string
		if err := cause.Details(&detail); err != nil || detail != "detail" {
			t.Fatal("Nexus result error decoder changed", err)
		}
		capabilityEvidence(t, fixture, "execution.error-details")
	}
	semanticRequest[*workflowservice.PollNexusOperationExecutionRequest](t, peer.requests)
	if peer.polls.Load() != 1 || len(peer.requests) != 0 {
		t.Fatal("cached Nexus failure was polled again")
	}
}

func TestPublicNexusVisibilityKeepsEmptyPagesMetadataAndCountGroups(t *testing.T) {
	peer := newPublicNexusPeer(t)
	peer.list = func(request *workflowservice.ListNexusOperationExecutionsRequest) *workflowservice.ListNexusOperationExecutionsResponse {
		if len(request.NextPageToken) == 0 {
			return &workflowservice.ListNexusOperationExecutionsResponse{NextPageToken: []byte("second")}
		}
		return &workflowservice.ListNexusOperationExecutionsResponse{Operations: []*nexuspb.NexusOperationExecutionListInfo{{OperationId: "operation", RunId: "run", Endpoint: "endpoint", Service: "service", Operation: "definition",
			Status: enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, ScheduleTime: timestamppb.New(time.Unix(123, 0)), StateTransitionCount: 3, ExecutionDuration: durationpb.New(2 * time.Second)}}, NextPageToken: []byte("must-not-fetch")}
	}
	peer.count = &workflowservice.CountNexusOperationExecutionsResponse{Count: 9, Groups: []*workflowservice.CountNexusOperationExecutionsResponse_AggregationGroup{{Count: 9, GroupValues: semanticPayloads(t, "Completed").Payloads}}}
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	stop := errors.New("stop Nexus visitor")
	visits := 0
	err := fixture.owner.Client().WalkNexusOperations(fixture.ctx, sdk.ListNexusOperationsOptions{Query: "Status = 'Completed'"}, func(_ context.Context, value *sdk.NexusOperationMetadata) error {
		visits++
		if value.OperationID != "operation" || value.OperationRunID != "run" || value.Endpoint != "endpoint" || value.Service != "service" || value.Operation != "definition" ||
			value.ExecutionDuration != 2*time.Second || value.StateTransitionCount != 3 || value.ScheduledTime != time.Unix(123, 0).UTC() {
			t.Error("native Nexus list metadata conversion changed")
		}
		return stop
	})
	if !errors.Is(err, stop) || visits != 1 {
		t.Fatal("Nexus visitor/empty-page semantics changed", err)
	}
	first := semanticRequest[*workflowservice.ListNexusOperationExecutionsRequest](t, peer.requests)
	second := semanticRequest[*workflowservice.ListNexusOperationExecutionsRequest](t, peer.requests)
	if first.Namespace != "test" || first.Query != "Status = 'Completed'" || string(second.NextPageToken) != "second" || len(peer.requests) != 0 {
		t.Fatal("Nexus continuation target changed or visitor stop fetched another page")
	}
	capabilityEvidence(t, fixture, "nexus.list")
	count, err := fixture.owner.Client().CountNexusOperations(fixture.ctx, sdk.CountNexusOperationsOptions{Query: "GROUP BY Status"})
	if err != nil || count.Count != 9 || len(count.Groups) != 1 || count.Groups[0].Count != 9 || count.Groups[0].GroupValues[0] != "Completed" {
		t.Fatal("native Nexus count aggregation changed", err)
	}
	request := semanticRequest[*workflowservice.CountNexusOperationExecutionsRequest](t, peer.requests)
	if request.Namespace != "test" || request.Query != "GROUP BY Status" {
		t.Fatal("Nexus count query changed")
	}
	capabilityEvidence(t, fixture, "nexus.count")
}

func TestPublicNexusUnavailableProfilesRemainNativeFailures(t *testing.T) {
	for _, profile := range []string{"namespace feature disabled", "route removed"} {
		t.Run(profile, func(t *testing.T) {
			peer := newPublicNexusPeer(t)
			peer.unavailable = status.Error(codes.Unimplemented, profile)
			fixture := newCapabilityFixture(t, peer, NativeOptions{})
			client := fixture.owner.Client()
			run, err := client.GetNexusOperationHandle(sdk.GetNexusOperationHandleOptions{OperationID: "operation", RunID: "run"})
			if err != nil {
				t.Fatal(err)
			}
			for _, test := range []struct {
				operation string
				call      func() error
			}{
				{"nexus.start", func() error {
					value, err := client.ExecuteNexusOperation(fixture.ctx, sdk.NexusClientOptions{Endpoint: "endpoint", Service: "service"}, "definition", "input", sdk.StartNexusOperationOptions{ID: "operation"})
					if value != nil {
						t.Error("unavailable Nexus start returned a handle")
					}
					return err
				}},
				{"nexus.result", func() error { return run.Get(fixture.ctx, new(string)) }},
				{"nexus.describe", func() error {
					value, err := run.Describe(fixture.ctx, sdk.DescribeNexusOperationOptions{})
					if value != nil {
						t.Error("unavailable Nexus describe returned data")
					}
					return err
				}},
				{"nexus.cancel", func() error { return run.Cancel(fixture.ctx, sdk.CancelNexusOperationOptions{}) }},
				{"nexus.terminate", func() error { return run.Terminate(fixture.ctx, sdk.TerminateNexusOperationOptions{}) }},
				{"nexus.list", func() error {
					return client.WalkNexusOperations(fixture.ctx, sdk.ListNexusOperationsOptions{}, func(context.Context, *sdk.NexusOperationMetadata) error {
						t.Error("unavailable Nexus list invoked visitor")
						return nil
					})
				}},
				{"nexus.count", func() error {
					value, err := client.CountNexusOperations(fixture.ctx, sdk.CountNexusOperationsOptions{})
					if value != nil {
						t.Error("unavailable Nexus count returned success data")
					}
					return err
				}},
			} {
				err := test.call()
				native, captured := NativeError(err)
				unavailable, exact := native.(*serviceerror.Unimplemented)
				if !captured || !exact || unavailable.Message != profile {
					t.Fatal("optional/removed Nexus profile lost native failure", test.operation, err)
				}
				value := capabilityEvidence(t, fixture, test.operation)
				if !value.Execution.NativeCalled || value.Execution.Accepted || value.Execution.ResultObtained {
					t.Fatal("Unimplemented was represented as accepted/successful native work", test.operation)
				}
				semanticRequest[proto.Message](t, peer.requests)
			}
			if len(peer.requests) != 0 {
				t.Fatal("unsupported native requests were retried or leaked extra RPCs")
			}
		})
	}
}
