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
	"slices"
	"sync/atomic"
	"testing"
	"time"

	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdktemporal "go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func semanticPayloads(t testing.TB, values ...any) *commonpb.Payloads {
	t.Helper()
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(values...)
	if err != nil {
		t.Fatal(err)
	}
	return payloads
}

func semanticRequest[T proto.Message](t testing.TB, requests <-chan proto.Message) T {
	t.Helper()
	select {
	case request := <-requests:
		value, ok := request.(T)
		if !ok {
			var want T
			t.Fatalf("native wire route changed: got %T, want %T", request, want)
		}
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("native wire request missing")
		var zero T
		return zero
	}
}

type publicActivityPeer struct {
	testServer
	requests    chan proto.Message
	result      *commonpb.Payloads
	poll        func(context.Context, *workflowservice.PollActivityExecutionRequest) (*workflowservice.PollActivityExecutionResponse, error)
	description *workflowservice.DescribeActivityExecutionResponse
	heartbeat   *workflowservice.RecordActivityTaskHeartbeatResponse
	byID        *workflowservice.RecordActivityTaskHeartbeatByIdResponse
	controlErr  error
	list        func(*workflowservice.ListActivityExecutionsRequest) *workflowservice.ListActivityExecutionsResponse
	count       *workflowservice.CountActivityExecutionsResponse
	polls       atomic.Int32
	cancels     atomic.Int32
	terminates  atomic.Int32
}

func newPublicActivityPeer(t testing.TB) *publicActivityPeer {
	return &publicActivityPeer{requests: make(chan proto.Message, 128), result: semanticPayloads(t, "activity-result")}
}

func (peer *publicActivityPeer) record(request proto.Message) { peer.requests <- proto.Clone(request) }
func (peer *publicActivityPeer) StartActivityExecution(_ context.Context, request *workflowservice.StartActivityExecutionRequest) (*workflowservice.StartActivityExecutionResponse, error) {
	peer.record(request)
	return &workflowservice.StartActivityExecutionResponse{RunId: "activity-run"}, nil
}
func (peer *publicActivityPeer) PollActivityExecution(ctx context.Context, request *workflowservice.PollActivityExecutionRequest) (*workflowservice.PollActivityExecutionResponse, error) {
	peer.record(request)
	peer.polls.Add(1)
	if peer.poll != nil {
		return peer.poll(ctx, request)
	}
	return &workflowservice.PollActivityExecutionResponse{Outcome: &activitypb.ActivityExecutionOutcome{Value: &activitypb.ActivityExecutionOutcome_Result{Result: peer.result}}}, nil
}
func (peer *publicActivityPeer) RequestCancelActivityExecution(_ context.Context, request *workflowservice.RequestCancelActivityExecutionRequest) (*workflowservice.RequestCancelActivityExecutionResponse, error) {
	peer.record(request)
	peer.cancels.Add(1)
	return &workflowservice.RequestCancelActivityExecutionResponse{}, nil
}
func (peer *publicActivityPeer) TerminateActivityExecution(_ context.Context, request *workflowservice.TerminateActivityExecutionRequest) (*workflowservice.TerminateActivityExecutionResponse, error) {
	peer.record(request)
	peer.terminates.Add(1)
	return &workflowservice.TerminateActivityExecutionResponse{}, nil
}
func (peer *publicActivityPeer) PauseActivityExecution(_ context.Context, request *workflowservice.PauseActivityExecutionRequest) (*workflowservice.PauseActivityExecutionResponse, error) {
	peer.record(request)
	return &workflowservice.PauseActivityExecutionResponse{}, peer.controlErr
}
func (peer *publicActivityPeer) UnpauseActivityExecution(_ context.Context, request *workflowservice.UnpauseActivityExecutionRequest) (*workflowservice.UnpauseActivityExecutionResponse, error) {
	peer.record(request)
	return &workflowservice.UnpauseActivityExecutionResponse{}, peer.controlErr
}
func (peer *publicActivityPeer) UpdateActivityExecutionOptions(_ context.Context, request *workflowservice.UpdateActivityExecutionOptionsRequest) (*workflowservice.UpdateActivityExecutionOptionsResponse, error) {
	peer.record(request)
	return &workflowservice.UpdateActivityExecutionOptionsResponse{ActivityOptions: &activitypb.ActivityOptions{
		TaskQueue: &taskqueuepb.TaskQueue{Name: "updated"}, HeartbeatTimeout: durationpb.New(7 * time.Second), RetryPolicy: &commonpb.RetryPolicy{MaximumAttempts: 9}}}, peer.controlErr
}
func (peer *publicActivityPeer) DescribeActivityExecution(_ context.Context, request *workflowservice.DescribeActivityExecutionRequest) (*workflowservice.DescribeActivityExecutionResponse, error) {
	peer.record(request)
	return proto.Clone(peer.description).(*workflowservice.DescribeActivityExecutionResponse), nil
}
func (peer *publicActivityPeer) RespondActivityTaskCompleted(_ context.Context, request *workflowservice.RespondActivityTaskCompletedRequest) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	peer.record(request)
	return &workflowservice.RespondActivityTaskCompletedResponse{}, nil
}
func (peer *publicActivityPeer) RespondActivityTaskFailed(_ context.Context, request *workflowservice.RespondActivityTaskFailedRequest) (*workflowservice.RespondActivityTaskFailedResponse, error) {
	peer.record(request)
	return &workflowservice.RespondActivityTaskFailedResponse{}, nil
}
func (peer *publicActivityPeer) RespondActivityTaskCanceled(_ context.Context, request *workflowservice.RespondActivityTaskCanceledRequest) (*workflowservice.RespondActivityTaskCanceledResponse, error) {
	peer.record(request)
	return &workflowservice.RespondActivityTaskCanceledResponse{}, nil
}
func (peer *publicActivityPeer) RespondActivityTaskCompletedById(_ context.Context, request *workflowservice.RespondActivityTaskCompletedByIdRequest) (*workflowservice.RespondActivityTaskCompletedByIdResponse, error) {
	peer.record(request)
	return &workflowservice.RespondActivityTaskCompletedByIdResponse{}, nil
}
func (peer *publicActivityPeer) RespondActivityTaskFailedById(_ context.Context, request *workflowservice.RespondActivityTaskFailedByIdRequest) (*workflowservice.RespondActivityTaskFailedByIdResponse, error) {
	peer.record(request)
	return &workflowservice.RespondActivityTaskFailedByIdResponse{}, nil
}
func (peer *publicActivityPeer) RespondActivityTaskCanceledById(_ context.Context, request *workflowservice.RespondActivityTaskCanceledByIdRequest) (*workflowservice.RespondActivityTaskCanceledByIdResponse, error) {
	peer.record(request)
	return &workflowservice.RespondActivityTaskCanceledByIdResponse{}, nil
}
func (peer *publicActivityPeer) RecordActivityTaskHeartbeat(_ context.Context, request *workflowservice.RecordActivityTaskHeartbeatRequest) (*workflowservice.RecordActivityTaskHeartbeatResponse, error) {
	peer.record(request)
	return peer.heartbeat, nil
}
func (peer *publicActivityPeer) RecordActivityTaskHeartbeatById(_ context.Context, request *workflowservice.RecordActivityTaskHeartbeatByIdRequest) (*workflowservice.RecordActivityTaskHeartbeatByIdResponse, error) {
	peer.record(request)
	return peer.byID, nil
}
func (peer *publicActivityPeer) ListActivityExecutions(_ context.Context, request *workflowservice.ListActivityExecutionsRequest) (*workflowservice.ListActivityExecutionsResponse, error) {
	peer.record(request)
	return peer.list(request), nil
}
func (peer *publicActivityPeer) CountActivityExecutions(_ context.Context, request *workflowservice.CountActivityExecutionsRequest) (*workflowservice.CountActivityExecutionsResponse, error) {
	peer.record(request)
	return peer.count, nil
}

func TestPublicActivityStartOptionsCachedResultAndClosedUse(t *testing.T) {
	peer := newPublicActivityPeer(t)
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	client, err := fixture.owner.Client().Borrow(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := sdktemporal.NewSearchAttributeKeyString("Category")
	options := sdk.StartActivityOptions{ID: "activity", TaskQueue: "queue", ScheduleToCloseTimeout: 20 * time.Second,
		ScheduleToStartTimeout: 2 * time.Second, StartToCloseTimeout: 7 * time.Second, HeartbeatTimeout: 3 * time.Second, StartDelay: 4 * time.Second,
		ActivityIDConflictPolicy: enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING, ActivityIDReusePolicy: enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		RetryPolicy:           &sdktemporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, BackoffCoefficient: 1.5, MaximumAttempts: 3, NonRetryableErrorTypes: []string{"fatal"}},
		TypedSearchAttributes: sdktemporal.NewSearchAttributes(key.ValueSet("blue")), Summary: "summary", StaticDetails: "details",
		Priority: sdktemporal.Priority{PriorityKey: 5, FairnessKey: "tenant", FairnessWeight: 1.5}}
	run, err := client.ExecuteActivity(fixture.ctx, options, "activity-definition", 17)
	if err != nil || run.GetID() != options.ID || run.GetRunID() != "activity-run" {
		t.Fatal("public Activity start lost native handle identity", err)
	}
	request := semanticRequest[*workflowservice.StartActivityExecutionRequest](t, peer.requests)
	if request.Namespace != "test" || request.ActivityId != "activity" || request.ActivityType.GetName() != "activity-definition" || request.TaskQueue.GetName() != "queue" ||
		request.GetScheduleToCloseTimeout().AsDuration() != 20*time.Second || request.GetScheduleToStartTimeout().AsDuration() != 2*time.Second ||
		request.GetStartToCloseTimeout().AsDuration() != 7*time.Second || request.GetHeartbeatTimeout().AsDuration() != 3*time.Second || request.GetStartDelay().AsDuration() != 4*time.Second ||
		request.GetRetryPolicy().GetMaximumAttempts() != 3 || request.GetRetryPolicy().GetBackoffCoefficient() != 1.5 || request.GetPriority().GetPriorityKey() != 5 ||
		request.GetPriority().GetFairnessKey() != "tenant" || request.GetPriority().GetFairnessWeight() != 1.5 || request.GetRequestId() == "" ||
		request.GetIdConflictPolicy() != options.ActivityIDConflictPolicy || request.GetIdReusePolicy() != options.ActivityIDReusePolicy {
		t.Fatal("Activity options were not converted into the native request")
	}
	var argument int
	var summary, details, category string
	dc := converter.GetDefaultDataConverter()
	if dc.FromPayloads(request.Input, &argument) != nil || argument != 17 || dc.FromPayload(request.GetUserMetadata().GetSummary(), &summary) != nil || summary != "summary" ||
		dc.FromPayload(request.GetUserMetadata().GetDetails(), &details) != nil || details != "details" || dc.FromPayload(request.GetSearchAttributes().GetIndexedFields()["Category"], &category) != nil || category != "blue" {
		t.Fatal("Activity input/metadata/search-attribute serialization changed")
	}
	started := capabilityEvidence(t, fixture, "activity.start")
	if !started.Execution.Accepted || started.Execution.ActivityID != "activity" || started.Execution.RunID != "activity-run" || started.Execution.WorkflowID != "" || started.Source != client.Attribution() {
		t.Fatal("Activity evidence invented Workflow ownership or lost its retained use")
	}
	for range 2 {
		var result string
		if err := run.Get(fixture.ctx, &result); err != nil || result != "activity-result" {
			t.Fatal("public Activity result/cached decoder failed", err)
		}
		if !capabilityEvidence(t, fixture, "activity.result").Execution.ResultObtained {
			t.Fatal("Activity result evidence missing")
		}
	}
	poll := semanticRequest[*workflowservice.PollActivityExecutionRequest](t, peer.requests)
	if poll.ActivityId != "activity" || poll.RunId != "activity-run" || peer.polls.Load() != 1 || len(peer.requests) != 0 {
		t.Fatal("cached Activity result repolled or changed native target")
	}
	if err := client.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if err := run.Get(fixture.ctx, new(string)); !errors.Is(err, ErrState) || peer.polls.Load() != 1 {
		t.Fatal("closed Activity use revived its cached decoder", err)
	}
	peerRun, err := fixture.owner.Client().GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "activity", RunID: "activity-run"})
	if err != nil || peerRun.Get(fixture.ctx, new(string)) != nil || peer.polls.Load() != 2 {
		t.Fatal("closing one use revoked a live Activity peer", err)
	}
	semanticRequest[*workflowservice.PollActivityExecutionRequest](t, peer.requests)
	capabilityEvidence(t, fixture, "activity.result")
}

func TestPublicActivityCompletionSeparatesTokenWorkflowAndStandaloneIDs(t *testing.T) {
	peer := newPublicActivityPeer(t)
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	client := fixture.owner.Client()
	nativeFailure := sdktemporal.NewNonRetryableApplicationError("fixture", "completion", nil, "failure-detail")
	for _, mode := range []string{"success", "failure", "canceled"} {
		var cause error
		if mode == "failure" {
			cause = nativeFailure
		} else if mode == "canceled" {
			cause = sdktemporal.NewCanceledError("canceled-detail")
		}
		if err := client.CompleteActivity(fixture.ctx, sdk.CompleteActivityOptions{TaskToken: []byte("token-canary"), Namespace: "test", WorkflowID: "serialization-only", Result: "result", Err: cause}); err != nil {
			t.Fatal(err)
		}
		switch mode {
		case "success":
			request := semanticRequest[*workflowservice.RespondActivityTaskCompletedRequest](t, peer.requests)
			var value string
			if string(request.TaskToken) != "token-canary" || request.Namespace != "test" || converter.GetDefaultDataConverter().FromPayloads(request.Result, &value) != nil || value != "result" {
				t.Fatal("token completion changed native target/result")
			}
		case "failure":
			request := semanticRequest[*workflowservice.RespondActivityTaskFailedRequest](t, peer.requests)
			if string(request.TaskToken) != "token-canary" || !proto.Equal(request.Failure, sdktemporal.GetDefaultFailureConverter().ErrorToFailure(cause)) {
				t.Fatal("token failure completion lost native failure semantics")
			}
		case "canceled":
			request := semanticRequest[*workflowservice.RespondActivityTaskCanceledRequest](t, peer.requests)
			var detail string
			if converter.GetDefaultDataConverter().FromPayloads(request.Details, &detail) != nil || detail != "canceled-detail" {
				t.Fatal("token cancellation details changed")
			}
		}
		value := capabilityEvidence(t, fixture, "activity.complete")
		if !value.Execution.CompletionAcknowledged || value.Execution.WorkflowID != "" || value.Execution.ActivityID != "" {
			t.Fatal("token completion invented IDs or lost acknowledgement")
		}
		for _, standalone := range []bool{false, true} {
			operation := "activity.complete-id"
			var err error
			if standalone {
				operation = "activity.complete-standalone"
				err = client.CompleteActivityByActivityID(fixture.ctx, sdk.CompleteActivityByActivityIDOptions{Namespace: "test", ActivityID: "standalone", ActivityRunID: "activity-run", WorkflowID: "serialization-only", Result: "result", Err: cause})
			} else {
				err = client.CompleteActivityByID(fixture.ctx, sdk.CompleteActivityByIDOptions{Namespace: "test", WorkflowID: "workflow", RunID: "workflow-run", ActivityID: "workflow-activity", Result: "result", Err: cause})
			}
			if err != nil {
				t.Fatal(err)
			}
			var workflowID, runID, activityID string
			switch mode {
			case "success":
				request := semanticRequest[*workflowservice.RespondActivityTaskCompletedByIdRequest](t, peer.requests)
				workflowID, runID, activityID = request.WorkflowId, request.RunId, request.ActivityId
			case "failure":
				request := semanticRequest[*workflowservice.RespondActivityTaskFailedByIdRequest](t, peer.requests)
				workflowID, runID, activityID = request.WorkflowId, request.RunId, request.ActivityId
				if !proto.Equal(request.Failure, sdktemporal.GetDefaultFailureConverter().ErrorToFailure(cause)) {
					t.Fatal("by-ID failure completion changed its Failure")
				}
			case "canceled":
				request := semanticRequest[*workflowservice.RespondActivityTaskCanceledByIdRequest](t, peer.requests)
				workflowID, runID, activityID = request.WorkflowId, request.RunId, request.ActivityId
			}
			wantWorkflow, wantRun, wantActivity := "workflow", "workflow-run", "workflow-activity"
			if standalone {
				wantWorkflow, wantRun, wantActivity = "", "activity-run", "standalone"
			}
			value := capabilityEvidence(t, fixture, operation)
			if workflowID != wantWorkflow || runID != wantRun || activityID != wantActivity || value.Execution.WorkflowID != wantWorkflow || value.Execution.RunID != wantRun || value.Execution.ActivityID != wantActivity || !value.Execution.CompletionAcknowledged {
				t.Fatal("Workflow and standalone completion identities were conflated")
			}
		}
	}
	if err := client.CompleteActivity(fixture.ctx, sdk.CompleteActivityOptions{TaskToken: []byte("pending"), Err: activity.ErrResultPending}); err != nil || len(peer.requests) != 0 {
		t.Fatal("native pending sentinel sent a completion", err)
	}
	if capabilityEvidence(t, fixture, "activity.complete").Execution.CompletionAcknowledged {
		t.Fatal("pending sentinel invented remote completion acknowledgement")
	}
	if err := client.CompleteActivityByID(fixture.ctx, sdk.CompleteActivityByIDOptions{Namespace: "foreign", WorkflowID: "workflow", ActivityID: "activity"}); !errors.Is(err, ErrAuthority) || len(peer.requests) != 0 {
		t.Fatal("completion crossed namespace before refusal", err)
	}
	if capabilityEvidence(t, fixture, "activity.complete-id").Execution.NativeCalled {
		t.Fatal("namespace refusal entered native completion")
	}
}

func TestPublicActivityHeartbeatKeepsAcknowledgementAndNativeDirectiveSemantics(t *testing.T) {
	for _, directive := range []string{"success", "cancel", "pause", "reset"} {
		for _, byID := range []bool{false, true} {
			t.Run(directive+map[bool]string{false: "/token", true: "/by-id"}[byID], func(t *testing.T) {
				peer := newPublicActivityPeer(t)
				peer.heartbeat = &workflowservice.RecordActivityTaskHeartbeatResponse{CancelRequested: directive == "cancel", ActivityPaused: directive == "pause", ActivityReset: directive == "reset"}
				peer.byID = &workflowservice.RecordActivityTaskHeartbeatByIdResponse{CancelRequested: directive == "cancel", ActivityPaused: directive == "pause", ActivityReset: directive == "reset"}
				fixture := newCapabilityFixture(t, peer, NativeOptions{})
				var err error
				operation := "activity.heartbeat"
				if byID {
					operation = "activity.heartbeat-id"
					err = fixture.owner.Client().RecordActivityHeartbeatByID(fixture.ctx, sdk.RecordActivityHeartbeatByIDOptions{WorkflowID: "workflow", RunID: "run", ActivityID: "activity", Details: []any{17}})
					request := semanticRequest[*workflowservice.RecordActivityTaskHeartbeatByIdRequest](t, peer.requests)
					var detail int
					if request.Namespace != "test" || request.WorkflowId != "workflow" || request.RunId != "run" || request.ActivityId != "activity" || converter.GetDefaultDataConverter().FromPayloads(request.Details, &detail) != nil || detail != 17 {
						t.Fatal("by-ID heartbeat target or details changed")
					}
				} else {
					err = fixture.owner.Client().RecordActivityHeartbeat(fixture.ctx, sdk.RecordActivityHeartbeatOptions{TaskToken: []byte("heartbeat-token"), Details: []any{17}})
					request := semanticRequest[*workflowservice.RecordActivityTaskHeartbeatRequest](t, peer.requests)
					var detail int
					if request.Namespace != "test" || string(request.TaskToken) != "heartbeat-token" || converter.GetDefaultDataConverter().FromPayloads(request.Details, &detail) != nil || detail != 17 {
						t.Fatal("token heartbeat target or details changed")
					}
				}
				if directive == "cancel" && !sdktemporal.IsCanceledError(err) || !byID && directive == "pause" && !errors.Is(err, activity.ErrActivityPaused) || !byID && directive == "reset" && !errors.Is(err, activity.ErrActivityReset) {
					t.Fatal("native heartbeat directive error was rewritten", err)
				}
				if (directive == "success" || byID && directive != "cancel") && err != nil {
					t.Fatal("native by-ID pause/reset limitation changed", err)
				}
				value := capabilityEvidence(t, fixture, operation)
				if !value.Execution.HeartbeatAcknowledged || value.Execution.CancellationRequested != (directive == "cancel") || value.Execution.ActivityPaused != (directive == "pause") || value.Execution.ActivityReset != (directive == "reset") {
					t.Fatal("acknowledged heartbeat lost independently observed response flags")
				}
				if err != nil {
					if native, present := NativeError(err); !present || native == nil {
						t.Fatal("directive error lost exact native semantic access")
					}
				}
			})
		}
	}
}

func TestPublicActivityOperationalGrantsAndSetClearRestore(t *testing.T) {
	peer := newPublicActivityPeer(t)
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	run, err := fixture.owner.Client().GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "activity", RunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		operation string
		call      func() error
	}{
		{"activity.pause", func() error { return run.Pause(fixture.ctx, sdk.PauseActivityOptions{}) }},
		{"activity.unpause", func() error { return run.Unpause(fixture.ctx, sdk.UnpauseActivityOptions{}) }},
		{"activity.update-options", func() error { _, err := run.UpdateOptions(fixture.ctx, sdk.ActivityOptionsUpdate{}); return err }},
		{"activity.restore-options", func() error { _, err := run.RestoreOriginalOptions(fixture.ctx); return err }},
	} {
		if err := test.call(); !errors.Is(err, ErrAuthority) || len(peer.requests) != 0 {
			t.Fatal("ungranted Activity control entered the SDK/wire", test.operation, err)
		}
		if capabilityEvidence(t, fixture, test.operation).Execution.NativeCalled {
			t.Fatal("refused Activity control claimed native entry")
		}
	}
	allowedPeer := newPublicActivityPeer(t)
	allowed := newCapabilityFixture(t, allowedPeer, NativeOptions{}, "PauseActivityExecution", "UnpauseActivityExecution", "UpdateActivityExecutionOptions")
	run, err = allowed.owner.Client().GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "activity", RunID: "run"})
	if err != nil || run.Pause(allowed.ctx, sdk.PauseActivityOptions{Reason: "pause"}) != nil || run.Unpause(allowed.ctx, sdk.UnpauseActivityOptions{Reason: "resume", Jitter: 321 * time.Millisecond}) != nil {
		t.Fatal("granted Activity pause/unpause failed", err)
	}
	pause := semanticRequest[*workflowservice.PauseActivityExecutionRequest](t, allowedPeer.requests)
	unpause := semanticRequest[*workflowservice.UnpauseActivityExecutionRequest](t, allowedPeer.requests)
	if pause.ActivityId != "activity" || pause.RunId != "run" || pause.Reason != "pause" || pause.RequestId == "" || unpause.Reason != "resume" || unpause.GetJitter().AsDuration() != 321*time.Millisecond || pause.RequestId == unpause.RequestId {
		t.Fatal("native pause/unpause request semantics changed")
	}
	capabilityEvidence(t, allowed, "activity.pause")
	capabilityEvidence(t, allowed, "activity.unpause")
	queue := "new-queue"
	zero := time.Duration(0)
	updated, err := run.UpdateOptions(allowed.ctx, sdk.ActivityOptionsUpdate{TaskQueue: &sdk.ActivityOptionChange[string]{Value: &queue}, HeartbeatTimeout: &sdk.ActivityOptionChange[time.Duration]{Value: &zero}, RetryPolicy: &sdk.ActivityOptionChange[sdktemporal.RetryPolicy]{}})
	if err != nil || updated.TaskQueue != "updated" || updated.HeartbeatTimeout != 7*time.Second || updated.RetryPolicy.MaximumAttempts != 9 {
		t.Fatal("native Activity option response conversion changed", err)
	}
	request := semanticRequest[*workflowservice.UpdateActivityExecutionOptionsRequest](t, allowedPeer.requests)
	if !slices.Equal(request.GetUpdateMask().GetPaths(), []string{"task_queue.name", "heartbeat_timeout", "retry_policy"}) || request.GetActivityOptions().GetTaskQueue().GetName() != queue || request.ActivityOptions.HeartbeatTimeout == nil || request.ActivityOptions.RetryPolicy != nil || request.RestoreOriginal {
		t.Fatal("Activity set/explicit-zero/clear distinctions were lost")
	}
	capabilityEvidence(t, allowed, "activity.update-options")
	if _, err := run.RestoreOriginalOptions(allowed.ctx); err != nil {
		t.Fatal(err)
	}
	restore := semanticRequest[*workflowservice.UpdateActivityExecutionOptionsRequest](t, allowedPeer.requests)
	if !restore.RestoreOriginal || len(restore.GetUpdateMask().GetPaths()) != 0 || restore.RequestId == request.RequestId {
		t.Fatal("native restore-original was synthesized as an ordinary update")
	}
	capabilityEvidence(t, allowed, "activity.restore-options")
	unsupportedPeer := newPublicActivityPeer(t)
	unsupportedPeer.controlErr = status.Error(codes.Unimplemented, "fixture gated extension")
	unsupported := newCapabilityFixture(t, unsupportedPeer, NativeOptions{}, "PauseActivityExecution")
	unsupportedRun, _ := unsupported.owner.Client().GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "activity"})
	err = unsupportedRun.Pause(unsupported.ctx, sdk.PauseActivityOptions{})
	if native, present := NativeError(err); !present {
		t.Fatalf("gated native capability refusal changed: captured=%t type=%T status=%v", present, native, status.Code(native))
	} else if _, exact := native.(*serviceerror.Unimplemented); !exact {
		t.Fatalf("gated native capability refusal changed concrete type: %T", native)
	}
	semanticRequest[*workflowservice.PauseActivityExecutionRequest](t, unsupportedPeer.requests)
	if capabilityEvidence(t, unsupported, "activity.pause").Execution.Accepted {
		t.Fatal("Unimplemented was recorded as accepted control")
	}
}

func TestPublicActivityCanceledWaitDoesNotCancelRemoteExecution(t *testing.T) {
	peer := newPublicActivityPeer(t)
	entered := make(chan struct{})
	peer.poll = func(ctx context.Context, _ *workflowservice.PollActivityExecutionRequest) (*workflowservice.PollActivityExecutionResponse, error) {
		if peer.polls.Load() == 1 {
			close(entered)
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return &workflowservice.PollActivityExecutionResponse{Outcome: &activitypb.ActivityExecutionOutcome{Value: &activitypb.ActivityExecutionOutcome_Result{Result: peer.result}}}, nil
	}
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	run, err := fixture.owner.Client().GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "activity"})
	if err != nil || run.GetRunID() != "" || len(peer.requests) != 0 {
		t.Fatal("latest-run Activity handle performed eager native work", err)
	}
	wait, cancel := context.WithCancel(fixture.ctx)
	done := make(chan error, 1)
	go func() { done <- run.Get(wait, new(string)) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("native result poll did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("result wait cancellation was lost", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled Activity wait did not return")
	}
	semanticRequest[*workflowservice.PollActivityExecutionRequest](t, peer.requests)
	if value := capabilityEvidence(t, fixture, "activity.result"); value.Execution.ResultObtained || peer.cancels.Load() != 0 || peer.terminates.Load() != 0 {
		t.Fatal("canceled wait claimed completion or canceled the remote Activity")
	}
	var result string
	if err := run.Get(fixture.ctx, &result); err != nil || result != "activity-result" || peer.polls.Load() != 2 {
		t.Fatal("a fresh result wait could not observe the still-running Activity", err)
	}
	semanticRequest[*workflowservice.PollActivityExecutionRequest](t, peer.requests)
	capabilityEvidence(t, fixture, "activity.result")
	if err := run.Cancel(fixture.ctx, sdk.CancelActivityOptions{Reason: "explicit cancel"}); err != nil {
		t.Fatal(err)
	}
	if err := run.Terminate(fixture.ctx, sdk.TerminateActivityOptions{Reason: "explicit terminate"}); err != nil {
		t.Fatal(err)
	}
	canceled := semanticRequest[*workflowservice.RequestCancelActivityExecutionRequest](t, peer.requests)
	terminated := semanticRequest[*workflowservice.TerminateActivityExecutionRequest](t, peer.requests)
	if canceled.Reason != "explicit cancel" || terminated.Reason != "explicit terminate" || canceled.RunId != "" || terminated.RunId != "" || canceled.RequestId == "" || canceled.RequestId == terminated.RequestId {
		t.Fatal("explicit Activity cancellation/termination target changed")
	}
	capabilityEvidence(t, fixture, "activity.cancel")
	capabilityEvidence(t, fixture, "activity.terminate")
}

func TestPublicActivityDescriptionDecodersFailuresAndClosedUse(t *testing.T) {
	peer := newPublicActivityPeer(t)
	failure := &failurepb.Failure{Message: "native attempt", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "attempt", NonRetryable: true, Details: semanticPayloads(t, "detail")}}}
	peer.description = &workflowservice.DescribeActivityExecutionResponse{RunId: "run", Info: &activitypb.ActivityExecutionInfo{
		ActivityId: "activity", RunId: "run", ActivityType: &commonpb.ActivityType{Name: "definition"}, TaskQueue: "queue", SearchAttributes: &commonpb.SearchAttributes{},
		Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, Attempt: 2, ScheduleTime: timestamppb.New(time.Unix(123, 0)), HeartbeatDetails: semanticPayloads(t, "heartbeat"), LastFailure: failure,
		UserMetadata: &sdkpb.UserMetadata{Summary: semanticPayloads(t, "summary").Payloads[0], Details: semanticPayloads(t, "details").Payloads[0]}},
		Input: semanticPayloads(t, "input"), Outcome: &activitypb.ActivityExecutionOutcome{Value: &activitypb.ActivityExecutionOutcome_Result{Result: semanticPayloads(t, "result")}}}
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	client, err := fixture.owner.Client().Borrow(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := client.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "activity", RunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	description, err := run.Describe(fixture.ctx, sdk.DescribeActivityOptions{IncludeInput: true, IncludeOutcome: true, IncludeHeartbeatDetails: true, IncludeLastFailure: true})
	if err != nil || !description.HasInput() || !description.HasResult() || !description.HasHeartbeatDetails() || !description.HasLastFailure() || description.HasOutcomeFailure() {
		t.Fatal("Activity description presence semantics changed", err)
	}
	request := semanticRequest[*workflowservice.DescribeActivityExecutionRequest](t, peer.requests)
	if !request.IncludeInput || !request.IncludeOutcome || !request.IncludeHeartbeatDetails || !request.IncludeLastFailure || request.ActivityId != "activity" || request.RunId != "run" {
		t.Fatal("description options were not preserved")
	}
	capabilityEvidence(t, fixture, "activity.describe")
	metadata := description.Metadata()
	metadata.RawResponse.Input.Payloads[0].Data = []byte("mutation")
	if description.Metadata().Attempt != 2 || description.Metadata().ScheduleTime != time.Unix(123, 0).UTC() {
		t.Fatal("Activity description metadata conversion changed")
	}
	for _, test := range []struct {
		operation, want string
		read            func(*string) error
	}{
		{"activity.describe-input", "input", func(out *string) error { return description.GetInput(fixture.ctx, out) }},
		{"activity.describe-heartbeat", "heartbeat", func(out *string) error { return description.GetHeartbeatDetails(fixture.ctx, out) }},
		{"activity.describe-result", "result", func(out *string) error { return description.GetResult(fixture.ctx, out) }},
	} {
		var output string
		if err := test.read(&output); err != nil || output != test.want {
			t.Fatal("description lazy decode changed or metadata copy aliased", test.operation, err)
		}
		capabilityEvidence(t, fixture, test.operation)
	}
	if value, err := description.GetSummary(fixture.ctx); err != nil || value != "summary" {
		t.Fatal("Activity summary conversion changed", err)
	}
	capabilityEvidence(t, fixture, "activity.describe-summary")
	if value, err := description.GetStaticDetails(fixture.ctx); err != nil || value != "details" {
		t.Fatal("Activity static details conversion changed", err)
	}
	capabilityEvidence(t, fixture, "activity.describe-details")
	err = description.GetLastFailure(fixture.ctx)
	var application *sdktemporal.ApplicationError
	if native, captured := NativeError(err); !captured || !errors.As(native, &application) || !application.NonRetryable() || application.Type() != "attempt" || !proto.Equal(sdktemporal.GetDefaultFailureConverter().ErrorToFailure(native), failure) {
		t.Fatal("Activity last-failure accessor lost native semantics", err)
	}
	capabilityEvidence(t, fixture, "activity.describe-last-failure")
	var detail string
	if err := application.Details(&detail); err != nil || detail != "detail" {
		t.Fatal("Activity failure lazy details lost retained ownership", err)
	}
	capabilityEvidence(t, fixture, "execution.error-details")
	if err := description.GetOutcomeFailure(fixture.ctx); err != nil {
		t.Fatal("successful outcome invented a failure", err)
	}
	capabilityEvidence(t, fixture, "activity.describe-outcome")
	if err := client.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if !description.HasInput() || description.GetInput(fixture.ctx, new(string)) == nil || application.Details(new(string)) == nil || len(peer.requests) != 0 {
		t.Fatal("closed description/error use retained a decoder or lost local presence")
	}
	failedPeer := newPublicActivityPeer(t)
	failedPeer.description = proto.Clone(peer.description).(*workflowservice.DescribeActivityExecutionResponse)
	failedPeer.description.Info.Status = enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
	failedPeer.description.Outcome = &activitypb.ActivityExecutionOutcome{Value: &activitypb.ActivityExecutionOutcome_Failure{Failure: failure}}
	failed := newCapabilityFixture(t, failedPeer, NativeOptions{})
	failedRun, err := failed.owner.Client().GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "activity", RunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	failedDescription, err := failedRun.Describe(failed.ctx, sdk.DescribeActivityOptions{IncludeOutcome: true})
	if err != nil || !failedDescription.HasOutcomeFailure() || failedDescription.HasResult() {
		t.Fatal("terminal Activity failure presence was conflated with success", err)
	}
	semanticRequest[*workflowservice.DescribeActivityExecutionRequest](t, failedPeer.requests)
	capabilityEvidence(t, failed, "activity.describe")
	err = failedDescription.GetOutcomeFailure(failed.ctx)
	if native, captured := NativeError(err); !captured || !proto.Equal(sdktemporal.GetDefaultFailureConverter().ErrorToFailure(native), failure) {
		t.Fatal("terminal Activity failure conversion changed", err)
	}
	capabilityEvidence(t, failed, "activity.describe-outcome")
	if err := failedDescription.GetResult(failed.ctx, new(string)); !errors.Is(err, sdktemporal.ErrNoData) {
		t.Fatal("failed Activity invented a result", err)
	}
	capabilityEvidence(t, failed, "activity.describe-result")
}

func TestPublicActivityVisibilityConvertsCountsAndStopsNativePagination(t *testing.T) {
	peer := newPublicActivityPeer(t)
	peer.list = func(request *workflowservice.ListActivityExecutionsRequest) *workflowservice.ListActivityExecutionsResponse {
		if len(request.NextPageToken) == 0 {
			return &workflowservice.ListActivityExecutionsResponse{NextPageToken: []byte("second")}
		}
		return &workflowservice.ListActivityExecutionsResponse{Executions: []*activitypb.ActivityExecutionListInfo{{ActivityId: "activity", RunId: "run", ActivityType: &commonpb.ActivityType{Name: "definition"}, SearchAttributes: &commonpb.SearchAttributes{}, Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}}, NextPageToken: []byte("must-not-fetch")}
	}
	peer.count = &workflowservice.CountActivityExecutionsResponse{Count: 7, Groups: []*workflowservice.CountActivityExecutionsResponse_AggregationGroup{{Count: 7, GroupValues: semanticPayloads(t, "Completed").Payloads}}}
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	stop := errors.New("stop visitor")
	visited := 0
	err := fixture.owner.Client().WalkActivities(fixture.ctx, sdk.ListActivitiesOptions{Query: "Status = 'Completed'"}, func(_ context.Context, value *sdk.ActivityExecutionInfo) error {
		visited++
		if value.ActivityID != "activity" || value.ActivityRunID != "run" || value.ActivityType != "definition" || value.Status != enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED {
			t.Error("native Activity list metadata conversion changed")
		}
		return stop
	})
	if !errors.Is(err, stop) || visited != 1 {
		t.Fatal("Activity visitor error/empty continuation behavior changed", err)
	}
	first := semanticRequest[*workflowservice.ListActivityExecutionsRequest](t, peer.requests)
	second := semanticRequest[*workflowservice.ListActivityExecutionsRequest](t, peer.requests)
	if first.Namespace != "test" || first.Query != "Status = 'Completed'" || string(second.NextPageToken) != "second" || len(peer.requests) != 0 {
		t.Fatal("Activity pagination changed target or fetched after visitor stop")
	}
	capabilityEvidence(t, fixture, "activity.list")
	count, err := fixture.owner.Client().CountActivities(fixture.ctx, sdk.CountActivitiesOptions{Query: "GROUP BY Status"})
	if err != nil || count.Count != 7 || len(count.Groups) != 1 || count.Groups[0].Count != 7 || count.Groups[0].GroupValues[0] != "Completed" {
		t.Fatal("Activity aggregation conversion changed", err)
	}
	request := semanticRequest[*workflowservice.CountActivityExecutionsRequest](t, peer.requests)
	if request.Namespace != "test" || request.Query != "GROUP BY Status" {
		t.Fatal("Activity count query changed")
	}
	capabilityEvidence(t, fixture, "activity.count")
}
