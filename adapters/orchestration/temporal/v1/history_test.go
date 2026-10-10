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
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type categoryHistoryPeer struct {
	testServer
	requests      chan proto.Message
	historyCalls  atomic.Int32
	metadataCalls atomic.Int32
}

func categoryHistoryInfo() *workflowpb.WorkflowExecutionInfo {
	return &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: "listed", RunId: "run"},
		Type: &commonpb.WorkflowType{Name: "definition"}, TaskQueue: "queue", Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		HistoryLength: 17, StartTime: timestamppb.New(time.Unix(100, 0)), CloseTime: timestamppb.New(time.Unix(110, 0))}
}

func (peer *categoryHistoryPeer) record(request proto.Message) {
	peer.metadataCalls.Add(1)
	peer.requests <- proto.Clone(request)
}

func (peer *categoryHistoryPeer) ListWorkflowExecutions(_ context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	peer.record(request)
	return &workflowservice.ListWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{categoryHistoryInfo()}, NextPageToken: []byte("next")}, nil
}
func (peer *categoryHistoryPeer) ListOpenWorkflowExecutions(_ context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	peer.record(request)
	return &workflowservice.ListOpenWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{categoryHistoryInfo()}, NextPageToken: []byte("next")}, nil
}
func (peer *categoryHistoryPeer) ListClosedWorkflowExecutions(_ context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	peer.record(request)
	return &workflowservice.ListClosedWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{categoryHistoryInfo()}, NextPageToken: []byte("next")}, nil
}
func (peer *categoryHistoryPeer) ListArchivedWorkflowExecutions(_ context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	peer.record(request)
	if request.Query == "unavailable" {
		return nil, serviceerror.ToStatus(serviceerror.NewUnimplemented("controlled unavailable archive")).Err()
	}
	return &workflowservice.ListArchivedWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{categoryHistoryInfo()}, NextPageToken: []byte("next")}, nil
}
func (peer *categoryHistoryPeer) ScanWorkflowExecutions(_ context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
	peer.record(request)
	if request.Query == "removed" {
		return nil, serviceerror.ToStatus(serviceerror.NewUnimplemented("controlled removed scan")).Err()
	}
	return &workflowservice.ScanWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{categoryHistoryInfo()}, NextPageToken: []byte("next")}, nil
}
func (peer *categoryHistoryPeer) CountWorkflowExecutions(_ context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	peer.record(request)
	return &workflowservice.CountWorkflowExecutionsResponse{Count: 7}, nil
}
func (peer *categoryHistoryPeer) GetSearchAttributes(_ context.Context, request *workflowservice.GetSearchAttributesRequest) (*workflowservice.GetSearchAttributesResponse, error) {
	peer.record(request)
	return &workflowservice.GetSearchAttributesResponse{Keys: map[string]enumspb.IndexedValueType{"custom": enumspb.INDEXED_VALUE_TYPE_KEYWORD}}, nil
}
func (peer *categoryHistoryPeer) DescribeTaskQueue(_ context.Context, request *workflowservice.DescribeTaskQueueRequest) (*workflowservice.DescribeTaskQueueResponse, error) {
	peer.record(request)
	if request.GetTaskQueue().GetName() == "unsupported" {
		return &workflowservice.DescribeTaskQueueResponse{Pollers: []*taskqueuepb.PollerInfo{{Identity: "legacy-only"}}}, nil
	}
	return &workflowservice.DescribeTaskQueueResponse{
		Pollers:         []*taskqueuepb.PollerInfo{{Identity: "legacy", RatePerSecond: 3}},
		TaskQueueStatus: &taskqueuepb.TaskQueueStatus{BacklogCountHint: 9},
		VersioningInfo: &taskqueuepb.TaskQueueVersioningInfo{
			CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "current"},
			RampingDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "ramp"},
			RampingVersionPercentage: 25, UpdateTime: timestamppb.New(time.Unix(300, 0))},
		VersionsInfo: map[string]*taskqueuepb.TaskQueueVersionInfo{"build": {TaskReachability: enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE,
			TypesInfo: map[int32]*taskqueuepb.TaskQueueTypeInfo{int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY): {
				Pollers: []*taskqueuepb.PollerInfo{{Identity: "worker", RatePerSecond: 5, LastAccessTime: timestamppb.New(time.Unix(200, 0))}},
				Stats:   &taskqueuepb.TaskQueueStats{ApproximateBacklogCount: 11, ApproximateBacklogAge: durationpb.New(3 * time.Second), TasksAddRate: 7, TasksDispatchRate: 2},
			}}}},
	}, nil
}

func TestPublicHistoryVisibilityPagesCopyInputsAndPreserveNativeMetadata(t *testing.T) {
	peer := &categoryHistoryPeer{requests: make(chan proto.Message, 32)}
	fixture := newCapabilityFixture(t, peer, NativeOptions{}, "GetSearchAttributes", "DescribeTaskQueue")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cases := []struct {
		name, operation string
		request         proto.Message
		response        proto.Message
		invoke          func(context.Context, *Client, proto.Message) (proto.Message, error)
	}{
		{"list", "workflow.listworkflow", &workflowservice.ListWorkflowExecutionsRequest{Query: "query", NextPageToken: []byte("cursor")},
			&workflowservice.ListWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{categoryHistoryInfo()}, NextPageToken: []byte("next")},
			func(ctx context.Context, client *Client, request proto.Message) (proto.Message, error) {
				return client.ListWorkflow(ctx, request.(*workflowservice.ListWorkflowExecutionsRequest))
			}},
		{"open", "workflow.listopenworkflow", &workflowservice.ListOpenWorkflowExecutionsRequest{MaximumPageSize: 7, NextPageToken: []byte("cursor"),
			Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{WorkflowId: "selected"}}},
			&workflowservice.ListOpenWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{categoryHistoryInfo()}, NextPageToken: []byte("next")},
			func(ctx context.Context, client *Client, request proto.Message) (proto.Message, error) {
				return client.ListOpenWorkflow(ctx, request.(*workflowservice.ListOpenWorkflowExecutionsRequest))
			}},
		{"closed", "workflow.listclosedworkflow", &workflowservice.ListClosedWorkflowExecutionsRequest{NextPageToken: []byte("cursor"),
			Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_StatusFilter{StatusFilter: &filterpb.StatusFilter{Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED}}},
			&workflowservice.ListClosedWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{categoryHistoryInfo()}, NextPageToken: []byte("next")},
			func(ctx context.Context, client *Client, request proto.Message) (proto.Message, error) {
				return client.ListClosedWorkflow(ctx, request.(*workflowservice.ListClosedWorkflowExecutionsRequest))
			}},
		{"archive", "workflow.listarchivedworkflow", &workflowservice.ListArchivedWorkflowExecutionsRequest{Query: "query", PageSize: 7, NextPageToken: []byte("cursor")},
			&workflowservice.ListArchivedWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{categoryHistoryInfo()}, NextPageToken: []byte("next")},
			func(ctx context.Context, client *Client, request proto.Message) (proto.Message, error) {
				return client.ListArchivedWorkflow(ctx, request.(*workflowservice.ListArchivedWorkflowExecutionsRequest))
			}},
		{"scan", "workflow.scan", &workflowservice.ScanWorkflowExecutionsRequest{Query: "query", NextPageToken: []byte("cursor")},
			&workflowservice.ScanWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{categoryHistoryInfo()}, NextPageToken: []byte("next")},
			func(ctx context.Context, client *Client, request proto.Message) (proto.Message, error) {
				return client.ScanWorkflow(ctx, request.(*workflowservice.ScanWorkflowExecutionsRequest))
			}},
		{"count", "workflow.countworkflow", &workflowservice.CountWorkflowExecutionsRequest{Query: "query"}, &workflowservice.CountWorkflowExecutionsResponse{Count: 7},
			func(ctx context.Context, client *Client, request proto.Message) (proto.Message, error) {
				return client.CountWorkflow(ctx, request.(*workflowservice.CountWorkflowExecutionsRequest))
			}},
	}
	for _, scenario := range cases {
		t.Run(scenario.name, func(t *testing.T) {
			original := proto.Clone(scenario.request)
			response, err := scenario.invoke(ctx, fixture.owner.Client(), scenario.request)
			if err != nil || !proto.Equal(response, scenario.response) || !proto.Equal(original, scenario.request) {
				t.Fatal("public visibility page changed a native field or mutated its request", err)
			}
			expected := proto.Clone(original)
			expected.ProtoReflect().Set(expected.ProtoReflect().Descriptor().Fields().ByName("namespace"), protoreflect.ValueOfString("test"))
			select {
			case request := <-peer.requests:
				if !proto.Equal(request, expected) {
					t.Fatal("native namespace/default/filter/page mapping changed")
				}
			case <-ctx.Done():
				t.Fatal("visibility request was not observed")
			}
			evidence := capabilityEvidence(t, fixture, scenario.operation)
			if evidence.Source != fixture.owner.Client().Attribution() || evidence.Execution.Namespace != "test" || !evidence.Execution.ResultObtained {
				t.Fatal("visibility evidence lost its effective source or success")
			}
			explicit := proto.Clone(expected)
			response, err = scenario.invoke(ctx, fixture.owner.Client(), explicit)
			if err != nil || !proto.Equal(response, scenario.response) || !proto.Equal(explicit, expected) || !proto.Equal(<-peer.requests, expected) {
				t.Fatal("explicit matching namespace or caller-owned request changed", err)
			}
			if !capabilityEvidence(t, fixture, scenario.operation).Execution.ResultObtained {
				t.Fatal("matching explicit namespace did not produce result evidence")
			}
			foreign := proto.Clone(original)
			foreign.ProtoReflect().Set(foreign.ProtoReflect().Descriptor().Fields().ByName("namespace"), protoreflect.ValueOfString("another"))
			before := peer.metadataCalls.Load()
			if _, err := scenario.invoke(ctx, fixture.owner.Client(), foreign); !errors.Is(err, ErrAuthority) {
				t.Fatal("cross-namespace visibility call was not refused", err)
			}
			if peer.metadataCalls.Load() != before {
				t.Fatal("refused namespace reached the peer")
			}
			capabilityEvidence(t, fixture, scenario.operation)
		})
	}
}

func TestPublicHistoryMetadataAndRemovedServiceSemantics(t *testing.T) {
	peer := &categoryHistoryPeer{requests: make(chan proto.Message, 16)}
	fixture := newCapabilityFixture(t, peer, NativeOptions{}, "GetSearchAttributes", "DescribeTaskQueue")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attributes, err := fixture.owner.Client().GetSearchAttributes(ctx)
	if err != nil || attributes.GetKeys()["custom"] != enumspb.INDEXED_VALUE_TYPE_KEYWORD {
		t.Fatal("native search attribute metadata changed", err)
	}
	<-peer.requests
	capabilityEvidence(t, fixture, "workflow.search-attributes")
	legacy, err := fixture.owner.Client().DescribeTaskQueue(ctx, "queue", enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	if err != nil || legacy.GetTaskQueueStatus().GetBacklogCountHint() != 9 || legacy.GetPollers()[0].GetIdentity() != "legacy" {
		t.Fatal("legacy task-queue metadata changed", err)
	}
	request := (<-peer.requests).(*workflowservice.DescribeTaskQueueRequest)
	if request.Namespace != "test" || request.TaskQueue.Name != "queue" || request.TaskQueue.Kind != enumspb.TASK_QUEUE_KIND_NORMAL || request.TaskQueueType != enumspb.TASK_QUEUE_TYPE_ACTIVITY {
		t.Fatal("legacy task-queue request changed")
	}
	capabilityEvidence(t, fixture, "task-queue.describe")
	options := sdk.DescribeTaskQueueEnhancedOptions{TaskQueue: "queue", Versions: &sdk.TaskQueueVersionSelection{BuildIDs: []string{"build"}},
		TaskQueueTypes: []sdk.TaskQueueType{sdk.TaskQueueTypeActivity}, ReportPollers: true, ReportStats: true, ReportTaskReachability: true}
	enhanced, err := fixture.owner.Client().DescribeTaskQueueEnhanced(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	request = (<-peer.requests).(*workflowservice.DescribeTaskQueueRequest)
	if request.Namespace != "test" || request.GetApiMode() != enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED || !request.ReportStats || !request.ReportPollers || !request.ReportTaskReachability ||
		!reflect.DeepEqual(request.GetVersions().GetBuildIds(), []string{"build"}) || !reflect.DeepEqual(options.Versions.BuildIDs, []string{"build"}) {
		t.Fatal("enhanced task-queue options changed")
	}
	typeInfo := enhanced.VersionsInfo["build"].TypesInfo[sdk.TaskQueueTypeActivity]
	if typeInfo.Stats == nil || typeInfo.Stats.ApproximateBacklogCount != 11 || typeInfo.Stats.ApproximateBacklogAge != 3*time.Second || typeInfo.Stats.TasksAddRate != 7 || typeInfo.Stats.TasksDispatchRate != 2 ||
		len(typeInfo.Pollers) != 1 || typeInfo.Pollers[0].Identity != "worker" || !typeInfo.Pollers[0].LastAccessTime.Equal(time.Unix(200, 0)) {
		t.Fatal("enhanced task-queue result conversion changed")
	}
	versioning := enhanced.VersioningInfo
	if enhanced.VersionsInfo["build"].TaskReachability != sdk.BuildIDTaskReachabilityReachable || versioning == nil ||
		versioning.CurrentVersion == nil || versioning.CurrentVersion.DeploymentName != "deployment" || versioning.CurrentVersion.BuildID != "current" ||
		versioning.RampingVersion == nil || versioning.RampingVersion.BuildID != "ramp" || versioning.RampingVersionPercentage != 25 || !versioning.UpdateTime.Equal(time.Unix(300, 0)) {
		t.Fatal("enhanced task-queue routing metadata changed")
	}
	capabilityEvidence(t, fixture, "task-queue.describe-enhanced")
	_, err = fixture.owner.Client().DescribeTaskQueueEnhanced(ctx, sdk.DescribeTaskQueueEnhancedOptions{TaskQueue: "unsupported"})
	native, present := NativeError(err)
	if err == nil || !present || !strings.Contains(native.Error(), "server does not support") {
		t.Fatal("native enhanced-description refusal was lost", err)
	}
	<-peer.requests
	if capabilityEvidence(t, fixture, "task-queue.describe-enhanced").Execution.ResultObtained {
		t.Fatal("legacy-only server was certified as enhanced")
	}
	for _, scenario := range []struct {
		operation string
		invoke    func() error
	}{
		{"workflow.scan", func() error {
			_, err := fixture.owner.Client().ScanWorkflow(ctx, &workflowservice.ScanWorkflowExecutionsRequest{Query: "removed"})
			return err
		}},
		{"workflow.listarchivedworkflow", func() error {
			_, err := fixture.owner.Client().ListArchivedWorkflow(ctx, &workflowservice.ListArchivedWorkflowExecutionsRequest{Query: "unavailable"})
			return err
		}},
	} {
		err := scenario.invoke()
		var unavailable *serviceerror.Unimplemented
		if !errors.As(err, &unavailable) {
			t.Fatal("native service-removal refusal was flattened", err)
		}
		if native, present := NativeError(err); !present || !errors.As(native, &unavailable) {
			t.Fatal("exact native refusal was not captured")
		}
		<-peer.requests
		evidence := capabilityEvidence(t, fixture, scenario.operation)
		if evidence.Execution.ResultObtained {
			t.Fatal("native refusal was reported as a result")
		}
	}
}

func (peer *categoryHistoryPeer) GetWorkflowExecutionHistory(_ context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	peer.historyCalls.Add(1)
	peer.requests <- proto.Clone(request)
	if request.GetExecution().GetWorkflowId() == "refused" {
		return nil, serviceerror.ToStatus(serviceerror.NewPermissionDenied("controlled history refusal", "")).Err()
	}
	switch string(request.NextPageToken) {
	case "":
		return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{}, NextPageToken: []byte("empty")}, nil
	case "empty":
		return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{EventId: 10, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED}}}, NextPageToken: []byte("last")}, nil
	case "last":
		return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{EventId: 11, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED}}}}, nil
	default:
		return nil, serviceerror.ToStatus(serviceerror.NewInvalidArgument("unexpected history continuation")).Err()
	}
}

func TestPublicHistoryWalkEmptyPagesOwnsVisitorsAndStopsOnFailure(t *testing.T) {
	for _, mode := range []string{"complete", "visitor-error", "cancel", "closed-use", "native-refusal"} {
		t.Run(mode, func(t *testing.T) {
			peer := &categoryHistoryPeer{requests: make(chan proto.Message, 8)}
			fixture := newCapabilityFixture(t, peer, NativeOptions{})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := fixture.owner.Client().Borrow(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "closed-use" {
				if err := client.Close(ctx); err != nil {
					t.Fatal(err)
				}
			}
			marker := errors.New("controlled history visitor failure")
			var ids []int64
			var visitorContext context.Context
			workflowID := "workflow"
			if mode == "native-refusal" {
				workflowID = "refused"
			}
			err = client.WalkHistory(ctx, workflowID, "run", true, enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT, func(work context.Context, event *historypb.HistoryEvent) error {
				visitorContext = work
				ids = append(ids, event.EventId)
				switch mode {
				case "visitor-error":
					return marker
				case "cancel":
					cancel()
				}
				return nil
			})
			switch mode {
			case "complete":
				if err != nil || !reflect.DeepEqual(ids, []int64{10, 11}) || peer.historyCalls.Load() != 3 {
					t.Fatal("empty history page was mistaken for exhaustion", ids, err)
				}
			case "visitor-error":
				if !errors.Is(err, marker) || peer.historyCalls.Load() != 2 || len(ids) != 1 {
					t.Fatal("visitor failure was lost or another page was fetched", err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) || peer.historyCalls.Load() != 2 || len(ids) != 1 {
					t.Fatal("visitor cancellation did not stop pagination", err)
				}
			case "closed-use":
				if err == nil || peer.historyCalls.Load() != 0 || len(ids) != 0 {
					t.Fatal("closed use fetched history or invoked a visitor", err)
				}
			case "native-refusal":
				var refused *serviceerror.PermissionDenied
				if !errors.As(err, &refused) || peer.historyCalls.Load() != 1 || len(ids) != 0 {
					t.Fatal("native history refusal changed", err)
				}
			}
			if visitorContext != nil && visitorContext.Err() == nil {
				t.Fatal("history visitor context escaped its admitted lifetime")
			}
			var tokens []string
			for range peer.historyCalls.Load() {
				request := (<-peer.requests).(*workflowservice.GetWorkflowExecutionHistoryRequest)
				tokens = append(tokens, string(request.NextPageToken))
				if request.Namespace != "test" || request.GetExecution().GetWorkflowId() != workflowID || request.GetExecution().GetRunId() != "run" || !request.WaitNewEvent || request.HistoryEventFilterType != enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT {
					t.Fatal("history identity, filter or long-poll options changed")
				}
			}
			var expectedTokens []string
			switch mode {
			case "complete":
				expectedTokens = []string{"", "empty", "last"}
			case "visitor-error", "cancel":
				expectedTokens = []string{"", "empty"}
			case "native-refusal":
				expectedTokens = []string{""}
			}
			if !reflect.DeepEqual(tokens, expectedTokens) {
				t.Fatal("history continuation tokens were not forwarded exactly", tokens)
			}
			if mode != "closed-use" {
				evidence := capabilityEvidence(t, fixture, "workflow.history")
				if evidence.Source != client.Attribution() || evidence.Execution.WorkflowID != workflowID || evidence.Execution.RunID != "run" || evidence.Execution.ResultObtained != (mode == "complete") {
					t.Fatal("history operation evidence changed")
				}
			}
		})
	}
}

func TestPublicHistoryRejectsNilAndClosedRequestsWithoutWire(t *testing.T) {
	peer := &categoryHistoryPeer{requests: make(chan proto.Message, 16)}
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	client := fixture.owner.Client()
	calls := []func(*Client) error{
		func(client *Client) error { _, err := client.ListWorkflow(fixture.ctx, nil); return err },
		func(client *Client) error { _, err := client.ListOpenWorkflow(fixture.ctx, nil); return err },
		func(client *Client) error { _, err := client.ListClosedWorkflow(fixture.ctx, nil); return err },
		func(client *Client) error { _, err := client.ListArchivedWorkflow(fixture.ctx, nil); return err },
		func(client *Client) error { _, err := client.CountWorkflow(fixture.ctx, nil); return err },
		func(client *Client) error { _, err := client.ScanWorkflow(fixture.ctx, nil); return err },
		func(client *Client) error {
			return client.WalkHistory(fixture.ctx, "workflow", "run", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT, nil)
		},
		func(client *Client) error {
			return client.WalkActivities(fixture.ctx, sdk.ListActivitiesOptions{}, nil)
		},
	}
	for _, invoke := range calls {
		if err := invoke(client); !errors.Is(err, ErrInput) {
			t.Fatal("invalid history/visibility input was not rejected", err)
		}
	}
	if peer.metadataCalls.Load() != 0 || peer.historyCalls.Load() != 0 {
		t.Fatal("invalid history input reached the peer")
	}
	if _, err := client.ListWorkflow(fixture.ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: strings.Repeat("x", (64<<10)+1)}); !errors.Is(err, ErrLimit) || peer.metadataCalls.Load() != 0 {
		t.Fatal("oversized visibility request crossed the public wire bound", err)
	}
	if capabilityEvidence(t, fixture, "workflow.listworkflow").Execution.ResultObtained {
		t.Fatal("oversized visibility request acquired result evidence")
	}
	if err := client.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CountWorkflow(fixture.ctx, &workflowservice.CountWorkflowExecutionsRequest{Query: "query"}); err == nil || peer.metadataCalls.Load() != 0 {
		t.Fatal("closed visibility use reached the peer", err)
	}
	var empty Client
	if _, err := empty.ListWorkflow(fixture.ctx, &workflowservice.ListWorkflowExecutionsRequest{}); err == nil {
		t.Fatal("zero Client accepted visibility work")
	}
}
