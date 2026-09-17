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
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	sdktemporal "go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func scheduleDescription() *workflowservice.DescribeScheduleResponse {
	return &workflowservice.DescribeScheduleResponse{ConflictToken: []byte("observed-token"),
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{TimezoneName: "America/New_York"},
			Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId: "action-id", WorkflowType: &commonpb.WorkflowType{Name: "definition"}, TaskQueue: &taskqueuepb.TaskQueue{Name: "unit"},
				Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{activityPayload("\"value\"")}}}}},
			Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP},
			State:    &schedulepb.ScheduleState{Paused: true, Notes: "original"}},
		Info: &schedulepb.ScheduleInfo{}}
}

func TestScheduleNativeLifecycleAndTimeOptions(t *testing.T) {
	creates := make(chan *workflowservice.CreateScheduleRequest, 2)
	patches := make(chan *workflowservice.PatchScheduleRequest, 4)
	updates := make(chan *workflowservice.UpdateScheduleRequest, 1)
	var deletions atomic.Int32
	fixture := newFixture(t, 1, func(options *temporal.OptionsV1, limits *resource.Limits, peer *rpcServer) {
		options.MaxRequestBytes, options.MaxResponseBytes = 4096, 4096
		limits.Bytes += 6144
		peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			switch request := request.(type) {
			case *workflowservice.CreateScheduleRequest:
				creates <- request
				return &workflowservice.CreateScheduleResponse{}, nil
			case *workflowservice.DescribeScheduleRequest:
				return scheduleDescription(), nil
			case *workflowservice.UpdateScheduleRequest:
				updates <- request
				return &workflowservice.UpdateScheduleResponse{}, nil
			case *workflowservice.PatchScheduleRequest:
				patches <- request
				return &workflowservice.PatchScheduleResponse{}, nil
			case *workflowservice.DeleteScheduleRequest:
				deletions.Add(1)
				return &workflowservice.DeleteScheduleResponse{}, nil
			}
			return next(ctx, request)
		}
	})
	executions, inbox := executionBinding(t, fixture)
	ctx := context.Background()
	start := time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	end := start.Add(48 * time.Hour)
	handle, err := executions.CreateSchedule(ctx, fault.Correlation{Call: "create"}, sdk.ScheduleOptions{
		ID: "schedule-id", Paused: true, Note: "owned", RemainingActions: 3, TriggerImmediately: true,
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, PauseOnFailure: true,
		Spec: sdk.ScheduleSpec{Calendars: []sdk.ScheduleCalendarSpec{{Hour: []sdk.ScheduleRange{{Start: 2}}, Minute: []sdk.ScheduleRange{{Start: 30}}}},
			Intervals:       []sdk.ScheduleIntervalSpec{{Every: 90 * time.Minute, Offset: 5 * time.Minute}},
			CronExpressions: []string{"@daily"}, Skip: []sdk.ScheduleCalendarSpec{{DayOfWeek: []sdk.ScheduleRange{{Start: 0}}}},
			TimeZoneName: "America/New_York", StartAt: start, EndAt: end, Jitter: 17 * time.Second},
		Action: &sdk.ScheduleWorkflowAction{ID: "action-id", Workflow: "definition", TaskQueue: "unit", Args: []any{"value"},
			StaticSummary: "summary", StaticDetails: "details", Memo: map[string]any{"key": "value"}},
		Memo: map[string]any{"schedule-key": "schedule-value"},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := <-creates
	spec := request.Schedule.Spec
	if handle.GetID() != "schedule-id" || request.Namespace != "test" || request.ScheduleId != "schedule-id" || request.RequestId == "" ||
		spec.TimezoneName != "America/New_York" || !spec.StartTime.AsTime().Equal(start) || !spec.EndTime.AsTime().Equal(end) ||
		spec.Jitter.AsDuration() != 17*time.Second || !reflect.DeepEqual(spec.CronString, []string{"@daily"}) ||
		len(spec.StructuredCalendar) != 1 || spec.StructuredCalendar[0].Hour[0].Start != 2 || spec.StructuredCalendar[0].Minute[0].Start != 30 ||
		len(spec.ExcludeStructuredCalendar) != 1 || len(spec.Interval) != 1 || spec.Interval[0].Interval.AsDuration() != 90*time.Minute || spec.Interval[0].Phase.AsDuration() != 5*time.Minute ||
		request.Schedule.Policies.CatchupWindow != nil || !request.Schedule.Policies.PauseOnFailure ||
		request.InitialPatch.GetTriggerImmediately().GetOverlapPolicy() != enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE ||
		!request.Schedule.State.Paused || request.Schedule.State.RemainingActions != 3 {
		t.Fatal("native Schedule options or server-default catchup semantics changed")
	}
	record := receiveExecution(t, inbox)
	if !record.Outcome.Value.Accepted || record.Outcome.Value.ScheduleID != "schedule-id" {
		t.Fatal("Schedule create response/identity was not recorded")
	}
	described, err := handle.Describe(ctx, fault.Correlation{Call: "describe"})
	if err != nil || described.Schedule.State.Note != "original" || described.Schedule.Spec.TimeZoneName != "America/New_York" {
		t.Fatal("native Schedule description was lost")
	}
	action := described.Schedule.Action.(*sdk.ScheduleWorkflowAction)
	payload, ok := action.Args[0].(*commonpb.Payload)
	if !ok || string(payload.Data) != "\"value\"" {
		t.Fatal("described action arguments did not retain native payload representation")
	}
	receiveExecution(t, inbox)
	if err := handle.Update(ctx, fault.Correlation{Call: "skip"}, sdk.ScheduleUpdateOptions{DoUpdate: func(sdk.ScheduleUpdateInput) (*sdk.ScheduleUpdate, error) {
		return nil, sdktemporal.ErrSkipScheduleUpdate
	}}); err != nil {
		t.Fatal(err)
	}
	if receiveExecution(t, inbox).Outcome.Value.Accepted || len(updates) != 0 {
		t.Fatal("skipped native update was reported as a mutation ACK")
	}
	original := errors.New("callback failure")
	if err := handle.Update(ctx, fault.Correlation{Call: "handled-error"}, sdk.ScheduleUpdateOptions{DoUpdate: func(sdk.ScheduleUpdateInput) (*sdk.ScheduleUpdate, error) {
		return nil, original
	}}); !errors.Is(err, original) {
		t.Fatal("native callback error identity was changed")
	}
	if record := receiveExecution(t, inbox); !errors.Is(record.Err(), original) || record.Outcome.Value.Accepted || len(updates) != 0 {
		t.Fatal("handled callback error lost evidence or submitted an update")
	}
	if err := handle.Update(ctx, fault.Correlation{Call: "update"}, sdk.ScheduleUpdateOptions{DoUpdate: func(input sdk.ScheduleUpdateInput) (*sdk.ScheduleUpdate, error) {
		input.Description.Schedule.State.Note = "replacement"
		input.Description.Schedule.Policy.CatchupWindow = 2 * time.Minute
		return &sdk.ScheduleUpdate{Schedule: &input.Description.Schedule}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	update := <-updates
	if len(update.ConflictToken) != 0 || update.Schedule.State.Notes != "replacement" || update.Schedule.Policies.CatchupWindow.AsDuration() != 2*time.Minute {
		t.Fatal("native update semantics changed, including its absent conflict token")
	}
	if !receiveExecution(t, inbox).Outcome.Value.Accepted {
		t.Fatal("update response not observed")
	}
	if err := handle.Trigger(ctx, fault.Correlation{Call: "trigger"}, sdk.ScheduleTriggerOptions{Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}); err != nil {
		t.Fatal(err)
	}
	if (<-patches).Patch.GetTriggerImmediately().GetOverlapPolicy() != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
		t.Fatal("native trigger overlap override lost")
	}
	receiveExecution(t, inbox)
	if err := handle.Backfill(ctx, fault.Correlation{Call: "backfill"}, sdk.ScheduleBackfillOptions{Backfill: []sdk.ScheduleBackfill{{Start: start, End: end, Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}}}); err != nil {
		t.Fatal(err)
	}
	backfill := (<-patches).Patch.BackfillRequest
	if len(backfill) != 1 || !backfill[0].StartTime.AsTime().Equal(start) || !backfill[0].EndTime.AsTime().Equal(end) {
		t.Fatal("native backfill range changed")
	}
	receiveExecution(t, inbox)
	if err := handle.Pause(ctx, fault.Correlation{Call: "pause"}, sdk.SchedulePauseOptions{}); err != nil {
		t.Fatal(err)
	}
	if (<-patches).Patch.GetPause() != "Paused via Go SDK" {
		t.Fatal("native pause default changed")
	}
	receiveExecution(t, inbox)
	if err := handle.Unpause(ctx, fault.Correlation{Call: "unpause"}, sdk.ScheduleUnpauseOptions{Note: "resume"}); err != nil {
		t.Fatal(err)
	}
	if (<-patches).Patch.GetUnpause() != "resume" {
		t.Fatal("native unpause note changed")
	}
	receiveExecution(t, inbox)
	if err := handle.Delete(ctx, fault.Correlation{Call: "delete"}); err != nil || deletions.Load() != 1 {
		t.Fatal("native delete not called")
	}
	receiveExecution(t, inbox)
	if _, err := executions.GetSchedule(strings.Repeat("x", 1025)); !errors.Is(err, temporal.ErrInput) {
		t.Fatal("unbounded Schedule handle identity accepted")
	}
	if _, err := executions.CreateSchedule(ctx, fault.Correlation{Call: "oversized"}, sdk.ScheduleOptions{ID: strings.Repeat("x", 1025)}); !errors.Is(err, temporal.ErrLimit) || len(creates) != 0 {
		t.Fatal("unbounded Schedule identity reached native code")
	}
}

func TestScheduleUpdateCallbackRetainsOwnerAfterCancellation(t *testing.T) {
	var updates atomic.Int32
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			switch request.(type) {
			case *workflowservice.DescribeScheduleRequest:
				return scheduleDescription(), nil
			case *workflowservice.UpdateScheduleRequest:
				updates.Add(1)
				return &workflowservice.UpdateScheduleResponse{}, nil
			}
			return next(ctx, request)
		}
	})
	executions, inbox := executionBinding(t, fixture)
	handle, err := executions.GetSchedule("owned")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	defer once.Do(func() { close(release) })
	original := errors.New("canceled callback finished")
	go func() {
		done <- handle.Update(ctx, fault.Correlation{Call: "blocked-update"}, sdk.ScheduleUpdateOptions{DoUpdate: func(sdk.ScheduleUpdateInput) (*sdk.ScheduleUpdate, error) {
			close(entered)
			<-release
			return nil, original
		}})
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("native update callback not entered")
	}
	cancel()
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if err := fixture.assembly.Close(short); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("source released while a canceled native callback still used it")
	}
	select {
	case <-done:
		t.Fatal("cancellation was mistaken for callback termination")
	default:
	}
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if !errors.Is(err, original) {
			t.Fatal("callback failure was lost after cancellation")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("released callback did not finish")
	}
	record := receiveExecution(t, inbox)
	if !record.Released || !errors.Is(record.Err(), original) || record.Outcome.Value.Accepted || updates.Load() != 0 {
		t.Fatal("callback release/effect evidence disagreed with controlled execution")
	}
}

func TestScheduleLazyPaginationAndUnknownUpdate(t *testing.T) {
	var lists atomic.Int32
	var applied atomic.Bool
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			switch request := request.(type) {
			case *workflowservice.ListSchedulesRequest:
				lists.Add(1)
				if request.Namespace != "test" || request.MaximumPageSize != 1 || request.Query != "custom-filter" {
					return nil, status.Error(codes.InvalidArgument, "pagination options changed")
				}
				if len(request.NextPageToken) == 0 {
					return &workflowservice.ListSchedulesResponse{Schedules: []*schedulepb.ScheduleListEntry{{ScheduleId: "first", Info: &schedulepb.ScheduleListInfo{}}}, NextPageToken: []byte("next-page")}, nil
				}
				if string(request.NextPageToken) != "next-page" {
					return nil, status.Error(codes.InvalidArgument, "page token changed")
				}
				return &workflowservice.ListSchedulesResponse{Schedules: []*schedulepb.ScheduleListEntry{{ScheduleId: "second", Info: &schedulepb.ScheduleListInfo{}}}}, nil
			case *workflowservice.DescribeScheduleRequest:
				return scheduleDescription(), nil
			case *workflowservice.UpdateScheduleRequest:
				applied.Store(true)
				return nil, status.Error(codes.DeadlineExceeded, "synthetic response loss after application")
			}
			return next(ctx, request)
		}
	})
	executions, inbox := executionBinding(t, fixture)
	options := sdk.ScheduleListOptions{PageSize: 1, Query: "custom-filter"}
	var found []string
	if err := executions.WalkSchedules(context.Background(), fault.Correlation{Call: "list"}, options, func(_ context.Context, entry *sdk.ScheduleListEntry) error {
		found = append(found, entry.ID)
		return nil
	}); err != nil || !reflect.DeepEqual(found, []string{"first", "second"}) || lists.Load() != 2 {
		t.Fatal("native lazy pagination did not run inside admission")
	}
	if !receiveExecution(t, inbox).Outcome.Value.ResultObtained {
		t.Fatal("finished list observation was not recorded")
	}
	stopped := errors.New("visitor stopped")
	err := executions.WalkSchedules(context.Background(), fault.Correlation{Call: "early-stop"}, options, func(context.Context, *sdk.ScheduleListEntry) error { return stopped })
	if !errors.Is(err, stopped) || lists.Load() != 3 || !errors.Is(receiveExecution(t, inbox).Err(), stopped) {
		t.Fatal("visitor failure was lost or another page was fetched")
	}
	handle, err := executions.GetSchedule("unknown-update")
	if err != nil {
		t.Fatal(err)
	}
	err = handle.Update(context.Background(), fault.Correlation{Call: "lost-update"}, sdk.ScheduleUpdateOptions{DoUpdate: func(input sdk.ScheduleUpdateInput) (*sdk.ScheduleUpdate, error) {
		return &sdk.ScheduleUpdate{Schedule: &input.Description.Schedule}, nil
	}})
	if err == nil || !applied.Load() {
		t.Fatal("response-loss control did not independently observe application")
	}
	record := receiveExecution(t, inbox)
	if record.Err() == nil || record.Outcome.Value.Accepted || !record.Outcome.Value.NativeCalled {
		t.Fatal("unknown update acceptance was reported as success or no attempt")
	}
}

func TestScheduleEmptyContinuationPageDoesNotEndWalk(t *testing.T) {
	var requests atomic.Int32
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			if page, ok := request.(*workflowservice.ListSchedulesRequest); ok {
				requests.Add(1)
				switch string(page.NextPageToken) {
				case "":
					return &workflowservice.ListSchedulesResponse{NextPageToken: []byte("empty-again")}, nil
				case "empty-again":
					return &workflowservice.ListSchedulesResponse{NextPageToken: []byte("last")}, nil
				case "last":
					return &workflowservice.ListSchedulesResponse{Schedules: []*schedulepb.ScheduleListEntry{{ScheduleId: "after-empty-pages", Info: &schedulepb.ScheduleListInfo{}}}}, nil
				}
				return nil, status.Error(codes.InvalidArgument, "unexpected continuation")
			}
			return next(ctx, request)
		}
	})
	executions, inbox := executionBinding(t, fixture)
	var found []string
	err := executions.WalkSchedules(context.Background(), fault.Correlation{Call: "empty-pages"}, sdk.ScheduleListOptions{PageSize: 1}, func(_ context.Context, entry *sdk.ScheduleListEntry) error {
		found = append(found, entry.ID)
		return nil
	})
	if err != nil || requests.Load() != 3 || !reflect.DeepEqual(found, []string{"after-empty-pages"}) {
		t.Fatalf("empty continuation was mistaken for exhaustion: requests=%d results=%d error=%v", requests.Load(), len(found), err)
	}
	receiveExecution(t, inbox)
}
