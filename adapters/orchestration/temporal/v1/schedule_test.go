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
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdktemporal "go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type scheduleCapabilityServer struct {
	testServer
	creates      chan *workflowservice.CreateScheduleRequest
	updates      chan *workflowservice.UpdateScheduleRequest
	patches      chan *workflowservice.PatchScheduleRequest
	deletes      chan *workflowservice.DeleteScheduleRequest
	describes    atomic.Int32
	lists        atomic.Int32
	describeErr  error
	listResponse func(*workflowservice.ListSchedulesRequest) (*workflowservice.ListSchedulesResponse, error)
}

func newScheduleCapabilityServer() *scheduleCapabilityServer {
	return &scheduleCapabilityServer{
		creates: make(chan *workflowservice.CreateScheduleRequest, 4),
		updates: make(chan *workflowservice.UpdateScheduleRequest, 4),
		patches: make(chan *workflowservice.PatchScheduleRequest, 8),
		deletes: make(chan *workflowservice.DeleteScheduleRequest, 4),
	}
}

func (server *scheduleCapabilityServer) CreateSchedule(_ context.Context, request *workflowservice.CreateScheduleRequest) (*workflowservice.CreateScheduleResponse, error) {
	server.creates <- request
	return &workflowservice.CreateScheduleResponse{}, nil
}

func (server *scheduleCapabilityServer) DescribeSchedule(_ context.Context, request *workflowservice.DescribeScheduleRequest) (*workflowservice.DescribeScheduleResponse, error) {
	server.describes.Add(1)
	if request.Namespace != "test" || request.ScheduleId == "" {
		return nil, status.Error(codes.InvalidArgument, "missing Schedule target")
	}
	if server.describeErr != nil {
		return nil, server.describeErr
	}
	return &workflowservice.DescribeScheduleResponse{
		ConflictToken: []byte("observed-not-CAS"),
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{TimezoneName: "America/New_York"},
			Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId: "action-id", WorkflowType: &commonpb.WorkflowType{Name: "definition"}, TaskQueue: &taskqueuepb.TaskQueue{Name: "queue"},
				Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(`"value"`)}}},
			}}},
			Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP},
			State:    &schedulepb.ScheduleState{Paused: true, Notes: "original"},
		},
		Info: &schedulepb.ScheduleInfo{}, SearchAttributes: &commonpb.SearchAttributes{},
	}, nil
}

func (server *scheduleCapabilityServer) UpdateSchedule(_ context.Context, request *workflowservice.UpdateScheduleRequest) (*workflowservice.UpdateScheduleResponse, error) {
	server.updates <- request
	return &workflowservice.UpdateScheduleResponse{}, nil
}

func (server *scheduleCapabilityServer) PatchSchedule(_ context.Context, request *workflowservice.PatchScheduleRequest) (*workflowservice.PatchScheduleResponse, error) {
	server.patches <- request
	return &workflowservice.PatchScheduleResponse{}, nil
}

func (server *scheduleCapabilityServer) DeleteSchedule(_ context.Context, request *workflowservice.DeleteScheduleRequest) (*workflowservice.DeleteScheduleResponse, error) {
	server.deletes <- request
	return &workflowservice.DeleteScheduleResponse{}, nil
}

func (server *scheduleCapabilityServer) ListSchedules(_ context.Context, request *workflowservice.ListSchedulesRequest) (*workflowservice.ListSchedulesResponse, error) {
	server.lists.Add(1)
	if server.listResponse == nil {
		return &workflowservice.ListSchedulesResponse{}, nil
	}
	return server.listResponse(request)
}

func scheduleRequest[T any](t *testing.T, requests <-chan T) T {
	t.Helper()
	select {
	case request := <-requests:
		return request
	default:
		t.Fatal("successful Schedule call did not reach the native peer")
		var zero T
		return zero
	}
}

func TestPublicScheduleNativeOptionsReusedActionAndLifecycle(t *testing.T) {
	peer := newScheduleCapabilityServer()
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	client := fixture.owner.Client()
	start := time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	end := start.Add(48 * time.Hour)
	action := &sdk.ScheduleWorkflowAction{Workflow: "definition", TaskQueue: "queue", Args: []any{"value"}, Memo: map[string]any{"memo": "value"}, StaticSummary: "summary", StaticDetails: "details"}
	options := sdk.ScheduleOptions{ID: "schedule-one", Action: action, Paused: true, Note: "owned", RemainingActions: 3,
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, PauseOnFailure: true, TriggerImmediately: true,
		ScheduleBackfill: []sdk.ScheduleBackfill{{Start: start, End: end, Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}},
		Memo:             map[string]any{"schedule-memo": "owned"},
		Spec: sdk.ScheduleSpec{Calendars: []sdk.ScheduleCalendarSpec{{Hour: []sdk.ScheduleRange{{Start: 2}}, Minute: []sdk.ScheduleRange{{Start: 30}}}},
			Intervals: []sdk.ScheduleIntervalSpec{{Every: 90 * time.Minute, Offset: 5 * time.Minute}}, CronExpressions: []string{"@daily"},
			Skip: []sdk.ScheduleCalendarSpec{{DayOfWeek: []sdk.ScheduleRange{{Start: 0}}}}, TimeZoneName: "America/New_York", StartAt: start, EndAt: end, Jitter: 17 * time.Second},
	}
	handle, err := client.CreateSchedule(fixture.ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	first := scheduleRequest(t, peer.creates)
	spec := first.Schedule.Spec
	if handle.GetID() != options.ID || first.Namespace != "test" || first.ScheduleId != options.ID || first.RequestId == "" ||
		spec.TimezoneName != "America/New_York" || !spec.StartTime.AsTime().Equal(start) || !spec.EndTime.AsTime().Equal(end) ||
		spec.Jitter.AsDuration() != 17*time.Second || !reflect.DeepEqual(spec.CronString, []string{"@daily"}) ||
		len(spec.StructuredCalendar) != 1 || spec.StructuredCalendar[0].Hour[0].Start != 2 || spec.StructuredCalendar[0].Minute[0].Start != 30 ||
		len(spec.ExcludeStructuredCalendar) != 1 || len(spec.Interval) != 1 || spec.Interval[0].Interval.AsDuration() != 90*time.Minute || spec.Interval[0].Phase.AsDuration() != 5*time.Minute ||
		first.Schedule.Policies.CatchupWindow != nil || !first.Schedule.Policies.PauseOnFailure || !first.Schedule.State.Paused || first.Schedule.State.RemainingActions != 3 ||
		first.InitialPatch.GetTriggerImmediately().GetOverlapPolicy() != enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE || len(first.InitialPatch.BackfillRequest) != 1 {
		t.Fatal("public CreateSchedule changed native options or zero catchup default")
	}
	var argument, memo string
	createdAction := first.Schedule.Action.GetStartWorkflow()
	if err := converter.GetDefaultDataConverter().FromPayloads(createdAction.Input, &argument); err != nil || argument != "value" {
		t.Fatal("native action input conversion lost", err)
	}
	if err := converter.GetDefaultDataConverter().FromPayload(first.Memo.Fields["schedule-memo"], &memo); err != nil || memo != "owned" || createdAction.UserMetadata.Summary == nil || createdAction.UserMetadata.Details == nil {
		t.Fatal("native memo or user metadata lost", err)
	}
	result := capabilityEvidence(t, fixture, "schedule.create")
	if result.Source != client.Attribution() || result.Execution.ScheduleID != options.ID || !result.Execution.Accepted {
		t.Fatal("create evidence lost originating use or acknowledged identity")
	}
	options.ID = "schedule-two"
	if _, err := client.CreateSchedule(fixture.ctx, options); err != nil {
		t.Fatal(err)
	}
	second := scheduleRequest(t, peer.creates)
	if action.ID != "" || action.Args[0] != "value" || action.Memo["memo"] != "value" ||
		createdAction.WorkflowId == "" || second.Schedule.Action.GetStartWorkflow().WorkflowId == createdAction.WorkflowId || second.RequestId == first.RequestId {
		t.Fatal("reused caller action was mutated or native fresh identities were conflated")
	}
	capabilityEvidence(t, fixture, "schedule.create")
	description, err := handle.Describe(fixture.ctx)
	if err != nil || description.Schedule.State.Note != "original" || description.Schedule.Spec.TimeZoneName != "America/New_York" {
		t.Fatal("Schedule description conversion changed", err)
	}
	describedAction := description.Schedule.Action.(*sdk.ScheduleWorkflowAction)
	if payload, ok := describedAction.Args[0].(*commonpb.Payload); !ok || string(payload.Data) != `"value"` {
		t.Fatal("description did not retain native already-encoded action arguments")
	}
	capabilityEvidence(t, fixture, "schedule.describe")
	if err := handle.Update(fixture.ctx, sdk.ScheduleUpdateOptions{DoUpdate: func(input sdk.ScheduleUpdateInput) (*sdk.ScheduleUpdate, error) {
		input.Description.Schedule.State.Note = "replacement"
		input.Description.Schedule.Policy.CatchupWindow = 2 * time.Minute
		return &sdk.ScheduleUpdate{Schedule: &input.Description.Schedule}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	update := scheduleRequest(t, peer.updates)
	if update.Namespace != "test" || update.ScheduleId != handle.GetID() || update.RequestId == "" || len(update.ConflictToken) != 0 ||
		update.Schedule.State.Notes != "replacement" || update.Schedule.Policies.CatchupWindow.AsDuration() != 2*time.Minute {
		t.Fatal("native read/update conversion or intentionally absent CAS token changed")
	}
	if !capabilityEvidence(t, fixture, "schedule.update").Execution.Accepted {
		t.Fatal("successful native update lacked acknowledgement")
	}
	for _, test := range []struct {
		operation string
		invoke    func() error
		valid     func(*schedulepb.SchedulePatch) bool
	}{
		{"trigger", func() error {
			return handle.Trigger(fixture.ctx, sdk.ScheduleTriggerOptions{Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL})
		}, func(patch *schedulepb.SchedulePatch) bool {
			return patch.GetTriggerImmediately().GetOverlapPolicy() == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
		}},
		{"backfill", func() error {
			return handle.Backfill(fixture.ctx, sdk.ScheduleBackfillOptions{Backfill: options.ScheduleBackfill})
		}, func(patch *schedulepb.SchedulePatch) bool {
			return len(patch.BackfillRequest) == 1 && patch.BackfillRequest[0].StartTime.AsTime().Equal(start) && patch.BackfillRequest[0].EndTime.AsTime().Equal(end)
		}},
		{"pause", func() error { return handle.Pause(fixture.ctx, sdk.SchedulePauseOptions{}) }, func(patch *schedulepb.SchedulePatch) bool { return patch.GetPause() == "Paused via Go SDK" }},
		{"unpause", func() error { return handle.Unpause(fixture.ctx, sdk.ScheduleUnpauseOptions{Note: "resume"}) }, func(patch *schedulepb.SchedulePatch) bool { return patch.GetUnpause() == "resume" }},
	} {
		if err := test.invoke(); err != nil {
			t.Fatal(test.operation, err)
		}
		request := scheduleRequest(t, peer.patches)
		if request.Namespace != "test" || request.ScheduleId != handle.GetID() || request.RequestId == "" || !test.valid(request.Patch) {
			t.Fatal(test.operation, "changed native patch")
		}
		if !capabilityEvidence(t, fixture, "schedule."+test.operation).Execution.Accepted {
			t.Fatal(test.operation, "lost acknowledged mutation evidence")
		}
	}
	if err := handle.Delete(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if request := scheduleRequest(t, peer.deletes); request.Namespace != "test" || request.ScheduleId != handle.GetID() {
		t.Fatal("delete lost native target")
	}
	capabilityEvidence(t, fixture, "schedule.delete")
}

func TestPublicScheduleSkipCallbackFailureAndCancellationJoin(t *testing.T) {
	peer := newScheduleCapabilityServer()
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	client := fixture.owner.Client()
	handle, err := client.GetSchedule("schedule")
	if err != nil || peer.describes.Load() != 0 {
		t.Fatal("GetSchedule was not local", err)
	}
	if err := handle.Update(fixture.ctx, sdk.ScheduleUpdateOptions{DoUpdate: func(sdk.ScheduleUpdateInput) (*sdk.ScheduleUpdate, error) {
		return nil, sdktemporal.ErrSkipScheduleUpdate
	}}); err != nil {
		t.Fatal(err)
	}
	if capabilityEvidence(t, fixture, "schedule.update").Execution.Accepted || len(peer.updates) != 0 {
		t.Fatal("skip update sent or claimed mutation")
	}
	private := errors.New("private Schedule callback canary")
	ctx, cancel := context.WithCancel(fixture.ctx)
	defer cancel()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	defer once.Do(func() { close(release) })
	go func() {
		done <- handle.Update(ctx, sdk.ScheduleUpdateOptions{DoUpdate: func(sdk.ScheduleUpdateInput) (*sdk.ScheduleUpdate, error) {
			close(entered)
			<-release
			return nil, private
		}})
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("native update callback did not enter")
	}
	cancel()
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if err := client.Close(short); err == nil || client.Closed() {
		t.Fatal("cancellation was mistaken for callback cleanup", err)
	}
	select {
	case <-done:
		t.Fatal("blocked callback returned before release")
	default:
	}
	once.Do(func() { close(release) })
	select {
	case err = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("released callback did not join")
	}
	semantic, present := NativeError(err)
	if !errors.Is(err, ErrExecution) || !errors.Is(err, private) || !present || semantic != private || strings.Contains(fmt.Sprintf("%+v", err), "canary") {
		t.Fatal("public callback failure lost native identity or safe presentation")
	}
	result := capabilityEvidence(t, fixture, "schedule.update")
	semantic, present = result.NativeError()
	if result.Execution.Accepted || result.Source != client.Attribution() || !present || semantic != private || len(peer.updates) != 0 {
		t.Fatal("handled callback failure lost independent origin/effect evidence")
	}
	wait, stopWait := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopWait()
	if err := client.Close(wait); err != nil || !client.Closed() {
		t.Fatal("same cleanup did not finish after callback joined", err)
	}
}

func TestPublicScheduleEmptyPagesVisitorFailureAndClosedOrigin(t *testing.T) {
	peer := newScheduleCapabilityServer()
	peer.listResponse = func(request *workflowservice.ListSchedulesRequest) (*workflowservice.ListSchedulesResponse, error) {
		if request.Namespace != "test" || request.MaximumPageSize != 1 || request.Query != "fixture-filter" {
			return nil, status.Error(codes.InvalidArgument, "list options changed")
		}
		switch string(request.NextPageToken) {
		case "":
			return &workflowservice.ListSchedulesResponse{NextPageToken: []byte("empty")}, nil
		case "empty":
			return &workflowservice.ListSchedulesResponse{Schedules: []*schedulepb.ScheduleListEntry{{ScheduleId: "first", Info: &schedulepb.ScheduleListInfo{Notes: "converted"}}}, NextPageToken: []byte("last")}, nil
		case "last":
			return &workflowservice.ListSchedulesResponse{Schedules: []*schedulepb.ScheduleListEntry{{ScheduleId: "second", Info: &schedulepb.ScheduleListInfo{}}}}, nil
		default:
			return nil, status.Error(codes.InvalidArgument, "continuation changed")
		}
	}
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	client := fixture.owner.Client()
	other, err := client.Borrow(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	options := sdk.ScheduleListOptions{PageSize: 1, Query: "fixture-filter"}
	var found []string
	if err := client.WalkSchedules(fixture.ctx, options, func(ctx context.Context, entry *sdk.ScheduleListEntry) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		found = append(found, entry.ID)
		return nil
	}); err != nil || !reflect.DeepEqual(found, []string{"first", "second"}) || peer.lists.Load() != 3 {
		t.Fatal("public walker ended at empty continuation", err)
	}
	if !capabilityEvidence(t, fixture, "schedule.list").Execution.ResultObtained {
		t.Fatal("exhausted walk lacked result evidence")
	}
	stopped := errors.New("private visitor canary")
	err = client.WalkSchedules(fixture.ctx, options, func(_ context.Context, entry *sdk.ScheduleListEntry) error {
		if entry.ID != "first" || entry.Note != "converted" {
			t.Error("native list entry conversion changed")
		}
		return stopped
	})
	if !errors.Is(err, stopped) || strings.Contains(err.Error(), "canary") || peer.lists.Load() != 5 {
		t.Fatal("visitor failure was erased, exposed, or followed by another page")
	}
	result := capabilityEvidence(t, fixture, "schedule.list")
	if cause, ok := result.NativeError(); !ok || cause != stopped || result.Execution.ResultObtained {
		t.Fatal("visitor failure evidence differs from returned native failure")
	}
	handle, err := client.GetSchedule("schedule")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Describe(fixture.ctx); !errors.Is(err, ErrState) || peer.describes.Load() != 0 || handle.GetID() != "schedule" {
		t.Fatal("retained Schedule rehomed to a live peer or lost local ID", err)
	}
	otherHandle, err := other.GetSchedule("schedule")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := otherHandle.Describe(fixture.ctx); err != nil {
		t.Fatal("closing one origin revoked independent Borrow", err)
	}
	if result := capabilityEvidence(t, fixture, "schedule.describe"); result.Source != other.Attribution() {
		t.Fatal("peer Schedule evidence retained wrong use")
	}
}

func TestPublicScheduleNativeFailureAndInvalidInputs(t *testing.T) {
	peer := newScheduleCapabilityServer()
	peer.describeErr = status.Error(codes.NotFound, "private missing Schedule canary")
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	client := fixture.owner.Client()
	handle, err := client.GetSchedule("missing")
	if err != nil {
		t.Fatal(err)
	}
	_, err = handle.Describe(fixture.ctx)
	semantic, known := NativeError(err)
	notFound, exact := semantic.(*serviceerror.NotFound)
	if !known || !exact || !strings.Contains(notFound.Error(), "canary") || !errors.Is(err, ErrExecution) || strings.Contains(fmt.Sprintf("%+v", err), "canary") {
		t.Fatal("Schedule native failure shape or safe core lost")
	}
	result := capabilityEvidence(t, fixture, "schedule.describe")
	evidence, present := result.NativeError()
	if _, exact := evidence.(*serviceerror.NotFound); !present || !exact || result.Execution.ResultObtained || result.Execution.Accepted {
		t.Fatal("Schedule native failure evidence lost")
	}
	if _, err := client.CreateSchedule(fixture.ctx, sdk.ScheduleOptions{}); err == nil || len(peer.creates) != 0 {
		t.Fatal("missing native create ID silently defaulted", err)
	}
	capabilityEvidence(t, fixture, "schedule.create")
	if _, err := client.GetSchedule(""); !errors.Is(err, ErrInput) {
		t.Fatal("empty Schedule handle accepted", err)
	}
	if err := handle.Update(fixture.ctx, sdk.ScheduleUpdateOptions{}); !errors.Is(err, ErrInput) || peer.describes.Load() != 1 {
		t.Fatal("missing update callback reached native code", err)
	}
	if err := client.WalkSchedules(fixture.ctx, sdk.ScheduleListOptions{}, nil); !errors.Is(err, ErrInput) || peer.lists.Load() != 0 {
		t.Fatal("nil visitor reached native code", err)
	}
	for _, zero := range []*Schedule{nil, {}} {
		if zero.GetID() != "" {
			t.Fatal("zero Schedule invented identity")
		}
		for _, invoke := range []func() error{
			func() error { _, err := zero.Describe(fixture.ctx); return err },
			func() error { return zero.Update(fixture.ctx, sdk.ScheduleUpdateOptions{}) },
			func() error { return zero.Delete(fixture.ctx) },
			func() error { return zero.Trigger(fixture.ctx, sdk.ScheduleTriggerOptions{}) },
			func() error { return zero.Backfill(fixture.ctx, sdk.ScheduleBackfillOptions{}) },
			func() error { return zero.Pause(fixture.ctx, sdk.SchedulePauseOptions{}) },
			func() error { return zero.Unpause(fixture.ctx, sdk.ScheduleUnpauseOptions{}) },
		} {
			if err := invoke(); !errors.Is(err, ErrInput) {
				t.Fatal("zero Schedule operation did not refuse safely", err)
			}
		}
	}
}
