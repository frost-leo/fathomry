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
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func scheduledWorkflow(ctx workflow.Context, value string) (string, error) {
	finish := workflow.GetSignalChannel(ctx, "finish")
	if err := workflow.Await(ctx, func() bool { return finish.Len() > 0 }); err != nil {
		return "", err
	}
	return "scheduled:" + value, nil
}

func observeSchedule(t *testing.T, ctx context.Context, fixture *executionServiceFixture, handle *temporal.Schedule, condition func(*sdk.ScheduleDescription) bool) *sdk.ScheduleDescription {
	t.Helper()
	return pollSchedule(t, ctx, fixture, handle, true, condition)
}

func pollSchedule(t *testing.T, ctx context.Context, fixture *executionServiceFixture, handle *temporal.Schedule, drain bool, condition func(*sdk.ScheduleDescription) bool) *sdk.ScheduleDescription {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		description, err := handle.Describe(ctx, fault.Correlation{Call: "schedule-observe"})
		if err != nil {
			t.Fatal("cannot observe test-owned Schedule", err)
		}
		if drain {
			releaseServiceEvidence(t, fixture.evidence)
		}
		if condition(description) {
			return description
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("Schedule state did not converge: actions=%d tracked-running=%d skipped-overlap=%d recent=%d", description.Info.NumActions, len(description.Info.RunningWorkflows), description.Info.NumActionsSkippedOverlap, len(description.Info.RecentActions))
		}
	}
}

func createServiceSchedule(t *testing.T, ctx context.Context, fixture *executionServiceFixture, options sdk.ScheduleOptions) *temporal.Schedule {
	t.Helper()
	if !strings.HasPrefix(options.ID, fixture.prefix+"-") {
		t.Fatal("Schedule ownership prefix is missing")
	}
	handle, err := fixture.executions.CreateSchedule(ctx, fault.Correlation{Call: "schedule-create"}, options)
	if err != nil {
		t.Fatal("cannot create test-owned Schedule", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		var missing *serviceerror.NotFound
		description, err := handle.Describe(cleanup, fault.Correlation{Call: "schedule-cleanup-actions"})
		if err != nil && !errors.As(err, &missing) {
			t.Error("cannot observe owned Schedule actions before cleanup", err)
			return
		}
		if description != nil {
			for _, action := range description.Info.RecentActions {
				execution := action.StartWorkflowResult
				if execution != nil && strings.HasPrefix(execution.WorkflowID, fixture.prefix+"-action") {
					fixture.cleanupWorkflow(t, execution.WorkflowID, execution.FirstExecutionRunID)
				}
			}
		}
		if err := handle.Delete(cleanup, fault.Correlation{Call: "schedule-delete"}); err != nil && !errors.As(err, &missing) {
			t.Error("cannot delete test-owned Schedule", err)
			return
		}
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			_, err := handle.Describe(cleanup, fault.Correlation{Call: "schedule-absence"})
			releaseServiceEvidence(t, fixture.evidence)
			if errors.As(err, &missing) {
				return
			}
			if err != nil {
				t.Error("Schedule absence not established", err)
				return
			}
			select {
			case <-ticker.C:
			case <-cleanup.Done():
				t.Error("Schedule deletion never became observable")
				return
			}
		}
	})
	return handle
}

func TestAuthorizedScheduleLifecycleAndConcurrentUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	fixture := newExecutionServiceFixture(t, ctx, workflowPrefix+"GetWorkflowExecutionHistory", workflowPrefix+"DeleteWorkflowExecution")
	lifetime, stopLifetime := context.WithCancel(context.Background())
	t.Cleanup(stopLifetime)
	managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "schedule-worker"}, temporal.WorkerSpec{
		TaskQueue: fixture.prefix, MaxHandlers: 2, Bytes: 2 * fixture.envelope,
		Options:   worker.Options{MaxConcurrentWorkflowTaskExecutionSize: 2, MaxConcurrentWorkflowTaskPollers: 2, WorkerStopTimeout: time.Second},
		Workflows: []temporal.WorkflowRegistration{{Definition: scheduledWorkflow, Options: workflow.RegisterOptions{Name: "scheduled-workflow"}}},
	}, fixture.workers, fixture.tasks)
	if managed != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := managed.Stop(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	handle := createServiceSchedule(t, ctx, fixture, sdk.ScheduleOptions{ID: fixture.prefix + "-schedule", Paused: true, Note: "initial",
		Spec:    sdk.ScheduleSpec{Intervals: []sdk.ScheduleIntervalSpec{{Every: time.Hour}}},
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, CatchupWindow: time.Minute,
		Action: &sdk.ScheduleWorkflowAction{ID: fixture.prefix + "-action", Workflow: "scheduled-workflow", TaskQueue: fixture.prefix,
			Args: []any{"value"}, WorkflowExecutionTimeout: 60 * time.Second, StaticSummary: "Schedule acceptance fixture"},
	})
	if err := handle.Trigger(ctx, fault.Correlation{Call: "trigger"}, sdk.ScheduleTriggerOptions{}); err != nil {
		t.Fatal(err)
	}
	first := observeSchedule(t, ctx, fixture, handle, func(description *sdk.ScheduleDescription) bool {
		return description.Info.NumActions == 1 && len(description.Info.RunningWorkflows) == 1
	})
	firstExecution := first.Info.RunningWorkflows[0]
	if !strings.HasPrefix(firstExecution.WorkflowID, fixture.prefix+"-action") {
		t.Fatal("Schedule action is outside test ownership")
	}
	t.Cleanup(func() { fixture.cleanupWorkflow(t, firstExecution.WorkflowID, firstExecution.FirstExecutionRunID) })
	if err := handle.Trigger(ctx, fault.Correlation{Call: "overlap"}, sdk.ScheduleTriggerOptions{Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}); err != nil {
		t.Fatal(err)
	}
	observeSchedule(t, ctx, fixture, handle, func(description *sdk.ScheduleDescription) bool {
		return description.Info.NumActions == 1 && description.Info.NumActionsSkippedOverlap == 1
	})
	finish := func(execution sdk.ScheduleWorkflowExecution) {
		t.Helper()
		if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "finish-action"}, execution.WorkflowID, execution.FirstExecutionRunID, "finish", true); err != nil {
			t.Fatal(err)
		}
		run, err := fixture.executions.GetWorkflow(execution.WorkflowID, execution.FirstExecutionRunID)
		if err != nil {
			t.Fatal(err)
		}
		var value string
		if err := run.Get(ctx, fault.Correlation{Call: "action-result"}, &value); err != nil || value != "scheduled:value" {
			t.Fatal("Schedule action result differs from the independent oracle")
		}
		history := fixture.history(t, ctx, execution.WorkflowID, execution.FirstExecutionRunID)
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflowWithOptions(scheduledWorkflow, workflow.RegisterOptions{Name: "scheduled-workflow"})
		if err := replayer.ReplayWorkflowHistory(executionLogger{}, history); err != nil {
			t.Fatal("Schedule action history did not replay", err)
		}
	}
	finish(firstExecution)
	observeSchedule(t, ctx, fixture, handle, func(description *sdk.ScheduleDescription) bool { return len(description.Info.RunningWorkflows) == 0 })
	backfillTime := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	if err := handle.Backfill(ctx, fault.Correlation{Call: "backfill"}, sdk.ScheduleBackfillOptions{Backfill: []sdk.ScheduleBackfill{
		{Start: backfillTime, End: backfillTime, Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL},
	}}); err != nil {
		t.Fatal(err)
	}
	second := observeSchedule(t, ctx, fixture, handle, func(description *sdk.ScheduleDescription) bool {
		return description.Info.NumActions == 2 && len(description.Info.RecentActions) == 2
	})
	if second.Info.RecentActions[1].StartWorkflowResult == nil {
		t.Fatal("backfill action result identity was not observed")
	}
	secondExecution := *second.Info.RecentActions[1].StartWorkflowResult
	if !strings.HasPrefix(secondExecution.WorkflowID, fixture.prefix+"-action") || secondExecution.WorkflowID == firstExecution.WorkflowID {
		t.Fatal("backfill execution identity is not independently owned")
	}
	t.Cleanup(func() { fixture.cleanupWorkflow(t, secondExecution.WorkflowID, secondExecution.FirstExecutionRunID) })
	if !second.Info.RecentActions[len(second.Info.RecentActions)-1].ScheduleTime.Equal(backfillTime) {
		t.Fatal("backfill changed the selected Server's inclusive time boundaries")
	}
	finish(secondExecution)

	future := time.Now().UTC().Add(24 * time.Hour)
	if err := handle.Update(ctx, fault.Correlation{Call: "baseline-update"}, sdk.ScheduleUpdateOptions{DoUpdate: func(input sdk.ScheduleUpdateInput) (*sdk.ScheduleUpdate, error) {
		input.Description.Schedule.Spec.StartAt = future
		input.Description.Schedule.State.Note = "baseline"
		return &sdk.ScheduleUpdate{Schedule: &input.Description.Schedule}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	observeSchedule(t, ctx, fixture, handle, func(description *sdk.ScheduleDescription) bool { return description.Schedule.State.Note == "baseline" })

	firstReady, secondReady := make(chan struct{}), make(chan struct{})
	firstRelease, secondRelease := make(chan struct{}), make(chan struct{})
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	startUpdate := func(note string, ready, release chan struct{}, done chan error, policy enumspb.ScheduleOverlapPolicy) {
		go func() {
			done <- handle.Update(ctx, fault.Correlation{Call: "concurrent-" + note}, sdk.ScheduleUpdateOptions{DoUpdate: func(input sdk.ScheduleUpdateInput) (*sdk.ScheduleUpdate, error) {
				if input.Description.Schedule.State.Note != "baseline" || input.Description.Schedule.Policy.Overlap != enumspb.SCHEDULE_OVERLAP_POLICY_SKIP {
					return nil, errors.New("concurrent updates did not read the same baseline")
				}
				close(ready)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				input.Description.Schedule.State.Note = note
				input.Description.Schedule.Policy.Overlap = policy
				return &sdk.ScheduleUpdate{Schedule: &input.Description.Schedule}, nil
			}})
		}()
	}
	startUpdate("first", firstReady, firstRelease, firstDone, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	startUpdate("second", secondReady, secondRelease, secondDone, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
	for _, ready := range []chan struct{}{firstReady, secondReady} {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("concurrent Schedule callbacks did not enter")
		}
	}
	close(firstRelease)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	observing, stopObservation := context.WithTimeout(ctx, 5*time.Second)
	defer stopObservation()
	pollSchedule(t, observing, fixture, handle, false, func(description *sdk.ScheduleDescription) bool {
		return description.Schedule.State.Note == "first" && description.Schedule.Policy.Overlap == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	})
	close(secondRelease)
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	observeSchedule(t, ctx, fixture, handle, func(description *sdk.ScheduleDescription) bool {
		return description.Schedule.State.Note == "second" && description.Schedule.Policy.Overlap == enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	})
	if err := handle.Unpause(ctx, fault.Correlation{Call: "unpause"}, sdk.ScheduleUnpauseOptions{Note: "resumed"}); err != nil {
		t.Fatal(err)
	}
	observeSchedule(t, ctx, fixture, handle, func(description *sdk.ScheduleDescription) bool {
		return !description.Schedule.State.Paused && description.Schedule.State.Note == "resumed"
	})
	if err := handle.Pause(ctx, fault.Correlation{Call: "pause"}, sdk.SchedulePauseOptions{Note: "paused"}); err != nil {
		t.Fatal(err)
	}
	observeSchedule(t, ctx, fixture, handle, func(description *sdk.ScheduleDescription) bool { return description.Schedule.State.Paused })
	found := false
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for !found {
		err := fixture.executions.WalkSchedules(ctx, fault.Correlation{Call: "schedule-list"}, sdk.ScheduleListOptions{PageSize: 1}, func(_ context.Context, entry *sdk.ScheduleListEntry) error {
			found = found || entry.ID == handle.GetID()
			return nil
		})
		if err != nil {
			t.Fatal("native Schedule list failed", err)
		}
		releaseServiceEvidence(t, fixture.evidence)
		if !found {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal("test-owned Schedule never appeared in Visibility")
			}
		}
	}
	t.Log("Create/get/describe/list/update/trigger/overlap/backfill/pause/unpause and two action-history replays passed; ordered concurrent updates independently reproduced native lost-update risk")
}

func TestAuthorizedScheduleNativeDST(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	fixture := newExecutionServiceFixture(t, ctx)
	year := time.Now().UTC().Year() + 1
	springDay := 8 + (7-int(time.Date(year, time.March, 8, 0, 0, 0, 0, time.UTC).Weekday()))%7
	fallDay := 1 + (7-int(time.Date(year, time.November, 1, 0, 0, 0, 0, time.UTC).Weekday()))%7
	cases := []struct {
		name      string
		month     time.Month
		day, hour int
		want      []time.Time
	}{
		{"spring-gap", time.March, springDay, 2, []time.Time{
			time.Date(year, time.March, springDay-1, 7, 30, 0, 0, time.UTC),
			time.Date(year, time.March, springDay+1, 6, 30, 0, 0, time.UTC),
		}},
		{"fall-fold", time.November, fallDay, 1, []time.Time{
			time.Date(year, time.November, fallDay-1, 5, 30, 0, 0, time.UTC),
			time.Date(year, time.November, fallDay, 5, 30, 0, 0, time.UTC),
			time.Date(year, time.November, fallDay, 6, 30, 0, 0, time.UTC),
			time.Date(year, time.November, fallDay+1, 6, 30, 0, 0, time.UTC),
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			handle := createServiceSchedule(t, ctx, fixture, sdk.ScheduleOptions{ID: fixture.prefix + "-" + test.name, Paused: true,
				Spec: sdk.ScheduleSpec{TimeZoneName: "America/New_York", Calendars: []sdk.ScheduleCalendarSpec{{
					Year: []sdk.ScheduleRange{{Start: year}}, Month: []sdk.ScheduleRange{{Start: int(test.month)}},
					DayOfMonth: []sdk.ScheduleRange{{Start: test.day - 1, End: test.day + 1}}, Hour: []sdk.ScheduleRange{{Start: test.hour}},
					Minute: []sdk.ScheduleRange{{Start: 30}},
				}}},
				Action: &sdk.ScheduleWorkflowAction{ID: fixture.prefix + "-unused", Workflow: "unused-paused-definition", TaskQueue: fixture.prefix, WorkflowExecutionTimeout: time.Second},
			})
			description, err := handle.Describe(ctx, fault.Correlation{Call: "dst-describe"})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(description.Info.NextActionTimes, test.want) {
				t.Fatalf("native DST preview differs from independent UTC oracle: got %v want %v", description.Info.NextActionTimes, test.want)
			}
		})
	}
	t.Log("Actual Server native timezone preview skipped the nonexistent spring 02:30 and retained both fall 01:30 occurrences; no local cron/DST evaluator substituted")
}
