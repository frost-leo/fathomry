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
	"slices"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type pendingActivity struct {
	token                                                []byte
	workflowID, workflowRunID, activityID, activityRunID string
	attempt                                              int32
}

func pendingActivityInfo(ctx context.Context) pendingActivity {
	info := activity.GetInfo(ctx)
	return pendingActivity{token: slices.Clone(info.TaskToken), workflowID: info.WorkflowExecution.ID, workflowRunID: info.WorkflowExecution.RunID,
		activityID: info.ActivityID, activityRunID: info.ActivityRunID, attempt: info.Attempt}
}

func awaitPendingActivity(t *testing.T, ctx context.Context, pending <-chan pendingActivity, activityID string, attempt int32) pendingActivity {
	t.Helper()
	select {
	case value := <-pending:
		if value.activityID != activityID || value.attempt != attempt {
			t.Fatal("unexpected Activity attempt identity")
		}
		return value
	case <-ctx.Done():
		t.Fatal("Activity attempt was not observed")
		return pendingActivity{}
	}
}

func activityLifecycleWorkflow(ctx workflow.Context, local bool) (string, error) {
	var result string
	if local {
		ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: 2 * time.Second,
			RetryPolicy: &sdktemporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, BackoffCoefficient: 1, MaximumAttempts: 2}})
		err := workflow.ExecuteLocalActivity(ctx, "lifecycle-local").Get(ctx, &result)
		return result, err
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ActivityID: "async-activity", StartToCloseTimeout: 15 * time.Second,
		ScheduleToCloseTimeout: 20 * time.Second, HeartbeatTimeout: 5 * time.Second, RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	err := workflow.ExecuteActivity(ctx, "lifecycle-async").Get(ctx, &result)
	return result, err
}

func (fixture *executionServiceFixture) cleanupActivity(t *testing.T, activityID, runID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	handle, err := fixture.executions.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: activityID, RunID: runID})
	if err != nil {
		t.Error(err)
		return
	}
	description, err := handle.Describe(ctx, fault.Correlation{Call: "cleanup-activity-describe"}, sdk.DescribeActivityOptions{})
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		return
	}
	if err != nil {
		t.Error("Activity cleanup read failed", err)
		return
	}
	if description.ActivityID != activityID || runID != "" && description.ActivityRunID != runID {
		t.Error("Activity cleanup ownership mismatch")
		return
	}
	if description.Status == enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING || description.Status == enumspb.ACTIVITY_EXECUTION_STATUS_PAUSED {
		if err := handle.Terminate(ctx, fault.Correlation{Call: "cleanup-activity-terminate"}, sdk.TerminateActivityOptions{Reason: "test-owned cleanup"}); err != nil && !errors.As(err, &missing) {
			t.Error(err)
			return
		}
	}
	_, err = fixture.raw.WorkflowService(fault.Correlation{Call: "cleanup-activity-delete"}).DeleteActivityExecution(ctx, &workflowservice.DeleteActivityExecutionRequest{
		Namespace: fixture.namespace, ActivityId: activityID, RunId: description.ActivityRunID})
	if err != nil && !errors.As(err, &missing) {
		t.Error(err)
		return
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := handle.Describe(ctx, fault.Correlation{Call: "cleanup-activity-absence"}, sdk.DescribeActivityOptions{})
		releaseServiceEvidence(t, fixture.evidence)
		if errors.As(err, &missing) {
			return
		}
		if err != nil {
			t.Error(err)
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Error("Activity absence was not observed")
			return
		}
	}
}

func TestAuthorizedActivityLifecycleAndReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	fixture := newExecutionServiceFixture(t, ctx, workflowPrefix+"DeleteActivityExecution", workflowPrefix+"DeleteWorkflowExecution", workflowPrefix+"GetWorkflowExecutionHistory")
	pending := make(chan pendingActivity, 8)
	heartbeatStarted := make(chan pendingActivity, 1)
	async := func(ctx context.Context) (string, error) {
		select {
		case pending <- pendingActivityInfo(ctx):
		case <-ctx.Done():
			return "", ctx.Err()
		}
		return "", activity.ErrResultPending
	}
	heartbeat := func(ctx context.Context) (string, error) {
		activity.RecordHeartbeat(ctx, int64(1))
		select {
		case heartbeatStarted <- pendingActivityInfo(ctx):
		case <-ctx.Done():
			return "", ctx.Err()
		}
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for count := int64(2); ; count++ {
			select {
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, count)
			case <-ctx.Done():
				return "", sdktemporal.NewCanceledError()
			}
		}
	}
	local := func(ctx context.Context) (string, error) {
		if activity.GetInfo(ctx).Attempt == 1 {
			return "", sdktemporal.NewApplicationError("local retry", "retry-local")
		}
		return "local-retried", nil
	}
	taskInbox, err := invocation.NewInbox[temporal.TaskResult](16, 16*temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseServiceEvidence(t, taskInbox) })
	lifetime, stopLifetime := context.WithCancel(context.Background())
	t.Cleanup(stopLifetime)
	managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "activity-worker"}, temporal.WorkerSpec{
		TaskQueue: fixture.prefix, MaxHandlers: 4, Bytes: 4 * fixture.envelope,
		Options: worker.Options{MaxConcurrentWorkflowTaskExecutionSize: 2, MaxConcurrentWorkflowTaskPollers: 2, MaxConcurrentActivityExecutionSize: 4, MaxConcurrentActivityTaskPollers: 2,
			MaxConcurrentLocalActivityExecutionSize: 2, DefaultHeartbeatThrottleInterval: 100 * time.Millisecond, MaxHeartbeatThrottleInterval: 100 * time.Millisecond, WorkerStopTimeout: time.Second},
		Workflows: []temporal.WorkflowRegistration{{Definition: activityLifecycleWorkflow, Options: workflow.RegisterOptions{Name: "activity-lifecycle"}}},
		Activities: []temporal.ActivityRegistration{{Definition: async, Options: activity.RegisterOptions{Name: "lifecycle-async"}}, {Definition: heartbeat, Options: activity.RegisterOptions{Name: "lifecycle-heartbeat"}},
			{Definition: local, Options: activity.RegisterOptions{Name: "lifecycle-local"}}},
	}, fixture.workers, taskInbox)
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
	startActivity := func(suffix, definition string, options sdk.StartActivityOptions) *temporal.ActivityRun {
		t.Helper()
		id := fixture.prefix + "-" + suffix
		t.Cleanup(func() { fixture.cleanupActivity(t, id, "") })
		options.ID = id
		options.TaskQueue = fixture.prefix
		run, err := fixture.executions.ExecuteActivity(ctx, fault.Correlation{Call: suffix + "-start"}, options, definition)
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	workflowID := fixture.prefix + "-workflow"
	t.Cleanup(func() { fixture.cleanupWorkflow(t, workflowID, "") })
	workflowRun, err := fixture.executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "workflow-start"}, sdk.StartWorkflowOptions{
		ID: workflowID, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: 40 * time.Second}, "activity-lifecycle", false)
	if err != nil {
		t.Fatal(err)
	}
	workflowAttempt := awaitPendingActivity(t, ctx, pending, "async-activity", 1)
	if workflowAttempt.workflowID != workflowID || workflowAttempt.workflowRunID != workflowRun.GetRunID() {
		t.Fatal("Workflow Activity identity mismatch")
	}
	if err := fixture.executions.RecordActivityHeartbeatByID(ctx, fault.Correlation{Call: "workflow-heartbeat"}, sdk.RecordActivityHeartbeatByIDOptions{
		Namespace: fixture.namespace, WorkflowID: workflowID, RunID: workflowAttempt.workflowRunID, ActivityID: workflowAttempt.activityID, Details: []any{"progress"}}); err != nil {
		t.Fatal(err)
	}
	workflowDescription, err := fixture.executions.DescribeWorkflowExecution(ctx, fault.Correlation{Call: "workflow-heartbeat-read"}, workflowID, workflowAttempt.workflowRunID)
	if err != nil || len(workflowDescription.GetPendingActivities()) != 1 {
		t.Fatal("Workflow Activity heartbeat state unavailable")
	}
	var heartbeatDetail string
	if err := converter.GetDefaultDataConverter().FromPayloads(workflowDescription.PendingActivities[0].HeartbeatDetails, &heartbeatDetail); err != nil || heartbeatDetail != "progress" {
		t.Fatal("Workflow heartbeat details did not persist")
	}
	if err := fixture.executions.CompleteActivityByID(ctx, fault.Correlation{Call: "workflow-complete"}, sdk.CompleteActivityByIDOptions{
		Namespace: fixture.namespace, WorkflowID: workflowID, RunID: workflowAttempt.workflowRunID, ActivityID: workflowAttempt.activityID, Result: "by-id"}); err != nil {
		t.Fatal(err)
	}
	var result string
	if err := workflowRun.Get(ctx, fault.Correlation{Call: "workflow-result"}, &result); err != nil || result != "by-id" {
		t.Fatal("Workflow asynchronous completion failed")
	}
	err = fixture.executions.CompleteActivityByID(ctx, fault.Correlation{Call: "workflow-duplicate"}, sdk.CompleteActivityByIDOptions{
		Namespace: fixture.namespace, WorkflowID: workflowID, RunID: workflowAttempt.workflowRunID, ActivityID: workflowAttempt.activityID, Result: "must-not-replace"})
	var missing *serviceerror.NotFound
	if !errors.As(err, &missing) {
		t.Fatal("late Workflow completion did not retain the native refusal")
	}

	retried := startActivity("retry", "lifecycle-async", sdk.StartActivityOptions{StartToCloseTimeout: 8 * time.Second, ScheduleToCloseTimeout: 20 * time.Second,
		RetryPolicy: &sdktemporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, BackoffCoefficient: 1, MaximumAttempts: 2}})
	first := awaitPendingActivity(t, ctx, pending, retried.GetID(), 1)
	observation, stopObservation := context.WithCancel(ctx)
	waited := make(chan error, 1)
	go func() { waited <- retried.Get(observation, fault.Correlation{Call: "canceled-result-wait"}, nil) }()
	select {
	case <-waited:
		stopObservation()
		t.Fatal("pending Activity completed before its observer canceled")
	case <-time.After(50 * time.Millisecond):
	}
	stopObservation()
	if err := <-waited; !errors.Is(err, context.Canceled) {
		t.Fatal("Activity result wait did not honor cancellation")
	}
	if err := fixture.executions.CompleteActivity(ctx, fault.Correlation{Call: "retry-first-failure"}, sdk.CompleteActivityOptions{TaskToken: first.token, Err: sdktemporal.NewApplicationError("retry remotely", "retry-fixture")}); err != nil {
		t.Fatal(err)
	}
	second := awaitPendingActivity(t, ctx, pending, retried.GetID(), 2)
	if second.activityRunID != first.activityRunID || slices.Equal(second.token, first.token) {
		t.Fatal("retry attempt/run identity was not distinct")
	}
	err = fixture.executions.CompleteActivity(ctx, fault.Correlation{Call: "late-old-token"}, sdk.CompleteActivityOptions{TaskToken: first.token, Result: "stale"})
	if !errors.As(err, &missing) {
		t.Fatal("old attempt token was not rejected")
	}
	description, err := retried.Describe(ctx, fault.Correlation{Call: "retry-state"}, sdk.DescribeActivityOptions{IncludeLastFailure: true})
	if err != nil || description.Attempt != 2 || description.Status != enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING || !description.HasLastFailure() {
		t.Fatal("late completion damaged the active retry")
	}
	var application *sdktemporal.ApplicationError
	if err := description.GetLastFailure(ctx, fault.Correlation{Call: "retry-failure-read"}); !errors.As(err, &application) || application.Type() != "retry-fixture" {
		t.Fatal("previous attempt failure was not preserved")
	}
	if err := fixture.executions.CompleteActivityByActivityID(ctx, fault.Correlation{Call: "retry-current-complete"}, sdk.CompleteActivityByActivityIDOptions{
		Namespace: fixture.namespace, ActivityID: second.activityID, ActivityRunID: second.activityRunID, Result: "retried"}); err != nil {
		t.Fatal(err)
	}
	if err := retried.Get(ctx, fault.Correlation{Call: "retry-result"}, &result); err != nil || result != "retried" {
		t.Fatal("new attempt did not complete independently")
	}

	canceled := startActivity("cancel", "lifecycle-async", sdk.StartActivityOptions{StartToCloseTimeout: 10 * time.Second, RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	cancelAttempt := awaitPendingActivity(t, ctx, pending, canceled.GetID(), 1)
	if err := canceled.Cancel(ctx, fault.Correlation{Call: "cancel-request"}, sdk.CancelActivityOptions{Reason: "fixture cancellation"}); err != nil {
		t.Fatal(err)
	}
	err = fixture.executions.RecordActivityHeartbeat(ctx, fault.Correlation{Call: "cancel-heartbeat"}, sdk.RecordActivityHeartbeatOptions{TaskToken: cancelAttempt.token, Details: []any{"cancel-check"}})
	var nativeCanceled *sdktemporal.CanceledError
	if !errors.As(err, &nativeCanceled) {
		t.Fatal("heartbeat did not deliver native cancellation")
	}
	if err := fixture.executions.CompleteActivityByActivityID(ctx, fault.Correlation{Call: "cancel-complete"}, sdk.CompleteActivityByActivityIDOptions{
		Namespace: fixture.namespace, ActivityID: canceled.GetID(), ActivityRunID: canceled.GetRunID(), Err: sdktemporal.NewCanceledError()}); err != nil {
		t.Fatal(err)
	}
	if err := canceled.Get(ctx, fault.Correlation{Call: "cancel-result"}, nil); !errors.As(err, &nativeCanceled) {
		t.Fatal("asynchronous cancellation did not reach terminal outcome")
	}

	heartbeating := startActivity("heartbeat", "lifecycle-heartbeat", sdk.StartActivityOptions{StartToCloseTimeout: 10 * time.Second, HeartbeatTimeout: 2 * time.Second, RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	awaitPendingActivity(t, ctx, heartbeatStarted, heartbeating.GetID(), 1)
	if err := heartbeating.Cancel(ctx, fault.Correlation{Call: "heartbeat-cancel"}, sdk.CancelActivityOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := heartbeating.Get(ctx, fault.Correlation{Call: "heartbeat-result"}, nil); !errors.As(err, &nativeCanceled) {
		t.Fatal("native heartbeat/callback cancellation did not complete")
	}

	timed := startActivity("timeout", "lifecycle-async", sdk.StartActivityOptions{StartToCloseTimeout: time.Second, ScheduleToCloseTimeout: 5 * time.Second, RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	awaitPendingActivity(t, ctx, pending, timed.GetID(), 1)
	var timeout *sdktemporal.TimeoutError
	if err := timed.Get(ctx, fault.Correlation{Call: "timeout-result"}, nil); !errors.As(err, &timeout) || timeout.TimeoutType() != enumspb.TIMEOUT_TYPE_START_TO_CLOSE {
		t.Fatal("native Activity attempt timeout semantics changed")
	}

	localID := fixture.prefix + "-local"
	t.Cleanup(func() { fixture.cleanupWorkflow(t, localID, "") })
	localRun, err := fixture.executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "local-start"}, sdk.StartWorkflowOptions{ID: localID, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: 20 * time.Second}, "activity-lifecycle", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := localRun.Get(ctx, fault.Correlation{Call: "local-result"}, &result); err != nil || result != "local-retried" {
		t.Fatal("Local Activity retry did not preserve its native result")
	}
	for _, run := range []*temporal.WorkflowRun{workflowRun, localRun} {
		history := fixture.history(t, ctx, run.GetID(), run.GetRunID())
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflowWithOptions(activityLifecycleWorkflow, workflow.RegisterOptions{Name: "activity-lifecycle"})
		if err := replayer.ReplayWorkflowHistoryWithOptions(executionLogger{}, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: run.GetID(), RunID: run.GetRunID()}}); err != nil {
			t.Fatal("Activity history replay failed", err)
		}
	}
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	asyncCount, localAttempts := 0, 0
	handledLocalFailure := false
	for taskInbox.Usage().Outstanding > 0 {
		delivery, err := taskInbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		record, err := delivery.Receipt().WaitReleased(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if record.Outcome.Value.AsyncCompletion {
			asyncCount++
		}
		if record.Outcome.Value.Local {
			localAttempts++
			if errors.As(record.Err(), &application) && application.Type() == "retry-local" {
				handledLocalFailure = true
			}
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if asyncCount != 5 || localAttempts != 2 || !handledLocalFailure {
		t.Fatal("attempt/async evidence was erased by final success")
	}
	seen := map[string]bool{}
	for fixture.evidence.Usage().Outstanding > 0 {
		record := receiveExecution(t, fixture.evidence)
		value := record.Outcome.Value
		switch record.Context.Correlation.Call {
		case "cancel-heartbeat":
			seen["heartbeat"] = value.HeartbeatAcknowledged && value.CancellationRequested && errors.As(record.Err(), &nativeCanceled)
		case "retry-first-failure":
			seen["completion"] = value.CompletionAcknowledged && value.Accepted
		case "late-old-token":
			seen["late"] = !value.CompletionAcknowledged && errors.As(record.Err(), &missing)
		case "canceled-result-wait":
			seen["wait"] = errors.Is(record.Err(), context.Canceled) && !value.ResultObtained
		}
	}
	for _, fact := range []string{"heartbeat", "completion", "late", "wait"} {
		if !seen[fact] {
			t.Fatalf("missing independent Activity fact: %s", fact)
		}
	}
	t.Log("Workflow/Standalone async completion, persisted heartbeat, native cancellation, old-token rejection across retries, waiter cancellation, attempt timeout and Local Activity retry/replay passed")
}
