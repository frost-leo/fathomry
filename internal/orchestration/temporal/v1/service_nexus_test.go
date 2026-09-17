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
	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type backedNexusInput struct {
	ID, Value, Mode string
	UpdateID        string
	Continued       bool
}

func nexusUpdateTarget(ctx workflow.Context) (string, error) {
	if err := workflow.SetQueryHandler(ctx, "value", func() (string, error) { return "query:value", nil }); err != nil {
		return "", err
	}
	if err := workflow.SetUpdateHandler(ctx, "echo", func(_ workflow.Context, value string) (string, error) { return "updated:" + value, nil }); err != nil {
		return "", err
	}
	if err := workflow.SetUpdateHandler(ctx, "wait", func(updateContext workflow.Context, value string) (string, error) {
		var release bool
		workflow.GetSignalChannel(updateContext, "release-update").Receive(updateContext, &release)
		return "updated:" + value, nil
	}); err != nil {
		return "", err
	}
	var finish bool
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, &finish)
	if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return "", err
	}
	return "finished", nil
}

func nexusBackedActivity(_ context.Context, input backedNexusInput) (string, error) {
	return "activity:" + input.Value, nil
}

type callerNexusInput struct {
	Endpoint, Operation string
	Backend             backedNexusInput
	Cancel              bool
}

func backedNexusWorkflow(ctx workflow.Context, input backedNexusInput) (string, error) {
	if input.Continued {
		return "continued:" + input.Value, nil
	}
	released := workflow.GetSignalChannel(ctx, "release")
	if err := workflow.Await(ctx, func() bool { return released.Len() > 0 }); err != nil {
		return "", err
	}
	var release bool
	released.Receive(ctx, &release)
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if input.Mode == "continue" {
		input.Continued = true
		return "", workflow.NewContinueAsNewError(ctx, "nexus-backed-workflow", input)
	}
	return "backed:" + input.Value, nil
}

func callerNexusWorkflow(ctx workflow.Context, input callerNexusInput) (string, error) {
	ready := false
	if err := workflow.SetQueryHandler(ctx, "ready", func() (bool, error) { return ready, nil }); err != nil {
		return "", err
	}
	operationContext, cancel := workflow.WithCancel(ctx)
	defer cancel()
	future := workflow.NewNexusClient(input.Endpoint, "backed-service").ExecuteOperation(operationContext, input.Operation, input.Backend,
		workflow.NexusOperationOptions{ScheduleToCloseTimeout: 45 * time.Second, CancellationType: workflow.NexusOperationCancellationTypeWaitCompleted})
	var execution workflow.NexusOperationExecution
	if err := future.GetNexusOperationExecution().Get(ctx, &execution); err != nil {
		return "", err
	}
	ready = true
	if input.Cancel {
		var request bool
		workflow.GetSignalChannel(ctx, "cancel-operation").Receive(ctx, &request)
		cancel()
	}
	var result string
	err := future.Get(ctx, &result)
	return result, err
}

func createTestNexusEndpoint(t *testing.T, ctx context.Context, fixture *executionServiceFixture) {
	t.Helper()
	created, err := fixture.raw.OperatorService(fault.Correlation{Call: "endpoint-create"}).CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{Name: fixture.prefix, Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{Namespace: fixture.namespace, TaskQueue: fixture.prefix}}}}})
	if err != nil {
		t.Fatal(err)
	}
	id := created.GetEndpoint().GetId()
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		current, err := fixture.raw.OperatorService(fault.Correlation{Call: "endpoint-read"}).GetNexusEndpoint(cleanup, &operatorservice.GetNexusEndpointRequest{Id: id})
		if err != nil || current.GetEndpoint().GetSpec().GetName() != fixture.prefix {
			t.Error("Nexus endpoint ownership unavailable")
			return
		}
		_, err = fixture.raw.OperatorService(fault.Correlation{Call: "endpoint-delete"}).DeleteNexusEndpoint(cleanup, &operatorservice.DeleteNexusEndpointRequest{Id: id, Version: current.Endpoint.Version})
		if err != nil {
			t.Error(err)
			return
		}
		_, err = fixture.raw.OperatorService(fault.Correlation{Call: "endpoint-absent"}).GetNexusEndpoint(cleanup, &operatorservice.GetNexusEndpointRequest{Id: id})
		var missing *serviceerror.NotFound
		if !errors.As(err, &missing) {
			t.Error("Nexus endpoint absence was not observed")
		}
	})
}

func TestAuthorizedWorkflowBackedNexusCallbacksAndCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	operatorPrefix := "/temporal.api.operatorservice.v1.OperatorService/"
	fixture := newExecutionServiceFixture(t, ctx, operatorPrefix+"CreateNexusEndpoint", operatorPrefix+"GetNexusEndpoint", operatorPrefix+"DeleteNexusEndpoint",
		workflowPrefix+"GetWorkflowExecutionHistory", workflowPrefix+"DeleteWorkflowExecution")
	createTestNexusEndpoint(t, ctx, fixture)
	service := nexus.NewService("backed-service")
	operation := temporalnexus.NewWorkflowRunOperation("workflow", backedNexusWorkflow, func(_ context.Context, input backedNexusInput, _ nexus.StartOperationOptions) (sdk.StartWorkflowOptions, error) {
		return sdk.StartWorkflowOptions{ID: input.ID, WorkflowExecutionTimeout: 40 * time.Second, WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING}, nil
	})
	if err := service.Register(operation); err != nil {
		t.Fatal(err)
	}
	var lost atomic.Bool
	var attempts atomic.Int32
	var requestMu sync.Mutex
	var firstRequestID string
	recovering, err := temporalnexus.NewWorkflowRunOperationWithOptions(temporalnexus.WorkflowRunOperationOptions[backedNexusInput, string]{Name: "recover",
		Handler: func(ctx context.Context, input backedNexusInput, options nexus.StartOperationOptions) (temporalnexus.WorkflowHandle[string], error) {
			attempts.Add(1)
			requestMu.Lock()
			if firstRequestID == "" {
				firstRequestID = options.RequestID
			}
			consistent := firstRequestID != "" && options.RequestID == firstRequestID
			requestMu.Unlock()
			if !consistent {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "request identity changed on retry")
			}
			handle, err := temporalnexus.ExecuteWorkflow(ctx, options, sdk.StartWorkflowOptions{ID: input.ID, WorkflowExecutionTimeout: 40 * time.Second,
				WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING}, backedNexusWorkflow, input)
			if err != nil {
				return nil, err
			}
			if lost.CompareAndSwap(false, true) {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "synthetic failure after backend acceptance")
			}
			return handle, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Register(recovering); err != nil {
		t.Fatal(err)
	}
	tasks, err := invocation.NewInbox[temporal.TaskResult](16, 16*temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseServiceEvidence(t, tasks) })
	lifetime, stopLifetime := context.WithCancel(context.Background())
	t.Cleanup(stopLifetime)
	managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "nexus-worker"}, temporal.WorkerSpec{
		TaskQueue: fixture.prefix, MaxHandlers: 4, Bytes: 4 * fixture.envelope,
		Options: worker.Options{MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2, MaxConcurrentNexusTaskExecutionSize: 2, MaxConcurrentNexusTaskPollers: 2, WorkerStopTimeout: time.Second},
		Workflows: []temporal.WorkflowRegistration{{Definition: backedNexusWorkflow, Options: workflow.RegisterOptions{Name: "nexus-backed-workflow"}},
			{Definition: callerNexusWorkflow, Options: workflow.RegisterOptions{Name: "nexus-caller"}}}, NexusServices: []*nexus.Service{service},
	}, fixture.workers, tasks)
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
	startCaller := func(suffix, mode string, cancelOperation bool) *temporal.WorkflowRun {
		t.Helper()
		callerID, backendID := fixture.prefix+"-"+suffix, fixture.prefix+"-"+suffix+"-backend"
		operation := "workflow"
		if mode == "retry-start" {
			operation = "recover"
		}
		t.Cleanup(func() { fixture.cleanupWorkflow(t, callerID, "") })
		t.Cleanup(func() { fixture.cleanupWorkflow(t, backendID, "") })
		run, err := fixture.executions.ExecuteWorkflow(ctx, fault.Correlation{Call: suffix + "-start"}, sdk.StartWorkflowOptions{ID: callerID, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: 60 * time.Second},
			"nexus-caller", callerNexusInput{Endpoint: fixture.prefix, Operation: operation, Backend: backedNexusInput{ID: backendID, Value: "value", Mode: mode}, Cancel: cancelOperation})
		if err != nil {
			t.Fatal(err)
		}
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			var ready bool
			if err := fixture.executions.QueryWorkflow(ctx, fault.Correlation{Call: "nexus-ready"}, callerID, run.GetRunID(), "ready", &ready); err != nil {
				t.Fatal(err)
			}
			releaseServiceEvidence(t, fixture.evidence)
			if ready {
				return run
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal("Nexus start was not observed")
			}
		}
	}
	run := startCaller("continue", "continue", false)
	backendID := run.GetID() + "-backend"
	backend, err := fixture.executions.DescribeWorkflowExecution(ctx, fault.Correlation{Call: "backend-start"}, backendID, "")
	if err != nil {
		t.Fatal(err)
	}
	firstBackendRun := backend.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	t.Cleanup(func() { fixture.cleanupWorkflow(t, backendID, firstBackendRun) })
	if len(backend.GetCallbacks()) != 1 {
		t.Fatal("native Nexus callback was not attached to the backend workflow")
	}
	if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "backend-release"}, backendID, firstBackendRun, "release", true); err != nil {
		t.Fatal(err)
	}
	var result string
	if err := run.Get(ctx, fault.Correlation{Call: "nexus-result"}, &result); err != nil || result != "continued:value" {
		executionCauses(t, err, 0)
		t.Fatal("Nexus callback did not follow the backend Continue-As-New chain")
	}
	callerHistory := fixture.history(t, ctx, run.GetID(), run.GetRunID())
	started, completed, linked := false, false, false
	for _, event := range callerHistory.Events {
		switch event.EventType {
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED:
			started = event.GetNexusOperationStartedEventAttributes().GetOperationToken() != ""
			for _, link := range event.Links {
				if link.GetWorkflowEvent().GetWorkflowId() == backendID {
					linked = true
				}
			}
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:
			completed = true
		}
	}
	if !started || !completed || !linked {
		t.Fatal("native async Nexus history or forward link was missing")
	}
	initialHistory := fixture.history(t, ctx, backendID, firstBackendRun)
	backlink := false
	for _, link := range initialHistory.Events[0].Links {
		if link.GetWorkflowEvent().GetWorkflowId() == run.GetID() {
			backlink = true
		}
	}
	for _, callback := range initialHistory.Events[0].GetWorkflowExecutionStartedEventAttributes().GetCompletionCallbacks() {
		for _, link := range callback.GetLinks() {
			if link.GetWorkflowEvent().GetWorkflowId() == run.GetID() {
				backlink = true
			}
		}
	}
	if !backlink {
		t.Fatalf("native Nexus backward link was not persisted (event links=%d callbacks=%d)", len(initialHistory.Events[0].Links), len(initialHistory.Events[0].GetWorkflowExecutionStartedEventAttributes().GetCompletionCallbacks()))
	}
	finalBackend, err := fixture.executions.DescribeWorkflowExecution(ctx, fault.Correlation{Call: "backend-final"}, backendID, "")
	if err != nil {
		t.Fatal(err)
	}
	finalHistory := fixture.history(t, ctx, backendID, finalBackend.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	for _, history := range []*historypb.History{callerHistory, initialHistory, finalHistory} {
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflowWithOptions(callerNexusWorkflow, workflow.RegisterOptions{Name: "nexus-caller"})
		replayer.RegisterWorkflowWithOptions(backedNexusWorkflow, workflow.RegisterOptions{Name: "nexus-backed-workflow"})
		if err := replayer.ReplayWorkflowHistory(executionLogger{}, history); err != nil {
			t.Fatal("Nexus history replay failed", err)
		}
	}
	retried := startCaller("recovered", "retry-start", false)
	if attempts.Load() != 2 {
		t.Fatal("native Nexus retry did not preserve the accepted backend")
	}
	if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "recovered-backend-release"}, retried.GetID()+"-backend", "", "release", true); err != nil {
		t.Fatal(err)
	}
	if err := retried.Get(ctx, fault.Correlation{Call: "recovered-nexus-result"}, &result); err != nil || result != "backed:value" {
		t.Fatal("retried Nexus start lost or duplicated its backend result")
	}
	canceled := startCaller("cancel", "wait", true)
	if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "cancel-nexus"}, canceled.GetID(), canceled.GetRunID(), "cancel-operation", true); err != nil {
		t.Fatal(err)
	}
	err = canceled.Get(ctx, fault.Correlation{Call: "nexus-cancel-result"}, nil)
	var canceledError *sdktemporal.CanceledError
	if !errors.As(err, &canceledError) {
		executionCauses(t, err, 0)
		t.Fatal("Nexus cancellation did not reach terminal completion")
	}
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	async, cancelCalls, failedStarts := 0, 0, 0
	for tasks.Usage().Outstanding > 0 {
		delivery, err := tasks.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		record, err := delivery.Receipt().WaitReleased(ctx)
		if err != nil {
			t.Fatal("Nexus handler evidence incomplete")
		}
		value := record.Outcome.Value
		if value.NexusService != "backed-service" || value.NexusOperation != "workflow" && value.NexusOperation != "recover" {
			t.Fatal("Nexus operation attribution lost")
		}
		if record.Err() != nil {
			var failure *nexus.HandlerError
			if value.NexusOperation != "recover" || !errors.As(record.Err(), &failure) || failure.Type != nexus.HandlerErrorTypeUnavailable {
				t.Fatal("unexpected Nexus callback failure")
			}
			failedStarts++
			if err := delivery.Release(); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if value.Kind == "nexus-start" {
			if !value.AsyncCompletion || !value.NexusCallbackRequested || value.NexusRequestID == "" || value.NexusRequestLinks < 1 {
				t.Fatal("async handler evidence lost callback/link facts")
			}
			async++
		}
		if value.Kind == "nexus-cancel" {
			cancelCalls++
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if async != 3 || cancelCalls != 1 || failedStarts != 1 {
		t.Fatal("Nexus Start/Cancel callback counts disagreed with the independent scenario")
	}
	t.Log("Workflow-backed Nexus async callback, Continue-As-New, forward/back links, native cancellation and three-history replay passed")
}

func TestAuthorizedNativeTemporalNexusHelpers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	operatorPrefix := "/temporal.api.operatorservice.v1.OperatorService/"
	fixture := newRuntimeExecutionServiceFixture(t, ctx, temporal.RuntimeOptions{Interceptors: []interceptor.ClientInterceptor{&interceptor.ClientInterceptorBase{}}}, operatorPrefix+"CreateNexusEndpoint", operatorPrefix+"GetNexusEndpoint", operatorPrefix+"DeleteNexusEndpoint",
		workflowPrefix+"GetWorkflowExecutionHistory", workflowPrefix+"DeleteWorkflowExecution", workflowPrefix+"DeleteActivityExecution")
	createTestNexusEndpoint(t, ctx, fixture)
	service := nexus.NewService("backed-service")
	query := nexus.NewSyncOperation("query", func(ctx context.Context, input backedNexusInput, _ nexus.StartOperationOptions) (string, error) {
		encoded, err := temporalnexus.GetClient(ctx).QueryWorkflow(ctx, input.ID, "", "value")
		if err != nil {
			return "", err
		}
		var value string
		if err := encoded.Get(&value); err != nil {
			return "", err
		}
		return value, nil
	})
	startActivity, err := temporalnexus.NewTemporalOperation(temporalnexus.TemporalOperationOptions[backedNexusInput, string]{Name: "activity",
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input backedNexusInput, _ temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[string], error) {
			return nc.StartActivity(ctx, sdk.StartActivityOptions{ID: input.ID, StartToCloseTimeout: 10 * time.Second, ScheduleToCloseTimeout: 20 * time.Second,
				RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}}, nexusBackedActivity, input)
		}})
	if err != nil {
		t.Fatal(err)
	}
	startUpdate, err := temporalnexus.NewTemporalOperation(temporalnexus.TemporalOperationOptions[backedNexusInput, string]{Name: "update",
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input backedNexusInput, _ temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[string], error) {
			return nc.StartUpdateWorkflow[string](ctx, sdk.UpdateWorkflowOptions{WorkflowID: input.ID, UpdateID: input.UpdateID,
				UpdateName: input.Mode, Args: []any{input.Value}, WaitForStage: sdk.WorkflowUpdateStageAccepted})
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Register(query, startActivity, startUpdate); err != nil {
		t.Fatal(err)
	}
	tasks, err := invocation.NewInbox[temporal.TaskResult](16, 16*temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseServiceEvidence(t, tasks) })
	lifetime, stopLifetime := context.WithCancel(context.Background())
	t.Cleanup(stopLifetime)
	managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "generic-nexus-worker"}, temporal.WorkerSpec{
		TaskQueue: fixture.prefix, MaxHandlers: 4, Bytes: 4 * fixture.envelope,
		Options: worker.Options{MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2, MaxConcurrentActivityExecutionSize: 2, MaxConcurrentActivityTaskPollers: 2,
			MaxConcurrentNexusTaskExecutionSize: 2, MaxConcurrentNexusTaskPollers: 2, WorkerStopTimeout: time.Second},
		Workflows: []temporal.WorkflowRegistration{{Definition: callerNexusWorkflow, Options: workflow.RegisterOptions{Name: "nexus-caller"}},
			{Definition: nexusUpdateTarget, Options: workflow.RegisterOptions{Name: "nexus-update-target"}}},
		Activities: []temporal.ActivityRegistration{{Definition: nexusBackedActivity, Options: activity.RegisterOptions{Name: "nexus-backed-activity"}}}, NexusServices: []*nexus.Service{service},
	}, fixture.workers, tasks)
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
	targetID := fixture.prefix + "-target"
	t.Cleanup(func() { fixture.cleanupWorkflow(t, targetID, "") })
	target, err := fixture.executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "target-start"}, sdk.StartWorkflowOptions{
		ID: targetID, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: 70 * time.Second}, "nexus-update-target")
	if err != nil {
		t.Fatal(err)
	}
	completed, err := fixture.executions.UpdateWorkflow(ctx, fault.Correlation{Call: "seed-update"}, sdk.UpdateWorkflowOptions{
		WorkflowID: targetID, UpdateID: "sync-update", UpdateName: "echo", Args: []any{"value"}, WaitForStage: sdk.WorkflowUpdateStageCompleted})
	if err != nil {
		t.Fatal(err)
	}
	var value string
	if err := completed.Get(ctx, fault.Correlation{Call: "seed-result"}, &value); err != nil || value != "updated:value" {
		t.Fatal("seed Update did not complete")
	}
	startCaller := func(suffix, operation string, input backedNexusInput) *temporal.WorkflowRun {
		t.Helper()
		id := fixture.prefix + "-" + suffix
		t.Cleanup(func() { fixture.cleanupWorkflow(t, id, "") })
		run, err := fixture.executions.ExecuteWorkflow(ctx, fault.Correlation{Call: suffix + "-start"}, sdk.StartWorkflowOptions{
			ID: id, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: 50 * time.Second}, "nexus-caller", callerNexusInput{Endpoint: fixture.prefix, Operation: operation, Backend: input})
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	queried := startCaller("query", "query", backedNexusInput{ID: targetID})
	if err := queried.Get(ctx, fault.Correlation{Call: "query-result"}, &value); err != nil || value != "query:value" {
		executionCauses(t, err, 0)
		t.Fatal("query-backed Nexus result failed")
	}
	activityID := fixture.prefix + "-activity"
	t.Cleanup(func() { fixture.cleanupActivity(t, activityID, "") })
	activityCaller := startCaller("activity-caller", "activity", backedNexusInput{ID: activityID, Value: "value"})
	if err := activityCaller.Get(ctx, fault.Correlation{Call: "activity-nexus-result"}, &value); err != nil || value != "activity:value" {
		executionCauses(t, err, 0)
		var application *sdktemporal.ApplicationError
		if errors.As(err, &application) && application.Type() == "InvalidArgument" && application.Message() == "completion callbacks are not enabled for this namespace" {
			t.Log("confirmed service gate: activity.enableCallbacks is disabled for the test namespace")
		}
		t.Fatal("native generic Activity-backed Nexus callback failed")
	}
	syncCaller := startCaller("sync-update", "update", backedNexusInput{ID: targetID, Value: "value", Mode: "echo", UpdateID: "sync-update"})
	if err := syncCaller.Get(ctx, fault.Correlation{Call: "sync-update-result"}, &value); err != nil || value != "updated:value" {
		executionCauses(t, err, 0)
		t.Fatal("native completed-Update synchronous Nexus branch failed")
	}
	asyncCaller := startCaller("async-update", "update", backedNexusInput{ID: targetID, Value: "value", Mode: "wait", UpdateID: "async-update"})
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		var ready bool
		if err := fixture.executions.QueryWorkflow(ctx, fault.Correlation{Call: "update-nexus-ready"}, asyncCaller.GetID(), asyncCaller.GetRunID(), "ready", &ready); err != nil {
			t.Fatal(err)
		}
		releaseServiceEvidence(t, fixture.evidence)
		if ready {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("asynchronous Nexus Update did not reach its start response")
		}
	}
	if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "update-release"}, targetID, target.GetRunID(), "release-update", true); err != nil {
		t.Fatal(err)
	}
	if err := asyncCaller.Get(ctx, fault.Correlation{Call: "async-update-result"}, &value); err != nil || value != "updated:value" {
		executionCauses(t, err, 0)
		t.Fatal("native asynchronous Update Nexus callback failed")
	}
	if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "target-finish"}, targetID, target.GetRunID(), "finish", true); err != nil {
		t.Fatal(err)
	}
	if err := target.Get(ctx, fault.Correlation{Call: "target-result"}, &value); err != nil || value != "finished" {
		t.Fatal("Update target cleanup did not complete")
	}
	for _, run := range []*temporal.WorkflowRun{queried, activityCaller, syncCaller, asyncCaller, target} {
		history := fixture.history(t, ctx, run.GetID(), run.GetRunID())
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflowWithOptions(callerNexusWorkflow, workflow.RegisterOptions{Name: "nexus-caller"})
		replayer.RegisterWorkflowWithOptions(nexusUpdateTarget, workflow.RegisterOptions{Name: "nexus-update-target"})
		if err := replayer.ReplayWorkflowHistory(executionLogger{}, history); err != nil {
			t.Fatal("native generic Nexus history replay failed", err)
		}
	}
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	querySync, activityAsync, updateSync, updateAsync := 0, 0, 0, 0
	for tasks.Usage().Outstanding > 0 {
		delivery, err := tasks.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		record, err := delivery.Receipt().WaitReleased(ctx)
		if err != nil || record.Err() != nil {
			t.Fatal("generic Nexus task evidence incomplete")
		}
		evidence := record.Outcome.Value
		if evidence.Kind == "nexus-start" {
			switch evidence.NexusOperation {
			case "query":
				if evidence.AsyncCompletion {
					t.Fatal("query operation became asynchronous")
				}
				querySync++
			case "activity":
				if !evidence.AsyncCompletion {
					t.Fatal("Activity operation lost its async result")
				}
				activityAsync++
			case "update":
				if evidence.AsyncCompletion {
					updateAsync++
				} else {
					updateSync++
				}
			}
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if querySync != 1 || activityAsync != 1 || updateSync != 1 || updateAsync != 1 {
		t.Fatal("native synchronous/asynchronous result branches were not distinguished")
	}
	t.Log("Native Query, Activity, already-completed Update and pending Update Nexus helpers passed through formal Worker/Client; Go 1.27 generic methods and five histories replayed")
}

func TestAuthorizedStandaloneNexusDefaultOff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	fixture := newExecutionServiceFixture(t, ctx)
	_, err := fixture.executions.ExecuteNexusOperation(ctx, fault.Correlation{Call: "capability-off"}, sdk.NexusClientOptions{Endpoint: fixture.prefix, Service: "not-created"},
		"echo", "fixture", sdk.StartNexusOperationOptions{ID: fixture.prefix + "-off", ScheduleToCloseTimeout: time.Second})
	var unsupported *serviceerror.Unimplemented
	if !errors.As(err, &unsupported) {
		t.Fatal("default-off standalone Nexus was not explicitly refused", err)
	}
	record := receiveExecution(t, fixture.evidence)
	if !record.Outcome.Value.NativeCalled || record.Outcome.Value.Accepted {
		t.Fatal("capability refusal became accepted execution")
	}
}

func (fixture *executionServiceFixture) cleanupNexus(t *testing.T, run *temporal.NexusRun) {
	t.Helper()
	if !strings.HasPrefix(run.GetID(), fixture.prefix+"-") {
		t.Error("Nexus cleanup ownership is missing")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	description, err := run.Describe(ctx, fault.Correlation{Call: "nexus-cleanup-describe"}, sdk.DescribeNexusOperationOptions{})
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		return
	}
	if err != nil || description.OperationID != run.GetID() || description.OperationRunID != run.GetRunID() {
		t.Error("Nexus cleanup identity could not be verified")
		return
	}
	_, err = fixture.raw.WorkflowService(fault.Correlation{Call: "nexus-delete"}).DeleteNexusOperationExecution(ctx,
		&workflowservice.DeleteNexusOperationExecutionRequest{Namespace: fixture.namespace, OperationId: run.GetID(), RunId: run.GetRunID()})
	if err != nil {
		t.Error(err)
		return
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := run.Describe(ctx, fault.Correlation{Call: "nexus-absence"}, sdk.DescribeNexusOperationOptions{})
		releaseServiceEvidence(t, fixture.evidence)
		if errors.As(err, &missing) {
			return
		}
		if err != nil {
			t.Error("Nexus absence not established", err)
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Error("Nexus delete did not become observable")
			return
		}
	}
}

func TestAuthorizedStandaloneNexusLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	operator := "/temporal.api.operatorservice.v1.OperatorService/"
	fixture := newExecutionServiceFixture(t, ctx, operator+"CreateNexusEndpoint", operator+"GetNexusEndpoint", operator+"DeleteNexusEndpoint",
		workflowPrefix+"DeleteWorkflowExecution", workflowPrefix+"DeleteNexusOperationExecution")
	createTestNexusEndpoint(t, ctx, fixture)
	service := nexus.NewService("standalone-service")
	if err := service.Register(nexus.NewSyncOperation("echo", func(_ context.Context, input string, _ nexus.StartOperationOptions) (string, error) {
		return "echo:" + input, nil
	}),
		temporalnexus.NewWorkflowRunOperation("workflow", backedNexusWorkflow, func(_ context.Context, input backedNexusInput, _ nexus.StartOperationOptions) (sdk.StartWorkflowOptions, error) {
			return sdk.StartWorkflowOptions{ID: input.ID, WorkflowExecutionTimeout: 45 * time.Second}, nil
		})); err != nil {
		t.Fatal(err)
	}
	tasks, err := invocation.NewInbox[temporal.TaskResult](16, 16*temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseServiceEvidence(t, tasks) })
	lifetime, stopLifetime := context.WithCancel(context.Background())
	t.Cleanup(stopLifetime)
	managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "standalone-nexus-worker"}, temporal.WorkerSpec{
		TaskQueue: fixture.prefix, MaxHandlers: 4, Bytes: 4 * fixture.envelope,
		Options:       worker.Options{MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2, MaxConcurrentNexusTaskExecutionSize: 2, MaxConcurrentNexusTaskPollers: 2},
		Workflows:     []temporal.WorkflowRegistration{{Definition: backedNexusWorkflow, Options: workflow.RegisterOptions{Name: "nexus-backed-workflow"}}},
		NexusServices: []*nexus.Service{service},
	}, fixture.workers, tasks)
	if managed != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if err := managed.Stop(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	start := func(suffix, operation string, input any) *temporal.NexusRun {
		t.Helper()
		run, err := fixture.executions.ExecuteNexusOperation(ctx, fault.Correlation{Call: "nexus-" + suffix}, sdk.NexusClientOptions{Endpoint: fixture.prefix, Service: "standalone-service"},
			operation, input, sdk.StartNexusOperationOptions{ID: fixture.prefix + "-" + suffix, ScheduleToCloseTimeout: 40 * time.Second, Summary: "standalone fixture"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { fixture.cleanupNexus(t, run) })
		return run
	}
	syncRun := start("sync", "echo", "value")
	var result string
	if err := syncRun.Get(ctx, fault.Correlation{Call: "sync-result"}, &result); err != nil || result != "echo:value" {
		t.Fatal("standalone synchronous result failed", err)
	}
	description, err := syncRun.Describe(ctx, fault.Correlation{Call: "sync-describe"}, sdk.DescribeNexusOperationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := description.GetSummary(ctx, fault.Correlation{Call: "sync-summary"})
	if err != nil || summary != "standalone fixture" || description.OperationID != syncRun.GetID() {
		t.Fatal("standalone metadata mismatch", err)
	}
	awaitStarted := func(run *temporal.NexusRun) {
		t.Helper()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			description, err := run.Describe(ctx, fault.Correlation{Call: "nexus-started"}, sdk.DescribeNexusOperationOptions{})
			if err != nil {
				t.Fatal(err)
			}
			releaseServiceEvidence(t, fixture.evidence)
			if description.OperationToken != "" {
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal("async standalone Nexus never started")
			}
		}
	}
	backendCancel := fixture.prefix + "-cancel-backend"
	t.Cleanup(func() { fixture.cleanupWorkflow(t, backendCancel, "") })
	canceled := start("cancel", "workflow", backedNexusInput{ID: backendCancel, Value: "value"})
	awaitStarted(canceled)
	short, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	err = canceled.Get(short, fault.Correlation{Call: "canceled-observer"}, &result)
	stop()
	if err == nil {
		t.Fatal("observer cancellation was not observed")
	}
	if err := canceled.Cancel(ctx, fault.Correlation{Call: "cancel-operation"}, sdk.CancelNexusOperationOptions{Reason: "fixture cancellation"}); err != nil {
		t.Fatal(err)
	}
	err = canceled.Get(ctx, fault.Correlation{Call: "cancel-result"}, &result)
	var cancellation *sdktemporal.CanceledError
	if !errors.As(err, &cancellation) {
		executionCauses(t, err, 0)
		t.Fatal("standalone cancellation did not complete")
	}
	backendTerminated := fixture.prefix + "-terminate-backend"
	t.Cleanup(func() { fixture.cleanupWorkflow(t, backendTerminated, "") })
	terminated := start("terminate", "workflow", backedNexusInput{ID: backendTerminated, Value: "value"})
	awaitStarted(terminated)
	if err := terminated.Terminate(ctx, fault.Correlation{Call: "terminate-operation"}, sdk.TerminateNexusOperationOptions{Reason: "fixture termination"}); err != nil {
		t.Fatal(err)
	}
	err = terminated.Get(ctx, fault.Correlation{Call: "terminate-result"}, &result)
	var termination *sdktemporal.TerminatedError
	if !errors.As(err, &termination) {
		executionCauses(t, err, 0)
		t.Fatal("standalone termination did not complete")
	}
	query := "Endpoint = '" + fixture.prefix + "'"
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		count, err := fixture.executions.CountNexusOperations(ctx, fault.Correlation{Call: "nexus-count"}, sdk.CountNexusOperationsOptions{Query: query})
		if err != nil {
			t.Fatal(err)
		}
		releaseServiceEvidence(t, fixture.evidence)
		if count.Count == 3 {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("standalone Nexus count did not converge")
		}
	}
	found := make(map[string]bool)
	if err := fixture.executions.WalkNexusOperations(ctx, fault.Correlation{Call: "nexus-list"}, sdk.ListNexusOperationsOptions{Query: query}, func(_ context.Context, metadata *sdk.NexusOperationMetadata) error {
		found[metadata.OperationID] = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(found) != 3 || !found[syncRun.GetID()] || !found[canceled.GetID()] || !found[terminated.GetID()] {
		t.Fatal("standalone Nexus list targets differed")
	}
	t.Log("Experimental standalone Nexus start/result/description/summary, canceled observer, explicit cancellation/termination, filtered count/list and owned deletion passed on the enabled namespace profile")
}
