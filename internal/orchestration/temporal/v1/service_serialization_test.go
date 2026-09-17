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
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/nexus-rpc/sdk-go/nexus"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
)

type serializationInput struct{ Value, Endpoint string }

func serializationEcho(_ context.Context, value string) (string, error) {
	return value + ":activity", nil
}

func serializationFailure(context.Context) error {
	return sdktemporal.NewNonRetryableApplicationError("fixture failure", "fixture", nil, "retained-detail")
}

func serializationWorkflow(ctx workflow.Context, input serializationInput) (string, error) {
	ready := false
	if err := workflow.SetQueryHandler(ctx, "ready", func() (bool, error) { return ready, nil }); err != nil {
		return "", err
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Second, RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	var remote, local, linked string
	if err := workflow.ExecuteActivity(ctx, "serialization-echo", input.Value).Get(ctx, &remote); err != nil {
		return "", err
	}
	localContext := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: 5 * time.Second})
	if err := workflow.ExecuteLocalActivity(localContext, "serialization-echo", remote).Get(ctx, &local); err != nil {
		return "", err
	}
	if err := workflow.NewNexusClient(input.Endpoint, "serialization-service").ExecuteOperation(ctx, "echo", local,
		workflow.NexusOperationOptions{ScheduleToCloseTimeout: 10 * time.Second}).Get(ctx, &linked); err != nil {
		return "", err
	}
	if err := workflow.UpsertMemo(ctx, map[string]any{"phase": "legacy-ready"}); err != nil {
		return "", err
	}
	workflow.SetCurrentDetails(ctx, "waiting for compatible reader")
	ready = true
	var advance bool
	workflow.GetSignalChannel(ctx, "advance").Receive(ctx, &advance)
	err := workflow.ExecuteActivity(ctx, "serialization-failure").Get(ctx, nil)
	var application *sdktemporal.ApplicationError
	if !errors.As(err, &application) || application.Type() != "fixture" || !application.NonRetryable() {
		return "", errors.New("native application failure semantics changed")
	}
	var detail string
	if err := application.Details(&detail); err != nil {
		return "", err
	}
	return linked + ":" + detail, nil
}

func TestAuthorizedSerializationReaderUpgradeAndExternalPayloads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	store := newRetainedPayloadStore()
	legacy := serializationRuntime("1", false, store)
	legacy.FailureConverter = sdktemporal.NewDefaultFailureConverter(sdktemporal.DefaultFailureConverterOptions{DataConverter: legacy.DataConverter, EncodeCommonAttributes: true})
	operator := "/temporal.api.operatorservice.v1.OperatorService/"
	old := newRuntimeExecutionServiceFixture(t, ctx, legacy, operator+"CreateNexusEndpoint", operator+"GetNexusEndpoint", operator+"DeleteNexusEndpoint",
		workflowPrefix+"GetWorkflowExecutionHistory", workflowPrefix+"DeleteWorkflowExecution")
	createTestNexusEndpoint(t, ctx, old)
	service := nexus.NewService("serialization-service")
	if err := service.Register(nexus.NewSyncOperation("echo", func(_ context.Context, value string, _ nexus.StartOperationOptions) (string, error) {
		return value + ":nexus", nil
	})); err != nil {
		t.Fatal(err)
	}
	startWorker := func(fixture *executionServiceFixture) *temporal.Worker {
		t.Helper()
		tasks, err := invocation.NewInbox[temporal.TaskResult](16, 16*temporal.ExecutionEvidenceBytes)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { releaseServiceEvidence(t, tasks) })
		lifetime, stopLifetime := context.WithCancel(context.Background())
		t.Cleanup(stopLifetime)
		managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "serialization-worker"}, temporal.WorkerSpec{
			TaskQueue: old.prefix, MaxHandlers: 4, Bytes: 4 * fixture.envelope,
			Options: worker.Options{MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2,
				MaxConcurrentActivityExecutionSize: 2, MaxConcurrentActivityTaskPollers: 2, MaxConcurrentNexusTaskExecutionSize: 2, MaxConcurrentNexusTaskPollers: 2,
				WorkerStopTimeout: time.Second},
			Workflows: []temporal.WorkflowRegistration{{Definition: serializationWorkflow, Options: workflow.RegisterOptions{Name: "serialization-workflow"}}},
			Activities: []temporal.ActivityRegistration{{Definition: serializationEcho, Options: activity.RegisterOptions{Name: "serialization-echo"}},
				{Definition: serializationFailure, Options: activity.RegisterOptions{Name: "serialization-failure"}}},
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
		return managed
	}
	firstWorker := startWorker(old)
	id := old.prefix + "-workflow"
	t.Cleanup(func() { old.cleanupWorkflow(t, id, "") })
	run, err := old.executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "legacy-start"}, sdk.StartWorkflowOptions{ID: id, TaskQueue: old.prefix,
		WorkflowExecutionTimeout: time.Minute, StaticSummary: "versioned fixture", StaticDetails: "compatible reader acceptance", Memo: map[string]any{"initial": "legacy"}},
		"serialization-workflow", serializationInput{Value: "input", Endpoint: old.prefix})
	if err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		var ready bool
		if err := old.executions.QueryWorkflow(ctx, fault.Correlation{Call: "legacy-ready"}, id, run.GetRunID(), "ready", &ready); err != nil {
			t.Fatal(err)
		}
		releaseServiceEvidence(t, old.evidence)
		if ready {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("legacy writer did not reach its checkpoint")
		}
	}
	if err := firstWorker.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	current := serializationRuntime("2", true, store)
	current.FailureConverter = sdktemporal.NewDefaultFailureConverter(sdktemporal.DefaultFailureConverterOptions{DataConverter: current.DataConverter, EncodeCommonAttributes: true})
	next := newRuntimeExecutionServiceFixture(t, ctx, current, workflowPrefix+"GetWorkflowExecutionHistory", workflowPrefix+"DeleteWorkflowExecution", workflowPrefix+"DeleteActivityExecution")
	secondWorker := startWorker(next)
	description, err := next.executions.DescribeWorkflow(ctx, fault.Correlation{Call: "description"}, id, run.GetRunID())
	if err != nil {
		t.Fatal(err)
	}
	summary, err := description.GetStaticSummary(ctx, fault.Correlation{Call: "summary"})
	if err != nil || summary != "versioned fixture" {
		t.Fatal("legacy metadata did not decode through admitted getter", err)
	}
	var phase string
	if err := description.GetMemoValue(ctx, fault.Correlation{Call: "memo"}, "phase", &phase); err != nil || phase != "legacy-ready" {
		t.Fatal("legacy memo not retained", err)
	}
	if err := next.executions.SignalWorkflow(ctx, fault.Correlation{Call: "advance"}, id, run.GetRunID(), "advance", true); err != nil {
		t.Fatal(err)
	}
	follow, err := next.executions.GetWorkflow(id, run.GetRunID())
	if err != nil {
		t.Fatal(err)
	}
	var result string
	if err := follow.Get(ctx, fault.Correlation{Call: "upgraded-result"}, &result); err != nil || result != "input:activity:activity:nexus:retained-detail" {
		executionCauses(t, err, 0)
		t.Fatal("compatible codec/FailureConverter worker upgrade failed")
	}
	activityID := old.prefix + "-standalone"
	t.Cleanup(func() { next.cleanupActivity(t, activityID, "") })
	standalone, err := next.executions.ExecuteActivity(ctx, fault.Correlation{Call: "standalone"}, sdk.StartActivityOptions{
		ID: activityID, TaskQueue: old.prefix, StartToCloseTimeout: 5 * time.Second, ScheduleToCloseTimeout: 10 * time.Second}, "serialization-echo", "standalone")
	if err != nil {
		t.Fatal(err)
	}
	if err := standalone.Get(ctx, fault.Correlation{Call: "standalone-result"}, &result); err != nil || result != "standalone:activity" {
		t.Fatal("external standalone result failed", err)
	}
	if err := secondWorker.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	history := next.history(t, ctx, id, run.GetRunID())
	replay := func(runtime temporal.RuntimeOptions) error {
		replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{DataConverter: runtime.DataConverter, FailureConverter: runtime.FailureConverter, ExternalStorage: runtime.ExternalStorage})
		if err != nil {
			return err
		}
		replayer.RegisterWorkflowWithOptions(serializationWorkflow, workflow.RegisterOptions{Name: "serialization-workflow"})
		return replayer.ReplayWorkflowHistory(executionLogger{}, proto.Clone(history).(*historypb.History))
	}
	if err := replay(current); err != nil {
		t.Fatal("mixed-version external history replay failed", err)
	}
	incompatible := serializationRuntime("2", false, store)
	incompatible.FailureConverter = current.FailureConverter
	if err := replay(incompatible); err == nil {
		t.Fatal("incompatible historical reader was accepted")
	}
	store.mu.Lock()
	stores, reads := store.stores, store.retrieves
	store.failReads = true
	store.mu.Unlock()
	missing := replay(current)
	store.mu.Lock()
	store.failReads = false
	store.mu.Unlock()
	if missing == nil || stores == 0 || reads == 0 {
		t.Fatal("missing external payloads became empty success")
	}
	t.Log("Actual Worker restart consumed codec-v1 history with a v2 writer; Workflow/Activity/Local/Nexus/Standalone paths and encoded native failure details passed; compatible replay passed, incompatible reader and unavailable payload store were rejected")
}
