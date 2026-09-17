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
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"go.temporal.io/api/serviceerror"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func deploymentWorkflow(ctx workflow.Context) (string, error) {
	if err := workflow.SetQueryHandler(ctx, "build", func() (string, error) { return workflow.GetInfo(ctx).GetCurrentBuildID(), nil }); err != nil {
		return "", err
	}
	var finish bool
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, &finish)
	return workflow.GetInfo(ctx).GetCurrentBuildID(), nil
}

func TestAuthorizedWorkerDeploymentRoutingAndConflictTokens(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	methods := []string{"DescribeWorkerDeployment", "SetWorkerDeploymentCurrentVersion", "SetWorkerDeploymentRampingVersion", "SetWorkerDeploymentManager",
		"DescribeWorkerDeploymentVersion", "DeleteWorkerDeploymentVersion", "UpdateWorkerDeploymentVersionMetadata", "ListWorkerDeployments", "DeleteWorkerDeployment",
		"DeleteWorkflowExecution", "GetWorkflowExecutionHistory"}
	for index := range methods {
		methods[index] = workflowPrefix + methods[index]
	}
	fixture := newExecutionServiceFixture(t, ctx, methods...)
	t.Log("test-owned deployment:", fixture.prefix)
	handle, err := fixture.executions.GetWorkerDeployment(fixture.prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
		defer cancel()
		description, err := handle.Describe(cleanup, fault.Correlation{Call: "deployment-cleanup-describe"}, sdk.WorkerDeploymentDescribeOptions{})
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			return
		}
		if err != nil || description.Info.Name != fixture.prefix {
			t.Error("test deployment ownership unavailable")
			return
		}
		if _, err := handle.SetManagerIdentity(cleanup, fault.Correlation{Call: "deployment-clear-manager"}, sdk.WorkerDeploymentSetManagerIdentityOptions{}); err != nil {
			t.Error(err)
			return
		}
		if _, err := handle.SetCurrentVersion(cleanup, fault.Correlation{Call: "deployment-clear-current"}, sdk.WorkerDeploymentSetCurrentVersionOptions{}); err != nil {
			t.Error(err)
			return
		}
		if _, err := handle.SetRampingVersion(cleanup, fault.Correlation{Call: "deployment-clear-ramp"}, sdk.WorkerDeploymentSetRampingVersionOptions{}); err != nil {
			t.Error(err)
			return
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for _, build := range []string{"A", "B"} {
			for {
				_, err := handle.DeleteVersion(cleanup, fault.Correlation{Call: "deployment-delete-version"}, sdk.WorkerDeploymentDeleteVersionOptions{BuildID: build, SkipDrainage: true})
				releaseServiceEvidence(t, fixture.evidence)
				if err == nil || errors.As(err, &missing) {
					break
				}
				var pending *serviceerror.FailedPrecondition
				if !errors.As(err, &pending) {
					t.Error("owned version cleanup failed", err)
					return
				}
				select {
				case <-ticker.C:
				case <-cleanup.Done():
					t.Error("test version still has native poller ownership")
					return
				}
			}
		}
		if _, err := fixture.executions.DeleteWorkerDeployment(cleanup, fault.Correlation{Call: "deployment-delete"}, sdk.WorkerDeploymentDeleteOptions{Name: fixture.prefix}); err != nil {
			t.Error(err)
			return
		}
		_, err = handle.Describe(cleanup, fault.Correlation{Call: "deployment-absence"}, sdk.WorkerDeploymentDescribeOptions{})
		if !errors.As(err, &missing) {
			t.Error("deployment absence was not observed")
		}
	})
	startWorker := func(build string) *temporal.Worker {
		t.Helper()
		results, tasks := workerInboxes(t)
		t.Cleanup(func() { releaseServiceEvidence(t, results); releaseServiceEvidence(t, tasks) })
		lifetime, stopLifetime := context.WithCancel(context.Background())
		t.Cleanup(stopLifetime)
		managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "deployment-worker-" + build}, temporal.WorkerSpec{
			TaskQueue: fixture.prefix, MaxHandlers: 2, Bytes: 2 * fixture.envelope,
			Options: worker.Options{LocalActivityWorkerOnly: true, MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2,
				DeploymentOptions: worker.DeploymentOptions{UseVersioning: true, Version: worker.WorkerDeploymentVersion{DeploymentName: fixture.prefix, BuildID: build}}},
			Workflows: []temporal.WorkflowRegistration{
				{Definition: deploymentWorkflow, Options: workflow.RegisterOptions{Name: "deployment-pinned", VersioningBehavior: workflow.VersioningBehaviorPinned}},
				{Definition: deploymentWorkflow, Options: workflow.RegisterOptions{Name: "deployment-auto", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade}},
			},
		}, results, tasks)
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
	startWorker("A")
	startWorker("B")
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var description sdk.WorkerDeploymentDescribeResponse
	for {
		description, err = handle.Describe(ctx, fault.Correlation{Call: "deployment-ready"}, sdk.WorkerDeploymentDescribeOptions{})
		releaseServiceEvidence(t, fixture.evidence)
		if err == nil && len(description.Info.VersionSummaries) == 2 {
			break
		}
		var missing *serviceerror.NotFound
		if err != nil && !errors.As(err, &missing) {
			t.Fatal(err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("deployment versions did not register")
		}
	}
	stale := append([]byte(nil), description.ConflictToken...)
	current, err := handle.SetCurrentVersion(ctx, fault.Correlation{Call: "current-A"}, sdk.WorkerDeploymentSetCurrentVersionOptions{BuildID: "A", ConflictToken: stale})
	if err != nil {
		t.Fatal(err)
	}
	_, err = handle.SetCurrentVersion(ctx, fault.Correlation{Call: "stale-token"}, sdk.WorkerDeploymentSetCurrentVersionOptions{BuildID: "B", ConflictToken: stale})
	var conflict *serviceerror.FailedPrecondition
	if !errors.As(err, &conflict) {
		t.Fatal("stale native conflict token was not rejected", err)
	}
	start := func(name string) *temporal.WorkflowRun {
		t.Helper()
		id := fixture.prefix + "-" + name
		t.Cleanup(func() { fixture.cleanupWorkflow(t, id, "") })
		run, err := fixture.executions.ExecuteWorkflow(ctx, fault.Correlation{Call: name}, sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: 90 * time.Second}, name)
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	pinned, upgrading := start("deployment-pinned"), start("deployment-auto")
	queryBuild := func(run *temporal.WorkflowRun, expected string) {
		t.Helper()
		var build string
		if err := fixture.executions.QueryWorkflow(ctx, fault.Correlation{Call: "build"}, run.GetID(), run.GetRunID(), "build", &build); err != nil || build != expected {
			t.Fatalf("execution route=%q want=%q error=%v", build, expected, err)
		}
	}
	queryBuild(pinned, "A")
	queryBuild(upgrading, "A")
	_, err = handle.SetRampingVersion(ctx, fault.Correlation{Call: "ramp-B"}, sdk.WorkerDeploymentSetRampingVersionOptions{BuildID: "B", Percentage: 50, ConflictToken: current.ConflictToken})
	if err != nil {
		t.Fatal(err)
	}
	description, err = handle.Describe(ctx, fault.Correlation{Call: "routing"}, sdk.WorkerDeploymentDescribeOptions{})
	if err != nil || description.Info.RoutingConfig.RampingVersion == nil || description.Info.RoutingConfig.RampingVersion.BuildID != "B" || description.Info.RoutingConfig.RampingVersionPercentage != 50 {
		t.Fatal("native ramp state not observed", err)
	}
	manager, err := handle.SetManagerIdentity(ctx, fault.Correlation{Call: "manager"}, sdk.WorkerDeploymentSetManagerIdentityOptions{ManagerIdentity: "fixture-manager", ConflictToken: description.ConflictToken})
	if err != nil {
		t.Fatal(err)
	}
	_, err = handle.SetCurrentVersion(ctx, fault.Correlation{Call: "wrong-manager"}, sdk.WorkerDeploymentSetCurrentVersionOptions{BuildID: "B", Identity: "wrong-manager", ConflictToken: manager.ConflictToken})
	if !errors.As(err, &conflict) {
		t.Fatal("native manager identity was not enforced", err)
	}
	if _, err := handle.SetCurrentVersion(ctx, fault.Correlation{Call: "current-B"}, sdk.WorkerDeploymentSetCurrentVersionOptions{BuildID: "B", Identity: "fixture-manager", ConflictToken: manager.ConflictToken}); err != nil {
		t.Fatal(err)
	}
	for {
		if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "route-wakeup"}, upgrading.GetID(), upgrading.GetRunID(), "routing-checkpoint", true); err != nil {
			t.Fatal(err)
		}
		var build string
		if err := fixture.executions.QueryWorkflow(ctx, fault.Correlation{Call: "route-observe"}, upgrading.GetID(), upgrading.GetRunID(), "build", &build); err != nil {
			t.Fatal(err)
		}
		releaseServiceEvidence(t, fixture.evidence)
		if build == "B" {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("auto-upgrade never observed the current version")
		}
	}
	queryBuild(pinned, "A")
	metadata, err := handle.UpdateVersionMetadata(ctx, fault.Correlation{Call: "version-metadata"}, sdk.WorkerDeploymentUpdateVersionMetadataOptions{
		Version:        worker.WorkerDeploymentVersion{DeploymentName: fixture.prefix, BuildID: "B"},
		MetadataUpdate: sdk.WorkerDeploymentMetadataUpdate{UpsertEntries: map[string]any{"fixture": "metadata-value"}}})
	if err != nil {
		t.Fatal(err)
	}
	var metadataValue string
	if err := converter.GetDefaultDataConverter().FromPayload(metadata.Metadata["fixture"], &metadataValue); err != nil || metadataValue != "metadata-value" {
		t.Fatal("version metadata lost", err)
	}
	if _, err := handle.DeleteVersion(ctx, fault.Correlation{Call: "active-delete"}, sdk.WorkerDeploymentDeleteVersionOptions{BuildID: "B", SkipDrainage: true}); err == nil {
		t.Fatal("current version was deleted")
	}
	for _, run := range []*temporal.WorkflowRun{pinned, upgrading} {
		if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "finish"}, run.GetID(), run.GetRunID(), "finish", true); err != nil {
			t.Fatal(err)
		}
		var build string
		if err := run.Get(ctx, fault.Correlation{Call: "result"}, &build); err != nil {
			t.Fatal(err)
		}
		expected := "B"
		if run == pinned {
			expected = "A"
		}
		if build != expected {
			t.Fatalf("completion route=%q want=%q", build, expected)
		}
		history := fixture.history(t, ctx, run.GetID(), run.GetRunID())
		replayer := worker.NewWorkflowReplayer()
		name := "deployment-auto"
		if run == pinned {
			name = "deployment-pinned"
		}
		replayer.RegisterWorkflowWithOptions(deploymentWorkflow, workflow.RegisterOptions{Name: name})
		if err := replayer.ReplayWorkflowHistoryWithOptions(executionLogger{}, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: run.GetID(), RunID: run.GetRunID()}}); err != nil {
			t.Fatal("deployment history replay failed", err)
		}
	}
	found := false
	if err := fixture.executions.WalkWorkerDeployments(ctx, fault.Correlation{Call: "deployment-list"}, sdk.WorkerDeploymentListOptions{PageSize: 1}, func(_ context.Context, entry *sdk.WorkerDeploymentListEntry) error {
		found = found || entry.Name == fixture.prefix
		return nil
	}); err != nil || !found {
		t.Fatal("native deployment list missed the owned deployment", err)
	}
	t.Log("Actual current/ramping version routing, pinned vs auto-upgrade, conflict/manager rejection, metadata, active-delete refusal and replay passed")
}
