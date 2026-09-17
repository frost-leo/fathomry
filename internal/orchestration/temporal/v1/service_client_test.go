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

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
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

func TestAuthorizedLazyAndSharedClientAcquisition(t *testing.T) {
	config := authorizedServiceConfiguration(t)
	for _, target := range []struct{ name, prefix string }{{"host-port", ""}, {"dns-uri", "dns:///"}, {"passthrough-uri", "passthrough:///"}} {
		t.Run(target.name, func(t *testing.T) {
			selected := config
			selected.Temporal.Endpoint = target.prefix + config.Temporal.Endpoint
			testAuthorizedLazyAndSharedClientAcquisition(t, selected)
		})
	}
}

func testAuthorizedLazyAndSharedClientAcquisition(t *testing.T, config serviceConfiguration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const envelope = int64((2 << 20) + (16 << 10))
	parent, err := temporal.Select(temporal.OptionsV1{Name: "lazy-service", Endpoint: config.Temporal.Endpoint, Namespace: config.Temporal.Namespace,
		Plaintext: true, Lazy: true, MaxActive: 2, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20, RPCTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	parent = resource.WithLimits(parent, resource.Limits{Active: 2, Bytes: 4 * envelope, MaxLeases: 16})
	assembly, err := resource.Assemble(ctx, ctx, "lazy-service", parent)
	if assembly != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			if err := assembly.Close(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[temporal.RPCResult](8, 8*1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseServiceEvidence(t, inbox) })
	base, err := temporal.Bind(assembly, parent, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if base.Profile().ServiceVersion.Kind != compatibility.UnknownFact {
		t.Fatal("lazy acquisition invented service discovery")
	}
	plugin := &controlledClientPlugin{create: func(_ context.Context, options sdk.PluginNewClientOptions, next func(context.Context, sdk.PluginNewClientOptions) error) error {
		return next(context.Background(), options)
	}}
	selected, err := temporal.SelectFromExisting(temporal.OptionsV1{Name: "shared-service", Namespace: config.Temporal.Namespace, MaxActive: 1, RPCs: []string{countMethod}},
		temporal.RuntimeOptions{Plugins: []sdk.Plugin{plugin}}, base, 2*envelope)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, resource.Limits{Active: 1, Bytes: 2 * envelope, MaxLeases: 8})
	shared, err := resource.Assemble(ctx, ctx, "shared-service", selected)
	if shared != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			if err := shared.Close(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	client, err := temporal.Bind(shared, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fact := client.Profile().ServiceVersion; fact.Kind != compatibility.Observed || fact.Value != "1.32.0" {
		t.Fatal("shared acquisition did not record actual discovery")
	}
	query := "WorkflowId = 'gh61-absent-" + uuid.NewString() + "'"
	for _, phase := range []string{"before-close", "after-close-request"} {
		if phase == "after-close-request" {
			if err := assembly.Close(ctx); !errors.Is(err, resource.ErrIncomplete) {
				t.Fatal("parent did not retain shared source", err)
			}
		}
		count, err := client.WorkflowService(fault.Correlation{Call: phase}).CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: config.Temporal.Namespace, Query: query})
		if err != nil || count.GetCount() != 0 {
			t.Fatal("shared live-service control failed", err)
		}
	}
	if err := shared.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log("Actual lazy discovery, plugin-replaced context, native shared transport, parent retention and final release passed; no service resources were created")
}

type executionInput struct{ Text, Endpoint string }

type nexusInput struct{ Text, WorkflowID, RunID string }

func executionWorkflow(ctx workflow.Context, input executionInput) (string, error) {
	workflow.GetLogger(ctx).Info("gh61.workflow", "marker", "workflow")
	recordExecutionMetrics(workflow.GetMetricsHandler(ctx), "gh61_workflow", ctx.Value(nativeContextKey{}))
	if err := workflow.SetQueryHandler(ctx, "state", func() (string, error) { return "ready", nil }); err != nil {
		return "", err
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Second, RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	var remote string
	if err := workflow.ExecuteActivity(ctx, "execution-remote", input.Text).Get(ctx, &remote); err != nil {
		return "", err
	}
	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: 5 * time.Second, RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	var local string
	if err := workflow.ExecuteLocalActivity(ctx, "execution-local", remote).Get(ctx, &local); err != nil {
		return "", err
	}
	info := workflow.GetInfo(ctx)
	var result string
	err := workflow.NewNexusClient(input.Endpoint, "execution-service").ExecuteOperation(ctx, "echo",
		nexusInput{Text: local, WorkflowID: info.WorkflowExecution.ID, RunID: info.WorkflowExecution.RunID},
		workflow.NexusOperationOptions{ScheduleToCloseTimeout: 10 * time.Second}).Get(ctx, &result)
	return result, err
}

func executionRemote(ctx context.Context, input string) (string, error) {
	activity.GetLogger(ctx).Info("gh61.activity", "marker", "remote")
	recordExecutionMetrics(activity.GetMetricsHandler(ctx), "gh61_activity", ctx.Value(nativeContextKey{}))
	info := activity.GetInfo(ctx)
	value, err := activity.GetClient(ctx).QueryWorkflow(ctx, info.WorkflowExecution.ID, info.WorkflowExecution.RunID, "state")
	if err != nil {
		return "", err
	}
	var state string
	if err := value.Get(&state); err != nil {
		return "", err
	}
	if state != "ready" {
		return "", errors.New("unexpected workflow query result")
	}
	return input + ":remote", nil
}

func executionLocal(ctx context.Context, input string) (string, error) {
	message := "gh61.standalone"
	if activity.GetInfo(ctx).IsLocalActivity {
		message = "gh61.local"
	}
	activity.GetLogger(ctx).Info(message, "marker", "local-function")
	metric := "gh61_standalone"
	if activity.GetInfo(ctx).IsLocalActivity {
		metric = "gh61_local"
	}
	recordExecutionMetrics(activity.GetMetricsHandler(ctx), metric, ctx.Value(nativeContextKey{}))
	return input + ":local", nil
}

func TestAuthorizedManagedExecutionChain(t *testing.T) {
	config := authorizedServiceConfiguration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, nativeContextKey{}, "service-context")
	prefix := "gh61-" + uuid.NewString()
	operatorPrefix := "/temporal.api.operatorservice.v1.OperatorService/"
	methods := []string{operatorPrefix + "CreateNexusEndpoint", operatorPrefix + "GetNexusEndpoint", operatorPrefix + "DeleteNexusEndpoint",
		workflowPrefix + "GetWorkflowExecutionHistory", workflowPrefix + "DeleteWorkflowExecution", workflowPrefix + "DeleteActivityExecution", workflowPrefix + "DescribeWorkflowExecution"}
	logger := newNativeTestLogger()
	metrics := newNativeTestMetrics()
	tracer := &nativeTestTracer{}
	tracing := interceptor.NewTracingInterceptor(tracer)
	propagator := &nativeTestPropagator{}
	selection, err := temporal.SelectWithRuntime(temporal.OptionsV1{Name: "execution-service", Endpoint: config.Temporal.Endpoint, Namespace: config.Temporal.Namespace,
		Plaintext: true, RPCs: methods, MaxActive: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20, RPCTimeout: 30 * time.Second}, temporal.RuntimeOptions{Logger: logger, MetricsHandler: metrics,
		Interceptors: []interceptor.ClientInterceptor{tracing}, ContextPropagators: []workflow.ContextPropagator{propagator}})
	if err != nil {
		t.Fatal(err)
	}
	envelope := int64((2 << 20) + (16 << 10))
	selection = resource.WithLimits(selection, resource.Limits{Active: 4, Bytes: 16 * envelope, MaxLeases: 64})
	assembly, err := resource.Assemble(ctx, ctx, "execution-service", selection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := assembly.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	rpcs, err := invocation.NewInbox[temporal.RPCResult](32, 32*1024)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := temporal.Bind(assembly, selection, rpcs, nil)
	if err != nil {
		t.Fatal(err)
	}
	executionInbox, err := invocation.NewInbox[temporal.Execution](32, 32*temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	executions, err := temporal.BindExecutions(assembly, selection, executionInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	workers, err := invocation.NewInbox[temporal.WorkerResult](1, temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := invocation.NewInbox[temporal.TaskResult](16, 16*temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	created, err := raw.OperatorService(fault.Correlation{Call: "endpoint-create"}).CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{Name: prefix, Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{Namespace: config.Temporal.Namespace, TaskQueue: prefix}}}}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := created.GetEndpoint()
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		current, err := raw.OperatorService(fault.Correlation{Call: "endpoint-check"}).GetNexusEndpoint(cleanup, &operatorservice.GetNexusEndpointRequest{Id: endpoint.Id})
		if err != nil {
			t.Error(err)
			return
		}
		if current.GetEndpoint().GetSpec().GetName() != prefix {
			t.Error("endpoint ownership changed")
			return
		}
		_, err = raw.OperatorService(fault.Correlation{Call: "endpoint-delete"}).DeleteNexusEndpoint(cleanup, &operatorservice.DeleteNexusEndpointRequest{Id: endpoint.Id, Version: current.Endpoint.Version})
		if err != nil {
			t.Error(err)
		}
		_, err = raw.OperatorService(fault.Correlation{Call: "endpoint-absent"}).GetNexusEndpoint(cleanup, &operatorservice.GetNexusEndpointRequest{Id: endpoint.Id})
		var absent *serviceerror.NotFound
		if !errors.As(err, &absent) {
			t.Error("test-owned endpoint absence not observed")
		}
	})
	service := nexus.NewService("execution-service")
	err = service.Register(nexus.NewSyncOperation("echo", func(ctx context.Context, input nexusInput, _ nexus.StartOperationOptions) (string, error) {
		temporalnexus.GetLogger(ctx).Info("gh61.nexus", "marker", "nexus")
		recordExecutionMetrics(temporalnexus.GetMetricsHandler(ctx), "gh61_nexus", ctx.Value(nativeContextKey{}))
		encoded, err := temporalnexus.GetClient(ctx).QueryWorkflow(ctx, input.WorkflowID, input.RunID, "state")
		if err != nil {
			return "", err
		}
		var state string
		if err := encoded.Get(&state); err != nil {
			return "", err
		}
		if state != "ready" {
			return "", errors.New("unexpected Nexus workflow query")
		}
		return input.Text + ":nexus", nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	lifetime, stopLifetime := context.WithCancel(context.Background())
	t.Cleanup(stopLifetime)
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "worker"},
		temporal.WorkerSpec{TaskQueue: prefix, MaxHandlers: 4, Bytes: 4 * envelope,
			Options: worker.Options{MaxConcurrentWorkflowTaskExecutionSize: 2, MaxConcurrentWorkflowTaskPollers: 2,
				MaxConcurrentActivityExecutionSize: 2, MaxConcurrentActivityTaskPollers: 2, MaxConcurrentLocalActivityExecutionSize: 2,
				MaxConcurrentNexusTaskExecutionSize: 2, MaxConcurrentNexusTaskPollers: 2, WorkerStopTimeout: time.Second},
			Workflows: []temporal.WorkflowRegistration{{Definition: executionWorkflow, Options: workflow.RegisterOptions{Name: "execution-flow"}}},
			Activities: []temporal.ActivityRegistration{
				{Definition: executionRemote, Options: activity.RegisterOptions{Name: "execution-remote"}},
				{Definition: executionLocal, Options: activity.RegisterOptions{Name: "execution-local"}},
			}, NexusServices: []*nexus.Service{service}}, workers, tasks)
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
	run, err := executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "start"}, sdk.StartWorkflowOptions{ID: prefix, TaskQueue: prefix,
		WorkflowExecutionTimeout: 30 * time.Second, WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE},
		"execution-flow", executionInput{Text: "input", Endpoint: prefix})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = executions.TerminateWorkflow(cleanup, fault.Correlation{Call: "cleanup-terminate"}, sdk.TerminateWorkflowOptions{WorkflowID: prefix, RunID: run.GetRunID(), Reason: "test cleanup"})
		_, err := raw.WorkflowService(fault.Correlation{Call: "workflow-delete"}).DeleteWorkflowExecution(cleanup, &workflowservice.DeleteWorkflowExecutionRequest{Namespace: config.Temporal.Namespace, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: prefix, RunId: run.GetRunID()}})
		if err != nil {
			t.Error(err)
		}
	})
	var result string
	if err := run.Get(ctx, fault.Correlation{Call: "result"}, &result); err != nil {
		executionCauses(t, err, 0)
		t.Fatal(err)
	}
	if result != "input:remote:local:nexus" {
		t.Fatal("native execution chain produced the wrong result")
	}
	standalone, err := executions.ExecuteActivity(ctx, fault.Correlation{Call: "standalone-start"}, sdk.StartActivityOptions{ID: prefix + "-activity", TaskQueue: prefix,
		ScheduleToCloseTimeout: 15 * time.Second, StartToCloseTimeout: 5 * time.Second, RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}}, "execution-local", "standalone")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, err := raw.WorkflowService(fault.Correlation{Call: "activity-delete"}).DeleteActivityExecution(cleanup, &workflowservice.DeleteActivityExecutionRequest{Namespace: config.Temporal.Namespace, ActivityId: standalone.GetID(), RunId: standalone.GetRunID()})
		if err != nil {
			t.Error(err)
		}
	})
	if err := standalone.Get(ctx, fault.Correlation{Call: "standalone-result"}, &result); err != nil {
		t.Fatal(err)
	}
	if result != "standalone:local" {
		t.Fatal("standalone activity result mismatch")
	}
	history, err := raw.WorkflowService(fault.Correlation{Call: "history"}).GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: config.Temporal.Namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: prefix, RunId: run.GetRunID()}})
	if err != nil || len(history.GetNextPageToken()) != 0 {
		t.Fatal("exact bounded test history unavailable")
	}
	logged := map[string]int{}
	for _, entry := range logger.entries() {
		logged[entry.message]++
	}
	for _, message := range []string{"gh61.workflow", "gh61.activity", "gh61.local", "gh61.standalone", "gh61.nexus"} {
		if logged[message] != 1 {
			t.Fatalf("native logger did not receive exactly one execution marker: %s", message)
		}
	}
	// The native replayer uses a no-op metrics handler. The separate live-worker
	// restart test verifies metric suppression; this replays tracing/headers.
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{ContextPropagators: []workflow.ContextPropagator{propagator}, Interceptors: []interceptor.WorkerInterceptor{tracing}})
	if err != nil {
		t.Fatal(err)
	}
	replayer.RegisterWorkflowWithOptions(executionWorkflow, workflow.RegisterOptions{Name: "execution-flow"})
	if err := replayer.ReplayWorkflowHistory(logger, history.History); err != nil {
		t.Fatal(err)
	}
	countWorkflowLogs := func() int {
		count := 0
		for _, entry := range logger.entries() {
			if entry.message == "gh61.workflow" {
				count++
			}
		}
		return count
	}
	if countWorkflowLogs() != 1 {
		t.Fatal("native default replay suppression was bypassed")
	}
	replayLogging, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{EnableLoggingInReplay: true, ContextPropagators: []workflow.ContextPropagator{propagator}, Interceptors: []interceptor.WorkerInterceptor{tracing}})
	if err != nil {
		t.Fatal(err)
	}
	replayLogging.RegisterWorkflowWithOptions(executionWorkflow, workflow.RegisterOptions{Name: "execution-flow"})
	if err := replayLogging.ReplayWorkflowHistory(logger, history.History); err != nil {
		t.Fatal(err)
	}
	if countWorkflowLogs() != 2 || logger.state.overflow.Load() {
		t.Fatal("explicit native replay logging control did not produce its marker")
	}
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	workerRecord, err := workers.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	workerResult, err := workerRecord.Receipt().WaitReleased(ctx)
	if err != nil || workerResult.Err() != nil || !workerResult.Outcome.Value.Started || !workerResult.Outcome.Value.Joined {
		t.Fatal("worker completion evidence missing")
	}
	if err := workerRecord.Release(); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for range 4 {
		delivery, err := tasks.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, err := delivery.Receipt().WaitReleased(ctx)
		if err != nil || result.Err() != nil || !result.Outcome.Value.HandlerReturned {
			t.Fatal("handler evidence missing")
		}
		value := result.Outcome.Value
		switch {
		case value.Kind == "nexus-start":
			seen["nexus"] = true
		case value.Local:
			seen["local"] = true
		case value.WorkflowID == "":
			seen["standalone"] = true
			if value.ActivityRunID != standalone.GetRunID() {
				t.Fatal("standalone identity lost")
			}
		default:
			seen["remote"] = true
		}
		if value.Namespace != config.Temporal.Namespace {
			t.Fatal("task namespace changed")
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []string{"remote", "local", "nexus", "standalone"} {
		if !seen[kind] {
			t.Fatalf("missing task evidence: %s", kind)
		}
	}
	if logger.state.closes.Load() != 0 || logger.state.syncs.Load() != 0 {
		t.Fatal("Temporal took ownership of the native logger")
	}
	metricKinds := map[string]map[string]bool{}
	for _, reading := range metrics.readings() {
		if reading.name == "temporal_request" {
			continue
		}
		if metricKinds[reading.name] == nil {
			metricKinds[reading.name] = map[string]bool{}
		}
		metricKinds[reading.name][reading.kind] = true
		if reading.tags["namespace"] != config.Temporal.Namespace {
			t.Fatal("native metric namespace was lost")
		}
		// Native Temporal payload propagators do not apply to Nexus HTTP headers;
		// the SDK tracing interceptor propagates its Nexus header independently.
		if reading.name != "gh61_nexus" && reading.tags["propagated"] != "service-context" {
			t.Fatalf("native context was lost for %s", reading.name)
		}
		switch reading.kind {
		case "counter":
			if reading.value != int64(3) {
				t.Fatal("counter changed")
			}
		case "gauge":
			if reading.value != 7.5 {
				t.Fatal("gauge changed")
			}
		case "timer":
			if reading.value != 125*time.Millisecond {
				t.Fatal("timer units changed")
			}
		}
	}
	for _, name := range []string{"gh61_workflow", "gh61_activity", "gh61_local", "gh61_standalone", "gh61_nexus"} {
		if len(metricKinds[name]) != 3 {
			t.Fatalf("missing native metric kinds: %s", name)
		}
	}
	operations := map[string]bool{}
	for _, span := range tracer.observations() {
		if !span.finished {
			t.Fatalf("native span was not finished: %s", span.operation)
		}
		operations[span.operation] = true
		if (span.operation == "RunActivity" || span.operation == "RunWorkflow" || span.operation == "RunStartNexusOperationHandler") && span.parent == "" {
			t.Fatalf("native span parent missing: %s", span.operation)
		}
	}
	for _, operation := range []string{"StartWorkflow", "RunWorkflow", "StartActivity", "RunActivity", "StartNexusOperation", "RunStartNexusOperationHandler"} {
		if !operations[operation] {
			t.Fatalf("missing native tracing operation: %s", operation)
		}
	}
	t.Log("Formal five-path execution chain preserved native logging, counter/gauge/timer values, context headers and tracing parent/finish behavior; replay controls passed")
}
