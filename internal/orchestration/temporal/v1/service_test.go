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
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type serviceConfiguration struct {
	Temporal struct {
		Endpoint  string `yaml:"grpc_endpoint"`
		Namespace string `yaml:"namespace"`
		Auth      string `yaml:"auth"`
		UIURL     string `yaml:"ui_url"`
	} `yaml:"temporal"`
}

func authorizedServiceConfiguration(t *testing.T) serviceConfiguration {
	t.Helper()
	path := os.Getenv("FATHOMRY_TEMPORAL_TEST_CONFIG")
	if path == "" {
		t.Skip("explicit isolated Temporal service configuration required")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("cannot read authorized test configuration")
	}
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	closeError := file.Close()
	if err != nil || closeError != nil || len(data) > 1<<20 {
		t.Fatal("authorized test configuration is unreadable or too large")
	}
	var config serviceConfiguration
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatal("invalid authorized test configuration")
	}
	if !strings.Contains(strings.ToLower(config.Temporal.Auth), "none") {
		t.Fatal("this service test requires the declared isolated no-auth profile")
	}
	return config
}

// This opt-in check performs only discovery/count reads against the explicitly
// supplied isolated service. It is not execution, fault-injection or UI acceptance.
func TestAuthorizedTemporalServiceRPCProfile(t *testing.T) {
	config := authorizedServiceConfiguration(t)
	selection, err := temporal.Select(temporal.OptionsV1{Name: "service-test", Endpoint: config.Temporal.Endpoint, Namespace: config.Temporal.Namespace, Plaintext: true,
		RPCs: []string{workflowPrefix + "GetSystemInfo", workflowPrefix + "DescribeNamespace", countMethod}, MaxActive: 1, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal("service profile preparation failed")
	}
	selection = resource.WithLimits(selection, resource.Limits{Active: 1, Bytes: (2 << 20) + (16 << 10), MaxLeases: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	assembly, err := resource.Assemble(ctx, ctx, "service-test", selection)
	if err != nil {
		t.Fatal("service assembly failed")
	}
	defer func() {
		if err := assembly.Close(ctx); err != nil {
			t.Error("service cleanup incomplete")
		}
	}()
	inbox, err := invocation.NewInbox[temporal.RPCResult](3, 3*1024)
	if err != nil {
		t.Fatal(err)
	}
	client, err := temporal.Bind(assembly, selection, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	info, err := client.WorkflowService(fault.Correlation{Call: "system"}).GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	if err != nil || info.GetServerVersion() != "1.32.0" {
		t.Fatal("authorized service is not the selected Server 1.32.0 profile")
	}
	namespace, err := client.WorkflowService(fault.Correlation{Call: "namespace"}).DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: config.Temporal.Namespace})
	if err != nil || namespace.GetNamespaceInfo().GetName() != config.Temporal.Namespace {
		t.Fatal("configured namespace could not be observed")
	}
	if !namespace.GetNamespaceInfo().GetCapabilities().GetStandaloneActivities() {
		t.Fatal("selected namespace does not advertise standalone activities")
	}
	_, err = client.WorkflowService(fault.Correlation{Call: "visibility"}).CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: config.Temporal.Namespace})
	if err != nil {
		t.Fatal("authorized visibility count failed")
	}
	for range 3 {
		delivery, err := inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, err := delivery.Receipt().WaitReleased(ctx)
		if err != nil || result.Err() != nil || !result.Outcome.Value.Acknowledged {
			t.Fatal("service RPC evidence incomplete")
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("Server 1.32.0, configured namespace, advertised standalone activity capability and three independently retained read RPCs verified")
}

type executionLogger struct{}

func (executionLogger) Debug(string, ...any) {}

func (executionLogger) Info(string, ...any) {}

func (executionLogger) Warn(string, ...any) {}

func (executionLogger) Error(string, ...any) {}

func executionCauses(t *testing.T, err error, depth int) {
	t.Helper()
	if err == nil || depth > 12 {
		return
	}
	t.Logf("cause type: %T", err)
	if technical, ok := err.(*fault.Error); ok {
		t.Log("technical location:", technical.Diagnostic().Context.Operation, technical.Diagnostic().Kind)
	}
	if application, ok := err.(*sdktemporal.ApplicationError); ok {
		t.Log("application type:", application.Type())
	}
	if panicError, ok := err.(*sdktemporal.PanicError); ok {
		t.Log("native panic:", panicError.Error())
	}
	if causes, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range causes.Unwrap() {
			executionCauses(t, cause, depth+1)
		}
	} else {
		executionCauses(t, errors.Unwrap(err), depth+1)
	}
}

type executionServiceFixture struct {
	namespace   string
	prefix      string
	executions  *temporal.Executions
	raw         *temporal.Client
	evidence    *invocation.Inbox[temporal.Execution]
	rawEvidence *invocation.Inbox[temporal.RPCResult]
	workers     *invocation.Inbox[temporal.WorkerResult]
	tasks       *invocation.Inbox[temporal.TaskResult]
	envelope    int64
}

func newExecutionServiceFixture(t *testing.T, ctx context.Context, methods ...string) *executionServiceFixture {
	t.Helper()
	return newRuntimeExecutionServiceFixture(t, ctx, temporal.RuntimeOptions{}, methods...)
}

func newRuntimeExecutionServiceFixture(t *testing.T, ctx context.Context, runtime temporal.RuntimeOptions, methods ...string) *executionServiceFixture {
	t.Helper()
	config := authorizedServiceConfiguration(t)
	prefix := "gh61-" + uuid.NewString()
	selection, err := temporal.SelectWithRuntime(temporal.OptionsV1{Name: "interaction-service", Endpoint: config.Temporal.Endpoint, Namespace: config.Temporal.Namespace,
		Plaintext: true, RPCs: methods, MaxActive: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20, RPCTimeout: 30 * time.Second}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	envelope := int64((2 << 20) + (16 << 10))
	selection = resource.WithLimits(selection, resource.Limits{Active: 4, Bytes: 16 * envelope, MaxLeases: 64})
	assembly, err := resource.Assemble(ctx, ctx, "interaction-service", selection)
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
	executionInbox, err := invocation.NewInbox[temporal.Execution](128, 128*temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	executions, err := temporal.BindExecutions(assembly, selection, executionInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	rpcs, err := invocation.NewInbox[temporal.RPCResult](64, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := temporal.Bind(assembly, selection, rpcs, nil)
	if err != nil {
		t.Fatal(err)
	}
	workers, tasks := workerInboxes(t)
	t.Cleanup(func() {
		releaseServiceEvidence(t, executionInbox)
		releaseServiceEvidence(t, rpcs)
		releaseServiceEvidence(t, workers)
		releaseServiceEvidence(t, tasks)
	})
	return &executionServiceFixture{namespace: config.Temporal.Namespace, prefix: prefix, executions: executions, raw: raw,
		evidence: executionInbox, rawEvidence: rpcs, workers: workers, tasks: tasks, envelope: envelope}
}

func releaseServiceEvidence[T any](t *testing.T, inbox *invocation.Inbox[T]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for inbox.Usage().Outstanding > 0 {
		delivery, err := inbox.Next(ctx)
		if err != nil {
			t.Error("evidence delivery did not finish", err)
			return
		}
		if _, err := delivery.Receipt().WaitReleased(ctx); err != nil {
			t.Error("evidence still owns a live operation", err)
			return
		}
		if err := delivery.Release(); err != nil {
			t.Error(err)
			return
		}
	}
}

func (fixture *executionServiceFixture) cleanupWorkflow(t *testing.T, workflowID, runID string) {
	t.Helper()
	cleanup, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	described, err := fixture.executions.DescribeWorkflowExecution(cleanup, fault.Correlation{Call: "cleanup-describe"}, workflowID, runID)
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		return
	}
	if err != nil {
		t.Error("cannot observe test-owned workflow before cleanup", err)
		return
	}
	execution := described.GetWorkflowExecutionInfo().GetExecution()
	if execution.GetWorkflowId() != workflowID || runID != "" && execution.GetRunId() != runID {
		t.Error("cleanup ownership identity mismatch")
		return
	}
	if described.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		if err := fixture.executions.TerminateWorkflow(cleanup, fault.Correlation{Call: "cleanup-terminate"}, sdk.TerminateWorkflowOptions{
			WorkflowID: workflowID, RunID: execution.GetRunId(), Reason: "test-owned execution cleanup"}); err != nil {
			t.Error("cannot terminate test-owned execution", err)
			return
		}
	}
	_, err = fixture.raw.WorkflowService(fault.Correlation{Call: "cleanup-delete"}).DeleteWorkflowExecution(cleanup,
		&workflowservice.DeleteWorkflowExecutionRequest{Namespace: fixture.namespace, WorkflowExecution: execution})
	if err != nil {
		t.Error("cannot delete test-owned execution", err)
		return
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := fixture.executions.DescribeWorkflowExecution(cleanup, fault.Correlation{Call: "cleanup-absence"}, workflowID, execution.GetRunId())
		receiveExecution(t, fixture.evidence)
		if errors.As(err, &missing) {
			return
		}
		if err != nil {
			t.Error("cleanup absence observation failed", err)
			return
		}
		select {
		case <-ticker.C:
		case <-cleanup.Done():
			t.Error("test-owned execution absence was not observed")
			return
		}
	}
}

func (fixture *executionServiceFixture) history(t *testing.T, ctx context.Context, workflowID, runID string) *historypb.History {
	t.Helper()
	history, err := fixture.raw.WorkflowService(fault.Correlation{Call: "history"}).GetWorkflowExecutionHistory(ctx,
		&workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: fixture.namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID}})
	if err != nil || len(history.GetNextPageToken()) != 0 {
		t.Fatal("bounded complete test history is unavailable")
	}
	encoded, err := protojson.Marshal(history.History)
	if err != nil || len(encoded) > 4<<20 {
		t.Fatal("bounded history JSON export failed", err)
	}
	imported, err := sdk.HistoryFromJSON(bytes.NewReader(encoded), sdk.HistoryJSONOptions{})
	if err != nil || !proto.Equal(imported, history.History) {
		t.Fatal("native JSON history import changed events", err)
	}
	if _, err := sdk.HistoryFromJSON(strings.NewReader("{invalid-history"), sdk.HistoryJSONOptions{}); err == nil {
		t.Fatal("malformed history import was accepted")
	}
	if len(imported.Events) > 1 {
		last := imported.Events[0].EventId
		partial, err := sdk.HistoryFromJSON(bytes.NewReader(encoded), sdk.HistoryJSONOptions{LastEventID: last})
		if err != nil || len(partial.Events) != 1 || !proto.Equal(partial.Events[0], imported.Events[0]) {
			t.Fatal("bounded partial history import changed the inclusive cutoff", err)
		}
	}
	return imported
}
