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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
)

type testServer struct {
	workflowservice.UnimplementedWorkflowServiceServer
	starts, signals, histories atomic.Int32
}

func (*testServer) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{ServerVersion: "1.32.0", Capabilities: &workflowservice.GetSystemInfoResponse_Capabilities{}}, nil
}
func (server *testServer) StartWorkflowExecution(context.Context, *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
	server.starts.Add(1)
	return &workflowservice.StartWorkflowExecutionResponse{RunId: "run"}, nil
}
func (server *testServer) SignalWorkflowExecution(context.Context, *workflowservice.SignalWorkflowExecutionRequest) (*workflowservice.SignalWorkflowExecutionResponse, error) {
	server.signals.Add(1)
	return &workflowservice.SignalWorkflowExecutionResponse{}, nil
}
func (server *testServer) GetWorkflowExecutionHistory(context.Context, *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	server.histories.Add(1)
	payload, _ := converter.GetDefaultDataConverter().ToPayloads("result")
	return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{
		EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{Result: payload}},
	}}}}, nil
}

type testFixture struct {
	owner        *Owner
	server       *testServer
	dependencies Dependencies
	ctx          context.Context
}

func newTestFixture(t *testing.T, options NativeOptions, configure func(*Settings, *Policy)) testFixture {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &testServer{}
	grpcServer := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(grpcServer, server)
	go grpcServer.Serve(listener)
	t.Cleanup(grpcServer.Stop)
	settings := Settings{Name: "fixture", Endpoint: listener.Addr().String(), Namespace: "test", Plaintext: true,
		MaxActive: 2, InnerEvidenceCapacity: 4, MaxRequestBytes: 1024, MaxResponseBytes: 1024}
	if configure != nil {
		configure(&settings, nil)
	}
	prepared, err := Prepare(settings, options)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	if configure != nil {
		configure(nil, &policy)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	workers, err := adapters.NewInbox[WorkerResult](policy.Workers)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := adapters.NewInbox[TaskResult](policy.Tasks)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := Dependencies{Runtime: runtime, Evidence: evidence, Workers: workers, Tasks: tasks, Native: options}
	owner, err := prepared.Open(ctx, dependencies)
	if err != nil {
		if owner != nil {
			_ = owner.Close(context.Background())
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		timeout, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := owner.Close(timeout); err != nil {
			t.Error("source cleanup", err)
		}
		if err := runtime.Close(timeout); err != nil {
			t.Error("runtime cleanup", err)
		}
	})
	return testFixture{owner: owner, server: server, dependencies: dependencies, ctx: ctx}
}

func TestSourceEvidenceBeforeNativeAndRedeliveryDoesNotResend(t *testing.T) {
	fixture := newTestFixture(t, NativeOptions{}, nil)
	client := fixture.owner.Client()
	for index := 0; index < 4; index++ {
		if err := client.SignalWorkflow(fixture.ctx, "workflow", "run", "signal", index); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.SignalWorkflow(fixture.ctx, "workflow", "run", "signal", 5); !errors.Is(err, adapters.ErrEvidence) {
		t.Fatal("full public receiver did not refuse", err)
	}
	if fixture.server.signals.Load() != 4 {
		t.Fatal("refused call reached native service")
	}
	wait, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	record, err := fixture.dependencies.Evidence.NextReleased(wait)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := record.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(wait)
	if err != nil {
		t.Fatal(err)
	}
	value, present := snapshot.ValueCopy()
	if !present || !value.Execution.Accepted || value.Execution.Operation != "workflow.signal" {
		t.Fatal("evidence mismatch")
	}
	if err := record.Retry(); err != nil {
		t.Fatal(err)
	}
	next, err := fixture.dependencies.Evidence.NextReleased(wait)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Ack(); err != nil {
		t.Fatal(err)
	}
	if fixture.server.signals.Load() != 4 {
		t.Fatal("record retry resent business operation")
	}
}

func TestRetainedUseOriginAndClosedResult(t *testing.T) {
	fixture := newTestFixture(t, NativeOptions{}, nil)
	first, err := fixture.owner.Client().Borrow(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.owner.Client().Borrow(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Attribution().SourceID == "" || first.Attribution().SourceID != second.Attribution().SourceID || first.Attribution().UseID == 0 || first.Attribution().UseID == second.Attribution().UseID {
		t.Fatal("independent source/use evidence identity missing")
	}
	copy, err := first.WithID("correlation-view")
	if err != nil || copy.Attribution() != first.Attribution() {
		t.Fatal("facade copy changed use identity", err)
	}
	intent, err := first.NewWithStartWorkflowOperation(sdk.StartWorkflowOptions{ID: "workflow", TaskQueue: "queue",
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING}, "workflow")
	if err != nil {
		t.Fatal(err)
	}
	run, err := first.ExecuteWorkflow(fixture.ctx, sdk.StartWorkflowOptions{ID: "workflow", TaskQueue: "queue"}, "workflow")
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := second.UpdateWithStartWorkflow(fixture.ctx, intent, sdk.UpdateWorkflowOptions{UpdateName: "update", WaitForStage: sdk.WorkflowUpdateStageCompleted}); !errors.Is(err, ErrAuthority) {
		t.Fatal("different use consumed old intention", err)
	}
	var output string
	if err := run.Get(fixture.ctx, &output); err == nil {
		t.Fatal("closed use performed delayed decode")
	}
	if fixture.server.histories.Load() != 0 {
		t.Fatal("closed result reached native retrieval")
	}
	if err := second.SignalWorkflow(fixture.ctx, "workflow", "run", "signal", nil); err != nil {
		t.Fatal("closing peer affected live use", err)
	}
}

type blockingConverter struct {
	converter.DataConverter
	encodeEntered, decodeEntered chan struct{}
	encodeRelease, decodeRelease chan struct{}
	encodes                      atomic.Int32
}

func (value *blockingConverter) ToPayloads(args ...any) (*commonpb.Payloads, error) {
	value.encodes.Add(1)
	if value.encodeEntered != nil {
		select {
		case value.encodeEntered <- struct{}{}:
		default:
		}
		<-value.encodeRelease
	}
	return value.DataConverter.ToPayloads(args...)
}
func (value *blockingConverter) FromPayloads(payload *commonpb.Payloads, args ...any) error {
	if value.decodeEntered != nil {
		select {
		case value.decodeEntered <- struct{}{}:
		default:
		}
		<-value.decodeRelease
	}
	return value.DataConverter.FromPayloads(payload, args...)
}

func TestInnerEvidenceSaturatesIndependentlyBeforeConversion(t *testing.T) {
	converter := &blockingConverter{DataConverter: converter.GetDefaultDataConverter(), encodeEntered: make(chan struct{}, 1), encodeRelease: make(chan struct{})}
	fixture := newTestFixture(t, NativeOptions{DataConverter: converter}, func(settings *Settings, policy *Policy) {
		if settings != nil {
			settings.InnerEvidenceCapacity = 1
		}
		if policy != nil {
			policy.Evidence.Capacity = 4
			policy.Evidence.MaxBytes = 4 * publicRecordBytes
		}
	})
	done := make(chan error, 1)
	go func() {
		done <- fixture.owner.Client().SignalWorkflow(fixture.ctx, "workflow", "run", "signal", "first")
	}()
	select {
	case <-converter.encodeEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("native conversion did not enter")
	}
	err := fixture.owner.Client().SignalWorkflow(fixture.ctx, "workflow", "run", "signal", "second")
	if !errors.Is(err, adapters.ErrEvidence) {
		t.Error("inner receiver did not refuse", err)
	}
	if converter.encodes.Load() != 1 {
		t.Error("refused call entered converter")
	}
	close(converter.encodeRelease)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCloseWaitCancellationRetainsActualDecoder(t *testing.T) {
	converter := &blockingConverter{DataConverter: converter.GetDefaultDataConverter(), decodeEntered: make(chan struct{}, 1), decodeRelease: make(chan struct{})}
	fixture := newTestFixture(t, NativeOptions{DataConverter: converter}, nil)
	run, err := fixture.owner.Client().GetWorkflow("workflow", "run")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { var output string; done <- run.Get(fixture.ctx, &output) }()
	select {
	case <-converter.decodeEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("decoder did not enter")
	}
	wait, cancel := context.WithCancel(context.Background())
	cancel()
	if err := fixture.owner.Close(wait); err == nil {
		t.Error("canceled cleanup reported success")
	}
	if fixture.owner.ShutdownComplete() {
		t.Error("physical source released before converter returned")
	}
	close(converter.decodeRelease)
	<-done
	timeout, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := fixture.owner.Close(timeout); err != nil {
		t.Fatal(err)
	}
	if !fixture.owner.ShutdownComplete() {
		t.Fatal("actual cleanup did not complete")
	}
}
