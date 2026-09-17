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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const workflowPrefix = "/temporal.api.workflowservice.v1.WorkflowService/"

const countMethod = workflowPrefix + "CountWorkflowExecutions"

const signalMethod = workflowPrefix + "SignalWorkflowExecution"

const completeMethod = workflowPrefix + "RespondActivityTaskFailed"

const queryMethod = workflowPrefix + "QueryWorkflow"

type rpcServer struct {
	workflowservice.UnimplementedWorkflowServiceServer
	namespaceCounts map[string]int64
	calls           atomic.Int32
	mutations       atomic.Int32
	block           atomic.Bool
	entered         chan struct{}
	release         chan struct{}
	once            sync.Once
	activityName    string
	activityIssued  atomic.Bool
	intercept       grpc.UnaryServerInterceptor
	starts          atomic.Int32
	cancels         atomic.Int32
	start           func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error)
	history         func(context.Context, *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error)
	describe        func(context.Context, *workflowservice.DescribeWorkflowExecutionRequest) (*workflowservice.DescribeWorkflowExecutionResponse, error)
	multi           func(context.Context, *workflowservice.ExecuteMultiOperationRequest) (*workflowservice.ExecuteMultiOperationResponse, error)
	update          func(context.Context, *workflowservice.UpdateWorkflowExecutionRequest) (*workflowservice.UpdateWorkflowExecutionResponse, error)
	pollUpdate      func(context.Context, *workflowservice.PollWorkflowExecutionUpdateRequest) (*workflowservice.PollWorkflowExecutionUpdateResponse, error)
}

func (*rpcServer) DescribeNamespace(_ context.Context, request *workflowservice.DescribeNamespaceRequest) (*workflowservice.DescribeNamespaceResponse, error) {
	return &workflowservice.DescribeNamespaceResponse{NamespaceInfo: &namespacepb.NamespaceInfo{Name: request.Namespace}}, nil
}

func (*rpcServer) ShutdownWorker(context.Context, *workflowservice.ShutdownWorkerRequest) (*workflowservice.ShutdownWorkerResponse, error) {
	return &workflowservice.ShutdownWorkerResponse{}, nil
}

func (server *rpcServer) PollActivityTaskQueue(ctx context.Context, request *workflowservice.PollActivityTaskQueueRequest) (*workflowservice.PollActivityTaskQueueResponse, error) {
	if server.activityName != "" && server.activityIssued.CompareAndSwap(false, true) {
		return &workflowservice.PollActivityTaskQueueResponse{TaskToken: []byte("fixture-token"), ActivityId: "fixture-activity", ActivityType: &commonpb.ActivityType{Name: server.activityName}, WorkflowNamespace: request.Namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "fixture-workflow", RunId: "fixture-run"}, StartedTime: timestamppb.Now(), ScheduledTime: timestamppb.Now(),
			StartToCloseTimeout: durationpb.New(time.Minute), ScheduleToCloseTimeout: durationpb.New(time.Minute), Attempt: 1}, nil
	}
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func (*rpcServer) RespondActivityTaskCompleted(context.Context, *workflowservice.RespondActivityTaskCompletedRequest) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	return &workflowservice.RespondActivityTaskCompletedResponse{}, nil
}

func (server *rpcServer) StartWorkflowExecution(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
	server.starts.Add(1)
	if server.start != nil {
		return server.start(ctx, request)
	}
	return &workflowservice.StartWorkflowExecutionResponse{RunId: "observed-run"}, nil
}

func (server *rpcServer) RequestCancelWorkflowExecution(context.Context, *workflowservice.RequestCancelWorkflowExecutionRequest) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
	server.cancels.Add(1)
	return &workflowservice.RequestCancelWorkflowExecutionResponse{}, nil
}

func (server *rpcServer) GetWorkflowExecutionHistory(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	if server.history != nil {
		return server.history(ctx, request)
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{EventId: 5, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte("\"expected-result\"")}}}}}}}}}, nil
}

func (server *rpcServer) DescribeWorkflowExecution(ctx context.Context, request *workflowservice.DescribeWorkflowExecutionRequest) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if server.describe != nil {
		return server.describe(ctx, request)
	}
	return nil, status.Error(codes.Unimplemented, "fixture has no execution metadata")
}

func (server *rpcServer) ExecuteMultiOperation(ctx context.Context, request *workflowservice.ExecuteMultiOperationRequest) (*workflowservice.ExecuteMultiOperationResponse, error) {
	if server.multi != nil {
		return server.multi(ctx, request)
	}
	return nil, status.Error(codes.Unimplemented, "fixture has no combined operation")
}

func (server *rpcServer) UpdateWorkflowExecution(ctx context.Context, request *workflowservice.UpdateWorkflowExecutionRequest) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
	if server.update != nil {
		return server.update(ctx, request)
	}
	return nil, status.Error(codes.Unimplemented, "fixture has no update")
}

func (server *rpcServer) PollWorkflowExecutionUpdate(ctx context.Context, request *workflowservice.PollWorkflowExecutionUpdateRequest) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
	if server.pollUpdate != nil {
		return server.pollUpdate(ctx, request)
	}
	return nil, status.Error(codes.Unimplemented, "fixture has no update result")
}

func (*rpcServer) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{ServerVersion: "1.32.0"}, nil
}

func (server *rpcServer) CountWorkflowExecutions(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	server.calls.Add(1)
	if server.block.Load() {
		server.once.Do(func() { close(server.entered) })
		select {
		case <-server.release:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	count, known := server.namespaceCounts[request.Namespace]
	if !known {
		return nil, status.Error(codes.InvalidArgument, "namespace")
	}
	return &workflowservice.CountWorkflowExecutionsResponse{Count: count}, nil
}

func (server *rpcServer) SignalWorkflowExecution(context.Context, *workflowservice.SignalWorkflowExecutionRequest) (*workflowservice.SignalWorkflowExecutionResponse, error) {
	server.mutations.Add(1)
	return nil, status.Error(codes.Unavailable, "private-native-cause")
}

func (server *rpcServer) RespondActivityTaskFailed(context.Context, *workflowservice.RespondActivityTaskFailedRequest) (*workflowservice.RespondActivityTaskFailedResponse, error) {
	server.calls.Add(1)
	return &workflowservice.RespondActivityTaskFailedResponse{}, nil
}

type fixture struct {
	endpoint  string
	assembly  *resource.Assembly
	selection resource.Selection[temporal.Source]
	client    *temporal.Client
	inbox     *invocation.Inbox[temporal.RPCResult]
	server    *rpcServer
	limits    resource.Limits
}

func newFixture(t *testing.T, capacity int, configure ...func(*temporal.OptionsV1, *resource.Limits, *rpcServer)) *fixture {
	t.Helper()
	return newRuntimeFixture(t, capacity, temporal.RuntimeOptions{}, nil, configure...)
}

func newRuntimeFixture(t *testing.T, capacity int, runtime temporal.RuntimeOptions, dependencies []resource.Spec, configure ...func(*temporal.OptionsV1, *resource.Limits, *rpcServer)) *fixture {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer := &rpcServer{namespaceCounts: map[string]int64{"test": 7, "other": 11}, entered: make(chan struct{}), release: make(chan struct{})}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if peer.intercept != nil {
			return peer.intercept(ctx, request, info, next)
		}
		return next(ctx, request)
	}))
	workflowservice.RegisterWorkflowServiceServer(server, peer)
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-joined })
	options := temporal.OptionsV1{Name: "temporal", Endpoint: listener.Addr().String(), Namespace: "test", Plaintext: true,
		RPCs: []string{countMethod, signalMethod, completeMethod, queryMethod}, MaxActive: 1, MaxRequestBytes: 1024, MaxResponseBytes: 1024, RPCTimeout: time.Second}
	limits := resource.Limits{Active: 1, Bytes: 1024 + 1024 + (16 << 10), MaxLeases: 8}
	for _, apply := range configure {
		apply(&options, &limits, peer)
	}
	selection, err := temporal.SelectWithRuntime(options, runtime)
	if err != nil {
		t.Fatal(err)
	}
	selection = resource.WithLimits(selection, limits)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	specs := append(append([]resource.Spec(nil), dependencies...), selection)
	assembly, err := resource.Assemble(ctx, ctx, "test-owner", specs...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-peer.release:
		default:
			close(peer.release)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := assembly.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	inbox, err := invocation.NewInbox[temporal.RPCResult](capacity, int64(capacity)*1024)
	if err != nil {
		t.Fatal(err)
	}
	client, err := temporal.Bind(assembly, selection, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{endpoint: listener.Addr().String(), assembly: assembly, selection: selection, client: client, inbox: inbox, server: peer, limits: limits}
}

func receive(t *testing.T, fixture *fixture) invocation.Result[temporal.RPCResult] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	delivery, err := fixture.inbox.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := delivery.Receipt().WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestNativeRPCAndIndependentEvidence(t *testing.T) {
	fixture := newFixture(t, 2)
	correlation := fault.Correlation{Call: "count"}
	reply, err := fixture.client.WorkflowService(correlation).CountWorkflowExecutions(context.Background(), &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
	if err != nil || reply.GetCount() != 7 {
		t.Fatalf("independent server count: %v", err)
	}
	reply.Count = 99
	result := receive(t, fixture)
	_, info, err := resource.Bind(fixture.assembly, fixture.selection)
	if err != nil {
		t.Fatal(err)
	}
	conformance.Result(t, result, conformance.Expected[temporal.RPCResult]{
		Context: fault.Context{Provider: temporal.ProviderID, Operation: "rpc.countworkflowexecutions", Source: "temporal", Scope: "test-owner", Correlation: correlation},
		Source:  info, Limits: fixture.limits, Shape: invocation.Finite, Present: true, Final: true, Released: true,
		Attempts: invocation.Attempts{Observed: 1},
		Value: func(t testing.TB, value temporal.RPCResult) {
			if !value.Invoked || !value.Acknowledged || value.Method != countMethod || value.RequestBytes != 6 || value.ResponseBytes != 2 {
				t.Error("wire evidence did not match the independent six-byte request/two-byte response")
			}
		},
	})
	if fixture.server.calls.Load() != 1 {
		t.Fatal("unexpected native attempts")
	}
	if fixture.client.Profile().ServiceVersion.Value != "1.32.0" {
		t.Fatal("observed server version missing")
	}
}

func TestHandledUnknownEffectRetainsNativeCause(t *testing.T) {
	fixture := newFixture(t, 1)
	_, err := fixture.client.WorkflowService(fault.Correlation{Call: "signal"}).SignalWorkflowExecution(context.Background(),
		&workflowservice.SignalWorkflowExecutionRequest{Namespace: "test", WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "synthetic"}, SignalName: "signal"})
	if err == nil || fixture.server.mutations.Load() != 1 {
		t.Fatal("response-loss control did not reach the independent effect")
	}
	conformance.Cause[*serviceerror.Unavailable](t, err, func(cause *serviceerror.Unavailable) bool { return cause.Message == "private-native-cause" })
	conformance.Private(t, err, "private-native-cause", "synthetic")
	if fixture.inbox.Usage().Outstanding != 1 {
		t.Fatal("handled error erased required evidence")
	}
	_, err = fixture.client.WorkflowService(fault.Correlation{Call: "blocked"}).CountWorkflowExecutions(context.Background(), &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
	if !errors.Is(err, invocation.ErrEvidence) || fixture.server.calls.Load() != 0 {
		t.Fatal("full evidence inbox allowed native work")
	}
	result := receive(t, fixture)
	if !result.Outcome.Value.Invoked || result.Outcome.Value.Acknowledged {
		t.Fatal("unknown acceptance was promoted to success or no invocation")
	}
	conformance.Cause[*serviceerror.Unavailable](t, result.Err(), func(cause *serviceerror.Unavailable) bool { return cause.Message == "private-native-cause" })
}

func TestBorrowingAliasCannotMultiplyAdmission(t *testing.T) {
	fixture := newFixture(t, 3)
	alias := resource.Borrow("alias", fixture.assembly, fixture.selection)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	borrower, err := resource.Assemble(ctx, ctx, "borrower", alias)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := borrower.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	client, err := temporal.Bind(borrower, alias, fixture.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	fixture.server.block.Store(true)
	completed := make(chan error, 1)
	go func() {
		_, err := fixture.client.WorkflowService(fault.Correlation{Call: "first"}).CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
		completed <- err
	}()
	select {
	case <-fixture.server.entered:
	case <-ctx.Done():
		t.Fatal("first RPC did not enter")
	}
	_, err = client.WorkflowService(fault.Correlation{Call: "alias"}).CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
	if err == nil || fixture.server.calls.Load() != 1 {
		t.Fatal("alias widened the single active allowance")
	}
	close(fixture.server.release)
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	receive(t, fixture)
}

func TestAuthorityWireLimitsAndOpaqueOptions(t *testing.T) {
	fixture := newFixture(t, 8)
	service := fixture.client.WorkflowService(fault.Correlation{Call: "refused"})
	request := &workflowservice.CountWorkflowExecutionsRequest{Namespace: "another"}
	if _, err := service.CountWorkflowExecutions(context.Background(), request); !errors.Is(err, temporal.ErrAuthority) {
		t.Fatal("cross-namespace request accepted")
	}
	request.Namespace = "test"
	if _, err := service.CountWorkflowExecutions(context.Background(), request, grpc.MaxCallRecvMsgSize(2048)); !errors.Is(err, temporal.ErrLimit) {
		t.Fatal("caller widened receive bound")
	}
	called := false
	if _, err := service.CountWorkflowExecutions(context.Background(), request, grpc.OnFinish(func(error) { called = true })); !errors.Is(err, temporal.ErrAuthority) || called {
		t.Fatal("unowned callback accepted")
	}
	request.Query = strings.Repeat("x", 1024)
	if _, err := service.CountWorkflowExecutions(context.Background(), request); !errors.Is(err, temporal.ErrLimit) {
		t.Fatal("oversized request accepted")
	}
	receive(t, fixture)
	cycle := &failurepb.Failure{}
	cycle.Cause = cycle
	_, err := service.RespondActivityTaskFailed(context.Background(), &workflowservice.RespondActivityTaskFailedRequest{Namespace: "test", Failure: cycle})
	if !errors.Is(err, temporal.ErrLimit) {
		t.Fatal("cyclic protobuf request accepted")
	}
	receive(t, fixture)
	if fixture.server.calls.Load() != 0 {
		t.Fatal("rejected inputs reached transport")
	}
}

func TestViewsCannotReleaseNativeOwner(t *testing.T) {
	fixture := newFixture(t, 2)
	view := fixture.client.WorkflowService(fault.Correlation{Call: "view"})
	methods := []string{"Format", "LogValue", "MarshalJSON", "UnmarshalJSON"}
	nativeInterface := reflect.TypeFor[workflowservice.WorkflowServiceClient]()
	for index := 0; index < nativeInterface.NumMethod(); index++ {
		methods = append(methods, nativeInterface.Method(index).Name)
	}
	conformance.Facade(t, view, methods...)
	conformance.Runtime(t, fixture.client, new(temporal.Client))
	conformance.Runtime(t, temporal.OptionsV1{APIKey: "secret-token"}, new(temporal.OptionsV1), "secret-token")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := fixture.assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := view.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
	if err == nil || fixture.server.calls.Load() != 0 {
		t.Fatal("retained view outlived source admission")
	}
	if len(temporal.KnownRPCs()) != 135 {
		t.Fatal("selected protocol inventory changed and needs review")
	}
}

func TestCanceledRPCWaitIsNotRemoteCompletion(t *testing.T) {
	fixture := newFixture(t, 1)
	fixture.server.block.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := fixture.client.WorkflowService(fault.Correlation{Call: "cancel"}).CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
		done <- err
	}()
	select {
	case <-fixture.server.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("RPC did not enter")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation cause lost: %v", err)
	}
	result := receive(t, fixture)
	if !result.Outcome.Value.Invoked || result.Outcome.Value.Acknowledged || fixture.server.calls.Load() != 1 {
		t.Fatal("canceled observation rewrote native invocation facts")
	}
}

func TestProtocolPresenceAndHealthyVersionDoNotHideUnimplemented(t *testing.T) {
	fixture := newFixture(t, 1)
	_, err := fixture.client.WorkflowService(fault.Correlation{Call: "unimplemented"}).QueryWorkflow(context.Background(), &workflowservice.QueryWorkflowRequest{Namespace: "test"})
	conformance.Cause[*serviceerror.Unimplemented](t, err, func(*serviceerror.Unimplemented) bool { return true })
	result := receive(t, fixture)
	conformance.Cause[*serviceerror.Unimplemented](t, result.Err(), func(*serviceerror.Unimplemented) bool { return true })
	if !result.Outcome.Value.Invoked || result.Outcome.Value.Acknowledged {
		t.Fatal("unimplemented became an empty success")
	}
}

func TestNamedNamespaceClientsDoNotRetargetEachOther(t *testing.T) {
	fixture := newFixture(t, 4)
	selection, err := temporal.Select(temporal.OptionsV1{Name: "other", Endpoint: fixture.endpoint, Namespace: "other", Plaintext: true,
		RPCs: []string{countMethod}, MaxActive: 1, MaxRequestBytes: 1024, MaxResponseBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	selection = resource.WithLimits(selection, fixture.limits)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	assembly, err := resource.Assemble(ctx, ctx, "other-owner", selection)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := assembly.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	other, err := temporal.Bind(assembly, selection, fixture.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := fixture.client.WorkflowService(fault.Correlation{Call: "first-ns"}).CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
	if err != nil || first.GetCount() != 7 {
		t.Fatal("first namespace result changed")
	}
	second, err := other.WorkflowService(fault.Correlation{Call: "second-ns"}).CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "other"})
	if err != nil || second.GetCount() != 11 {
		t.Fatal("second namespace result changed")
	}
	_, err = other.WorkflowService(fault.Correlation{Call: "wrong-ns"}).CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
	if !errors.Is(err, temporal.ErrAuthority) {
		t.Fatal("second source could be retargeted to the first namespace")
	}
	if fixture.server.calls.Load() != 2 {
		t.Fatal("forbidden namespace reached the server")
	}
	firstEvidence, secondEvidence := receive(t, fixture), receive(t, fixture)
	if firstEvidence.Context.Source != "temporal" || firstEvidence.Context.Scope != "test-owner" || secondEvidence.Context.Source != "other" || secondEvidence.Context.Scope != "other-owner" {
		t.Fatal("namespace source evidence was mixed")
	}
}

func TestNativeLifecycleControlsUseConsumingGraph(t *testing.T) {
	command := exec.Command("go", "test", "-mod=readonly", "-race", "-count=1", "-timeout=90s", "-json", "go.temporal.io/sdk/internal", "go.temporal.io/sdk/worker", "-run", "TestFathomry")
	command.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOSUMDB=off")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("native consuming-graph controls failed: %v\n%s", err, output)
	}
	expected := map[string]bool{
		"TestFathomryErrorDecodersPreserveFailureAndScope":       false,
		"TestFathomryErrorScopeKeepsSentinelsAndBoundedCycles":   false,
		"TestFathomryReviewEagerStorageFailureOwnership":         false,
		"TestFathomryScopedUpdatePreservesCompletion":            false,
		"TestFathomryMultipleNamespacesShareBinaryAndConnection": false,
		"TestFathomryLateEagerResponseReleasesPermit":            false,
		"TestFathomryHeartbeatSnapshotRetainsRemovedWorker":      false,
		"TestFathomryWaitJoinsActivity":                          false,
		"TestFathomryWaitJoinsLocalActivityTail":                 false,
		"TestFathomryWaitJoinsWorkflowEviction":                  false,
		"TestFathomryRetirementPreservesLivePeerCache":           false,
	}
	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		var event struct{ Action, Test string }
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatal("native test stream is not JSON")
		}
		if _, required := expected[event.Test]; required && event.Action == "pass" {
			expected[event.Test] = true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	for name, passed := range expected {
		if !passed {
			t.Errorf("required native control did not execute: %s", name)
		}
	}
}

func reviewReleasedCallbackFixture(t *testing.T, method string, runtime temporal.RuntimeOptions, invoke func(context.Context, sdk.Client) error, respond func(context.Context, any, grpc.UnaryHandler) (any, error)) sdk.Client {
	t.Helper()
	fixture := newRuntimeFixture(t, 1, runtime, nil, func(options *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		options.RPCs = append(options.RPCs, "/temporal.api.workflowservice.v1.WorkflowService/"+method)
		peer.activityName = "review-callback-surface"
		peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			return respond(ctx, request, next)
		}
	})
	executions, inbox := executionBinding(t, fixture)
	workers, tasks := workerInboxes(t)
	var borrowed sdk.Client
	done := make(chan error, 1)
	body := func(ctx context.Context) error {
		borrowed = activity.GetClient(ctx)
		err := invoke(ctx, borrowed)
		done <- err
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "review-callback"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1},
		Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "review-callback-surface"}}},
	}, workers, tasks)
	if managed != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := managed.Stop(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("live callback control failed", err)
		}
	case <-ctx.Done():
		t.Fatal("callback did not complete")
	}
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	releaseServiceEvidence(t, inbox)
	releaseServiceEvidence(t, workers)
	releaseServiceEvidence(t, tasks)
	if err := fixture.assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return borrowed
}
