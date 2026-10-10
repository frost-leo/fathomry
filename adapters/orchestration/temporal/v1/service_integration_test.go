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
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/testsuite"
	nativeworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const publicServiceWorkflowPrefix = "/temporal.api.workflowservice.v1.WorkflowService/"

var publicServiceCleanupPending atomic.Bool

type publicServiceConfiguration struct {
	Temporal struct {
		Endpoint  string `yaml:"grpc_endpoint"`
		Namespace string `yaml:"namespace"`
		Auth      string `yaml:"auth"`
	} `yaml:"temporal"`
}

func publicAuthorizedConfiguration(t *testing.T) publicServiceConfiguration {
	t.Helper()
	if publicServiceCleanupPending.Load() {
		t.Fatal("earlier owned cleanup is unresolved; refusing new service resources")
	}
	path := os.Getenv("FATHOMRY_TEMPORAL_TEST_CONFIG")
	if path == "" {
		t.Skip("explicit authorized isolated Temporal configuration required")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("cannot open authorized configuration")
	}
	data, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	closed := file.Close()
	if err != nil || closed != nil || len(data) > 1<<20 {
		t.Fatal("cannot read bounded authorized configuration")
	}
	var result publicServiceConfiguration
	if yaml.Unmarshal(data, &result) != nil || result.Temporal.Endpoint == "" || result.Temporal.Namespace == "" ||
		!strings.Contains(strings.ToLower(result.Temporal.Auth), "none") {
		t.Fatal("authorized configuration is not the selected isolated no-auth profile")
	}
	return result
}

type publicServiceLogger struct{}

func (publicServiceLogger) Debug(string, ...any) {}
func (publicServiceLogger) Info(string, ...any)  {}
func (publicServiceLogger) Warn(string, ...any)  {}
func (publicServiceLogger) Error(string, ...any) {}

type publicServiceFixture struct {
	namespace     string
	prefix        string
	client        *temporal.Client
	owner         *temporal.Owner
	dependencies  temporal.Dependencies
	workflows     []*commonpb.WorkflowExecution
	activities    []*temporal.ActivityRun
	workers       []*temporal.Worker
	expectedStops map[*temporal.Worker]error
	taskRecords   atomic.Int32
	asyncRecords  atomic.Int32
	nexusLinks    atomic.Int32
	describeRPCs  *atomic.Int32
	deletions     map[string]bool
}

type publicServiceTraffic struct {
	describes *atomic.Int32
	delegate  temporal.TrafficController
}

func (traffic *publicServiceTraffic) CheckCallAllowed(ctx context.Context, method string, request, response any) error {
	if strings.HasSuffix(method, "DescribeWorkflowExecution") {
		traffic.describes.Add(1)
	}
	if traffic.delegate != nil {
		return traffic.delegate.CheckCallAllowed(ctx, method, request, response)
	}
	return nil
}

func newPublicServiceFixture(t *testing.T, options temporal.NativeOptions, extraMethods ...string) *publicServiceFixture {
	t.Helper()
	return newPublicServiceFixtureMode(t, options, false, nil, extraMethods...)
}

func newPublicServiceFixtureMode(t *testing.T, options temporal.NativeOptions, lazy bool, parent *temporal.Client, extraMethods ...string) *publicServiceFixture {
	t.Helper()
	config := publicAuthorizedConfiguration(t)
	methods := []string{"GetSystemInfo", "DescribeNamespace", "CountWorkflowExecutions", "GetWorkflowExecutionHistory", "DeleteWorkflowExecution", "DeleteActivityExecution"}
	for index := range methods {
		methods[index] = publicServiceWorkflowPrefix + methods[index]
	}
	methods = append(methods, extraMethods...)
	if options.Logger == nil {
		options.Logger = publicServiceLogger{}
	}
	options.ReportWorkerEnvironment = false
	describes := &atomic.Int32{}
	if parent == nil {
		options.TrafficController = &publicServiceTraffic{describes: describes, delegate: options.TrafficController}
	}
	maxActive := 8
	if parent != nil {
		maxActive = 2
	}
	prepared, err := temporal.Prepare(temporal.Settings{Name: "public-service", Endpoint: config.Temporal.Endpoint, Namespace: config.Temporal.Namespace,
		Plaintext: true, Lazy: lazy, RPCs: methods, MaxActive: maxActive, MaxUses: 16, InnerEvidenceCapacity: 128,
		MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20, RPCTimeout: 30 * time.Second}, options)
	serviceRequire(t, err, "prepare")
	policy, err := prepared.Policy()
	serviceRequire(t, err, "policy")
	lifetime, cancel := context.WithCancel(context.Background())
	runtime, err := adapters.New(lifetime, policy.Runtime)
	serviceRequire(t, err, "runtime")
	operations, err := adapters.NewInbox[temporal.Result](policy.Evidence)
	serviceRequire(t, err, "operation evidence")
	workers, err := adapters.NewInbox[temporal.WorkerResult](policy.Workers)
	serviceRequire(t, err, "worker evidence")
	tasks, err := adapters.NewInbox[temporal.TaskResult](policy.Tasks)
	serviceRequire(t, err, "task evidence")
	dependencies := temporal.Dependencies{Runtime: runtime, Evidence: operations, Workers: workers, Tasks: tasks, Native: options}
	var owner *temporal.Owner
	if parent == nil {
		owner, err = prepared.Open(lifetime, dependencies)
	} else {
		owner, err = prepared.OpenFrom(lifetime, parent, dependencies)
	}
	if err != nil {
		if owner != nil {
			_ = owner.Close(context.Background())
		}
		cancel()
		serviceRequire(t, err, "open")
	}
	fixture := &publicServiceFixture{namespace: config.Temporal.Namespace, prefix: "gh134-" + uuid.NewString(),
		owner: owner, client: owner.Client(), dependencies: dependencies, describeRPCs: describes, deletions: make(map[string]bool), expectedStops: make(map[*temporal.Worker]error)}
	t.Log("test-owned prefix:", fixture.prefix)
	t.Cleanup(func() {
		for index := len(fixture.workflows) - 1; index >= 0; index-- {
			fixture.cleanupWorkflow(t, fixture.workflows[index])
		}
		for _, handle := range fixture.activities {
			fixture.cleanupActivity(t, handle)
		}
		ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		for _, worker := range fixture.workers {
			if err := worker.Stop(ctx); err != nil && !errors.Is(err, fixture.expectedStops[worker]) {
				serviceCleanupFailure(t, "worker cleanup failed: %T", err)
			}
			if !worker.Status().Joined {
				serviceCleanupFailure(t, "worker cleanup did not establish native join")
			}
		}
		if err := owner.Close(ctx); err != nil {
			serviceCleanupFailure(t, "source cleanup failed: %T", err)
		}
		fixture.drain(t)
		for _, receiver := range []interface {
			Inspect() (adapters.EvidenceStatus, error)
		}{operations, workers, tasks} {
			state, err := receiver.Inspect()
			if err != nil || state.Outstanding != 0 {
				serviceCleanupFailure(t, "final public evidence custody remained outstanding")
			}
		}
		if err := runtime.Close(ctx); err != nil {
			serviceCleanupFailure(t, "runtime cleanup failed: %T", err)
		}
		cancel()
	})
	return fixture
}

func serviceRequire(t *testing.T, err error, operation string) {
	t.Helper()
	if err != nil {
		if core, ok := failure.Inspect(err); ok {
			diagnostic := core.Diagnostic()
			t.Logf("classified failure: %s operation=%s", diagnostic.Definition.Identifier, diagnostic.Location.Operation)
		}
		if native, present := temporal.NativeError(err); present {
			t.Logf("captured native failure type: %T", native)
		}
		t.Fatalf("%s failed: %T", operation, err)
	}
}

func serviceCleanupFailure(t *testing.T, format string, arguments ...any) {
	t.Helper()
	publicServiceCleanupPending.Store(true)
	t.Errorf(format, arguments...)
}

func drainPublicServiceEvidence[T any](t *testing.T, inbox *adapters.Inbox[T], inspect func(T)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for {
		state, err := inbox.Inspect()
		serviceRequire(t, err, "inspect evidence")
		if state.Queued == 0 {
			return
		}
		available, stop := context.WithTimeout(ctx, 10*time.Millisecond)
		delivery, err := inbox.NextReleased(available)
		stop()
		if errors.Is(err, context.DeadlineExceeded) {
			return
		}
		serviceRequire(t, err, "receive evidence")
		receipt, err := delivery.Receipt()
		serviceRequire(t, err, "evidence receipt")
		result, err := receipt.WaitReleased(ctx)
		serviceRequire(t, err, "released evidence")
		if value, present := result.ValueCopy(); present && inspect != nil {
			inspect(value)
		}
		serviceRequire(t, delivery.Ack(), "acknowledge evidence")
	}
}

func (fixture *publicServiceFixture) drain(t *testing.T) {
	t.Helper()
	drainPublicServiceEvidence(t, fixture.dependencies.Evidence, nil)
	drainPublicServiceEvidence(t, fixture.dependencies.Workers, nil)
	drainPublicServiceEvidence(t, fixture.dependencies.Tasks, func(value temporal.TaskResult) {
		if value.HandlerReturned {
			fixture.taskRecords.Add(1)
		}
		if value.AsyncCompletion {
			fixture.asyncRecords.Add(1)
		}
		if value.NexusRequestLinks > 0 {
			fixture.nexusLinks.Add(1)
		}
	})
}

func (fixture *publicServiceFixture) trackWorkflow(id, runID string) {
	fixture.workflows = append(fixture.workflows, &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID})
}

func (fixture *publicServiceFixture) cleanupWorkflow(t *testing.T, execution *commonpb.WorkflowExecution) {
	t.Helper()
	if !strings.HasPrefix(execution.WorkflowId, fixture.prefix) {
		serviceCleanupFailure(t, "refusing non-owned workflow cleanup")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	current, err := fixture.client.DescribeWorkflowExecution(ctx, execution.WorkflowId, execution.RunId)
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		fixture.drain(t)
		return
	}
	if err != nil || current.GetWorkflowExecutionInfo().GetExecution().GetWorkflowId() != execution.WorkflowId {
		serviceCleanupFailure(t, "owned workflow cleanup identity unavailable: %T", err)
		return
	}
	actual := current.GetWorkflowExecutionInfo().GetExecution()
	if current.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		if err := fixture.client.TerminateWorkflow(ctx, sdk.TerminateWorkflowOptions{WorkflowID: actual.WorkflowId, RunID: actual.RunId, Reason: "gh134 isolated cleanup"}); err != nil {
			serviceCleanupFailure(t, "owned workflow termination failed: %T", err)
			return
		}
	}
	key := actual.WorkflowId + "/" + actual.RunId
	if _, attempted := fixture.deletions[key]; !attempted {
		fixture.deletions[key] = false
		_, err = fixture.client.WorkflowService().DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{Namespace: fixture.namespace, WorkflowExecution: actual})
		fixture.deletions[key] = err == nil
		if err != nil {
			t.Logf("owned deletion acknowledgement unknown; observing without resend: id=%s run=%s cause=%T", actual.WorkflowId, actual.RunId, err)
		}
	}
	before := fixture.describeRPCs.Load()
	var lastStatus enumspb.WorkflowExecutionStatus
	observe := func(wait context.Context) error {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			observed, err := fixture.client.DescribeWorkflowExecution(wait, actual.WorkflowId, actual.RunId)
			if observed != nil {
				lastStatus = observed.GetWorkflowExecutionInfo().GetStatus()
			}
			fixture.drain(t)
			if errors.As(err, &missing) {
				return nil
			}
			if err != nil {
				return err
			}
			select {
			case <-ticker.C:
			case <-wait.Done():
				return wait.Err()
			}
		}
	}
	initial, stopInitial := context.WithTimeout(ctx, 30*time.Second)
	err = observe(initial)
	stopInitial()
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		t.Log("cleanup observation incomplete after 30s; resuming same deletion without resend:", actual.WorkflowId, actual.RunId)
		err = observe(ctx)
	}
	if err != nil {
		serviceCleanupFailure(t, "PENDING owned workflow deletion: id=%s run=%s acknowledged=%t outbound_describes=%d last_status=%s cause=%T",
			actual.WorkflowId, actual.RunId, fixture.deletions[key], fixture.describeRPCs.Load()-before, lastStatus, err)
		return
	}
	if fixture.describeRPCs.Load() == before {
		serviceCleanupFailure(t, "workflow absence lacked an outbound native Describe attempt")
	}
	t.Log("deleted workflow:", actual.WorkflowId, actual.RunId, "outbound_describes", fixture.describeRPCs.Load()-before)
}

func (fixture *publicServiceFixture) cleanupActivity(t *testing.T, handle *temporal.ActivityRun) {
	t.Helper()
	if !strings.HasPrefix(handle.GetID(), fixture.prefix) {
		serviceCleanupFailure(t, "refusing non-owned activity cleanup")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	description, err := handle.Describe(ctx, sdk.DescribeActivityOptions{})
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		fixture.drain(t)
		return
	}
	if err != nil || description.Metadata().ActivityID != handle.GetID() {
		serviceCleanupFailure(t, "owned activity identity unavailable: %T", err)
		return
	}
	if description.Metadata().Status == enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING || description.Metadata().Status == enumspb.ACTIVITY_EXECUTION_STATUS_PAUSED {
		if err := handle.Terminate(ctx, sdk.TerminateActivityOptions{Reason: "gh134 isolated cleanup"}); err != nil {
			serviceCleanupFailure(t, "owned activity termination failed: %T", err)
			return
		}
	}
	_, err = fixture.client.WorkflowService().DeleteActivityExecution(ctx, &workflowservice.DeleteActivityExecutionRequest{
		Namespace: fixture.namespace, ActivityId: handle.GetID(), RunId: description.Metadata().ActivityRunID})
	if err != nil {
		serviceCleanupFailure(t, "owned activity deletion failed: %T", err)
		return
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err = handle.Describe(ctx, sdk.DescribeActivityOptions{})
		fixture.drain(t)
		if errors.As(err, &missing) {
			t.Log("deleted activity:", handle.GetID(), description.Metadata().ActivityRunID)
			return
		}
		if err != nil {
			serviceCleanupFailure(t, "owned activity absence read failed: %T", err)
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			serviceCleanupFailure(t, "owned activity absence was not confirmed")
			return
		}
	}
}

type publicServiceInput struct {
	Value      int
	Hold       bool
	Activities bool
}

func publicServiceWorkflow(ctx workflow.Context, input publicServiceInput) (int, error) {
	value := input.Value
	if err := workflow.SetQueryHandler(ctx, "value", func() (int, error) { return value, nil }); err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandler(ctx, "add", func(_ workflow.Context, amount int) (int, error) { value += amount; return value, nil }); err != nil {
		return 0, err
	}
	if input.Activities {
		local := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: 10 * time.Second,
			RetryPolicy: &sdktemporal.RetryPolicy{InitialInterval: time.Millisecond, MaximumAttempts: 2}})
		if err := workflow.ExecuteLocalActivity(local, "gh134-local", value).Get(ctx, &value); err != nil {
			return 0, err
		}
		remote := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second, HeartbeatTimeout: 5 * time.Second})
		if err := workflow.ExecuteActivity(remote, "gh134-heartbeat", value).Get(ctx, &value); err != nil {
			return 0, err
		}
	}
	if input.Hold {
		var finish bool
		signal := workflow.GetSignalChannel(ctx, "finish")
		if err := workflow.Await(ctx, func() bool { return signal.Len() > 0 }); err != nil {
			return 0, err
		}
		signal.Receive(ctx, &finish)
	}
	return value, nil
}

func publicServiceLocal(ctx context.Context, value int) (int, error) {
	if activity.GetInfo(ctx).Attempt == 1 {
		return 0, sdktemporal.NewApplicationError("controlled retry", "gh134-retry")
	}
	return value + 1, nil
}

func publicServiceHeartbeat(ctx context.Context, value int) (int, error) {
	activity.RecordHeartbeat(ctx, "gh134-heartbeat")
	return value + 1, nil
}

func TestPublicServiceWorkflowFixture(t *testing.T) {
	for _, cancelWorkflow := range []bool{false, true} {
		var suite testsuite.WorkflowTestSuite
		suite.SetLogger(publicServiceLogger{})
		environment := suite.NewTestWorkflowEnvironment()
		environment.RegisterDelayedCallback(func() {
			if cancelWorkflow {
				environment.CancelWorkflow()
			} else {
				environment.SignalWorkflow("finish", true)
			}
		}, time.Millisecond)
		environment.ExecuteWorkflow(publicServiceWorkflow, publicServiceInput{Value: 7, Hold: true})
		if cancelWorkflow {
			if !sdktemporal.IsCanceledError(environment.GetWorkflowError()) {
				t.Fatal("fixture did not honor native Workflow cancellation")
			}
		} else {
			serviceRequire(t, environment.GetWorkflowError(), "fixture completion")
			var value int
			serviceRequire(t, environment.GetWorkflowResult(&value), "fixture result")
			if value != 7 {
				t.Fatal("fixture changed its signal result")
			}
		}
	}
}

func (fixture *publicServiceFixture) startWorker(t *testing.T, ctx context.Context, autoscaling bool) *temporal.Worker {
	t.Helper()
	options := nativeworker.Options{MaxConcurrentWorkflowTaskPollers: 2, MaxConcurrentWorkflowTaskExecutionSize: 4,
		MaxConcurrentActivityTaskPollers: 2, WorkerStopTimeout: time.Second}
	if autoscaling {
		options.MaxConcurrentActivityTaskPollers = 0
		options.ActivityTaskPollerBehavior = nativeworker.NewPollerBehaviorAutoscaling(nativeworker.PollerBehaviorAutoscalingOptions{
			InitialNumberOfPollers: 1, MinimumNumberOfPollers: 1, MaximumNumberOfPollers: 2})
	}
	return fixture.startCustomWorker(t, ctx, temporal.WorkerSpec{TaskQueue: fixture.prefix, MaxHandlers: 4, Options: options,
		Workflows: []temporal.WorkflowRegistration{{Definition: publicServiceWorkflow, Options: workflow.RegisterOptions{Name: "gh134-workflow"}}},
		Activities: []temporal.ActivityRegistration{{Definition: publicServiceLocal, Options: activity.RegisterOptions{Name: "gh134-local"}},
			{Definition: publicServiceHeartbeat, Options: activity.RegisterOptions{Name: "gh134-heartbeat"}}}})
}

func (fixture *publicServiceFixture) startCustomWorker(t *testing.T, ctx context.Context, spec temporal.WorkerSpec) *temporal.Worker {
	t.Helper()
	lifetime, cancel := context.WithCancel(context.Background())
	worker, err := fixture.client.StartWorker(ctx, lifetime, spec)
	if worker != nil {
		fixture.workers = append(fixture.workers, worker)
	}
	t.Cleanup(cancel)
	serviceRequire(t, err, "worker startup")
	return worker
}

func TestAuthorizedPublicWorkflowActivityAndReplay(t *testing.T) {
	fixture := newPublicServiceFixture(t, temporal.NativeOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	worker := fixture.startWorker(t, ctx, false)
	info, err := fixture.client.WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	serviceRequire(t, err, "system information")
	if info.GetServerVersion() != "1.32.0" {
		t.Fatal("unexpected authorized Server profile")
	}
	namespace, err := fixture.client.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: fixture.namespace})
	serviceRequire(t, err, "namespace discovery")
	if !namespace.GetNamespaceInfo().GetCapabilities().GetStandaloneActivities() {
		t.Fatal("authorized namespace does not advertise standalone Activities")
	}
	_, err = fixture.client.WorkflowService().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{Namespace: fixture.namespace})
	if !errors.Is(err, temporal.ErrAuthority) {
		t.Fatal("raw view acquired implicit polling authority")
	}
	var headers, trailers metadata.MD
	_, err = fixture.client.WorkflowService().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: fixture.namespace, Query: "WorkflowId = '" + fixture.prefix + "-missing'"}, grpc.Header(&headers), grpc.Trailer(&trailers), grpc.WaitForReady(true))
	serviceRequire(t, err, "exact granted raw call options")
	fixture.drain(t)
	id := fixture.prefix + "-interaction"
	fixture.trackWorkflow(id, "")
	run, err := fixture.client.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: time.Minute, EnableEagerStart: true,
		StaticSummary: "gh134 summary", StaticDetails: "gh134 details", Memo: map[string]any{"fixture": "gh134 memo"}},
		"gh134-workflow", publicServiceInput{Value: 1, Hold: true, Activities: true})
	serviceRequire(t, err, "eager workflow start")
	fixture.trackWorkflow(id, run.GetRunID())
	var value int
	ready := time.NewTicker(100 * time.Millisecond)
	defer ready.Stop()
	for {
		serviceRequire(t, fixture.client.QueryWorkflow(ctx, id, run.GetRunID(), "value", &value), "query")
		fixture.drain(t)
		if value == 3 {
			break
		}
		select {
		case <-ready.C:
		case <-ctx.Done():
			t.Fatal("Activity and LocalActivity completion was not observed")
		}
	}
	queryUse, err := fixture.client.Borrow(ctx)
	serviceRequire(t, err, "retained Query use")
	retainedQuery, err := queryUse.QueryWorkflowValue(ctx, id, run.GetRunID(), "value")
	serviceRequire(t, err, "retained native Query value")
	if !retainedQuery.HasValue() {
		t.Fatal("native Query value lost its presence")
	}
	for range 2 {
		var decoded int
		serviceRequire(t, retainedQuery.Get(ctx, &decoded), "repeated Query decode")
		if decoded != 3 {
			t.Fatal("repeated Query decode changed the retained result")
		}
	}
	payloads, supported, err := retainedQuery.RawPayloads(ctx)
	serviceRequire(t, err, "optional Query raw payloads")
	if supported {
		if len(payloads.GetPayloads()) != 1 || len(payloads.Payloads[0].Data) == 0 {
			t.Fatal("native Query raw payload was absent")
		}
		payloads.Payloads[0].Data[0] ^= 1
		var decoded int
		serviceRequire(t, retainedQuery.Get(ctx, &decoded), "Query decode after detached payload mutation")
		if decoded != 3 {
			t.Fatal("raw Query payloads aliased the native value")
		}
	} else {
		t.Log("native retained Query value does not implement optional ValuesPayloads")
	}
	serviceRequire(t, queryUse.Close(ctx), "retained Query use release")
	if retainedQuery.Get(ctx, &value) == nil {
		t.Fatal("retained Query decoder outlived its originating use")
	}
	fixture.drain(t)
	update, err := fixture.client.UpdateWorkflow(ctx, sdk.UpdateWorkflowOptions{WorkflowID: id, RunID: run.GetRunID(), UpdateID: "gh134-add",
		UpdateName: "add", Args: []any{2}, WaitForStage: sdk.WorkflowUpdateStageCompleted})
	serviceRequire(t, err, "update")
	serviceRequire(t, update.Get(ctx, &value), "update result")
	serviceRequire(t, fixture.client.SignalWorkflow(ctx, id, run.GetRunID(), "finish", true), "signal")
	serviceRequire(t, run.Get(ctx, &value), "workflow result")
	if value != 5 {
		t.Fatalf("workflow/Activity/LocalActivity/update result=%d, want 5", value)
	}
	rejectedValue, rejection, err := fixture.client.QueryWorkflowValueWithOptions(ctx, &sdk.QueryWorkflowWithOptionsRequest{
		WorkflowID: id, RunID: run.GetRunID(), QueryType: "value", QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN})
	serviceRequire(t, err, "native Query rejection")
	if rejectedValue != nil || rejection.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		t.Fatal("native closed-Workflow Query rejection became a retained value")
	}
	metadata, err := fixture.client.DescribeWorkflow(ctx, id, run.GetRunID())
	serviceRequire(t, err, "Workflow metadata")
	summary, err := metadata.GetStaticSummary(ctx)
	serviceRequire(t, err, "Workflow static summary")
	details, err := metadata.GetStaticDetails(ctx)
	serviceRequire(t, err, "Workflow static details")
	var memo string
	serviceRequire(t, metadata.GetMemoValue(ctx, "fixture", &memo), "Workflow memo")
	if summary != "gh134 summary" || details != "gh134 details" || memo != "gh134 memo" || metadata.Metadata().Status != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		t.Fatal("native Workflow metadata changed")
	}
	for {
		page, err := fixture.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: "WorkflowId = '" + id + "'", PageSize: 2})
		serviceRequire(t, err, "exact Workflow visibility")
		fixture.drain(t)
		if len(page.Executions) == 1 {
			if page.Executions[0].GetExecution().GetRunId() != run.GetRunID() {
				t.Fatal("exact visibility returned a different execution")
			}
			break
		}
		if len(page.Executions) > 1 || len(page.NextPageToken) > 0 {
			t.Fatal("exact Workflow visibility exceeded owned execution")
		}
		select {
		case <-ready.C:
		case <-ctx.Done():
			t.Fatal("exact Workflow visibility was not observed")
		}
	}
	var history historypb.History
	serviceRequire(t, fixture.client.WalkHistory(ctx, id, run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
		func(_ context.Context, event *historypb.HistoryEvent) error {
			history.Events = append(history.Events, event)
			return nil
		}), "history walk")
	if len(history.Events) < 5 {
		t.Fatal("native workflow history was incomplete")
	}
	if !history.Events[0].GetWorkflowExecutionStartedEventAttributes().GetEagerExecutionAccepted() {
		t.Fatal("service did not accept the requested eager Workflow execution")
	}
	replayer := nativeworker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(publicServiceWorkflow, workflow.RegisterOptions{Name: "gh134-workflow"})
	serviceRequire(t, replayer.ReplayWorkflowHistoryWithOptions(publicServiceLogger{}, &history,
		nativeworker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: id, RunID: run.GetRunID()}}), "recorded native replay")
	activityID := fixture.prefix + "-activity"
	intended, err := fixture.client.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: activityID})
	serviceRequire(t, err, "standalone Activity intention")
	fixture.activities = append(fixture.activities, intended)
	standalone, err := fixture.client.ExecuteActivity(ctx, sdk.StartActivityOptions{ID: activityID, TaskQueue: fixture.prefix,
		StartToCloseTimeout: 10 * time.Second, HeartbeatTimeout: 5 * time.Second}, "gh134-heartbeat", 10)
	if standalone != nil {
		fixture.activities = append(fixture.activities, standalone)
	}
	serviceRequire(t, err, "standalone Activity start")
	serviceRequire(t, standalone.Get(ctx, &value), "standalone Activity result")
	if value != 11 {
		t.Fatal("standalone Activity result changed")
	}
	description, err := standalone.Describe(ctx, sdk.DescribeActivityOptions{IncludeInput: true, IncludeOutcome: true})
	serviceRequire(t, err, "standalone Activity description")
	serviceRequire(t, description.GetInput(ctx, &value), "standalone Activity input")
	if value != 10 {
		t.Fatal("standalone Activity input context changed")
	}
	fixture.drain(t)
	serviceRequire(t, worker.Stop(ctx), "first Worker join")
	fixture.startWorker(t, ctx, true)
	restartedID := fixture.prefix + "-restarted"
	fixture.trackWorkflow(restartedID, "")
	restarted, err := fixture.client.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{ID: restartedID, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: time.Minute},
		"gh134-workflow", publicServiceInput{Value: 20, Activities: true})
	serviceRequire(t, err, "workflow after worker restart")
	serviceRequire(t, restarted.Get(ctx, &value), "restart result")
	if value != 22 {
		t.Fatal("fixed/autoscaling Worker restart changed execution")
	}
	fixture.drain(t)
	if fixture.taskRecords.Load() < 7 {
		t.Fatal("public callback evidence did not cover retries, remote and standalone Activities")
	}
}

func TestAuthorizedPublicCombinedStartResetAndSchedules(t *testing.T) {
	fixture := newPublicServiceFixture(t, temporal.NativeOptions{}, publicServiceWorkflowPrefix+"ResetWorkflowExecution")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fixture.startWorker(t, ctx, false)
	id := fixture.prefix + "-combined"
	fixture.trackWorkflow(id, "")
	intention, err := fixture.client.NewWithStartWorkflowOperation(sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix,
		WorkflowExecutionTimeout: time.Minute, WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING},
		"gh134-workflow", publicServiceInput{Value: 4, Hold: true})
	serviceRequire(t, err, "combined intention")
	update, err := fixture.client.UpdateWithStartWorkflow(ctx, intention, sdk.UpdateWorkflowOptions{UpdateID: "gh134-add", UpdateName: "add",
		Args: []any{2}, WaitForStage: sdk.WorkflowUpdateStageCompleted})
	serviceRequire(t, err, "combined start")
	var value int
	serviceRequire(t, update.Get(ctx, &value), "combined update result")
	if value != 6 {
		t.Fatal("combined native Update result changed")
	}
	run, err := intention.Get(ctx)
	serviceRequire(t, err, "combined Workflow handle")
	fixture.trackWorkflow(id, run.GetRunID())
	serviceRequire(t, fixture.client.SignalWorkflow(ctx, id, run.GetRunID(), "finish", true), "combined completion signal")
	serviceRequire(t, run.Get(ctx, &value), "combined Workflow result")
	var resetEvent int64
	serviceRequire(t, fixture.client.WalkHistory(ctx, id, run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
		func(_ context.Context, event *historypb.HistoryEvent) error {
			if resetEvent == 0 && event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
				resetEvent = event.GetEventId()
			}
			return nil
		}), "reset-point history")
	if resetEvent == 0 {
		t.Fatal("native history lacked a reset point")
	}
	request := &workflowservice.ResetWorkflowExecutionRequest{Namespace: fixture.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: run.GetRunID()}, WorkflowTaskFinishEventId: resetEvent,
		Reason: "gh134 isolated reset", ResetReapplyType: enumspb.RESET_REAPPLY_TYPE_NONE}
	reset, err := fixture.client.ResetWorkflowExecution(ctx, request)
	serviceRequire(t, err, "Workflow reset")
	if reset.RequestID == "" || reset.Response.GetRunId() == "" || request.RequestId != "" {
		t.Fatal("reset effective identity or caller copy was lost")
	}
	fixture.trackWorkflow(id, reset.Response.GetRunId())
	resetRun, err := fixture.client.GetWorkflow(id, reset.Response.GetRunId())
	serviceRequire(t, err, "reset handle")
	serviceRequire(t, fixture.client.SignalWorkflow(ctx, id, reset.Response.GetRunId(), "finish", true), "reset completion signal")
	serviceRequire(t, resetRun.Get(ctx, &value), "reset result")
	fixture.drain(t)
	for _, operation := range []string{"signal-start", "cancel", "terminate"} {
		id := fixture.prefix + "-" + operation
		fixture.trackWorkflow(id, "")
		var run *temporal.WorkflowRun
		if operation == "signal-start" {
			run, err = fixture.client.SignalWithStartWorkflow(ctx, id, "finish", true,
				sdk.StartWorkflowOptions{TaskQueue: fixture.prefix, WorkflowExecutionTimeout: time.Minute}, "gh134-workflow", publicServiceInput{Value: 8, Hold: true})
		} else {
			run, err = fixture.client.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: time.Minute},
				"gh134-workflow", publicServiceInput{Hold: true})
		}
		serviceRequire(t, err, operation+" start")
		if operation == "cancel" {
			serviceRequire(t, fixture.client.CancelWorkflow(ctx, sdk.CancelWorkflowOptions{WorkflowID: id, RunID: run.GetRunID()}), "cancel")
		} else if operation == "terminate" {
			serviceRequire(t, fixture.client.TerminateWorkflow(ctx, sdk.TerminateWorkflowOptions{WorkflowID: id, RunID: run.GetRunID(), Reason: "gh134 controlled termination"}), "terminate")
		}
		err = run.Get(ctx, &value)
		switch operation {
		case "signal-start":
			serviceRequire(t, err, "signal-start result")
			if value != 8 {
				t.Fatal("Signal-with-Start payload changed")
			}
		case "cancel":
			var canceled *sdktemporal.CanceledError
			if !errors.As(err, &canceled) {
				t.Fatal("remote cancellation lost native semantics")
			}
		case "terminate":
			var terminated *sdktemporal.TerminatedError
			if !errors.As(err, &terminated) {
				t.Fatal("remote termination lost native semantics")
			}
		}
		fixture.drain(t)
	}
	action := &sdk.ScheduleWorkflowAction{ID: fixture.prefix + "-action", Workflow: "gh134-workflow", TaskQueue: fixture.prefix,
		Args: []any{publicServiceInput{Value: 42}}, WorkflowExecutionTimeout: time.Minute}
	schedule := fixture.createSchedule(t, ctx, sdk.ScheduleOptions{ID: fixture.prefix + "-schedule", Paused: true,
		Spec: sdk.ScheduleSpec{StartAt: time.Now().UTC().Add(24 * time.Hour), Intervals: []sdk.ScheduleIntervalSpec{{Every: time.Hour}}}, Action: action})
	serviceRequire(t, schedule.Update(ctx, sdk.ScheduleUpdateOptions{DoUpdate: func(input sdk.ScheduleUpdateInput) (*sdk.ScheduleUpdate, error) {
		input.Description.Schedule.State.Note = "gh134 updated"
		return &sdk.ScheduleUpdate{Schedule: &input.Description.Schedule}, nil
	}}), "Schedule update")
	entered, release, completed := make(chan struct{}, 2), make(chan struct{}), make(chan error, 2)
	for _, note := range []string{"gh134 concurrent one", "gh134 concurrent two"} {
		go func() {
			completed <- schedule.Update(ctx, sdk.ScheduleUpdateOptions{DoUpdate: func(input sdk.ScheduleUpdateInput) (*sdk.ScheduleUpdate, error) {
				entered <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				input.Description.Schedule.State.Note = note
				return &sdk.ScheduleUpdate{Schedule: &input.Description.Schedule}, nil
			}})
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			close(release)
			t.Fatal("concurrent Schedule callbacks did not reach the same read boundary")
		}
	}
	close(release)
	for range 2 {
		serviceRequire(t, <-completed, "concurrent native Schedule update")
	}
	concurrent, err := schedule.Describe(ctx)
	serviceRequire(t, err, "concurrent Schedule update result")
	if note := concurrent.Schedule.State.Note; note != "gh134 concurrent one" && note != "gh134 concurrent two" {
		t.Fatal("concurrent Schedule updates did not retain a native last writer")
	}
	t.Log("concurrent Schedule updates both acknowledged; native last writer retained, no CAS guarantee")
	serviceRequire(t, schedule.Unpause(ctx, sdk.ScheduleUnpauseOptions{Note: "gh134 unpaused"}), "Schedule unpause")
	serviceRequire(t, schedule.Pause(ctx, sdk.SchedulePauseOptions{Note: "gh134 paused"}), "Schedule pause")
	serviceRequire(t, schedule.Trigger(ctx, sdk.ScheduleTriggerOptions{}), "Schedule trigger")
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var actionRun *temporal.WorkflowRun
	for actionRun == nil {
		description, err := schedule.Describe(ctx)
		serviceRequire(t, err, "Schedule action observation")
		fixture.drain(t)
		if len(description.Info.RecentActions) > 0 {
			execution := description.Info.RecentActions[0].StartWorkflowResult
			if execution == nil || !strings.HasPrefix(execution.WorkflowID, action.ID) {
				t.Fatal("Schedule action is not an owned Workflow")
			}
			fixture.trackWorkflow(execution.WorkflowID, execution.FirstExecutionRunID)
			actionRun, err = fixture.client.GetWorkflow(execution.WorkflowID, execution.FirstExecutionRunID)
			serviceRequire(t, err, "Schedule Workflow handle")
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("Schedule action was not observed")
		}
	}
	serviceRequire(t, actionRun.Get(ctx, &value), "Schedule Workflow result")
	if value != 42 {
		t.Fatal("Schedule action arguments changed")
	}
	found := false
	for !found {
		serviceRequire(t, fixture.client.WalkSchedules(ctx, sdk.ScheduleListOptions{PageSize: 1, Query: "ScheduleId = '" + schedule.GetID() + "'"},
			func(_ context.Context, entry *sdk.ScheduleListEntry) error {
				if entry.ID != schedule.GetID() {
					return errors.New("exact Schedule filter returned another ID")
				}
				found = true
				return nil
			}), "exact Schedule walk")
		fixture.drain(t)
		if !found {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal("Schedule visibility was not observed")
			}
		}
	}
}

func (fixture *publicServiceFixture) createSchedule(t *testing.T, ctx context.Context, options sdk.ScheduleOptions) *temporal.Schedule {
	t.Helper()
	handle, err := fixture.client.GetSchedule(options.ID)
	serviceRequire(t, err, "Schedule intention")
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		description, err := handle.Describe(cleanup)
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			return
		}
		if err != nil {
			serviceCleanupFailure(t, "owned Schedule cleanup lookup failed: %T", err)
			return
		}
		for _, action := range description.Info.RecentActions {
			if execution := action.StartWorkflowResult; execution != nil && strings.HasPrefix(execution.WorkflowID, fixture.prefix) {
				fixture.trackWorkflow(execution.WorkflowID, execution.FirstExecutionRunID)
			}
		}
		if err := handle.Delete(cleanup); err != nil {
			serviceCleanupFailure(t, "owned Schedule deletion failed: %T", err)
			return
		}
		_, err = handle.Describe(cleanup)
		if !errors.As(err, &missing) {
			serviceCleanupFailure(t, "owned Schedule absence not confirmed: %T", err)
		} else {
			t.Log("deleted Schedule:", options.ID)
		}
		fixture.drain(t)
	})
	created, err := fixture.client.CreateSchedule(ctx, options)
	serviceRequire(t, err, "Schedule creation")
	return created
}

func TestAuthorizedPublicScheduleDST(t *testing.T) {
	fixture := newPublicServiceFixture(t, temporal.NativeOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	year := time.Now().UTC().Year() + 1
	spring := 8 + (7-int(time.Date(year, time.March, 8, 0, 0, 0, 0, time.UTC).Weekday()))%7
	fall := 1 + (7-int(time.Date(year, time.November, 1, 0, 0, 0, 0, time.UTC).Weekday()))%7
	for _, scenario := range []struct {
		name      string
		month     time.Month
		day, hour int
		want      []time.Time
	}{
		{"spring", time.March, spring, 2, []time.Time{time.Date(year, time.March, spring-1, 7, 30, 0, 0, time.UTC), time.Date(year, time.March, spring+1, 6, 30, 0, 0, time.UTC)}},
		{"fall", time.November, fall, 1, []time.Time{time.Date(year, time.November, fall-1, 5, 30, 0, 0, time.UTC), time.Date(year, time.November, fall, 5, 30, 0, 0, time.UTC),
			time.Date(year, time.November, fall, 6, 30, 0, 0, time.UTC), time.Date(year, time.November, fall+1, 6, 30, 0, 0, time.UTC)}},
	} {
		handle := fixture.createSchedule(t, ctx, sdk.ScheduleOptions{ID: fixture.prefix + "-" + scenario.name, Paused: true,
			Spec: sdk.ScheduleSpec{TimeZoneName: "America/New_York", Calendars: []sdk.ScheduleCalendarSpec{{Year: []sdk.ScheduleRange{{Start: year}},
				Month: []sdk.ScheduleRange{{Start: int(scenario.month)}}, DayOfMonth: []sdk.ScheduleRange{{Start: scenario.day - 1, End: scenario.day + 1}},
				Hour: []sdk.ScheduleRange{{Start: scenario.hour}}, Minute: []sdk.ScheduleRange{{Start: 30}}}}},
			Action: &sdk.ScheduleWorkflowAction{ID: fixture.prefix + "-unused", Workflow: "unused-paused-definition", TaskQueue: fixture.prefix, WorkflowExecutionTimeout: time.Second}})
		description, err := handle.Describe(ctx)
		serviceRequire(t, err, "native DST description")
		if !reflect.DeepEqual(description.Info.NextActionTimes, scenario.want) {
			t.Fatalf("native %s DST preview differs from the UTC oracle", scenario.name)
		}
		fixture.drain(t)
	}
}

type publicNexusInput struct {
	ID       string
	Endpoint string
	Value    string
	Hold     bool
}

func publicNexusBackend(ctx workflow.Context, input publicNexusInput) (string, error) {
	if input.Hold {
		if err := workflow.Await(ctx, func() bool { return false }); err != nil {
			return "", err
		}
	}
	return "done:" + input.Value, nil
}

func publicNexusCaller(ctx workflow.Context, input publicNexusInput) (string, error) {
	ready := false
	if err := workflow.SetQueryHandler(ctx, "ready", func() (bool, error) { return ready, nil }); err != nil {
		return "", err
	}
	operationContext, cancel := workflow.WithCancel(ctx)
	defer cancel()
	future := workflow.NewNexusClient(input.Endpoint, "gh134-service").ExecuteOperation(operationContext, "workflow", input,
		workflow.NexusOperationOptions{ScheduleToCloseTimeout: 45 * time.Second, CancellationType: workflow.NexusOperationCancellationTypeWaitCompleted})
	var execution workflow.NexusOperationExecution
	if err := future.GetNexusOperationExecution().Get(ctx, &execution); err != nil {
		return "", err
	}
	ready = true
	if input.Hold {
		var request bool
		workflow.GetSignalChannel(ctx, "cancel-operation").Receive(ctx, &request)
		cancel()
	}
	var result string
	err := future.Get(ctx, &result)
	return result, err
}

func TestAuthorizedPublicWorkflowBackedNexus(t *testing.T) {
	const operatorPrefix = "/temporal.api.operatorservice.v1.OperatorService/"
	fixture := newPublicServiceFixture(t, temporal.NativeOptions{}, operatorPrefix+"CreateNexusEndpoint", operatorPrefix+"ListNexusEndpoints",
		operatorPrefix+"GetNexusEndpoint", operatorPrefix+"DeleteNexusEndpoint")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	endpointID := ""
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		listed, err := fixture.client.OperatorService().ListNexusEndpoints(cleanup, &operatorservice.ListNexusEndpointsRequest{Name: fixture.prefix, PageSize: 1})
		if err != nil || len(listed.GetEndpoints()) > 1 || len(listed.GetNextPageToken()) != 0 {
			serviceCleanupFailure(t, "exact Nexus endpoint cleanup lookup failed: %T", err)
			return
		}
		if len(listed.Endpoints) == 0 {
			return
		}
		endpoint := listed.Endpoints[0]
		if endpoint.GetSpec().GetName() != fixture.prefix || endpoint.GetSpec().GetTarget().GetWorker().GetNamespace() != fixture.namespace ||
			endpoint.GetSpec().GetTarget().GetWorker().GetTaskQueue() != fixture.prefix || endpointID != "" && endpoint.GetId() != endpointID {
			serviceCleanupFailure(t, "Nexus endpoint cleanup ownership mismatch")
			return
		}
		_, err = fixture.client.OperatorService().DeleteNexusEndpoint(cleanup, &operatorservice.DeleteNexusEndpointRequest{Id: endpoint.GetId(), Version: endpoint.GetVersion()})
		if err != nil {
			serviceCleanupFailure(t, "owned Nexus endpoint deletion failed: %T", err)
			return
		}
		_, err = fixture.client.OperatorService().GetNexusEndpoint(cleanup, &operatorservice.GetNexusEndpointRequest{Id: endpoint.GetId()})
		var missing *serviceerror.NotFound
		if !errors.As(err, &missing) {
			serviceCleanupFailure(t, "owned Nexus endpoint absence not confirmed: %T", err)
		} else {
			t.Log("deleted Nexus endpoint:", fixture.prefix, endpoint.GetId())
		}
		fixture.drain(t)
	})
	created, err := fixture.client.OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{Spec: &nexuspb.EndpointSpec{
		Name: fixture.prefix, Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{Namespace: fixture.namespace, TaskQueue: fixture.prefix}}}}})
	if created != nil {
		endpointID = created.GetEndpoint().GetId()
	}
	serviceRequire(t, err, "Nexus endpoint creation")
	service := nexus.NewService("gh134-service")
	serviceRequire(t, service.Register(temporalnexus.NewWorkflowRunOperation("workflow", publicNexusBackend,
		func(_ context.Context, input publicNexusInput, _ nexus.StartOperationOptions) (sdk.StartWorkflowOptions, error) {
			return sdk.StartWorkflowOptions{ID: input.ID, WorkflowExecutionTimeout: time.Minute,
				WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING}, nil
		})), "Nexus service registration")
	fixture.startCustomWorker(t, ctx, temporal.WorkerSpec{TaskQueue: fixture.prefix, MaxHandlers: 4,
		Options: nativeworker.Options{MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2,
			MaxConcurrentNexusTaskExecutionSize: 2, MaxConcurrentNexusTaskPollers: 2, WorkerStopTimeout: time.Second},
		Workflows: []temporal.WorkflowRegistration{{Definition: publicNexusBackend, Options: workflow.RegisterOptions{Name: "gh134-nexus-backend"}},
			{Definition: publicNexusCaller, Options: workflow.RegisterOptions{Name: "gh134-nexus-caller"}}}, NexusServices: []*nexus.Service{service}})
	for _, hold := range []bool{false, true} {
		suffix := "-complete"
		if hold {
			suffix = "-cancel"
		}
		id, backend := fixture.prefix+suffix, fixture.prefix+suffix+"-backend"
		fixture.trackWorkflow(id, "")
		fixture.trackWorkflow(backend, "")
		run, err := fixture.client.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: time.Minute},
			"gh134-nexus-caller", publicNexusInput{ID: backend, Endpoint: fixture.prefix, Value: "value", Hold: hold})
		serviceRequire(t, err, "Nexus caller start")
		if hold {
			ticker := time.NewTicker(100 * time.Millisecond)
			for {
				var ready bool
				serviceRequire(t, fixture.client.QueryWorkflow(ctx, id, run.GetRunID(), "ready", &ready), "Nexus start observation")
				fixture.drain(t)
				if ready {
					break
				}
				select {
				case <-ticker.C:
				case <-ctx.Done():
					ticker.Stop()
					t.Fatal("Nexus asynchronous start was not observed")
				}
			}
			ticker.Stop()
			serviceRequire(t, fixture.client.SignalWorkflow(ctx, id, run.GetRunID(), "cancel-operation", true), "Nexus cancellation signal")
		}
		var result string
		err = run.Get(ctx, &result)
		if hold {
			if !sdktemporal.IsCanceledError(err) {
				t.Fatalf("Nexus cancellation lost native cause: %T", err)
			}
		} else {
			serviceRequire(t, err, "Nexus asynchronous completion")
			if result != "done:value" {
				t.Fatal("Nexus callback result changed")
			}
		}
		fixture.drain(t)
	}
	if fixture.nexusLinks.Load() == 0 {
		t.Fatal("Workflow-backed Nexus callback lost its native caller link")
	}
}

func publicDeploymentWorkflow(ctx workflow.Context) (string, error) {
	return workflow.GetInfo(ctx).GetCurrentBuildID(), nil
}

func TestAuthorizedPublicWorkerDeploymentRouting(t *testing.T) {
	methods := []string{"DescribeWorkerDeployment", "SetWorkerDeploymentCurrentVersion", "SetWorkerDeploymentRampingVersion",
		"DescribeWorkerDeploymentVersion", "DeleteWorkerDeploymentVersion", "DeleteWorkerDeployment"}
	for index := range methods {
		methods[index] = publicServiceWorkflowPrefix + methods[index]
	}
	fixture := newPublicServiceFixture(t, temporal.NativeOptions{}, methods...)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	handle, err := fixture.client.GetWorkerDeployment(fixture.prefix)
	serviceRequire(t, err, "deployment handle")
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 6*time.Minute)
		defer stop()
		for _, worker := range fixture.workers {
			if err := worker.Stop(cleanup); err != nil {
				serviceCleanupFailure(t, "versioned Worker cleanup failed: %T", err)
				return
			}
		}
		description, err := handle.Describe(cleanup, sdk.WorkerDeploymentDescribeOptions{})
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			return
		}
		if err != nil || description.Info.Name != fixture.prefix {
			serviceCleanupFailure(t, "owned deployment cleanup identity unavailable: %T", err)
			return
		}
		if _, err := handle.SetRampingVersion(cleanup, sdk.WorkerDeploymentSetRampingVersionOptions{}); err != nil {
			serviceCleanupFailure(t, "owned deployment ramp cleanup failed: %T", err)
			return
		}
		if _, err := handle.SetCurrentVersion(cleanup, sdk.WorkerDeploymentSetCurrentVersionOptions{}); err != nil {
			serviceCleanupFailure(t, "owned deployment current cleanup failed: %T", err)
			return
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for _, build := range []string{"one", "two"} {
			for {
				_, err := handle.DeleteVersion(cleanup, sdk.WorkerDeploymentDeleteVersionOptions{BuildID: build, SkipDrainage: true})
				fixture.drain(t)
				if err == nil || errors.As(err, &missing) {
					break
				}
				var pending *serviceerror.FailedPrecondition
				if !errors.As(err, &pending) {
					serviceCleanupFailure(t, "owned deployment version deletion failed: %T", err)
					return
				}
				select {
				case <-ticker.C:
				case <-cleanup.Done():
					serviceCleanupFailure(t, "owned deployment version still has native pollers")
					return
				}
			}
		}
		if _, err := fixture.client.DeleteWorkerDeployment(cleanup, sdk.WorkerDeploymentDeleteOptions{Name: fixture.prefix}); err != nil {
			serviceCleanupFailure(t, "owned deployment deletion failed: %T", err)
			return
		}
		_, err = handle.Describe(cleanup, sdk.WorkerDeploymentDescribeOptions{})
		if !errors.As(err, &missing) {
			serviceCleanupFailure(t, "owned deployment absence not confirmed: %T", err)
		} else {
			t.Log("deleted Worker Deployment and versions:", fixture.prefix)
		}
		fixture.drain(t)
	})
	for _, build := range []string{"one", "two"} {
		fixture.startCustomWorker(t, ctx, temporal.WorkerSpec{TaskQueue: fixture.prefix, MaxHandlers: 2,
			Options: nativeworker.Options{LocalActivityWorkerOnly: true, MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2,
				WorkerStopTimeout: time.Second, DeploymentOptions: nativeworker.DeploymentOptions{UseVersioning: true,
					Version: nativeworker.WorkerDeploymentVersion{DeploymentName: fixture.prefix, BuildID: build}}},
			Workflows: []temporal.WorkflowRegistration{{Definition: publicDeploymentWorkflow,
				Options: workflow.RegisterOptions{Name: "gh134-versioned", VersioningBehavior: workflow.VersioningBehaviorPinned}}}})
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var description sdk.WorkerDeploymentDescribeResponse
	for {
		description, err = handle.Describe(ctx, sdk.WorkerDeploymentDescribeOptions{})
		fixture.drain(t)
		if err == nil && len(description.Info.VersionSummaries) == 2 {
			break
		}
		var missing *serviceerror.NotFound
		if err != nil && !errors.As(err, &missing) {
			serviceRequire(t, err, "deployment discovery")
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("owned deployment versions were not discovered")
		}
	}
	current, err := handle.SetCurrentVersion(ctx, sdk.WorkerDeploymentSetCurrentVersionOptions{BuildID: "one", ConflictToken: description.ConflictToken})
	serviceRequire(t, err, "current deployment routing")
	_, err = handle.SetRampingVersion(ctx, sdk.WorkerDeploymentSetRampingVersionOptions{BuildID: "two", Percentage: 100, ConflictToken: description.ConflictToken})
	var stale *serviceerror.FailedPrecondition
	if !errors.As(err, &stale) {
		t.Fatal("deployment stale conflict token was not rejected")
	}
	for _, expected := range []string{"one", "two"} {
		if expected == "two" {
			_, err := handle.SetRampingVersion(ctx, sdk.WorkerDeploymentSetRampingVersionOptions{BuildID: "two", Percentage: 100, ConflictToken: current.ConflictToken})
			serviceRequire(t, err, "ramping deployment routing")
		}
		id := fixture.prefix + "-" + expected
		fixture.trackWorkflow(id, "")
		run, err := fixture.client.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: time.Minute}, "gh134-versioned")
		serviceRequire(t, err, "versioned Workflow start")
		var actual string
		serviceRequire(t, run.Get(ctx, &actual), "versioned Workflow result")
		if actual != expected {
			t.Fatalf("native deployment routing selected %q instead of %q", actual, expected)
		}
		fixture.drain(t)
	}
}

type publicPayloadStore struct {
	mu        sync.Mutex
	payloads  map[string]*commonpb.Payload
	bytes     int
	retrieved int
}

func (*publicPayloadStore) Name() string { return "gh134-memory" }
func (*publicPayloadStore) Type() string { return "bounded-memory-fixture" }
func (store *publicPayloadStore) Store(ctx converter.StorageDriverStoreContext, payloads []*commonpb.Payload) ([]converter.StorageDriverClaim, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Context.Err(); err != nil {
		return nil, err
	}
	claims := make([]converter.StorageDriverClaim, len(payloads))
	for index, payload := range payloads {
		size := proto.Size(payload)
		if size > 64<<10 || store.bytes+size > 4<<20 {
			return nil, errors.New("bounded fixture storage exhausted")
		}
		id := strconv.Itoa(len(store.payloads))
		store.payloads[id] = proto.Clone(payload).(*commonpb.Payload)
		store.bytes += size
		claims[index] = converter.StorageDriverClaim{ClaimData: map[string]string{"id": id}}
	}
	return claims, nil
}
func (store *publicPayloadStore) Retrieve(ctx converter.StorageDriverRetrieveContext, claims []converter.StorageDriverClaim) ([]*commonpb.Payload, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Context.Err(); err != nil {
		return nil, err
	}
	result := make([]*commonpb.Payload, len(claims))
	for index, claim := range claims {
		payload := store.payloads[claim.ClaimData["id"]]
		if payload == nil {
			return nil, errors.New("unknown fixture payload claim")
		}
		result[index] = proto.Clone(payload).(*commonpb.Payload)
		store.retrieved++
	}
	return result, nil
}

type publicCodecContexts struct{ workflow, activity, local, rejected atomic.Int32 }
type publicVersionCodec struct {
	version string
	legacy  bool
	seen    *publicCodecContexts
}

func (codec publicVersionCodec) WithSerializationContext(scope converter.SerializationContext) converter.PayloadCodec {
	switch value := scope.(type) {
	case converter.WorkflowSerializationContext:
		codec.seen.workflow.Add(1)
	case converter.ActivitySerializationContext:
		codec.seen.activity.Add(1)
		if value.IsLocal {
			codec.seen.local.Add(1)
		}
	}
	return codec
}
func (codec publicVersionCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for index, payload := range payloads {
		data, err := proto.Marshal(payload)
		if err != nil {
			return nil, err
		}
		result[index] = &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/gh134-fixture"), "version": []byte(codec.version)}, Data: data}
	}
	return result, nil
}
func (codec publicVersionCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for index, payload := range payloads {
		if string(payload.Metadata["encoding"]) != "binary/gh134-fixture" {
			result[index] = payload
			continue
		}
		version := string(payload.Metadata["version"])
		if version != codec.version && !(codec.legacy && version == "1") {
			codec.seen.rejected.Add(1)
			return nil, errors.New("unsupported fixture reader version")
		}
		result[index] = &commonpb.Payload{}
		if err := proto.Unmarshal(payload.Data, result[index]); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func publicSerializationOptions(version string, legacy bool, store *publicPayloadStore, seen *publicCodecContexts) temporal.NativeOptions {
	data := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), publicVersionCodec{version: version, legacy: legacy, seen: seen})
	return temporal.NativeOptions{DataConverter: data, FailureConverter: sdktemporal.NewDefaultFailureConverter(sdktemporal.DefaultFailureConverterOptions{
		DataConverter: data, EncodeCommonAttributes: true}), ExternalStorage: converter.ExternalStorage{Drivers: []converter.StorageDriver{store}, PayloadSizeThreshold: 1}}
}

func publicSerializationWorkflow(ctx workflow.Context, input string) (string, error) {
	if ctx.Value(publicPropagationKey{}) != "fixture-propagation" {
		return "", errors.New("workflow propagation changed")
	}
	workflow.GetLogger(ctx).Info("gh134 native workflow observation")
	workflow.GetMetricsHandler(ctx).Counter("gh134_fixture_workflow").Inc(1)
	ready := false
	if err := workflow.SetQueryHandler(ctx, "ready", func() (bool, error) { return ready, nil }); err != nil {
		return "", err
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	var value string
	if err := workflow.ExecuteActivity(ctx, "gh134-serialize-echo", input).Get(ctx, &value); err != nil {
		return "", err
	}
	local := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: 10 * time.Second})
	if err := workflow.ExecuteLocalActivity(local, "gh134-serialize-echo", value).Get(ctx, &value); err != nil {
		return "", err
	}
	ready = true
	advance := workflow.GetSignalChannel(ctx, "advance")
	if err := workflow.Await(ctx, func() bool { return advance.Len() > 0 }); err != nil {
		return "", err
	}
	var signal bool
	advance.Receive(ctx, &signal)
	workflow.GetLogger(ctx).Info("gh134 native workflow resumed observation")
	err := workflow.ExecuteActivity(ctx, "gh134-serialize-failure").Get(ctx, nil)
	var application *sdktemporal.ApplicationError
	if !errors.As(err, &application) || !application.NonRetryable() || application.Type() != "gh134-failure" {
		return "", errors.New("native failure semantics changed")
	}
	var detail string
	if err := application.Details(&detail); err != nil {
		return "", err
	}
	return value + ":" + detail, nil
}

func TestAuthorizedPublicCodecReaderRestartAndExternalStorage(t *testing.T) {
	store := &publicPayloadStore{payloads: make(map[string]*commonpb.Payload)}
	seen := &publicCodecContexts{}
	observations := &publicNativeObservations{}
	old := newPublicServiceFixture(t, publicSerializationHooks(t, publicSerializationOptions("1", false, store, seen), observations))
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), publicPropagationKey{}, "fixture-propagation"), 2*time.Minute)
	defer cancel()
	spec := temporal.WorkerSpec{TaskQueue: old.prefix, MaxHandlers: 4,
		Options: nativeworker.Options{MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2,
			MaxConcurrentActivityTaskPollers: 2, WorkerStopTimeout: time.Second},
		Workflows: []temporal.WorkflowRegistration{{Definition: publicSerializationWorkflow, Options: workflow.RegisterOptions{Name: "gh134-serialization"}}},
		Activities: []temporal.ActivityRegistration{
			{Definition: func(ctx context.Context, value string) (string, error) {
				if ctx.Value(publicPropagationKey{}) != "fixture-propagation" {
					return "", errors.New("activity propagation changed")
				}
				activity.GetLogger(ctx).Info("gh134 native activity observation")
				activity.GetMetricsHandler(ctx).Counter("gh134_fixture_activity").Inc(1)
				return value + ":echo", nil
			}, Options: activity.RegisterOptions{Name: "gh134-serialize-echo"}},
			{Definition: func(context.Context) error {
				return sdktemporal.NewNonRetryableApplicationError("controlled fixture failure", "gh134-failure", nil, "detail")
			},
				Options: activity.RegisterOptions{Name: "gh134-serialize-failure"}},
		}}
	first := old.startCustomWorker(t, ctx, spec)
	id := old.prefix + "-serialization"
	old.trackWorkflow(id, "")
	run, err := old.client.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{ID: id, TaskQueue: old.prefix, WorkflowExecutionTimeout: time.Minute}, "gh134-serialization", "input")
	serviceRequire(t, err, "legacy codec workflow")
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		var ready bool
		serviceRequire(t, old.client.QueryWorkflow(ctx, id, run.GetRunID(), "ready", &ready), "legacy reader query")
		old.drain(t)
		if ready {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("legacy serialization workflow did not reach its restart point")
		}
	}
	serviceRequire(t, first.Stop(ctx), "legacy Worker join")
	beforeRestart := observations.logs.Load()
	compatibleOptions := publicSerializationHooks(t, publicSerializationOptions("2", true, store, seen), observations)
	reader := newPublicServiceFixture(t, compatibleOptions)
	reader.startCustomWorker(t, ctx, spec)
	serviceRequire(t, reader.client.SignalWorkflow(ctx, id, run.GetRunID(), "advance", true), "compatible reader signal")
	retained, err := reader.client.GetWorkflow(id, run.GetRunID())
	serviceRequire(t, err, "compatible result handle")
	var value string
	serviceRequire(t, retained.Get(ctx, &value), "compatible reader result")
	if value != "input:echo:echo:detail" {
		t.Fatal("reader restart, failure details or external payloads changed the result")
	}
	if err := run.Get(ctx, &value); err == nil {
		t.Fatal("legacy reader accepted the new unsupported writer format")
	}
	var history historypb.History
	serviceRequire(t, reader.client.WalkHistory(ctx, id, run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
		func(_ context.Context, event *historypb.HistoryEvent) error {
			history.Events = append(history.Events, event)
			return nil
		}), "serialized history")
	replayer, err := nativeworker.NewWorkflowReplayerWithOptions(nativeworker.WorkflowReplayerOptions{
		DataConverter: compatibleOptions.DataConverter, FailureConverter: compatibleOptions.FailureConverter,
		ContextPropagators: compatibleOptions.ContextPropagators, ExternalStorage: compatibleOptions.ExternalStorage})
	serviceRequire(t, err, "compatible replayer")
	replayer.RegisterWorkflowWithOptions(publicSerializationWorkflow, workflow.RegisterOptions{Name: "gh134-serialization"})
	serviceRequire(t, replayer.ReplayWorkflowHistoryWithOptions(publicServiceLogger{}, &history,
		nativeworker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: id, RunID: run.GetRunID()}}), "compatible serialized replay")
	strict := publicSerializationOptions("2", false, store, seen)
	rejecting, err := nativeworker.NewWorkflowReplayerWithOptions(nativeworker.WorkflowReplayerOptions{
		DataConverter: strict.DataConverter, FailureConverter: strict.FailureConverter, ContextPropagators: compatibleOptions.ContextPropagators, ExternalStorage: strict.ExternalStorage})
	serviceRequire(t, err, "strict replayer")
	rejecting.RegisterWorkflowWithOptions(publicSerializationWorkflow, workflow.RegisterOptions{Name: "gh134-serialization"})
	rejectedBefore := seen.rejected.Load()
	if err := rejecting.ReplayWorkflowHistoryWithOptions(publicServiceLogger{}, &history,
		nativeworker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: id, RunID: run.GetRunID()}}); err == nil || seen.rejected.Load() == rejectedBefore {
		t.Fatal("incompatible reader replay did not fail at the intended version boundary")
	}
	store.mu.Lock()
	stored, retrieved := len(store.payloads), store.retrieved
	store.mu.Unlock()
	if stored == 0 || retrieved == 0 || seen.workflow.Load() == 0 || seen.activity.Load() == 0 || seen.local.Load() == 0 {
		t.Fatal("external storage or native serialization context hooks were bypassed")
	}
	if beforeRestart == 0 || observations.logs.Load() <= beforeRestart || observations.metrics.Load() == 0 ||
		observations.workflowSpans.Load() < 2 || observations.activitySpans.Load() < 2 || observations.finished.Load() == 0 || observations.parents.Load() == 0 {
		t.Fatal("native logger, metrics or tracing did not cross the Worker restart")
	}
	old.drain(t)
	reader.drain(t)
}

type publicPropagationKey struct{}
type publicTraceKey struct{}
type publicPropagator struct{}

func (publicPropagator) Inject(ctx context.Context, writer workflow.HeaderWriter) error {
	return publicInjectPropagation(ctx.Value(publicPropagationKey{}), writer)
}
func (publicPropagator) InjectFromWorkflow(ctx workflow.Context, writer workflow.HeaderWriter) error {
	return publicInjectPropagation(ctx.Value(publicPropagationKey{}), writer)
}
func publicInjectPropagation(value any, writer workflow.HeaderWriter) error {
	if value == nil {
		return nil
	}
	payload, err := converter.GetDefaultDataConverter().ToPayload(value)
	if err == nil {
		writer.Set("gh134-propagation", payload)
	}
	return err
}
func (publicPropagator) Extract(ctx context.Context, reader workflow.HeaderReader) (context.Context, error) {
	if payload, ok := reader.Get("gh134-propagation"); ok {
		var value string
		if err := converter.GetDefaultDataConverter().FromPayload(payload, &value); err != nil {
			return ctx, err
		}
		ctx = context.WithValue(ctx, publicPropagationKey{}, value)
	}
	return ctx, nil
}
func (publicPropagator) ExtractToWorkflow(ctx workflow.Context, reader workflow.HeaderReader) (workflow.Context, error) {
	if payload, ok := reader.Get("gh134-propagation"); ok {
		var value string
		if err := converter.GetDefaultDataConverter().FromPayload(payload, &value); err != nil {
			return ctx, err
		}
		ctx = workflow.WithValue(ctx, publicPropagationKey{}, value)
	}
	return ctx, nil
}

type publicNativeObservations struct {
	publicServiceLogger
	interceptor.BaseTracer
	logs, metrics, workflowSpans, activitySpans, finished, parents atomic.Int32
}

func (observations *publicNativeObservations) Info(message string, _ ...any) {
	if strings.HasPrefix(message, "gh134 native") {
		observations.logs.Add(1)
	}
}
func (observations *publicNativeObservations) WithTags(map[string]string) sdk.MetricsHandler {
	return observations
}
func (observations *publicNativeObservations) Counter(string) sdk.MetricsCounter { return observations }
func (observations *publicNativeObservations) Gauge(string) sdk.MetricsGauge     { return observations }
func (observations *publicNativeObservations) Timer(string) sdk.MetricsTimer     { return observations }
func (observations *publicNativeObservations) Inc(int64)                         { observations.metrics.Add(1) }
func (observations *publicNativeObservations) Update(float64)                    { observations.metrics.Add(1) }
func (observations *publicNativeObservations) Record(time.Duration)              { observations.metrics.Add(1) }
func (*publicNativeObservations) Options() interceptor.TracerOptions {
	return interceptor.TracerOptions{SpanContextKey: publicTraceKey{}, HeaderKey: "gh134-trace"}
}
func (observations *publicNativeObservations) UnmarshalSpan(map[string]string) (interceptor.TracerSpanRef, error) {
	return observations, nil
}
func (*publicNativeObservations) MarshalSpan(interceptor.TracerSpan) (map[string]string, error) {
	return map[string]string{"fixture": "trace"}, nil
}
func (*publicNativeObservations) SpanFromContext(ctx context.Context) interceptor.TracerSpan {
	span, _ := ctx.Value(publicTraceKey{}).(interceptor.TracerSpan)
	return span
}
func (*publicNativeObservations) ContextWithSpan(ctx context.Context, span interceptor.TracerSpan) context.Context {
	return context.WithValue(ctx, publicTraceKey{}, span)
}
func (observations *publicNativeObservations) StartSpan(options *interceptor.TracerStartSpanOptions) (interceptor.TracerSpan, error) {
	if options.Parent != nil {
		observations.parents.Add(1)
	}
	switch options.Operation {
	case "RunWorkflow":
		observations.workflowSpans.Add(1)
	case "RunActivity":
		observations.activitySpans.Add(1)
	}
	return observations, nil
}
func (observations *publicNativeObservations) Finish(*interceptor.TracerFinishSpanOptions) {
	observations.finished.Add(1)
}

func publicSerializationHooks(t *testing.T, options temporal.NativeOptions, observations *publicNativeObservations) temporal.NativeOptions {
	t.Helper()
	tracing := interceptor.NewTracingInterceptor(observations)
	options.Logger, options.MetricsHandler = observations, observations
	options.ContextPropagators = []workflow.ContextPropagator{publicPropagator{}}
	options.Interceptors = []interceptor.ClientInterceptor{tracing}
	return options
}

type publicLostAcknowledgement struct {
	interceptor.ClientInterceptorBase
}
type publicLostAcknowledgementOutbound struct {
	interceptor.ClientOutboundInterceptorBase
}

func (*publicLostAcknowledgement) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return &publicLostAcknowledgementOutbound{interceptor.ClientOutboundInterceptorBase{Next: next}}
}
func (outbound *publicLostAcknowledgementOutbound) ExecuteWorkflow(ctx context.Context, input *interceptor.ClientExecuteWorkflowInput) (sdk.WorkflowRun, error) {
	run, err := outbound.Next.ExecuteWorkflow(ctx, input)
	if err == nil && strings.HasSuffix(input.Options.ID, "-lost-ack") {
		return nil, context.DeadlineExceeded
	}
	return run, err
}

func TestAuthorizedPublicAcquisitionIdentityAndGrants(t *testing.T) {
	fixture := newPublicServiceFixtureMode(t, temporal.NativeOptions{Interceptors: []interceptor.ClientInterceptor{&publicLostAcknowledgement{}}}, true, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	fixture.startWorker(t, ctx, false)
	shared := newPublicServiceFixtureMode(t, temporal.NativeOptions{}, false, fixture.client, publicServiceWorkflowPrefix+"UpdateWorkflowExecutionOptions")
	_, err := shared.client.WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	serviceRequire(t, err, "shared namespace client acquisition")
	_, err = shared.client.WorkflowService().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: fixture.prefix + "-unauthorized-namespace", Query: "WorkflowId = '" + fixture.prefix + "-missing'"})
	if !errors.Is(err, temporal.ErrAuthority) {
		t.Fatal("exact grant crossed its configured namespace")
	}
	_, err = shared.client.OperatorService().ListNexusEndpoints(ctx, &operatorservice.ListNexusEndpointsRequest{Name: fixture.prefix, PageSize: 1})
	if !errors.Is(err, temporal.ErrAuthority) {
		t.Fatal("ungranted OperatorService request was not refused")
	}
	shared.drain(t)
	id := fixture.prefix + "-lost-ack"
	fixture.trackWorkflow(id, "")
	run, err := fixture.client.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: time.Minute},
		"gh134-workflow", publicServiceInput{Value: 29, Hold: true})
	if err == nil || run != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("controlled lost acknowledgement was not returned unchanged")
	}
	description, err := shared.client.DescribeWorkflowExecution(ctx, id, "")
	serviceRequire(t, err, "unknown-outcome exact identity recovery")
	execution := description.GetWorkflowExecutionInfo().GetExecution()
	if execution.GetWorkflowId() != id || execution.GetRunId() == "" {
		t.Fatal("recovered execution identity differs from the submitted identity")
	}
	fixture.trackWorkflow(id, execution.RunId)
	_, err = shared.client.UpdateWorkflowExecutionOptions(ctx, sdk.UpdateWorkflowExecutionOptionsRequest{WorkflowId: id, RunId: execution.RunId,
		WorkflowExecutionOptionsChanges: sdk.WorkflowExecutionOptionsChanges{VersioningOverride: &sdk.VersioningOverrideChange{}}})
	serviceRequire(t, err, "experimental execution-option override removal")
	serviceRequire(t, shared.client.SignalWorkflow(ctx, id, execution.RunId, "finish", true), "recovered Workflow completion")
	recovered, err := shared.client.GetWorkflow(id, execution.RunId)
	serviceRequire(t, err, "recovered handle")
	var value int
	serviceRequire(t, recovered.Get(ctx, &value), "recovered result")
	if value != 29 {
		t.Fatal("unknown acknowledgement recovery changed execution")
	}
	fixture.drain(t)
	shared.drain(t)
}

func publicSessionWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second, HeartbeatTimeout: 5 * time.Second})
	session, err := workflow.CreateSession(ctx, &workflow.SessionOptions{CreationTimeout: 15 * time.Second, ExecutionTimeout: 30 * time.Second, HeartbeatTimeout: 5 * time.Second})
	if err != nil {
		return "", err
	}
	defer workflow.CompleteSession(session)
	info := workflow.GetSessionInfo(session)
	if info == nil || info.SessionID == "" || info.SessionState != workflow.SessionStateOpen {
		return "", errors.New("session was not established")
	}
	var first, second string
	if err := workflow.ExecuteActivity(session, "gh134-session-worker").Get(session, &first); err != nil {
		return "", err
	}
	if err := workflow.ExecuteActivity(session, "gh134-session-worker").Get(session, &second); err != nil {
		return "", err
	}
	if first != second || first == "" {
		return "", errors.New("session did not retain its worker")
	}
	return "session-completed", nil
}

func publicAsyncWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second, HeartbeatTimeout: 10 * time.Second,
		RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	var value string
	err := workflow.ExecuteActivity(ctx, "gh134-async").Get(ctx, &value)
	return value, err
}

func publicDynamicWorkflow(ctx workflow.Context, values converter.EncodedValues) (string, error) {
	var value string
	if err := values.Get(&value); err != nil {
		return "", err
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
	err := workflow.ExecuteActivity(ctx, "gh134-dynamic-activity", value).Get(ctx, &value)
	return value, err
}

func TestAuthorizedPublicSessionsAsyncPluginsAndDynamic(t *testing.T) {
	var configured, started, stopped atomic.Int32
	plugin, err := sdktemporal.NewSimplePlugin(sdktemporal.SimplePluginOptions{Name: "gh134-plugin",
		ConfigureWorker: func(context.Context, nativeworker.PluginConfigureWorkerOptions) error { configured.Add(1); return nil },
		RunContextBefore: func(_ context.Context, options sdktemporal.SimplePluginRunContextBeforeOptions) error {
			started.Add(1)
			options.Registry.RegisterWorkflowWithOptions(publicSessionWorkflow, workflow.RegisterOptions{Name: "gh134-session"})
			return nil
		},
		RunContextAfter: func(context.Context, sdktemporal.SimplePluginRunContextAfterOptions) { stopped.Add(1) }})
	serviceRequire(t, err, "native plugin construction")
	fixture := newPublicServiceFixture(t, temporal.NativeOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tokens := make(chan []byte, 1)
	worker := fixture.startCustomWorker(t, ctx, temporal.WorkerSpec{TaskQueue: fixture.prefix, MaxHandlers: 6,
		Options: nativeworker.Options{MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2,
			MaxConcurrentActivityTaskPollers: 2, WorkerStopTimeout: time.Second, EnableSessionWorker: true,
			MaxConcurrentSessionExecutionSize: 1, Plugins: []nativeworker.Plugin{plugin}},
		Workflows: []temporal.WorkflowRegistration{{Definition: publicAsyncWorkflow, Options: workflow.RegisterOptions{Name: "gh134-async-workflow"}}},
		Activities: []temporal.ActivityRegistration{
			{Definition: func(ctx context.Context) (string, error) { return activity.GetInfo(ctx).TaskQueue, nil }, Options: activity.RegisterOptions{Name: "gh134-session-worker"}},
			{Definition: func(ctx context.Context) (string, error) {
				select {
				case tokens <- append([]byte(nil), activity.GetInfo(ctx).TaskToken...):
					return "", activity.ErrResultPending
				case <-ctx.Done():
					return "", ctx.Err()
				}
			}, Options: activity.RegisterOptions{Name: "gh134-async"}},
		},
		DynamicWorkflow: publicDynamicWorkflow,
		DynamicActivity: func(_ context.Context, values converter.EncodedValues) (string, error) {
			var value string
			if err := values.Get(&value); err != nil {
				return "", err
			}
			return "dynamic:" + value, nil
		}})
	for _, scenario := range []struct{ name, expected string }{{"session", "session-completed"}, {"async-workflow", "asynchronous"}, {"dynamic-workflow", "dynamic:input"}} {
		id := fixture.prefix + "-" + scenario.name
		fixture.trackWorkflow(id, "")
		var arguments []any
		if scenario.name == "dynamic-workflow" {
			arguments = []any{"input"}
		}
		run, err := fixture.client.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: time.Minute}, "gh134-"+scenario.name, arguments...)
		serviceRequire(t, err, scenario.name+" start")
		if scenario.name == "async-workflow" {
			var token []byte
			select {
			case token = <-tokens:
			case <-ctx.Done():
				t.Fatal("asynchronous Activity token was not observed")
			}
			serviceRequire(t, fixture.client.RecordActivityHeartbeat(ctx, sdk.RecordActivityHeartbeatOptions{TaskToken: token, Details: []any{"heartbeat"}}), "async heartbeat")
			serviceRequire(t, fixture.client.CompleteActivity(ctx, sdk.CompleteActivityOptions{TaskToken: token, Result: "asynchronous"}), "async completion")
		}
		var value string
		serviceRequire(t, run.Get(ctx, &value), scenario.name+" result")
		if value != scenario.expected {
			t.Fatal("native Session, asynchronous or dynamic result changed")
		}
		fixture.drain(t)
	}
	serviceRequire(t, worker.Stop(ctx), "plugin Worker join")
	fixture.drain(t)
	if configured.Load() != 1 || started.Load() != 1 || stopped.Load() != 1 || fixture.asyncRecords.Load() != 1 {
		t.Fatal("plugin lifecycle or asynchronous callback evidence was incomplete")
	}
	invalid, err := fixture.client.StartWorker(ctx, ctx, temporal.WorkerSpec{TaskQueue: fixture.prefix + "-invalid-session", MaxHandlers: 2,
		Options: nativeworker.Options{EnableSessionWorker: true, UseBuildIDForVersioning: true, BuildID: fixture.prefix + "-incompatible-session"}})
	if invalid != nil {
		fixture.workers = append(fixture.workers, invalid)
	}
	if err == nil {
		t.Fatal("legacy build-ID versioned Worker accepted unsupported native Sessions")
	}
	if !errors.Is(err, temporal.ErrInput) || invalid != nil {
		t.Fatal("versioned Session refusal did not occur at the explicit Adapter option boundary")
	}
	fixture.drain(t)
}

type publicPartialStartPlugin struct {
	nativeworker.PluginBase
	starts, stops atomic.Int32
	cause         error
}

func (*publicPartialStartPlugin) Name() string { return "gh134-partial-start" }
func (plugin *publicPartialStartPlugin) StartWorker(ctx context.Context, options nativeworker.PluginStartWorkerOptions, next func(context.Context, nativeworker.PluginStartWorkerOptions) error) error {
	if err := next(ctx, options); err != nil {
		return err
	}
	plugin.starts.Add(1)
	return plugin.cause
}
func (plugin *publicPartialStartPlugin) StopWorker(ctx context.Context, options nativeworker.PluginStopWorkerOptions, next func(context.Context, nativeworker.PluginStopWorkerOptions)) {
	plugin.stops.Add(1)
	next(ctx, options)
}

type publicFinalizationConverter struct {
	converter.DataConverter
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (codec *publicFinalizationConverter) ToPayloads(values ...any) (*commonpb.Payloads, error) {
	for _, value := range values {
		if text, ok := value.(string); ok && text == "gh134-held-finalization" {
			codec.once.Do(func() { close(codec.entered) })
			<-codec.release
		}
	}
	return codec.DataConverter.ToPayloads(values...)
}

func TestAuthorizedPublicPartialStartAndFinalizationJoin(t *testing.T) {
	codec := &publicFinalizationConverter{DataConverter: converter.GetDefaultDataConverter(), entered: make(chan struct{}), release: make(chan struct{})}
	fixture := newPublicServiceFixture(t, temporal.NativeOptions{DataConverter: codec})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(codec.release) }) })
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	plugin := &publicPartialStartPlugin{cause: errors.New("controlled plugin failure after native start")}
	partial, err := fixture.client.StartWorker(ctx, ctx, temporal.WorkerSpec{TaskQueue: fixture.prefix, MaxHandlers: 2,
		Options: nativeworker.Options{MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2, LocalActivityWorkerOnly: true,
			Plugins: []nativeworker.Plugin{plugin}}, Workflows: []temporal.WorkflowRegistration{{Definition: publicServiceWorkflow}}})
	if partial != nil {
		fixture.workers = append(fixture.workers, partial)
	}
	if err == nil || partial == nil {
		t.Fatal("partial native startup did not retain explicit Worker cleanup")
	}
	fixture.expectedStops[partial] = plugin.cause
	if !errors.Is(err, plugin.cause) || !errors.Is(partial.Stop(ctx), plugin.cause) {
		t.Fatal("partial-start return or cleanup lost its exact startup failure")
	}
	if !partial.Status().Joined || plugin.starts.Load() != 1 || plugin.stops.Load() != 1 {
		t.Fatal("partial startup plugin continuation was not joined exactly once")
	}
	worker := fixture.startCustomWorker(t, ctx, temporal.WorkerSpec{TaskQueue: fixture.prefix + "-activity", MaxHandlers: 2,
		Options:    nativeworker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityTaskPollers: 2, WorkerStopTimeout: time.Millisecond},
		Activities: []temporal.ActivityRegistration{{Definition: func(context.Context) (string, error) { return "gh134-held-finalization", nil }, Options: activity.RegisterOptions{Name: "gh134-finalization"}}}})
	id := fixture.prefix + "-finalization"
	intention, err := fixture.client.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: id})
	serviceRequire(t, err, "finalization Activity identity")
	fixture.activities = append(fixture.activities, intention)
	run, err := fixture.client.ExecuteActivity(ctx, sdk.StartActivityOptions{ID: id, TaskQueue: fixture.prefix + "-activity", StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}}, "gh134-finalization")
	serviceRequire(t, err, "finalization Activity start")
	fixture.activities = append(fixture.activities, run)
	select {
	case <-codec.entered:
	case <-ctx.Done():
		t.Fatal("native output converter was not entered")
	}
	wait, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	err = worker.Stop(wait)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) || worker.Status().Joined {
		t.Fatal("Worker claimed joined while native callback finalization remained blocked")
	}
	fixture.drain(t)
	if fixture.taskRecords.Load() == 0 {
		t.Fatal("independent callback record was not released after the handler returned")
	}
	release.Do(func() { close(codec.release) })
	serviceRequire(t, worker.Stop(ctx), "late finalization join")
	if !worker.Status().Joined {
		t.Fatal("released native finalization did not establish join")
	}
	t.Log("handler evidence released before finalization; canceled Stop retained ownership; later Stop joined")
}

func TestAuthorizedPublicStandaloneControlsAndGatedRefusals(t *testing.T) {
	fixture := newPublicServiceFixture(t, temporal.NativeOptions{}, publicServiceWorkflowPrefix+"DescribeDeployment",
		publicServiceWorkflowPrefix+"GetWorkerBuildIdCompatibility", publicServiceWorkflowPrefix+"GetWorkerVersioningRules",
		publicServiceWorkflowPrefix+"PauseActivityExecution", publicServiceWorkflowPrefix+"UnpauseActivityExecution", publicServiceWorkflowPrefix+"UpdateActivityExecutionOptions")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	namespace, err := fixture.client.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: fixture.namespace})
	serviceRequire(t, err, "standalone capability profile")
	if namespace.GetNamespaceInfo().GetCapabilities().GetStandaloneNexusOperation() {
		t.Fatal("standalone Nexus is unexpectedly enabled; its service qualification is not authorized")
	}
	_, err = fixture.client.CountNexusOperations(ctx, sdk.CountNexusOperationsOptions{Query: "NexusOperationId = '" + fixture.prefix + "-missing'"})
	var unimplemented *serviceerror.Unimplemented
	if !errors.As(err, &unimplemented) {
		t.Fatalf("default-off standalone Nexus did not preserve native Unimplemented: %T", err)
	}
	_, err = fixture.client.DeploymentClient().Describe(ctx, sdk.DeploymentDescribeOptions{Deployment: sdk.Deployment{SeriesName: fixture.prefix, BuildID: "missing"}})
	if !errors.As(err, &unimplemented) {
		t.Fatalf("removed Deployment route did not preserve native Unimplemented: %T", err)
	}
	if !strings.Contains(mustPublicNativeError(t, err).Error(), "Deployments are deprecated and no longer supported") {
		t.Fatal("Deployment refusal did not identify the known removed service route")
	}
	t.Log("deprecated Deployment removed_route_refusal verified=true")
	_, err = fixture.client.GetWorkerBuildIdCompatibility(ctx, &sdk.GetWorkerBuildIdCompatibilityOptions{TaskQueue: fixture.prefix})
	if err != nil {
		var denied *serviceerror.PermissionDenied
		if !errors.As(err, &denied) || mustPublicNativeError(t, err).Error() != "Worker versioning v0.1 (Version Set-based, deprecated) is disabled on this namespace." {
			t.Fatal("deprecated build-ID refusal did not identify the known namespace feature gate")
		}
		t.Log("deprecated build-ID namespace_feature_disabled_refusal verified=true")
	} else {
		t.Log("deprecated build-ID service read remains available")
	}
	_, err = fixture.client.GetWorkerVersioningRules(ctx, sdk.GetWorkerVersioningOptions{TaskQueue: fixture.prefix})
	if err != nil {
		var denied *serviceerror.PermissionDenied
		if !errors.As(err, &denied) || mustPublicNativeError(t, err).Error() != "Worker versioning v0.2 (Rules-based, deprecated) is disabled on this namespace." {
			t.Fatal("deprecated versioning-rule refusal did not identify the known namespace feature gate")
		}
		t.Log("deprecated versioning-rule namespace_feature_disabled_refusal verified=true")
	} else {
		t.Log("deprecated versioning-rule service read remains available")
	}
	fixture.drain(t)
	id := fixture.prefix + "-controlled-activity"
	intention, err := fixture.client.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: id})
	serviceRequire(t, err, "controlled Activity identity")
	fixture.activities = append(fixture.activities, intention)
	run, err := fixture.client.ExecuteActivity(ctx, sdk.StartActivityOptions{ID: id, TaskQueue: fixture.prefix, StartToCloseTimeout: 10 * time.Second}, "gh134-heartbeat", 7)
	serviceRequire(t, err, "controlled standalone Activity start")
	fixture.activities = append(fixture.activities, run)
	err = run.Pause(ctx, sdk.PauseActivityOptions{Reason: "gh134 isolated pause"})
	if errors.As(err, &unimplemented) {
		t.Log("standalone Activity execution enabled; namespace-gated pause remains native Unimplemented")
		timeout := 15 * time.Second
		_, updateErr := run.UpdateOptions(ctx, sdk.ActivityOptionsUpdate{StartToCloseTimeout: &sdk.ActivityOptionChange[time.Duration]{Value: &timeout}})
		_, restoreErr := run.RestoreOriginalOptions(ctx)
		unpauseErr := run.Unpause(ctx, sdk.UnpauseActivityOptions{Reason: "gh134 isolated unpause"})
		for _, refusal := range []error{updateErr, restoreErr, unpauseErr} {
			if !errors.As(refusal, &unimplemented) {
				t.Fatalf("gated standalone operator did not preserve native Unimplemented: %T", refusal)
			}
		}
		t.Log("standalone Activity update, restore and unpause individually retain native Unimplemented")
		fixture.startWorker(t, ctx, false)
		var result int
		serviceRequire(t, run.Get(ctx, &result), "Activity result after refused operator")
		if result != 8 {
			t.Fatal("refused standalone control changed the Activity")
		}
		fixture.drain(t)
		return
	}
	serviceRequire(t, err, "standalone Activity pause")
	description, err := run.Describe(ctx, sdk.DescribeActivityOptions{})
	serviceRequire(t, err, "paused Activity description")
	if description.Metadata().Status != enumspb.ACTIVITY_EXECUTION_STATUS_PAUSED {
		t.Fatal("standalone Activity pause was not observed")
	}
	updatedTimeout := 15 * time.Second
	options, err := run.UpdateOptions(ctx, sdk.ActivityOptionsUpdate{StartToCloseTimeout: &sdk.ActivityOptionChange[time.Duration]{Value: &updatedTimeout}})
	serviceRequire(t, err, "standalone Activity option update")
	if options.StartToCloseTimeout != 15*time.Second {
		t.Fatal("Activity option update was not preserved")
	}
	options, err = run.RestoreOriginalOptions(ctx)
	serviceRequire(t, err, "standalone Activity option restoration")
	if options.StartToCloseTimeout != 10*time.Second {
		t.Fatal("Activity original options were not restored")
	}
	serviceRequire(t, run.Unpause(ctx, sdk.UnpauseActivityOptions{Reason: "gh134 isolated unpause"}), "standalone Activity unpause")
	fixture.startWorker(t, ctx, false)
	var value int
	serviceRequire(t, run.Get(ctx, &value), "controlled standalone Activity result")
	if value != 8 {
		t.Fatal("standalone controls changed the Activity result")
	}
	fixture.drain(t)
}

func mustPublicNativeError(t *testing.T, err error) error {
	t.Helper()
	native, ok := temporal.NativeError(err)
	if !ok {
		t.Fatal("service refusal did not retain its exact native cause")
	}
	return native
}

type publicFatalTraffic struct{ attempts atomic.Int32 }

func (traffic *publicFatalTraffic) CheckCallAllowed(_ context.Context, method string, _, _ any) error {
	if strings.HasSuffix(method, "PollWorkflowTaskQueue") {
		traffic.attempts.Add(1)
		return serviceerror.ToStatus(serviceerror.NewInvalidArgument("gh134 controlled native poll refusal")).Err()
	}
	return nil
}

func TestAuthorizedPublicFatalWorkerCleanup(t *testing.T) {
	traffic := &publicFatalTraffic{}
	fixture := newPublicServiceFixture(t, temporal.NativeOptions{TrafficController: traffic})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	notified, release := make(chan error, 1), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	worker := fixture.startCustomWorker(t, ctx, temporal.WorkerSpec{TaskQueue: fixture.prefix, MaxHandlers: 2,
		Options: nativeworker.Options{LocalActivityWorkerOnly: true, MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2,
			OnFatalError: func(err error) { notified <- err; <-release }},
		Workflows: []temporal.WorkflowRegistration{{Definition: publicServiceWorkflow, Options: workflow.RegisterOptions{Name: "gh134-fatal"}}}})
	select {
	case err := <-notified:
		var invalid *serviceerror.InvalidArgument
		if !errors.As(err, &invalid) {
			t.Fatal("fatal callback lost its exact controlled native cause")
		}
		fixture.expectedStops[worker] = err
	case <-ctx.Done():
		t.Fatal("native fatal callback did not arrive after its built-in retry grace")
	}
	wait, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	err := worker.Stop(wait)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) || worker.Status().Joined {
		t.Fatal("fatal callback was mistaken for completed native cleanup")
	}
	releaseOnce.Do(func() { close(release) })
	if err := worker.Stop(ctx); !errors.Is(err, fixture.expectedStops[worker]) {
		t.Fatal("fatal Worker cleanup lost its exact primary failure")
	}
	if !worker.Status().Joined || traffic.attempts.Load() < 2 {
		t.Fatal("fatal retry/join evidence incomplete")
	}
	t.Log("controlled native poll refusal exercised retry grace, fatal notification and actual late join")
}

type publicCallbackFailureInput struct{ ID, RunID string }
type publicReturnedCallback struct {
	client sdk.Client
	err    error
}
type publicCallbackFailureConverter struct {
	converter.FailureConverter
	finalized chan publicFinalizedFailure
	scope     string
}
type publicFinalizedFailure struct {
	scope string
	err   error
}

func (codec *publicCallbackFailureConverter) WithSerializationContext(scope converter.SerializationContext) converter.FailureConverter {
	copy := *codec
	copy.FailureConverter = converter.WithFailureConverterSerializationContext(codec.FailureConverter, scope)
	if activityScope, ok := scope.(converter.ActivitySerializationContext); ok {
		copy.scope = "remote"
		if activityScope.IsLocal {
			copy.scope = "local"
		}
	}
	return &copy
}

func (codec *publicCallbackFailureConverter) ErrorToFailure(err error) *failurepb.Failure {
	var execution *sdktemporal.WorkflowExecutionError
	var application *sdktemporal.ApplicationError
	if errors.As(err, &execution) && errors.As(err, &application) && application.Type() == "gh134-origin-error" {
		var detail string
		failure := application.Details(&detail)
		if failure == nil && detail != "gh134-finalization-canary" {
			failure = errors.New("callback finalizer details changed")
		}
		select {
		case codec.finalized <- publicFinalizedFailure{scope: codec.scope, err: failure}:
		default:
		}
	}
	return codec.FailureConverter.ErrorToFailure(err)
}

func publicCallbackOriginWorkflow(workflow.Context) error {
	return sdktemporal.NewNonRetryableApplicationError("controlled origin failure", "gh134-origin-error", nil, "gh134-finalization-canary")
}

func publicCallbackFinalizationWorkflow(ctx workflow.Context, input publicCallbackFailureInput) (string, error) {
	remote := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	if err := workflow.ExecuteActivity(remote, "gh134-borrowed-failure", input).Get(remote, nil); err == nil {
		return "", errors.New("borrowed remote Activity failure was lost")
	}
	local := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	if err := workflow.ExecuteLocalActivity(local, "gh134-borrowed-failure", input).Get(local, nil); err == nil {
		return "", errors.New("borrowed LocalActivity failure was lost")
	}
	return "remote-and-local-finalized", nil
}

func TestAuthorizedPublicCallbackFailureFinalization(t *testing.T) {
	codec := &publicCallbackFailureConverter{FailureConverter: sdktemporal.NewDefaultFailureConverter(sdktemporal.DefaultFailureConverterOptions{}), finalized: make(chan publicFinalizedFailure, 8)}
	fixture := newPublicServiceFixture(t, temporal.NativeOptions{FailureConverter: codec})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	returned := make(chan publicReturnedCallback, 2)
	fixture.startCustomWorker(t, ctx, temporal.WorkerSpec{TaskQueue: fixture.prefix, MaxHandlers: 4,
		Options: nativeworker.Options{MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2, MaxConcurrentActivityTaskPollers: 2, WorkerStopTimeout: time.Second},
		Workflows: []temporal.WorkflowRegistration{
			{Definition: publicCallbackOriginWorkflow, Options: workflow.RegisterOptions{Name: "gh134-origin-failure"}},
			{Definition: publicCallbackFinalizationWorkflow, Options: workflow.RegisterOptions{Name: "gh134-callback-finalization"}}},
		Activities: []temporal.ActivityRegistration{{Definition: func(activityContext context.Context, input publicCallbackFailureInput) error {
			client := activity.GetClient(activityContext)
			var headers, trailers metadata.MD
			_, err := client.WorkflowService().CountWorkflowExecutions(activityContext, &workflowservice.CountWorkflowExecutionsRequest{
				Namespace: activity.GetInfo(activityContext).Namespace, Query: "WorkflowId = '" + input.ID + "'"},
				grpc.Header(&headers), grpc.Trailer(&trailers), grpc.WaitForReady(true))
			if err != nil {
				return err
			}
			err = client.GetWorkflow(activityContext, input.ID, input.RunID).Get(activityContext, nil)
			returned <- publicReturnedCallback{client: client, err: err}
			return err
		}, Options: activity.RegisterOptions{Name: "gh134-borrowed-failure"}}}})
	originID := fixture.prefix + "-origin"
	fixture.trackWorkflow(originID, "")
	origin, err := fixture.client.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{ID: originID, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: time.Minute}, "gh134-origin-failure")
	serviceRequire(t, err, "callback origin start")
	err = origin.Get(ctx, nil)
	var application *sdktemporal.ApplicationError
	if !errors.As(err, &application) || application.Type() != "gh134-origin-error" {
		t.Fatal("controlled origin did not fail as intended")
	}
	id := fixture.prefix + "-callback"
	fixture.trackWorkflow(id, "")
	run, err := fixture.client.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: time.Minute},
		"gh134-callback-finalization", publicCallbackFailureInput{ID: originID, RunID: origin.GetRunID()})
	serviceRequire(t, err, "callback finalization start")
	var result string
	serviceRequire(t, run.Get(ctx, &result), "callback finalization result")
	if result != "remote-and-local-finalized" {
		t.Fatal("callback finalization workflow result changed")
	}
	finalizedScopes := make(map[string]bool)
	for range 2 {
		select {
		case finalized := <-codec.finalized:
			serviceRequire(t, finalized.err, "managed native FailureConverter borrowed Details")
			finalizedScopes[finalized.scope] = true
		case <-ctx.Done():
			t.Fatal("managed native failure conversion did not observe both callback error origins")
		}
		select {
		case captured := <-returned:
			_, err := captured.client.DescribeWorkflowExecution(ctx, originID, origin.GetRunID())
			if err == nil {
				t.Fatal("original callback Client remained usable after handler return")
			}
			var application *sdktemporal.ApplicationError
			if !errors.As(captured.err, &application) {
				t.Fatal("original callback lost native failure semantics")
			}
			var detail string
			if err := application.Details(&detail); err == nil {
				t.Fatal("original callback lazy details remained usable after handler return")
			}
		case <-ctx.Done():
			t.Fatal("callback retention control was not observed")
		}
	}
	if !finalizedScopes["remote"] || !finalizedScopes["local"] {
		t.Fatal("native finalization did not independently cover remote and LocalActivity serialization contexts")
	}
	fixture.drain(t)
	t.Log("managed remote Activity and LocalActivity native failure finalizers decoded borrowed Details; original callback Client and error decoders remained revoked")
}

type publicRemoteRetryResult struct {
	Attempt           int32
	Checkpoint, Value int
}

func publicRemoteRetryWorkflow(ctx workflow.Context) (publicRemoteRetryResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second,
		ScheduleToCloseTimeout: 30 * time.Second, HeartbeatTimeout: 5 * time.Second,
		RetryPolicy: &sdktemporal.RetryPolicy{InitialInterval: 50 * time.Millisecond, MaximumInterval: 50 * time.Millisecond, MaximumAttempts: 2}})
	var result publicRemoteRetryResult
	err := workflow.ExecuteActivity(ctx, "gh134-retry-heartbeat", 17).Get(ctx, &result)
	return result, err
}

func TestAuthorizedPublicRemoteActivityRetryHeartbeat(t *testing.T) {
	fixture := newPublicServiceFixture(t, temporal.NativeOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	clients := make(chan sdk.Client, 2)
	worker := fixture.startCustomWorker(t, ctx, temporal.WorkerSpec{TaskQueue: fixture.prefix, MaxHandlers: 2,
		Options: nativeworker.Options{MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2,
			MaxConcurrentActivityTaskPollers: 2, WorkerStopTimeout: time.Second},
		Workflows: []temporal.WorkflowRegistration{{Definition: publicRemoteRetryWorkflow, Options: workflow.RegisterOptions{Name: "gh134-remote-retry"}}},
		Activities: []temporal.ActivityRegistration{{Definition: func(activityContext context.Context, value int) (publicRemoteRetryResult, error) {
			info := activity.GetInfo(activityContext)
			if info.IsLocalActivity || info.Attempt < 1 || info.Attempt > 2 {
				return publicRemoteRetryResult{}, sdktemporal.NewNonRetryableApplicationError("unexpected remote Activity attempt", "gh134-retry-invariant", nil)
			}
			client := activity.GetClient(activityContext)
			if _, err := client.DescribeWorkflowExecution(activityContext, info.WorkflowExecution.ID, info.WorkflowExecution.RunID); err != nil {
				return publicRemoteRetryResult{}, err
			}
			select {
			case clients <- client:
			case <-activityContext.Done():
				return publicRemoteRetryResult{}, activityContext.Err()
			}
			if info.Attempt == 1 {
				activity.RecordHeartbeat(activityContext, value)
				return publicRemoteRetryResult{}, sdktemporal.NewApplicationError("controlled remote retry", "gh134-remote-retry")
			}
			if info.Attempt != 2 || !activity.HasHeartbeatDetails(activityContext) {
				return publicRemoteRetryResult{}, sdktemporal.NewNonRetryableApplicationError("remote retry did not restore its heartbeat", "gh134-retry-invariant", nil)
			}
			var checkpoint int
			if err := activity.GetHeartbeatDetails(activityContext, &checkpoint); err != nil {
				return publicRemoteRetryResult{}, err
			}
			if checkpoint != value {
				return publicRemoteRetryResult{}, sdktemporal.NewNonRetryableApplicationError("remote heartbeat checkpoint changed", "gh134-retry-invariant", nil)
			}
			return publicRemoteRetryResult{Attempt: info.Attempt, Checkpoint: checkpoint, Value: value + 1}, nil
		}, Options: activity.RegisterOptions{Name: "gh134-retry-heartbeat"}}}})
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if err := worker.Stop(cleanup); err != nil {
			serviceCleanupFailure(t, "remote retry Worker cleanup failed before deletion: %T", err)
		}
		if !worker.Status().Joined {
			serviceCleanupFailure(t, "remote retry Worker was not joined before execution cleanup")
		}
	})
	id := fixture.prefix + "-retry"
	fixture.trackWorkflow(id, "")
	run, err := fixture.client.ExecuteWorkflow(ctx, sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: time.Minute}, "gh134-remote-retry")
	serviceRequire(t, err, "remote retry Workflow start")
	var result publicRemoteRetryResult
	serviceRequire(t, run.Get(ctx, &result), "remote retry Workflow result")
	if result != (publicRemoteRetryResult{Attempt: 2, Checkpoint: 17, Value: 18}) {
		t.Fatal("remote retry did not return the second attempt with its restored heartbeat")
	}
	seen := make(map[int32]bool, 2)
	sequences := make(map[uint64]bool, 2)
	activityID := ""
	for range 2 {
		delivery, err := fixture.dependencies.Tasks.NextReleased(ctx)
		serviceRequire(t, err, "remote retry independent task delivery")
		receipt, err := delivery.Receipt()
		serviceRequire(t, err, "remote retry task receipt")
		snapshot, err := receipt.WaitReleased(ctx)
		serviceRequire(t, err, "remote retry task release")
		task, present := snapshot.ValueCopy()
		if !present || task.Kind != "activity" || task.Local || !task.HandlerReturned || task.AsyncCompletion ||
			task.WorkflowID != id || task.RunID != run.GetRunID() || task.Namespace != fixture.namespace ||
			task.TaskQueue != fixture.prefix || task.ActivityType != "gh134-retry-heartbeat" || task.ActivityID == "" ||
			task.Attempt < 1 || task.Attempt > 2 || seen[task.Attempt] {
			t.Fatal("remote retry task evidence lost its independent native attempt identity")
		}
		if activityID == "" {
			activityID = task.ActivityID
		} else if task.ActivityID != activityID {
			t.Fatal("remote retry executed a different Activity rather than a second attempt")
		}
		if task.Attempt == 1 {
			var failure *sdktemporal.ApplicationError
			if snapshot.Err() == nil || !errors.As(snapshot.Primary(), &failure) || failure.Type() != "gh134-remote-retry" || failure.NonRetryable() {
				t.Fatal("first remote attempt did not retain its retryable native failure")
			}
		} else if snapshot.Err() != nil {
			t.Fatal("second remote attempt did not retain successful completion")
		}
		if snapshot.Cleanup() != nil || snapshot.Info().Sequence == 0 || sequences[snapshot.Info().Sequence] {
			t.Fatal("remote retry task receipts did not have separate released custody")
		}
		seen[task.Attempt], sequences[snapshot.Info().Sequence] = true, true
		serviceRequire(t, delivery.Ack(), "remote retry task acknowledgement")
	}
	state, err := fixture.dependencies.Tasks.Inspect()
	serviceRequire(t, err, "remote retry task custody")
	if state.Outstanding != 0 || !seen[1] || !seen[2] {
		t.Fatal("remote retry did not produce exactly two released and acknowledged task records")
	}
	for range 2 {
		select {
		case client := <-clients:
			before := fixture.describeRPCs.Load()
			_, err := client.DescribeWorkflowExecution(ctx, id, run.GetRunID())
			if err == nil || fixture.describeRPCs.Load() != before {
				t.Fatal("returned retry callback Client retained access or sent an outbound Describe")
			}
		case <-ctx.Done():
			t.Fatal("remote retry callback Client was not retained for its expiry control")
		}
	}
	var history historypb.History
	var startedAttempt int32
	var scheduledEvent int64
	serviceRequire(t, fixture.client.WalkHistory(ctx, id, run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
		func(_ context.Context, event *historypb.HistoryEvent) error {
			history.Events = append(history.Events, event)
			if scheduled := event.GetActivityTaskScheduledEventAttributes(); scheduled != nil && scheduled.GetActivityId() == activityID {
				scheduledEvent = event.GetEventId()
			}
			if started := event.GetActivityTaskStartedEventAttributes(); started != nil && scheduledEvent != 0 && started.GetScheduledEventId() == scheduledEvent {
				startedAttempt = started.GetAttempt()
			}
			return nil
		}), "remote retry native history")
	if startedAttempt != 2 {
		t.Fatal("native history did not record the second remote Activity attempt")
	}
	replayer := nativeworker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(publicRemoteRetryWorkflow, workflow.RegisterOptions{Name: "gh134-remote-retry"})
	serviceRequire(t, replayer.ReplayWorkflowHistoryWithOptions(publicServiceLogger{}, &history,
		nativeworker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: id, RunID: run.GetRunID()}}), "remote retry replay")
	serviceRequire(t, worker.Stop(ctx), "remote retry Worker join before deletion")
	if !worker.Status().Joined {
		t.Fatal("remote retry Worker join was not observed")
	}
	fixture.drain(t)
	t.Log("remote Activity attempts 1 and 2 retained separate failed/successful task receipts; heartbeat restored; callback clients revoked before wire; history replay passed; Worker joined before deletion")
}
