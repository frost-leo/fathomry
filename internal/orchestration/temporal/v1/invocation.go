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
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Execution records one process-side SDK operation. NativeCalled does not prove
// server acceptance; Accepted means a successful native request response, not
// application acceptance of an Update. UpdateStage is the observed protocol
// stage; a completed stage can contain a validator rejection. StartAccepted
// preserves a combined start acknowledgement even if waiting for its Update fails.
// Results and payloads remain caller-owned, not retained by this evidence record.
// IDs describe the latest observed native attempt and its successful response,
// not an atomic ledger of independent targets submitted by custom extensions.
// IdentityOmitted accompanies ErrLimit when observed IDs exceed the envelope;
// the original intention's IDs are retained instead, without erasing other facts.
type Execution struct {
	private
	Operation              string
	Namespace              string
	WorkflowID             string
	RunID                  string
	ActivityID             string
	UpdateID               string
	ScheduleID             string
	NexusOperationID       string
	DeploymentName         string
	NativeCalled           bool
	Accepted               bool
	ResultObtained         bool
	StartAccepted          bool
	UpdateStage            enumspb.UpdateWorkflowExecutionLifecycleStage
	IdentityOmitted        bool
	HeartbeatAcknowledged  bool
	CompletionAcknowledged bool
	CancellationRequested  bool
	ActivityPaused         bool
	ActivityReset          bool
}

// Executions preserves native Temporal operation/options semantics while owning
// process-local calls and independently retained evidence. It exposes no native
// client, source Close, or process-global Namespace setting.
type Executions struct {
	private
	owner    *connection
	access   *resource.Access
	inbox    *invocation.Inbox[Execution]
	observer *invocation.Observer
}

// BindExecutions binds a named source to native Workflow/Activity operations.
// The Inbox is separately owned by framework composition. Native options/args are
// borrowed during a call and must not be concurrently mutated.
func BindExecutions(assembly *resource.Assembly, selected resource.Selection[Source], inbox *invocation.Inbox[Execution], observer *invocation.Observer) (*Executions, error) {
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind-executions")
	}
	value, limits := source.owner.settings, access.Limits()
	if source.owner.sharedBytes > 0 && limits.Bytes > source.owner.sharedBytes || limits.Active > value.MaxActive || limits.Queued > value.QueuedCalls || limits.Bytes < value.reservation() || limits.MaxLeases < 1 ||
		limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Executions{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}

// Namespace is the immutable native namespace selected for this binding.
func (client *Executions) Namespace() string {
	if client == nil || client.owner == nil {
		return ""
	}
	return client.owner.settings.Namespace
}

func (*Executions) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

// ExecutionEvidenceBytes is the fixed declared evidence envelope. It excludes
// caller-owned payloads and arbitrary native error graphs, not measured RSS.
const ExecutionEvidenceBytes int64 = 4096

type nativeCallKey struct{}

type executionEvidenceKey struct{}

type callbackCallKey struct{}

type nativeCall struct {
	scopeOwner *sdk.FathomryScopeOwnerV1
	owner      *connection
	closed     atomic.Bool
	callback   bool
	borrower   *callbackClient
	resultsMu  sync.Mutex
	updates    []interceptorUpdateTransfer
	identity   *observedNativeIdentity
}

const (
	identityWorkflow = iota
	identityRun
	identityActivity
	identityUpdate
	identitySchedule
	identityNexus
	identityDeployment
	identityCount
)

type nativeIdentity struct {
	fields uint8
	values [identityCount]string
}

func (identity *nativeIdentity) set(field int, value string) {
	identity.fields |= 1 << field
	identity.values[field] = value
}

func (identity *nativeIdentity) workflow(execution *commonpb.WorkflowExecution) {
	identity.set(identityWorkflow, execution.GetWorkflowId())
	identity.set(identityRun, execution.GetRunId())
}

func (identity *nativeIdentity) activity(activityID, runID string) {
	identity.set(identityActivity, activityID)
	identity.set(identityRun, runID)
}

func (identity *nativeIdentity) nexus(operationID, runID string) {
	identity.set(identityNexus, operationID)
	identity.set(identityRun, runID)
}

func (identity *nativeIdentity) request(request any) {
	switch request := request.(type) {
	case *workflowservice.StartWorkflowExecutionRequest:
		identity.set(identityWorkflow, request.GetWorkflowId())
	case *workflowservice.SignalWithStartWorkflowExecutionRequest:
		identity.set(identityWorkflow, request.GetWorkflowId())
	case *workflowservice.SignalWorkflowExecutionRequest:
		identity.workflow(request.GetWorkflowExecution())
	case *workflowservice.RequestCancelWorkflowExecutionRequest:
		identity.workflow(request.GetWorkflowExecution())
	case *workflowservice.TerminateWorkflowExecutionRequest:
		identity.workflow(request.GetWorkflowExecution())
	case *workflowservice.QueryWorkflowRequest:
		identity.workflow(request.GetExecution())
	case *workflowservice.DescribeWorkflowExecutionRequest:
		identity.workflow(request.GetExecution())
	case *workflowservice.GetWorkflowExecutionHistoryRequest:
		identity.workflow(request.GetExecution())
	case *workflowservice.GetWorkflowExecutionHistoryReverseRequest:
		identity.workflow(request.GetExecution())
	case *workflowservice.ResetWorkflowExecutionRequest:
		identity.workflow(request.GetWorkflowExecution())
	case *workflowservice.UpdateWorkflowExecutionOptionsRequest:
		identity.workflow(request.GetWorkflowExecution())
	case *workflowservice.UpdateWorkflowExecutionRequest:
		identity.workflow(request.GetWorkflowExecution())
		identity.set(identityUpdate, request.GetRequest().GetMeta().GetUpdateId())
	case *workflowservice.PollWorkflowExecutionUpdateRequest:
		identity.workflow(request.GetUpdateRef().GetWorkflowExecution())
		identity.set(identityUpdate, request.GetUpdateRef().GetUpdateId())
	case *workflowservice.ExecuteMultiOperationRequest:
		for _, operation := range request.GetOperations() {
			if start := operation.GetStartWorkflow(); start != nil {
				identity.request(start)
			}
			if update := operation.GetUpdateWorkflow(); update != nil {
				identity.request(update)
			}
		}
	case *workflowservice.StartActivityExecutionRequest:
		identity.set(identityActivity, request.GetActivityId())
	case *workflowservice.DescribeActivityExecutionRequest:
		identity.activity(request.GetActivityId(), request.GetRunId())
	case *workflowservice.PollActivityExecutionRequest:
		identity.activity(request.GetActivityId(), request.GetRunId())
	case *workflowservice.RequestCancelActivityExecutionRequest:
		identity.activity(request.GetActivityId(), request.GetRunId())
	case *workflowservice.TerminateActivityExecutionRequest:
		identity.activity(request.GetActivityId(), request.GetRunId())
	case *workflowservice.PauseActivityExecutionRequest, *workflowservice.UnpauseActivityExecutionRequest,
		*workflowservice.UpdateActivityExecutionOptionsRequest, *workflowservice.ResetActivityExecutionRequest,
		*workflowservice.RespondActivityTaskCompletedByIdRequest, *workflowservice.RespondActivityTaskFailedByIdRequest,
		*workflowservice.RespondActivityTaskCanceledByIdRequest, *workflowservice.RecordActivityTaskHeartbeatByIdRequest:
		target := request.(interface {
			GetWorkflowId() string
			GetActivityId() string
			GetResourceId() string
			GetRunId() string
		})
		activityID := target.GetActivityId()
		if target.GetResourceId() != "" {
			activityID = target.GetResourceId()
		}
		identity.activity(activityID, target.GetRunId())
		identity.set(identityWorkflow, target.GetWorkflowId())
	case *workflowservice.StartNexusOperationExecutionRequest:
		identity.set(identityNexus, request.GetOperationId())
	case *workflowservice.DescribeNexusOperationExecutionRequest:
		identity.nexus(request.GetOperationId(), request.GetRunId())
	case *workflowservice.PollNexusOperationExecutionRequest:
		identity.nexus(request.GetOperationId(), request.GetRunId())
	case *workflowservice.RequestCancelNexusOperationExecutionRequest:
		identity.nexus(request.GetOperationId(), request.GetRunId())
	case *workflowservice.TerminateNexusOperationExecutionRequest:
		identity.nexus(request.GetOperationId(), request.GetRunId())
	case *workflowservice.DeleteNexusOperationExecutionRequest:
		identity.nexus(request.GetOperationId(), request.GetRunId())
	case *workflowservice.CreateScheduleRequest, *workflowservice.DescribeScheduleRequest,
		*workflowservice.UpdateScheduleRequest, *workflowservice.PatchScheduleRequest,
		*workflowservice.DeleteScheduleRequest, *workflowservice.ListScheduleMatchingTimesRequest:
		identity.set(identitySchedule, request.(interface{ GetScheduleId() string }).GetScheduleId())
	case *workflowservice.DescribeWorkerDeploymentRequest, *workflowservice.SetWorkerDeploymentCurrentVersionRequest,
		*workflowservice.SetWorkerDeploymentRampingVersionRequest, *workflowservice.SetWorkerDeploymentManagerRequest,
		*workflowservice.DeleteWorkerDeploymentRequest:
		identity.set(identityDeployment, request.(interface{ GetDeploymentName() string }).GetDeploymentName())
	case *workflowservice.DescribeWorkerDeploymentVersionRequest:
		identity.set(identityDeployment, request.GetDeploymentVersion().GetDeploymentName())
	case *workflowservice.DeleteWorkerDeploymentVersionRequest:
		identity.set(identityDeployment, request.GetDeploymentVersion().GetDeploymentName())
	case *workflowservice.UpdateWorkerDeploymentVersionMetadataRequest:
		identity.set(identityDeployment, request.GetDeploymentVersion().GetDeploymentName())
	}
}

func (identity *nativeIdentity) response(response any) {
	switch response := response.(type) {
	case *workflowservice.StartWorkflowExecutionResponse, *workflowservice.SignalWithStartWorkflowExecutionResponse,
		*workflowservice.ResetWorkflowExecutionResponse, *workflowservice.StartActivityExecutionResponse,
		*workflowservice.PollActivityExecutionResponse, *workflowservice.StartNexusOperationExecutionResponse,
		*workflowservice.PollNexusOperationExecutionResponse:
		identity.set(identityRun, response.(interface{ GetRunId() string }).GetRunId())
	case *workflowservice.UpdateWorkflowExecutionResponse:
		identity.workflow(response.GetUpdateRef().GetWorkflowExecution())
		identity.set(identityUpdate, response.GetUpdateRef().GetUpdateId())
	case *workflowservice.PollWorkflowExecutionUpdateResponse:
		identity.workflow(response.GetUpdateRef().GetWorkflowExecution())
		identity.set(identityUpdate, response.GetUpdateRef().GetUpdateId())
	case *workflowservice.DescribeWorkflowExecutionResponse:
		identity.workflow(response.GetWorkflowExecutionInfo().GetExecution())
	case *workflowservice.DescribeActivityExecutionResponse:
		identity.activity(response.GetInfo().GetActivityId(), response.GetInfo().GetRunId())
		if response.GetRunId() != "" {
			identity.set(identityRun, response.GetRunId())
		}
	case *workflowservice.DescribeNexusOperationExecutionResponse:
		identity.nexus(response.GetInfo().GetOperationId(), response.GetInfo().GetRunId())
		if response.GetRunId() != "" {
			identity.set(identityRun, response.GetRunId())
		}
	case *workflowservice.ExecuteMultiOperationResponse:
		for _, operation := range response.GetResponses() {
			if start := operation.GetStartWorkflow(); start != nil {
				identity.response(start)
			}
			if update := operation.GetUpdateWorkflow(); update != nil {
				identity.response(update)
			}
		}
	}
	for field, value := range identity.values {
		if value == "" {
			identity.fields &^= 1 << field
		}
	}
}

type observedNativeIdentity struct {
	mu       sync.Mutex
	sequence uint64
	identity nativeIdentity
	overflow bool
}

func (observed *observedNativeIdentity) merge(identity nativeIdentity) {
	candidate := observed.identity
	total := 0
	for field := range identity.values {
		if identity.fields&(1<<field) != 0 {
			candidate.values[field] = identity.values[field]
		}
		limit := 1024
		if field == identityDeployment {
			limit = 255
		}
		if len(candidate.values[field]) > limit {
			observed.overflow = true
			return
		}
		total += len(candidate.values[field])
	}
	if total > 3072 {
		observed.overflow = true
		return
	}
	for field := range identity.values {
		if identity.fields&(1<<field) != 0 {
			observed.identity.values[field] = strings.Clone(identity.values[field])
		}
	}
	observed.identity.fields |= identity.fields
}

func (observed *observedNativeIdentity) begin(request any) uint64 {
	var identity nativeIdentity
	identity.request(request)
	if identity.fields == 0 {
		return 0
	}
	observed.mu.Lock()
	defer observed.mu.Unlock()
	observed.sequence++
	observed.merge(identity)
	return observed.sequence
}

func (observed *observedNativeIdentity) complete(sequence uint64, reply any) {
	if sequence == 0 {
		return
	}
	var identity nativeIdentity
	identity.response(reply)
	observed.mu.Lock()
	defer observed.mu.Unlock()
	if observed.sequence == sequence {
		observed.merge(identity)
	}
}

func (observed *observedNativeIdentity) apply(evidence *Execution) bool {
	observed.mu.Lock()
	defer observed.mu.Unlock()
	fields := [...]*string{&evidence.WorkflowID, &evidence.RunID, &evidence.ActivityID, &evidence.UpdateID,
		&evidence.ScheduleID, &evidence.NexusOperationID, &evidence.DeploymentName}
	for field, target := range fields {
		if observed.identity.fields&(1<<field) != 0 {
			*target = observed.identity.values[field]
		}
	}
	return observed.overflow || !evidence.validIdentity()
}

type executionContext struct{ context.Context }

func (ctx executionContext) Value(key any) any {
	switch key.(type) {
	case nativeCallKey, activeRPCKey, taskBindingKey, taskRawKey, executionEvidenceKey, schedulePageKey, callbackCallKey, transportOwnerKey, captureTransportKey:
		return nil
	}
	return ctx.Context.Value(key)
}

func (value Execution) validIdentity() bool {
	return len(value.WorkflowID) <= 1024 && len(value.RunID) <= 1024 && len(value.ActivityID) <= 1024 && len(value.UpdateID) <= 1024 && len(value.ScheduleID) <= 1024 && len(value.NexusOperationID) <= 1024 && len(value.DeploymentName) <= 255 &&
		len(value.WorkflowID)+len(value.RunID)+len(value.ActivityID)+len(value.UpdateID)+len(value.ScheduleID)+len(value.NexusOperationID)+len(value.DeploymentName) <= 3072
}

func executeNative[T any](ctx context.Context, client *Executions, correlation fault.Correlation, evidence Execution, run func(context.Context, *Execution) (T, error)) (T, error) {
	var output T
	if client == nil || client.owner == nil || ctx == nil {
		return output, failure(ErrInput, "execution")
	}
	if !evidence.validIdentity() {
		return output, failure(ErrLimit, "execution-identity")
	}
	evidence.Namespace = client.Namespace()
	call, err := invocation.Begin(ctx, client.access, invocation.Request{Name: evidence.Operation, Correlation: correlation, Shape: invocation.Finite,
		Bytes: client.owner.settings.reservation(), EvidenceBytes: ExecutionEvidenceBytes,
		Admission: invocation.Budget{Limit: client.owner.settings.AdmissionTimeout}}, client.inbox, client.observer)
	if err != nil {
		return output, err
	}
	return finishNative(ctx, client, call, evidence, run)
}

func finishNative[T any](ctx context.Context, client *Executions, call *invocation.Call[Execution], evidence Execution, run func(context.Context, *Execution) (T, error)) (T, error) {
	var output T
	var err error
	intention := evidence
	var identity observedNativeIdentity
	returned := false
	defer func() {
		if returned {
			return
		}
		recovered := recover()
		cause, _ := recovered.(error)
		oversized := !evidence.validIdentity()
		if identity.apply(&evidence) || oversized {
			evidence.WorkflowID, evidence.RunID, evidence.ActivityID, evidence.UpdateID = intention.WorkflowID, intention.RunID, intention.ActivityID, intention.UpdateID
			evidence.ScheduleID = intention.ScheduleID
			evidence.NexusOperationID = intention.NexusOperationID
			evidence.DeploymentName = intention.DeploymentName
			evidence.IdentityOmitted = true
			cause = errors.Join(cause, failure(ErrLimit, "execution-identity"))
		}
		call.Complete(invocation.Outcome[Execution]{Value: evidence, Present: true, Primary: failure(ErrExecution, "native-exit", cause)})
		if recovered != nil {
			panic(recovered)
		}
	}()
	_ = call.Execute(ctx, invocation.Budget{Limit: client.owner.settings.RPCTimeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Execution] {
		borrower, callback := ctx.Value(callbackCallKey{}).(*callbackClient)
		authority := &nativeCall{owner: client.owner, callback: callback, borrower: borrower, scopeOwner: sdk.NewFathomryScopeOwnerV1(), identity: &identity}
		defer authority.closed.Store(true)
		// Native propagators/tracers need application values, but neither stale
		// ownership markers nor caller transport credentials carry into this call.
		work = metadata.NewOutgoingContext(executionContext{work}, nil)
		work = context.WithValue(work, nativeCallKey{}, authority)
		work = context.WithValue(work, activeRPCKey{}, call)
		work = context.WithValue(work, executionEvidenceKey{}, &evidence)
		evidence.NativeCalled = true
		output, err = run(work, &evidence)
		oversized := !evidence.validIdentity()
		if identity.apply(&evidence) || oversized {
			evidence.WorkflowID, evidence.RunID, evidence.ActivityID, evidence.UpdateID = intention.WorkflowID, intention.RunID, intention.ActivityID, intention.UpdateID
			evidence.ScheduleID = intention.ScheduleID
			evidence.NexusOperationID = intention.NexusOperationID
			evidence.DeploymentName = intention.DeploymentName
			evidence.IdentityOmitted = true
			err = errors.Join(err, failure(ErrLimit, "execution-identity"))
		}
		err = scopeNativeError(work, client, evidence, err)
		if err != nil {
			err = failure(ErrExecution, evidence.Operation, err, work.Err(), context.Cause(work))
		}
		return invocation.Outcome[Execution]{Value: evidence, Present: true, Primary: err}
	})
	returned = true
	result, ok := call.Receipt().Result()
	if !ok {
		return output, failure(ErrExecution, evidence.Operation)
	}
	return output, result.Err()
}

func scopeNativeError(ctx context.Context, client *Executions, origin Execution, cause error) error {
	if cause == nil {
		return nil
	}
	identity := Execution{Operation: "execution.error-details", WorkflowID: origin.WorkflowID, RunID: origin.RunID, ActivityID: origin.ActivityID,
		UpdateID: origin.UpdateID, ScheduleID: origin.ScheduleID, NexusOperationID: origin.NexusOperationID, DeploymentName: origin.DeploymentName}
	authority, _ := ctx.Value(nativeCallKey{}).(*nativeCall)
	if authority == nil {
		return cause
	}
	return sdk.FathomryScopeErrorV1(cause, authority.scopeOwner, func(decode func() error) error {
		invoke := func(_ context.Context, evidence *Execution) (struct{}, error) {
			err := decode()
			evidence.ResultObtained = err == nil
			return struct{}{}, err
		}
		if callback := authority.borrower; callback != nil {
			_, err := callbackNative(context.WithoutCancel(ctx), callback, "", identity, invoke)
			return err
		}
		_, err := executeNative(context.WithoutCancel(ctx), client, fault.Correlation{Call: uuid.NewString()}, identity, invoke)
		return err
	})
}

func validateNativeRequest(ctx context.Context, value settings, request any) error {
	input, ok := request.(proto.Message)
	if !ok || !input.ProtoReflect().IsValid() {
		return failure(ErrInput, "native-request")
	}
	if namespace := input.ProtoReflect().Descriptor().Fields().ByName("namespace"); namespace != nil {
		if namespace.Kind() != protoreflect.StringKind || input.ProtoReflect().Get(namespace).String() != value.Namespace {
			return failure(ErrAuthority, "namespace")
		}
	}
	_, err := messageSize(ctx, input, value.MaxRequestBytes)
	return err
}

func boundedNativeRPC(ctx context.Context, value settings, method string, request, reply any, connection *grpc.ClientConn, next grpc.UnaryInvoker, options ...grpc.CallOption) error {
	if err := validateNativeRequest(ctx, value, request); err != nil {
		return err
	}
	options = append(slices.Clone(options), grpc.MaxCallSendMsgSize(value.MaxRequestBytes), grpc.MaxCallRecvMsgSize(value.MaxResponseBytes))
	err := next(ctx, method, request, reply, connection, options...)
	if page, ok := ctx.Value(schedulePageKey{}).(*schedulePage); ok && err == nil {
		switch response := reply.(type) {
		case *workflowservice.ListSchedulesResponse:
			page.more = len(response.NextPageToken) > 0
		case *workflowservice.ListWorkerDeploymentsResponse:
			page.more = len(response.NextPageToken) > 0
		case *workflowservice.GetWorkflowExecutionHistoryResponse:
			page.more = len(response.NextPageToken) > 0
		}
	}
	if evidence, ok := ctx.Value(executionEvidenceKey{}).(*Execution); ok && err == nil {
		switch response := reply.(type) {
		case *workflowservice.CreateScheduleResponse, *workflowservice.UpdateScheduleResponse, *workflowservice.PatchScheduleResponse, *workflowservice.DeleteScheduleResponse:
			evidence.Accepted = true
		case *workflowservice.RespondActivityTaskCompletedResponse, *workflowservice.RespondActivityTaskFailedResponse, *workflowservice.RespondActivityTaskCanceledResponse,
			*workflowservice.RespondActivityTaskCompletedByIdResponse, *workflowservice.RespondActivityTaskFailedByIdResponse, *workflowservice.RespondActivityTaskCanceledByIdResponse:
			evidence.CompletionAcknowledged = true
		case *workflowservice.RecordActivityTaskHeartbeatResponse:
			evidence.HeartbeatAcknowledged = true
			evidence.CancellationRequested = response.GetCancelRequested()
			evidence.ActivityPaused = response.GetActivityPaused()
			evidence.ActivityReset = response.GetActivityReset()
		case *workflowservice.RecordActivityTaskHeartbeatByIdResponse:
			evidence.HeartbeatAcknowledged = true
			evidence.CancellationRequested = response.GetCancelRequested()
			evidence.ActivityPaused = response.GetActivityPaused()
			evidence.ActivityReset = response.GetActivityReset()
		case *workflowservice.ExecuteMultiOperationResponse:
			for _, operation := range response.Responses {
				if started := operation.GetStartWorkflow(); started != nil {
					evidence.StartAccepted = true
					evidence.RunID = started.RunId
				}
				if updated := operation.GetUpdateWorkflow(); updated != nil {
					evidence.UpdateStage = updated.GetStage()
				}
			}
		case *workflowservice.UpdateWorkflowExecutionResponse:
			evidence.UpdateStage = response.GetStage()
			if response.GetUpdateRef().GetWorkflowExecution().GetRunId() != "" {
				evidence.RunID = response.GetUpdateRef().GetWorkflowExecution().GetRunId()
			}
		case *workflowservice.PollWorkflowExecutionUpdateResponse:
			if response.GetStage() != enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED {
				evidence.UpdateStage = response.GetStage()
			}
			if response.GetOutcome() != nil {
				evidence.UpdateStage = enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
			}
		}
	}
	return err
}

type taskInterceptor struct {
	interceptor.WorkerInterceptorBase
	worker *Worker
}

func (worker *Worker) beginTask(ctx context.Context, evidence TaskResult) (*invocation.Call[TaskResult], *taskBinding, error) {
	if len(evidence.Namespace)+len(evidence.TaskQueue)+len(evidence.WorkflowID)+len(evidence.RunID)+len(evidence.ActivityID)+len(evidence.ActivityRunID)+len(evidence.ActivityType)+len(evidence.NexusService)+len(evidence.NexusOperation)+len(evidence.NexusRequestID) > 2048 {
		return nil, nil, taskAdmission(failure(ErrLimit, "task-identity"))
	}
	select {
	case worker.handlers <- struct{}{}:
	default:
		return nil, nil, taskAdmission(failure(ErrLimit, "task-handlers"))
	}
	correlation := fault.Correlation{Call: uuid.NewString(), Parent: worker.correlation.Call}
	call, err := invocation.BeginNested(ctx, worker.call.Scope(), invocation.Request{Name: "task." + evidence.Kind, Correlation: correlation, Shape: invocation.Finite,
		EvidenceBytes: ExecutionEvidenceBytes, Admission: invocation.Budget{Limit: worker.client.owner.settings.AdmissionTimeout}}, worker.tasks, worker.client.observer)
	if err != nil {
		<-worker.handlers
		return nil, nil, taskAdmission(err)
	}
	return call, &taskBinding{worker: worker, scope: call.Scope(), correlation: correlation}, nil
}

func taskAdmission(cause error) error {
	return sdktemporal.NewApplicationErrorWithCause("controlled handler admission refused", "fathomry.temporal.admission", cause)
}

type callbackClient struct {
	private
	nativeClient sdk.Client
	binding      *taskBinding
}

var _ sdk.Client = (*callbackClient)(nil)

// A borrowed native interface needs Close to satisfy the SDK contract, but it
// never carries source ownership. Refuse rather than silently close a peer.
func (*callbackClient) Close() { panic(failure(ErrAuthority, "borrowed-client-close")) }

func (worker *Worker) callbackClient(ctx context.Context) sdk.Client {
	binding, _ := ctx.Value(taskBindingKey{}).(*taskBinding)
	return &callbackClient{nativeClient: worker.client.owner.native, binding: binding}
}

func (client *callbackClient) WorkflowService() workflowservice.WorkflowServiceClient {
	return &workflowView{workflowService: workflowservice.NewWorkflowServiceClient(&callbackConnection{binding: client.binding})}
}

func (client *callbackClient) OperatorService() operatorservice.OperatorServiceClient {
	return &operatorView{operatorService: operatorservice.NewOperatorServiceClient(&callbackConnection{binding: client.binding})}
}

type callbackConnection struct{ binding *taskBinding }

func (connection *callbackConnection) Invoke(ctx context.Context, method string, request, reply any, options ...grpc.CallOption) error {
	if connection.binding == nil {
		return failure(ErrAuthority, "client-setup-rpc")
	}
	if ctx == nil || connection.binding == nil {
		return failure(ErrAuthority, "callback-context")
	}
	if len(options) > 1 {
		return failure(ErrAuthority, "callback-rpc-options")
	}
	for _, option := range options {
		if _, ok := option.(grpc.StaticMethodCallOption); !ok {
			return failure(ErrAuthority, "callback-rpc-options")
		}
	}
	ctx = metadata.NewOutgoingContext(executionContext{ctx}, nil)
	ctx = context.WithValue(ctx, taskBindingKey{}, connection.binding)
	ctx = context.WithValue(ctx, taskRawKey{}, true)
	return connection.binding.worker.client.owner.transport.Invoke(ctx, method, request, reply, options...)
}

func (*callbackConnection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, failure(ErrAuthority, "callback-stream")
}

func (binding *taskBinding) invoke(ctx context.Context, owner *connection, method string, request, reply any, connection *grpc.ClientConn, next grpc.UnaryInvoker, options ...grpc.CallOption) error {
	if binding.closed.Load() || binding.worker.client.owner != owner {
		return failure(ErrAuthority, "expired-callback")
	}
	if (ctx.Value(taskRawKey{}) == true || !ordinaryExecutionRPC(method)) && !slices.Contains(owner.settings.RPCs, method) {
		return failure(ErrAuthority, "callback-rpc")
	}
	name := "callback." + strings.ToLower(method[strings.LastIndexByte(method, '/')+1:])
	call, err := invocation.BeginNested(ctx, binding.scope, invocation.Request{Name: name, Correlation: fault.Correlation{Call: uuid.NewString(), Parent: binding.correlation.Call},
		Shape: invocation.Finite, EvidenceBytes: ExecutionEvidenceBytes, Admission: invocation.Budget{Limit: owner.settings.AdmissionTimeout}}, binding.worker.client.inbox, binding.worker.client.observer)
	if err != nil {
		return err
	}
	var nativeError error
	invoked := false
	_ = call.Execute(ctx, invocation.Budget{Limit: owner.settings.RPCTimeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Execution] {
		var identity observedNativeIdentity
		authority := &nativeCall{owner: owner, callback: true, scopeOwner: sdk.NewFathomryScopeOwnerV1(), identity: &identity}
		defer authority.closed.Store(true)
		work = metadata.NewOutgoingContext(work, nil)
		work = context.WithValue(work, activeRPCKey{}, call)
		work = context.WithValue(work, nativeCallKey{}, authority)
		invoked = true
		nativeError = boundedNativeRPC(work, owner.settings, method, request, reply, connection, next, options...)
		err := nativeError
		evidence := Execution{Operation: name, Namespace: owner.settings.Namespace, NativeCalled: true, Accepted: err == nil}
		if identity.apply(&evidence) {
			evidence.WorkflowID, evidence.RunID, evidence.ActivityID, evidence.UpdateID = "", "", "", ""
			evidence.ScheduleID, evidence.NexusOperationID, evidence.DeploymentName = "", "", ""
			evidence.IdentityOmitted = true
			nativeError = errors.Join(nativeError, failure(ErrLimit, "execution-identity"))
			err = nativeError
		}
		if err != nil {
			err = failure(ErrExecution, name, err, work.Err(), context.Cause(work))
		}
		return invocation.Outcome[Execution]{Value: evidence, Present: true, Primary: err}
	})
	if invoked {
		return nativeError
	}
	result, _ := call.Receipt().Result()
	return result.Err()
}

func ordinaryExecutionRPC(method string) bool {
	if method == "/grpc.health.v1.Health/Check" {
		return true
	}
	if !strings.HasPrefix(method, "/temporal.api.workflowservice.v1.WorkflowService/") {
		return false
	}
	switch method[strings.LastIndexByte(method, '/')+1:] {
	case "CreateSchedule", "DescribeSchedule", "UpdateSchedule", "PatchSchedule", "DeleteSchedule", "ListSchedules",
		"StartNexusOperationExecution", "PollNexusOperationExecution", "DescribeNexusOperationExecution", "RequestCancelNexusOperationExecution",
		"TerminateNexusOperationExecution", "ListNexusOperationExecutions", "CountNexusOperationExecutions":
		return true
	case "GetSystemInfo", "DescribeNamespace", "DescribeTaskQueue", "GetSearchAttributes":
		return true
	case "StartWorkflowExecution", "SignalWithStartWorkflowExecution", "ExecuteMultiOperation", "GetWorkflowExecutionHistory",
		"SignalWorkflowExecution", "QueryWorkflow", "UpdateWorkflowExecution", "PollWorkflowExecutionUpdate",
		"RequestCancelWorkflowExecution", "TerminateWorkflowExecution", "DescribeWorkflowExecution",
		"ListWorkflowExecutions", "ListOpenWorkflowExecutions", "ListClosedWorkflowExecutions", "ListArchivedWorkflowExecutions", "ScanWorkflowExecutions", "CountWorkflowExecutions",
		"StartActivityExecution", "DescribeActivityExecution", "PollActivityExecution", "GetActivityExecution", "ListActivityExecutions", "CountActivityExecutions",
		"RequestCancelActivityExecution", "TerminateActivityExecution", "RespondActivityTaskCompleted", "RespondActivityTaskFailed", "RespondActivityTaskCanceled",
		"RespondActivityTaskCompletedById", "RespondActivityTaskFailedById", "RespondActivityTaskCanceledById", "RecordActivityTaskHeartbeat", "RecordActivityTaskHeartbeatById":
		return true
	}
	return false
}

func callbackGranted[T any](ctx context.Context, client *callbackClient, method string, evidence Execution, invoke func(context.Context, *Execution) (T, error)) (T, error) {
	if method != "" && (client == nil || client.binding == nil || !slices.Contains(client.binding.worker.client.owner.settings.RPCs, method)) {
		var zero T
		return zero, failure(ErrAuthority, "callback-rpc")
	}
	return callbackNative(ctx, client, "", evidence, invoke)
}

// callbackNative owns conversion/interception as well as transport. Native
// return types and errors are retained: temporalnexus inspects concrete native
// Update handles to distinguish synchronous from asynchronous outcomes.
func callbackNative[T any](ctx context.Context, client *callbackClient, namespace string, evidence Execution, invoke func(context.Context, *Execution) (T, error)) (T, error) {
	var output T
	if ctx == nil || client == nil || client.binding == nil || client.binding.closed.Load() {
		return output, failure(ErrAuthority, "expired-callback")
	}
	executions := client.binding.worker.client
	if namespace != "" && namespace != executions.Namespace() {
		return output, failure(ErrAuthority, "callback-namespace")
	}
	if !evidence.validIdentity() {
		return output, failure(ErrLimit, "execution-identity")
	}
	evidence.Namespace = executions.Namespace()
	call, err := invocation.BeginNested(ctx, client.binding.scope, invocation.Request{Name: evidence.Operation,
		Correlation: fault.Correlation{Call: uuid.NewString(), Parent: client.binding.correlation.Call}, Shape: invocation.Finite,
		EvidenceBytes: ExecutionEvidenceBytes, Admission: invocation.Budget{Limit: executions.owner.settings.AdmissionTimeout}}, executions.inbox, executions.observer)
	if err != nil {
		return output, err
	}
	var nativeError error
	returned := false
	scopedContext := context.WithValue(ctx, callbackCallKey{}, client)
	output, err = finishNative(scopedContext, executions, call, evidence, func(work context.Context, evidence *Execution) (T, error) {
		result, err := invoke(work, evidence)
		nativeError = scopeNativeError(work, executions, *evidence, err)
		returned = true
		return result, err
	})
	if returned && nativeError != nil {
		return output, nativeError
	}
	return output, err
}
