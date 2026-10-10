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
	"github.com/frost-leo/fathomry/adapters/orchestration/v1"
	native "github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

// Attribution identifies actual local use, not a remote effect or business run.
type Attribution orchestration.Attribution

// Execution is detached process-operation evidence. NativeCalled alone proves
// neither remote acceptance nor remote completion. Payloads/tokens are excluded.
type Execution struct {
	private
	Operation, Namespace, WorkflowID, RunID, ActivityID, UpdateID                                       string
	ScheduleID, NexusOperationID, DeploymentName, RequestID                                             string
	NativeCalled, Accepted, ResultObtained, StartAccepted, IdentityOmitted                              bool
	UpdateStage                                                                                         enumspb.UpdateWorkflowExecutionLifecycleStage
	HeartbeatAcknowledged, CompletionAcknowledged, CancellationRequested, ActivityPaused, ActivityReset bool
}

// RPCResult distinguishes transmission from acknowledged response.
type RPCResult struct {
	private
	Method                                    string
	RequestBytes, ResponseBytes               int
	ResponseBytesKnown, Invoked, Acknowledged bool
}

// Result is required independent operation/source evidence, not its return value.
// Inspecting identity or NativeError deliberately crosses the redaction boundary.
type Result struct {
	private
	Source                       Attribution
	Execution                    Execution
	RPC                          RPCResult
	SourceOpened, SourceReleased bool
	semantic                     error
}

// NativeError exposes only an exact captured native return, never a guessed cause.
func (result Result) NativeError() (error, bool) { return result.semantic, result.semantic != nil }

// WorkerResult separates accepted startup, native Stop return and actual join.
type WorkerResult struct {
	private
	Source                              Attribution
	Namespace, TaskQueue                string
	Started, NativeStopReturned, Joined bool
}

// TaskResult records user-handler execution only. Decoding before the handler,
// output conversion/finalization and remote completion are separate Worker work.
type TaskResult struct {
	private
	Source                                                                                 Attribution
	Kind, Namespace, TaskQueue, WorkflowID, RunID, ActivityID, ActivityRunID, ActivityType string
	NexusService, NexusOperation, NexusRequestID                                           string
	NexusCallbackRequested                                                                 bool
	NexusRequestLinks                                                                      int
	Attempt                                                                                int32
	Local, HandlerReturned, AsyncCompletion                                                bool
}

func executionResult(value native.Execution, source Attribution) Result {
	semantic, _ := value.NativeCause()
	return Result{Source: source, semantic: semantic, Execution: Execution{Operation: value.Operation, Namespace: value.Namespace,
		WorkflowID: value.WorkflowID, RunID: value.RunID, ActivityID: value.ActivityID, UpdateID: value.UpdateID,
		ScheduleID: value.ScheduleID, NexusOperationID: value.NexusOperationID, DeploymentName: value.DeploymentName, RequestID: value.RequestID,
		NativeCalled: value.NativeCalled, Accepted: value.Accepted, ResultObtained: value.ResultObtained, StartAccepted: value.StartAccepted,
		UpdateStage: value.UpdateStage, IdentityOmitted: value.IdentityOmitted, HeartbeatAcknowledged: value.HeartbeatAcknowledged,
		CompletionAcknowledged: value.CompletionAcknowledged, CancellationRequested: value.CancellationRequested, ActivityPaused: value.ActivityPaused, ActivityReset: value.ActivityReset}}
}
func rpcResult(value native.RPCResult, source Attribution) Result {
	semantic, _ := value.NativeCause()
	return Result{Source: source, semantic: semantic, RPC: RPCResult{Method: value.Method, RequestBytes: value.RequestBytes,
		ResponseBytes: value.ResponseBytes, ResponseBytesKnown: value.ResponseBytesKnown, Invoked: value.Invoked, Acknowledged: value.Acknowledged}}
}
func workerResult(value native.WorkerResult, source Attribution) WorkerResult {
	return WorkerResult{Source: source, Namespace: value.Namespace, TaskQueue: value.TaskQueue, Started: value.Started, NativeStopReturned: value.NativeStopReturned, Joined: value.Joined}
}
func taskResult(value native.TaskResult, source Attribution) TaskResult {
	return TaskResult{Source: source, Kind: value.Kind, Namespace: value.Namespace, TaskQueue: value.TaskQueue, WorkflowID: value.WorkflowID, RunID: value.RunID,
		ActivityID: value.ActivityID, ActivityRunID: value.ActivityRunID, ActivityType: value.ActivityType, NexusService: value.NexusService, NexusOperation: value.NexusOperation,
		NexusRequestID: value.NexusRequestID, NexusCallbackRequested: value.NexusCallbackRequested, NexusRequestLinks: value.NexusRequestLinks,
		Attempt: value.Attempt, Local: value.Local, HandlerReturned: value.HandlerReturned, AsyncCompletion: value.AsyncCompletion}
}
