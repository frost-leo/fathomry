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
	native "github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	sdktemporal "go.temporal.io/sdk/temporal"
	nativeworker "go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"
)

func cloneMessage[T proto.Message](value T) T {
	if !value.ProtoReflect().IsValid() {
		var zero T
		return zero
	}
	return proto.Clone(value).(T)
}
func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// Metadata returns detached data only. It has no converter, payload retriever or
// source authority; sensitive payloads are explicitly inspectable by its caller.
func (description *WorkflowDescription) Metadata() sdk.WorkflowExecutionMetadata {
	if description == nil || description.native == nil {
		return sdk.WorkflowExecutionMetadata{}
	}
	return cloneWorkflowMetadata(description.metadata)
}
func cloneWorkflowMetadata(value sdk.WorkflowExecutionMetadata) sdk.WorkflowExecutionMetadata {
	value.Memo = cloneMessage(value.Memo)
	value.TypedSearchAttributes = sdktemporal.NewSearchAttributes(value.TypedSearchAttributes.Copy())
	value.ParentWorkflowExecution = clonePointer(value.ParentWorkflowExecution)
	value.RootWorkflowExecution = clonePointer(value.RootWorkflowExecution)
	value.WorkflowCloseTime = clonePointer(value.WorkflowCloseTime)
	value.ExecutionTime = clonePointer(value.ExecutionTime)
	return value
}

// ActivityMetadata preserves every selected native description data field
// without its private converters or lazy decoder methods.
type ActivityMetadata struct {
	private
	sdk.ActivityExecutionInfo
	RawResponse                                                                                       *workflowservice.DescribeActivityExecutionResponse
	ScheduleToCloseTimeout, ScheduleToStartTimeout, StartToCloseTimeout, HeartbeatTimeout, StartDelay time.Duration
	RunState                                                                                          enumspb.PendingActivityState
	LastHeartbeatTime, LastStartedTime                                                                time.Time
	Attempt                                                                                           int32
	TotalHeartbeatCount                                                                               int64
	RetryPolicy                                                                                       *sdktemporal.RetryPolicy
	ExpirationTime                                                                                    time.Time
	LastWorkerIdentity                                                                                string
	CurrentRetryInterval                                                                              time.Duration
	LastAttemptCompleteTime, NextAttemptScheduleTime                                                  time.Time
	LastDeploymentVersion                                                                             *nativeworker.WorkerDeploymentVersion
	Priority                                                                                          sdktemporal.Priority
	CanceledReason                                                                                    string
}

// Metadata copies all data containers. Caller mutation cannot change a later
// admitted description decode or another metadata copy.
func (description *ActivityDescription) Metadata() ActivityMetadata {
	if description == nil || description.native == nil {
		return ActivityMetadata{}
	}
	return cloneActivityMetadata(description.metadata)
}
func cloneActivityMetadata(value ActivityMetadata) ActivityMetadata {
	value.RawExecutionListInfo = cloneMessage(value.RawExecutionListInfo)
	value.TypedSearchAttributes = sdktemporal.NewSearchAttributes(value.TypedSearchAttributes.Copy())
	value.RawResponse = cloneMessage(value.RawResponse)
	value.RetryPolicy = clonePointer(value.RetryPolicy)
	if value.RetryPolicy != nil {
		value.RetryPolicy.NonRetryableErrorTypes = slices.Clone(value.RetryPolicy.NonRetryableErrorTypes)
	}
	value.LastDeploymentVersion = clonePointer(value.LastDeploymentVersion)
	return value
}
func captureActivityMetadata(native *native.ActivityDescription) ActivityMetadata {
	info := native.ClientActivityExecutionInfo
	info.RawExecutionListInfo = cloneMessage(info.RawExecutionListInfo)
	info.TypedSearchAttributes = sdktemporal.NewSearchAttributes(info.TypedSearchAttributes.Copy())
	retry := clonePointer(native.RetryPolicy)
	if retry != nil {
		retry.NonRetryableErrorTypes = slices.Clone(retry.NonRetryableErrorTypes)
	}
	return ActivityMetadata{ActivityExecutionInfo: info, RawResponse: cloneMessage(native.RawResponse),
		ScheduleToCloseTimeout: native.ScheduleToCloseTimeout, ScheduleToStartTimeout: native.ScheduleToStartTimeout, StartToCloseTimeout: native.StartToCloseTimeout,
		HeartbeatTimeout: native.HeartbeatTimeout, StartDelay: native.StartDelay, RunState: native.RunState, LastHeartbeatTime: native.LastHeartbeatTime, LastStartedTime: native.LastStartedTime,
		Attempt: native.Attempt, TotalHeartbeatCount: native.TotalHeartbeatCount, RetryPolicy: retry, ExpirationTime: native.ExpirationTime, LastWorkerIdentity: native.LastWorkerIdentity,
		CurrentRetryInterval: native.CurrentRetryInterval, LastAttemptCompleteTime: native.LastAttemptCompleteTime, NextAttemptScheduleTime: native.NextAttemptScheduleTime,
		LastDeploymentVersion: clonePointer(native.LastDeploymentVersion), Priority: native.Priority, CanceledReason: native.CanceledReason}
}
func (description *ActivityDescription) HasInput() bool {
	return description != nil && description.present[0]
}
func (description *ActivityDescription) HasHeartbeatDetails() bool {
	return description != nil && description.present[1]
}
func (description *ActivityDescription) HasResult() bool {
	return description != nil && description.present[2]
}
func (description *ActivityDescription) HasOutcomeFailure() bool {
	return description != nil && description.present[3]
}
func (description *ActivityDescription) HasLastFailure() bool {
	return description != nil && description.present[4]
}

// NexusMetadata is a field-only copy of the selected experimental description.
type NexusMetadata struct {
	private
	sdk.NexusOperationMetadata
	RawInfo                                                             *nexuspb.NexusOperationExecutionInfo
	State                                                               enumspb.PendingNexusOperationState
	ScheduleToCloseTimeout, ScheduleToStartTimeout, StartToCloseTimeout time.Duration
	Attempt                                                             int32
	ExpirationTime, LastAttemptCompleteTime, NextAttemptScheduleTime    time.Time
	LastAttemptFailure                                                  *failurepb.Failure
	BlockedReason, OperationToken, Identity                             string
	Cancellation                                                        *NexusCancellationMetadata
}
type NexusCancellationMetadata struct {
	private
	RawInfo                                          *nexuspb.NexusOperationExecutionCancellationInfo
	RequestedTime                                    time.Time
	State                                            enumspb.NexusOperationCancellationState
	Attempt                                          int32
	LastAttemptCompleteTime, NextAttemptScheduleTime time.Time
	BlockedReason, Reason                            string
}

func (description *NexusDescription) Metadata() NexusMetadata {
	if description == nil || description.native == nil {
		return NexusMetadata{}
	}
	return cloneNexusMetadata(description.metadata)
}
func cloneNexusMetadata(value NexusMetadata) NexusMetadata {
	value.RawExecutionListInfo = cloneMessage(value.RawExecutionListInfo)
	value.SearchAttributes = sdktemporal.NewSearchAttributes(value.SearchAttributes.Copy())
	value.RawInfo = cloneMessage(value.RawInfo)
	value.LastAttemptFailure = cloneMessage(value.LastAttemptFailure)
	value.Cancellation = clonePointer(value.Cancellation)
	if value.Cancellation != nil {
		value.Cancellation.RawInfo = cloneMessage(value.Cancellation.RawInfo)
	}
	return value
}
func captureNexusMetadata(native *native.NexusDescription) NexusMetadata {
	info := native.ClientNexusOperationMetadata
	info.RawExecutionListInfo = cloneMessage(info.RawExecutionListInfo)
	info.SearchAttributes = sdktemporal.NewSearchAttributes(info.SearchAttributes.Copy())
	value := NexusMetadata{NexusOperationMetadata: info, RawInfo: cloneMessage(native.RawInfo), State: native.State,
		ScheduleToCloseTimeout: native.ScheduleToCloseTimeout, ScheduleToStartTimeout: native.ScheduleToStartTimeout, StartToCloseTimeout: native.StartToCloseTimeout,
		Attempt: native.Attempt, ExpirationTime: native.ExpirationTime, LastAttemptCompleteTime: native.LastAttemptCompleteTime, NextAttemptScheduleTime: native.NextAttemptScheduleTime,
		LastAttemptFailure: cloneMessage(native.LastAttemptFailure), BlockedReason: native.BlockedReason, OperationToken: native.OperationToken, Identity: native.Identity}
	if cancellation := native.CancellationInfo; cancellation != nil {
		value.Cancellation = &NexusCancellationMetadata{RawInfo: cloneMessage(cancellation.RawInfo), RequestedTime: cancellation.RequestedTime, State: cancellation.State, Attempt: cancellation.Attempt,
			LastAttemptCompleteTime: cancellation.LastAttemptCompleteTime, NextAttemptScheduleTime: cancellation.NextAttemptScheduleTime, BlockedReason: cancellation.BlockedReason, Reason: cancellation.Reason}
	}
	return value
}

// Cancellation returns an independently guarded nested decoder, or nil when the
// service description had no cancellation information.
func (description *NexusDescription) Cancellation() *NexusCancellation {
	if description == nil || description.native == nil {
		return nil
	}
	return wrapNexusCancellation(description.native.CancellationInfo, description.client)
}
