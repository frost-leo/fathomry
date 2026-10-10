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

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/google/uuid"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"
)

// ResetWorkflowExecutionResult retains the effective deduplication identity even
// when a transport error leaves the remote outcome unknown. Response, when
// present, belongs to the caller; RequestID alone is not acceptance evidence.
// RequestID is empty if an effective identity was refused or could not fit the
// evidence bound. Before admission it retains the original generated intention.
type ResetWorkflowExecutionResult struct {
	RequestID string
	Response  *workflowservice.ResetWorkflowExecutionResponse
}

// ResetWorkflowExecution clones the request before native defaulting. The native
// namespace requirement is preserved, and the exact reset RPC grant is required.
// Retrying an uncertain reset requires reusing the returned RequestID explicitly.
func (client *Executions) ResetWorkflowExecution(ctx context.Context, correlation fault.Correlation, request *workflowservice.ResetWorkflowExecutionRequest) (ResetWorkflowExecutionResult, error) {
	if request == nil {
		return ResetWorkflowExecutionResult{}, failure(ErrInput, "workflow-reset")
	}
	requestID := request.RequestId
	if requestID == "" {
		requestID = uuid.NewString()
	}
	result := ResetWorkflowExecutionResult{RequestID: requestID}
	var final *Execution
	_, err := operationalNative(ctx, client, correlation, Execution{Operation: "workflow.reset", WorkflowID: request.GetWorkflowExecution().GetWorkflowId(), RunID: request.GetWorkflowExecution().GetRunId(), RequestID: requestID}, []string{"ResetWorkflowExecution"},
		func(work context.Context, evidence *Execution) (struct{}, error) {
			final = evidence
			evidence.NativeCalled = false
			if _, err := messageSize(work, request, client.owner.settings.MaxRequestBytes); err != nil {
				return struct{}{}, err
			}
			copied := proto.Clone(request).(*workflowservice.ResetWorkflowExecutionRequest)
			copied.RequestId = requestID
			evidence.NativeCalled = true
			response, err := client.owner.native.ResetWorkflowExecution(work, copied)
			evidence.Accepted = err == nil
			evidence.RequestID = ""
			if validText(copied.RequestId, 1024) {
				evidence.RequestID = copied.RequestId
			}
			result.Response = response
			return struct{}{}, err
		})
	if final != nil {
		result.RequestID = final.RequestID
		if final.IdentityOmitted {
			result.RequestID = ""
		}
	}
	return result, err
}

// UpdateWorkflowExecutionOptions preserves native validation, patch conversion
// and result conversion under the exact operational RPC grant.
//
// Experimental: requires compatible server support.
func (client *Executions) UpdateWorkflowExecutionOptions(ctx context.Context, correlation fault.Correlation, options sdk.UpdateWorkflowExecutionOptionsRequest) (sdk.WorkflowExecutionOptions, error) {
	return operationalNative(ctx, client, correlation, Execution{Operation: "workflow.execution-options"}, []string{"UpdateWorkflowExecutionOptions"},
		func(work context.Context, evidence *Execution) (sdk.WorkflowExecutionOptions, error) {
			value, err := client.owner.native.UpdateWorkflowExecutionOptions(work, options)
			evidence.Accepted = err == nil
			return value, err
		})
}
