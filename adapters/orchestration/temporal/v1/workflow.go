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
	context "context"
	native "github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
)

func (client *Client) executions() *native.Executions {
	if client == nil {
		return nil
	}
	return client.native
}

// WorkflowRun stays with its originating Client use and source generation.
// Identity getters are local. Get cancellation never cancels the remote Workflow.
type WorkflowRun struct {
	private
	native *native.WorkflowRun
	client *Client
}

func workflowRun(value *native.WorkflowRun, client *Client) *WorkflowRun {
	if value == nil {
		return nil
	}
	return &WorkflowRun{native: value, client: client}
}
func (run *WorkflowRun) GetID() string {
	if run == nil {
		return ""
	}
	return run.native.GetID()
}
func (run *WorkflowRun) GetRunID() string {
	if run == nil {
		return ""
	}
	return run.native.GetRunID()
}
func (run *WorkflowRun) GetFirstExecutionRunID() string {
	if run == nil {
		return ""
	}
	return run.native.GetFirstExecutionRunID()
}
func (run *WorkflowRun) Get(ctx context.Context, output any) error {
	return run.GetWithOptions(ctx, output, sdk.WorkflowRunGetOptions{})
}
func (run *WorkflowRun) GetWithOptions(ctx context.Context, output any, options sdk.WorkflowRunGetOptions) error {
	if run == nil {
		return fail(ErrInput, "workflow-result")
	}
	return translate(run.native.GetWithOptions(ctx, run.client.correlation(), output, options), "workflow-result")
}
func (client *Client) ExecuteWorkflow(ctx context.Context, options sdk.StartWorkflowOptions, definition any, args ...any) (*WorkflowRun, error) {
	value, err := client.executions().ExecuteWorkflow(ctx, client.correlation(), options, definition, args...)
	return workflowRun(value, client), translate(err, "workflow-start")
}
func (client *Client) GetWorkflow(workflowID, runID string) (*WorkflowRun, error) {
	value, err := client.executions().GetWorkflow(workflowID, runID)
	return workflowRun(value, client), translate(err, "workflow-handle")
}
func (client *Client) SignalWorkflow(ctx context.Context, workflowID, runID, signal string, argument any) error {
	return translate(client.executions().SignalWorkflow(ctx, client.correlation(), workflowID, runID, signal, argument), "workflow-signal")
}

// WithStartWorkflowOperation is a single-use intention bound to one retained
// Client identity. A facade copy shares it; another use cannot consume it.
type WithStartWorkflowOperation struct {
	private
	native *native.WithStartWorkflowOperation
	client *Client
}

func (client *Client) NewWithStartWorkflowOperation(options sdk.StartWorkflowOptions, definition any, args ...any) (*WithStartWorkflowOperation, error) {
	value, err := client.executions().NewWithStartWorkflowOperation(options, definition, args...)
	if value == nil {
		return nil, translate(err, "with-start")
	}
	return &WithStartWorkflowOperation{native: value, client: client}, translate(err, "with-start")
}
func (operation *WithStartWorkflowOperation) WorkflowID() string {
	if operation == nil {
		return ""
	}
	return operation.native.WorkflowID()
}
func (operation *WithStartWorkflowOperation) Get(ctx context.Context) (*WorkflowRun, error) {
	if operation == nil {
		return nil, fail(ErrInput, "with-start-result")
	}
	value, err := operation.native.Get(ctx, operation.client.correlation())
	return workflowRun(value, operation.client), translate(err, "with-start-result")
}

// WorkflowUpdate preserves native protocol-stage and delayed-result behavior.
type WorkflowUpdate struct {
	private
	native *native.WorkflowUpdate
	client *Client
}

func workflowUpdate(value *native.WorkflowUpdate, client *Client) *WorkflowUpdate {
	if value == nil {
		return nil
	}
	return &WorkflowUpdate{native: value, client: client}
}
func (client *Client) UpdateWithStartWorkflow(ctx context.Context, operation *WithStartWorkflowOperation, options sdk.UpdateWorkflowOptions) (*WorkflowUpdate, error) {
	if client == nil || client.use == nil || operation == nil || operation.client == nil || operation.client.use != client.use {
		return nil, fail(ErrAuthority, "workflow-update-start")
	}
	value, err := client.native.UpdateWithStartWorkflow(ctx, client.correlation(), operation.native, options)
	return workflowUpdate(value, client), translate(err, "workflow-update-start")
}
func (update *WorkflowUpdate) Get(ctx context.Context, output any) error {
	if update == nil {
		return fail(ErrInput, "update-result")
	}
	return translate(update.native.Get(ctx, update.client.correlation(), output), "update-result")
}
func (update *WorkflowUpdate) WorkflowID() string {
	if update == nil {
		return ""
	}
	return update.native.WorkflowID()
}
func (update *WorkflowUpdate) RunID() string {
	if update == nil {
		return ""
	}
	return update.native.RunID()
}
func (update *WorkflowUpdate) UpdateID() string {
	if update == nil {
		return ""
	}
	return update.native.UpdateID()
}

// WorkflowDescription retains its originating Client identity and native semantics.
type WorkflowDescription struct {
	private
	native   *native.WorkflowDescription
	client   *Client
	metadata sdk.WorkflowExecutionMetadata
}

func wrapWorkflowDescription(value *native.WorkflowDescription, client *Client) *WorkflowDescription {
	if value == nil {
		return nil
	}
	return &WorkflowDescription{native: value, client: client, metadata: cloneWorkflowMetadata(value.WorkflowExecutionMetadata)}
}

// ResetWorkflowExecutionResult preserves the effective request ID even when
// the response is lost. Response nil does not prove the reset did not happen.
type ResetWorkflowExecutionResult struct {
	private
	RequestID string
	Response  *workflowservice.ResetWorkflowExecutionResponse
}

// SignalWithStartWorkflow preserves the selected native contract within this retained use.
func (view *Client) SignalWithStartWorkflow(ctx context.Context, workflowID string, signal string, signalArgument any, options sdk.StartWorkflowOptions, definition any, args ...any) (*WorkflowRun, error) {
	value, err := view.executions().SignalWithStartWorkflow(ctx, view.correlation(), workflowID, signal, signalArgument, options, definition, args...)
	return workflowRun(value, view), translate(err, "signalwithstartworkflow")
}

// QueryWorkflow includes native result decoding within the admitted call.
func (view *Client) QueryWorkflow(ctx context.Context, workflowID string, runID string, query string, result any, args ...any) error {
	return translate(view.executions().QueryWorkflow(ctx, view.correlation(), workflowID, runID, query, result, args...), "queryworkflow")
}

// QueryWorkflowWithOptions preserves native headers and rejection conditions.
// A non-nil rejection is a native state rejection, not a decoded query result.
// Result decoding remains inside the admitted call; the rejection is caller-owned.
func (view *Client) QueryWorkflowWithOptions(ctx context.Context, request *sdk.QueryWorkflowWithOptionsRequest, result any) (*querypb.QueryRejected, error) {
	value, err := view.executions().QueryWorkflowWithOptions(ctx, view.correlation(), request, result)
	return value, translate(err, "queryworkflowwithoptions")
}

// DescribeWorkflowExecution returns caller-owned native execution metadata.
func (view *Client) DescribeWorkflowExecution(ctx context.Context, workflowID string, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	value, err := view.executions().DescribeWorkflowExecution(ctx, view.correlation(), workflowID, runID)
	return value, translate(err, "describeworkflowexecution")
}

// CancelWorkflow preserves the selected native contract within this retained use.
func (view *Client) CancelWorkflow(ctx context.Context, options sdk.CancelWorkflowOptions) error {
	return translate(view.executions().CancelWorkflow(ctx, view.correlation(), options), "cancelworkflow")
}

// TerminateWorkflow preserves the selected native contract within this retained use.
func (view *Client) TerminateWorkflow(ctx context.Context, options sdk.TerminateWorkflowOptions) error {
	return translate(view.executions().TerminateWorkflow(ctx, view.correlation(), options), "terminateworkflow")
}

// UpdateWorkflow preserves the selected native contract within this retained use.
func (view *Client) UpdateWorkflow(ctx context.Context, options sdk.UpdateWorkflowOptions) (*WorkflowUpdate, error) {
	value, err := view.executions().UpdateWorkflow(ctx, view.correlation(), options)
	return workflowUpdate(value, view), translate(err, "updateworkflow")
}

// GetWorkflowUpdateHandle preserves the selected native contract within this retained use.
func (view *Client) GetWorkflowUpdateHandle(options sdk.GetWorkflowUpdateHandleOptions) (*WorkflowUpdate, error) {
	value, err := view.executions().GetWorkflowUpdateHandle(options)
	return workflowUpdate(value, view), translate(err, "getworkflowupdatehandle")
}

// DescribeWorkflow preserves the selected native contract within this retained use.
func (view *Client) DescribeWorkflow(ctx context.Context, workflowID string, runID string) (*WorkflowDescription, error) {
	value, err := view.executions().DescribeWorkflow(ctx, view.correlation(), workflowID, runID)
	return wrapWorkflowDescription(value, view), translate(err, "describeworkflow")
}

// GetStaticSummary preserves the selected native contract within this retained use.
func (view *WorkflowDescription) GetStaticSummary(ctx context.Context) (string, error) {
	if view == nil || view.native == nil {
		var zero string
		return zero, fail(ErrInput, "getstaticsummary")
	}
	value, err := view.native.GetStaticSummary(ctx, view.client.correlation())
	return value, translate(err, "getstaticsummary")
}

// GetStaticDetails preserves the selected native contract within this retained use.
func (view *WorkflowDescription) GetStaticDetails(ctx context.Context) (string, error) {
	if view == nil || view.native == nil {
		var zero string
		return zero, fail(ErrInput, "getstaticdetails")
	}
	value, err := view.native.GetStaticDetails(ctx, view.client.correlation())
	return value, translate(err, "getstaticdetails")
}

// GetMemoValue preserves the selected native contract within this retained use.
func (view *WorkflowDescription) GetMemoValue(ctx context.Context, key string, output any) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "getmemovalue")
	}
	return translate(view.native.GetMemoValue(ctx, view.client.correlation(), key, output), "getmemovalue")
}

// ResetWorkflowExecution clones the request before native defaulting. The native
// namespace requirement is preserved, and the exact reset RPC grant is required.
// Retrying an uncertain reset requires reusing the returned RequestID explicitly.
func (view *Client) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (ResetWorkflowExecutionResult, error) {
	value, err := view.executions().ResetWorkflowExecution(ctx, view.correlation(), request)
	return ResetWorkflowExecutionResult{RequestID: value.RequestID, Response: value.Response}, translate(err, "resetworkflowexecution")
}

// UpdateWorkflowExecutionOptions preserves native validation, patch conversion
// and result conversion under the exact operational RPC grant.
//
// Experimental: requires compatible server support.
func (view *Client) UpdateWorkflowExecutionOptions(ctx context.Context, options sdk.UpdateWorkflowExecutionOptionsRequest) (sdk.WorkflowExecutionOptions, error) {
	value, err := view.executions().UpdateWorkflowExecutionOptions(ctx, view.correlation(), options)
	return value, translate(err, "updateworkflowexecutionoptions")
}
