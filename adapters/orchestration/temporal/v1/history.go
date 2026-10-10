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
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
)

// WalkHistory owns native lazy history pagination/long polling and synchronous
// visitors within one admitted call. Each page is wire-bounded; retained events
// belong to the visitor. Cancellation stops observation, not the remote Workflow.
// History filter, first/current run targeting and archival errors stay native.
func (view *Client) WalkHistory(ctx context.Context, workflowID string, runID string, longPoll bool, filter enumspb.HistoryEventFilterType, visit func(context.Context, *historypb.HistoryEvent) error) error {
	return translate(view.executions().WalkHistory(ctx, view.correlation(), workflowID, runID, longPoll, filter, visit), "walkhistory")
}

// ListWorkflow returns one caller-owned native page/result. The request is cloned
// because the SDK fills namespace and page defaults. Visibility is not a snapshot
// of execution state, and unavailable archives remain errors.
func (view *Client) ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	value, err := view.executions().ListWorkflow(ctx, view.correlation(), request)
	return value, translate(err, "listworkflow")
}

// ListOpenWorkflow returns one caller-owned native page/result. The request is cloned
// because the SDK fills namespace and page defaults. Visibility is not a snapshot
// of execution state, and unavailable archives remain errors.
func (view *Client) ListOpenWorkflow(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	value, err := view.executions().ListOpenWorkflow(ctx, view.correlation(), request)
	return value, translate(err, "listopenworkflow")
}

// ListClosedWorkflow returns one caller-owned native page/result. The request is cloned
// because the SDK fills namespace and page defaults. Visibility is not a snapshot
// of execution state, and unavailable archives remain errors.
func (view *Client) ListClosedWorkflow(ctx context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	value, err := view.executions().ListClosedWorkflow(ctx, view.correlation(), request)
	return value, translate(err, "listclosedworkflow")
}

// ListArchivedWorkflow returns one caller-owned native page/result. The request is cloned
// because the SDK fills namespace and page defaults. Visibility is not a snapshot
// of execution state, and unavailable archives remain errors.
func (view *Client) ListArchivedWorkflow(ctx context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	value, err := view.executions().ListArchivedWorkflow(ctx, view.correlation(), request)
	return value, translate(err, "listarchivedworkflow")
}

// CountWorkflow returns one caller-owned native page/result. The request is cloned
// because the SDK fills namespace and page defaults. Visibility is not a snapshot
// of execution state, and unavailable archives remain errors.
func (view *Client) CountWorkflow(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	value, err := view.executions().CountWorkflow(ctx, view.correlation(), request)
	return value, translate(err, "countworkflow")
}

// ScanWorkflow preserves the native request and page result without mutating
// caller-owned namespace or continuation state.
//
// Deprecated: use ListWorkflow. Server removal remains a native service error.
func (view *Client) ScanWorkflow(ctx context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
	value, err := view.executions().ScanWorkflow(ctx, view.correlation(), request)
	return value, translate(err, "scanworkflow")
}

// GetSearchAttributes preserves the native schema inspection result.
func (view *Client) GetSearchAttributes(ctx context.Context) (*workflowservice.GetSearchAttributesResponse, error) {
	value, err := view.executions().GetSearchAttributes(ctx, view.correlation())
	return value, translate(err, "getsearchattributes")
}

// DescribeTaskQueue preserves the native legacy task-queue view.
func (view *Client) DescribeTaskQueue(ctx context.Context, name string, kind enumspb.TaskQueueType) (*workflowservice.DescribeTaskQueueResponse, error) {
	value, err := view.executions().DescribeTaskQueue(ctx, view.correlation(), name, kind)
	return value, translate(err, "describetaskqueue")
}

// DescribeTaskQueueEnhanced preserves the selected native contract within this retained use.
func (view *Client) DescribeTaskQueueEnhanced(ctx context.Context, options sdk.DescribeTaskQueueEnhancedOptions) (sdk.TaskQueueDescription, error) {
	value, err := view.executions().DescribeTaskQueueEnhanced(ctx, view.correlation(), options)
	return value, translate(err, "describetaskqueueenhanced")
}

// WalkActivities owns the native lazy sequence and visits caller-owned metadata.
// The sequence and its expired context never escape the admitted invocation.
func (view *Client) WalkActivities(ctx context.Context, options sdk.ListActivitiesOptions, visit func(context.Context, *sdk.ActivityExecutionInfo) error) error {
	return translate(view.executions().WalkActivities(ctx, view.correlation(), options, visit), "walkactivities")
}
