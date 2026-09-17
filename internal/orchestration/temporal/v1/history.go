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
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// WalkHistory owns native lazy history pagination/long polling and synchronous
// visitors within one admitted call. Each page is wire-bounded; retained events
// belong to the visitor. Cancellation stops observation, not the remote Workflow.
// History filter, first/current run targeting and archival errors stay native.
func (client *Executions) WalkHistory(ctx context.Context, correlation fault.Correlation, workflowID, runID string, longPoll bool, filter enumspb.HistoryEventFilterType, visit func(context.Context, *historypb.HistoryEvent) error) error {
	if visit == nil {
		return failure(ErrInput, "history-visitor")
	}
	_, err := executeNative(ctx, client, correlation, Execution{Operation: "workflow.history", WorkflowID: workflowID, RunID: runID}, func(work context.Context, evidence *Execution) (struct{}, error) {
		page := &schedulePage{}
		work = context.WithValue(work, schedulePageKey{}, page)
		iterator := client.owner.native.GetWorkflowHistory(work, workflowID, runID, longPoll, filter)
		for {
			if err := work.Err(); err != nil {
				return struct{}{}, err
			}
			if !iterator.HasNext() {
				if page.more {
					continue
				}
				break
			}
			event, err := iterator.Next()
			if err != nil {
				return struct{}{}, err
			}
			if err := visit(work, event); err != nil {
				return struct{}{}, err
			}
		}
		evidence.ResultObtained = true
		return struct{}{}, nil
	})
	return err
}

// ListWorkflow returns one caller-owned native page/result. The request is cloned
// because the SDK fills namespace and page defaults. Visibility is not a snapshot
// of execution state, and unavailable archives remain errors.
func (client *Executions) ListWorkflow(ctx context.Context, correlation fault.Correlation, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	if request == nil {
		return nil, failure(ErrInput, "visibility-request")
	}
	return executeNative(ctx, client, correlation, Execution{Operation: "workflow.listworkflow"}, func(work context.Context, evidence *Execution) (*workflowservice.ListWorkflowExecutionsResponse, error) {
		if _, err := messageSize(work, request, client.owner.settings.MaxRequestBytes); err != nil {
			return nil, err
		}
		copy := proto.Clone(request).(*workflowservice.ListWorkflowExecutionsRequest)
		if copy.Namespace != "" && copy.Namespace != client.Namespace() {
			return nil, failure(ErrAuthority, "namespace")
		}
		response, err := client.owner.native.ListWorkflow(work, copy)
		evidence.ResultObtained = err == nil
		return response, err
	})
}

// ListOpenWorkflow returns one caller-owned native page/result. The request is cloned
// because the SDK fills namespace and page defaults. Visibility is not a snapshot
// of execution state, and unavailable archives remain errors.
func (client *Executions) ListOpenWorkflow(ctx context.Context, correlation fault.Correlation, request *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	if request == nil {
		return nil, failure(ErrInput, "visibility-request")
	}
	return executeNative(ctx, client, correlation, Execution{Operation: "workflow.listopenworkflow"}, func(work context.Context, evidence *Execution) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
		if _, err := messageSize(work, request, client.owner.settings.MaxRequestBytes); err != nil {
			return nil, err
		}
		copy := proto.Clone(request).(*workflowservice.ListOpenWorkflowExecutionsRequest)
		if copy.Namespace != "" && copy.Namespace != client.Namespace() {
			return nil, failure(ErrAuthority, "namespace")
		}
		response, err := client.owner.native.ListOpenWorkflow(work, copy)
		evidence.ResultObtained = err == nil
		return response, err
	})
}

// ListClosedWorkflow returns one caller-owned native page/result. The request is cloned
// because the SDK fills namespace and page defaults. Visibility is not a snapshot
// of execution state, and unavailable archives remain errors.
func (client *Executions) ListClosedWorkflow(ctx context.Context, correlation fault.Correlation, request *workflowservice.ListClosedWorkflowExecutionsRequest) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	if request == nil {
		return nil, failure(ErrInput, "visibility-request")
	}
	return executeNative(ctx, client, correlation, Execution{Operation: "workflow.listclosedworkflow"}, func(work context.Context, evidence *Execution) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
		if _, err := messageSize(work, request, client.owner.settings.MaxRequestBytes); err != nil {
			return nil, err
		}
		copy := proto.Clone(request).(*workflowservice.ListClosedWorkflowExecutionsRequest)
		if copy.Namespace != "" && copy.Namespace != client.Namespace() {
			return nil, failure(ErrAuthority, "namespace")
		}
		response, err := client.owner.native.ListClosedWorkflow(work, copy)
		evidence.ResultObtained = err == nil
		return response, err
	})
}

// ListArchivedWorkflow returns one caller-owned native page/result. The request is cloned
// because the SDK fills namespace and page defaults. Visibility is not a snapshot
// of execution state, and unavailable archives remain errors.
func (client *Executions) ListArchivedWorkflow(ctx context.Context, correlation fault.Correlation, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	if request == nil {
		return nil, failure(ErrInput, "visibility-request")
	}
	return executeNative(ctx, client, correlation, Execution{Operation: "workflow.listarchivedworkflow"}, func(work context.Context, evidence *Execution) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
		if _, err := messageSize(work, request, client.owner.settings.MaxRequestBytes); err != nil {
			return nil, err
		}
		copy := proto.Clone(request).(*workflowservice.ListArchivedWorkflowExecutionsRequest)
		if copy.Namespace != "" && copy.Namespace != client.Namespace() {
			return nil, failure(ErrAuthority, "namespace")
		}
		response, err := client.owner.native.ListArchivedWorkflow(work, copy)
		evidence.ResultObtained = err == nil
		return response, err
	})
}

// CountWorkflow returns one caller-owned native page/result. The request is cloned
// because the SDK fills namespace and page defaults. Visibility is not a snapshot
// of execution state, and unavailable archives remain errors.
func (client *Executions) CountWorkflow(ctx context.Context, correlation fault.Correlation, request *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	if request == nil {
		return nil, failure(ErrInput, "visibility-request")
	}
	return executeNative(ctx, client, correlation, Execution{Operation: "workflow.countworkflow"}, func(work context.Context, evidence *Execution) (*workflowservice.CountWorkflowExecutionsResponse, error) {
		if _, err := messageSize(work, request, client.owner.settings.MaxRequestBytes); err != nil {
			return nil, err
		}
		copy := proto.Clone(request).(*workflowservice.CountWorkflowExecutionsRequest)
		if copy.Namespace != "" && copy.Namespace != client.Namespace() {
			return nil, failure(ErrAuthority, "namespace")
		}
		response, err := client.owner.native.CountWorkflow(work, copy)
		evidence.ResultObtained = err == nil
		return response, err
	})
}

// GetSearchAttributes preserves the native schema inspection result.
func (client *Executions) GetSearchAttributes(ctx context.Context, correlation fault.Correlation) (*workflowservice.GetSearchAttributesResponse, error) {
	return executeNative(ctx, client, correlation, Execution{Operation: "workflow.search-attributes"}, func(work context.Context, evidence *Execution) (*workflowservice.GetSearchAttributesResponse, error) {
		response, err := client.owner.native.GetSearchAttributes(work)
		evidence.ResultObtained = err == nil
		return response, err
	})
}

// DescribeTaskQueue preserves the native legacy task-queue view.
func (client *Executions) DescribeTaskQueue(ctx context.Context, correlation fault.Correlation, name string, kind enumspb.TaskQueueType) (*workflowservice.DescribeTaskQueueResponse, error) {
	return executeNative(ctx, client, correlation, Execution{Operation: "task-queue.describe"}, func(work context.Context, evidence *Execution) (*workflowservice.DescribeTaskQueueResponse, error) {
		response, err := client.owner.native.DescribeTaskQueue(work, name, kind)
		evidence.ResultObtained = err == nil
		return response, err
	})
}

func (client *Executions) DescribeTaskQueueEnhanced(ctx context.Context, correlation fault.Correlation, options sdk.DescribeTaskQueueEnhancedOptions) (sdk.TaskQueueDescription, error) {
	return executeNative(ctx, client, correlation, Execution{Operation: "task-queue.describe-enhanced"}, func(work context.Context, evidence *Execution) (sdk.TaskQueueDescription, error) {
		response, err := client.owner.native.DescribeTaskQueueEnhanced(work, options)
		evidence.ResultObtained = err == nil
		return response, err
	})
}

// WalkActivities owns the native lazy sequence and visits caller-owned metadata.
// The sequence and its expired context never escape the admitted invocation.
func (client *Executions) WalkActivities(ctx context.Context, correlation fault.Correlation, options sdk.ListActivitiesOptions, visit func(context.Context, *sdk.ActivityExecutionInfo) error) error {
	if visit == nil {
		return failure(ErrInput, "activity-visitor")
	}
	_, err := executeNative(ctx, client, correlation, Execution{Operation: "activity.list"}, func(work context.Context, evidence *Execution) (struct{}, error) {
		response, err := client.owner.native.ListActivities(work, options)
		if err != nil {
			return struct{}{}, err
		}
		for entry, err := range response.Results {
			if err != nil {
				return struct{}{}, err
			}
			if err := work.Err(); err != nil {
				return struct{}{}, err
			}
			if err := visit(work, entry); err != nil {
				return struct{}{}, err
			}
		}
		evidence.ResultObtained = true
		return struct{}{}, nil
	})
	return err
}

func (client *callbackClient) GetWorkflowHistory(ctx context.Context, workflowID, runID string, longPoll bool, filter enumspb.HistoryEventFilterType) sdk.HistoryEventIterator {
	if ctx == nil {
		ctx = context.Background()
	}
	native := client.nativeClient.GetWorkflowHistory(client.iteratorContext(ctx), workflowID, runID, longPoll, filter)
	return newCallbackIterator(client, ctx, "workflow.history", "", native)
}

func (client *callbackClient) ListClosedWorkflow(ctx context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	if request == nil {
		return nil, failure(ErrInput, "callback-request")
	}
	return callbackGranted(ctx, client, "", Execution{Operation: "callback.listclosedworkflow"},
		func(work context.Context, evidence *Execution) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
			if _, err := messageSize(work, request, client.binding.worker.client.owner.settings.MaxRequestBytes); err != nil {
				return nil, err
			}
			copy := proto.Clone(request).(*workflowservice.ListClosedWorkflowExecutionsRequest)
			if copy.Namespace != "" && copy.Namespace != client.binding.worker.client.Namespace() {
				return nil, failure(ErrAuthority, "namespace")
			}
			value, err := client.nativeClient.ListClosedWorkflow(work, copy)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackClient) ListOpenWorkflow(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	if request == nil {
		return nil, failure(ErrInput, "callback-request")
	}
	return callbackGranted(ctx, client, "", Execution{Operation: "callback.listopenworkflow"},
		func(work context.Context, evidence *Execution) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
			if _, err := messageSize(work, request, client.binding.worker.client.owner.settings.MaxRequestBytes); err != nil {
				return nil, err
			}
			copy := proto.Clone(request).(*workflowservice.ListOpenWorkflowExecutionsRequest)
			if copy.Namespace != "" && copy.Namespace != client.binding.worker.client.Namespace() {
				return nil, failure(ErrAuthority, "namespace")
			}
			value, err := client.nativeClient.ListOpenWorkflow(work, copy)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackClient) ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	if request == nil {
		return nil, failure(ErrInput, "callback-request")
	}
	return callbackGranted(ctx, client, "", Execution{Operation: "callback.listworkflow"},
		func(work context.Context, evidence *Execution) (*workflowservice.ListWorkflowExecutionsResponse, error) {
			if _, err := messageSize(work, request, client.binding.worker.client.owner.settings.MaxRequestBytes); err != nil {
				return nil, err
			}
			copy := proto.Clone(request).(*workflowservice.ListWorkflowExecutionsRequest)
			if copy.Namespace != "" && copy.Namespace != client.binding.worker.client.Namespace() {
				return nil, failure(ErrAuthority, "namespace")
			}
			value, err := client.nativeClient.ListWorkflow(work, copy)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackClient) ListArchivedWorkflow(ctx context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	if request == nil {
		return nil, failure(ErrInput, "callback-request")
	}
	return callbackGranted(ctx, client, "", Execution{Operation: "callback.listarchivedworkflow"},
		func(work context.Context, evidence *Execution) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
			if _, err := messageSize(work, request, client.binding.worker.client.owner.settings.MaxRequestBytes); err != nil {
				return nil, err
			}
			copy := proto.Clone(request).(*workflowservice.ListArchivedWorkflowExecutionsRequest)
			if copy.Namespace != "" && copy.Namespace != client.binding.worker.client.Namespace() {
				return nil, failure(ErrAuthority, "namespace")
			}
			value, err := client.nativeClient.ListArchivedWorkflow(work, copy)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackClient) ScanWorkflow(ctx context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
	if request == nil {
		return nil, failure(ErrInput, "callback-request")
	}
	return callbackGranted(ctx, client, "", Execution{Operation: "callback.scanworkflow"},
		func(work context.Context, evidence *Execution) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
			if _, err := messageSize(work, request, client.binding.worker.client.owner.settings.MaxRequestBytes); err != nil {
				return nil, err
			}
			copy := proto.Clone(request).(*workflowservice.ScanWorkflowExecutionsRequest)
			if copy.Namespace != "" && copy.Namespace != client.binding.worker.client.Namespace() {
				return nil, failure(ErrAuthority, "namespace")
			}
			value, err := client.nativeClient.ScanWorkflow(work, copy)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackClient) CountWorkflow(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	if request == nil {
		return nil, failure(ErrInput, "callback-request")
	}
	return callbackGranted(ctx, client, "", Execution{Operation: "callback.countworkflow"},
		func(work context.Context, evidence *Execution) (*workflowservice.CountWorkflowExecutionsResponse, error) {
			if _, err := messageSize(work, request, client.binding.worker.client.owner.settings.MaxRequestBytes); err != nil {
				return nil, err
			}
			copy := proto.Clone(request).(*workflowservice.CountWorkflowExecutionsRequest)
			if copy.Namespace != "" && copy.Namespace != client.binding.worker.client.Namespace() {
				return nil, failure(ErrAuthority, "namespace")
			}
			value, err := client.nativeClient.CountWorkflow(work, copy)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackClient) DescribeTaskQueueEnhanced(ctx context.Context, options sdk.DescribeTaskQueueEnhancedOptions) (sdk.TaskQueueDescription, error) {
	return callbackGranted(ctx, client, "", Execution{Operation: "callback.describetaskqueueenhanced"},
		func(work context.Context, evidence *Execution) (sdk.TaskQueueDescription, error) {
			value, err := client.nativeClient.DescribeTaskQueueEnhanced(work, options)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackClient) DescribeTaskQueue(ctx context.Context, queue string, kind enumspb.TaskQueueType) (*workflowservice.DescribeTaskQueueResponse, error) {
	return callbackGranted(ctx, client, "", Execution{Operation: "callback.describetaskqueue"},
		func(work context.Context, evidence *Execution) (*workflowservice.DescribeTaskQueueResponse, error) {
			value, err := client.nativeClient.DescribeTaskQueue(work, queue, kind)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackClient) GetSearchAttributes(ctx context.Context) (*workflowservice.GetSearchAttributesResponse, error) {
	return callbackGranted(ctx, client, "", Execution{Operation: "callback.getsearchattributes"},
		func(work context.Context, evidence *Execution) (*workflowservice.GetSearchAttributesResponse, error) {
			value, err := client.nativeClient.GetSearchAttributes(work)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackClient) iteratorContext(ctx context.Context) context.Context {
	clean := metadata.NewOutgoingContext(executionContext{ctx}, nil)
	return context.WithValue(clean, taskBindingKey{}, client.binding)
}

type callbackIterator[T any] struct {
	borrower          *callbackClient
	ctx               context.Context
	operation, method string
	native            interface {
		HasNext() bool
		Next() (T, error)
	}
	waiting chan struct{}
	pending error
}

func newCallbackIterator[T any](client *callbackClient, ctx context.Context, operation, method string, native interface {
	HasNext() bool
	Next() (T, error)
}) *callbackIterator[T] {
	return &callbackIterator[T]{borrower: client, ctx: ctx, operation: operation, method: method, native: native, waiting: make(chan struct{}, 1)}
}

func (iterator *callbackIterator[T]) HasNext() bool {
	select {
	case iterator.waiting <- struct{}{}:
	case <-iterator.ctx.Done():
		return true
	}
	defer func() { <-iterator.waiting }()
	if iterator.pending != nil {
		return true
	}
	value, err := callbackGranted(iterator.ctx, iterator.borrower, iterator.method, Execution{Operation: "callback." + iterator.operation + ".has-next"},
		func(context.Context, *Execution) (bool, error) { return iterator.native.HasNext(), nil })
	if err != nil {
		iterator.pending = err
		return true
	}
	return value
}

func (iterator *callbackIterator[T]) Next() (T, error) {
	select {
	case iterator.waiting <- struct{}{}:
	case <-iterator.ctx.Done():
		var zero T
		return zero, iterator.ctx.Err()
	}
	defer func() { <-iterator.waiting }()
	if iterator.pending != nil {
		err := iterator.pending
		iterator.pending = nil
		var zero T
		return zero, err
	}
	return callbackGranted(iterator.ctx, iterator.borrower, iterator.method, Execution{Operation: "callback." + iterator.operation + ".next"},
		func(context.Context, *Execution) (T, error) { return iterator.native.Next() })
}
