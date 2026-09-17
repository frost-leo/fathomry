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
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

func (*WorkflowRun) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

func (*WorkflowUpdate) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

func (*WithStartWorkflowOperation) LogValue() slog.Value {
	return slog.StringValue("temporal[restricted]")
}

// WorkflowRun is a non-owning native execution handle. Each Get is separately
// admitted. Concurrent waits are serialized within that admission and respect
// the waiting context. Getters read the last observed identity without I/O.
// Canceling a Get does not request remote Workflow cancellation. Do not copy it.
type WorkflowRun struct {
	private
	client   *Executions
	native   sdk.WorkflowRun
	waiting  chan struct{}
	identity atomic.Pointer[workflowIdentity]
}

type workflowIdentity struct{ workflowID, runID, firstRunID string }

func newWorkflowRun(client *Executions, native sdk.WorkflowRun) *WorkflowRun {
	run := &WorkflowRun{client: client, native: native, waiting: make(chan struct{}, 1)}
	run.observeIdentity()
	return run
}

func (run *WorkflowRun) observeIdentity() {
	run.identity.Store(&workflowIdentity{run.native.GetID(), run.native.GetRunID(), run.native.GetFirstExecutionRunID()})
}

func (run *WorkflowRun) observedIdentity() workflowIdentity {
	if run != nil {
		if value := run.identity.Load(); value != nil {
			return *value
		}
	}
	return workflowIdentity{}
}

func (run *WorkflowRun) GetID() string { return run.observedIdentity().workflowID }

func (run *WorkflowRun) GetRunID() string { return run.observedIdentity().runID }

func (run *WorkflowRun) GetFirstExecutionRunID() string {
	return run.observedIdentity().firstRunID
}

// ExecuteWorkflow preserves all native start options. An omitted ID is generated
// before native entry and retained in evidence, including on an uncertain error.
// This does not equate a fresh call with a retry of an earlier start intention.
func (client *Executions) ExecuteWorkflow(ctx context.Context, correlation fault.Correlation, options sdk.StartWorkflowOptions, definition any, args ...any) (*WorkflowRun, error) {
	if options.ID == "" {
		options.ID = uuid.NewString()
	}
	return executeNative(ctx, client, correlation, Execution{Operation: "workflow.start", WorkflowID: options.ID}, func(work context.Context, evidence *Execution) (*WorkflowRun, error) {
		native, err := client.owner.native.ExecuteWorkflow(work, options, definition, args...)
		if native == nil {
			return nil, err
		}
		evidence.WorkflowID = native.GetID()
		evidence.RunID = native.GetRunID()
		evidence.Accepted = err == nil
		return newWorkflowRun(client, native), err
	})
}

// GetWorkflow makes a detached observation handle, preserving native empty-RunID
// targeting and execution-chain behavior. It does not contact the service.
func (client *Executions) GetWorkflow(workflowID, runID string) (*WorkflowRun, error) {
	if client == nil || client.owner == nil || !validText(workflowID, 1024) || len(runID) > 1024 {
		return nil, failure(ErrInput, "workflow-handle")
	}
	run := &WorkflowRun{client: client, waiting: make(chan struct{}, 1)}
	run.identity.Store(&workflowIdentity{workflowID: workflowID, runID: runID})
	return run, nil
}

func (run *WorkflowRun) Get(ctx context.Context, correlation fault.Correlation, result any) error {
	return run.GetWithOptions(ctx, correlation, result, sdk.WorkflowRunGetOptions{})
}

// GetWithOptions preserves native result-following options across retries and
// Continue-As-New. ResultObtained means successful result observation (including
// a nil destination), not external effects.
func (run *WorkflowRun) GetWithOptions(ctx context.Context, correlation fault.Correlation, result any, options sdk.WorkflowRunGetOptions) error {
	if run == nil || run.client == nil || run.waiting == nil {
		return failure(ErrInput, "workflow-result")
	}
	_, err := executeNative(ctx, run.client, correlation, Execution{Operation: "workflow.result", WorkflowID: run.GetID(), RunID: run.GetRunID()}, func(work context.Context, evidence *Execution) (struct{}, error) {
		evidence.NativeCalled = false
		select {
		case run.waiting <- struct{}{}:
			defer func() { <-run.waiting }()
		case <-work.Done():
			return struct{}{}, work.Err()
		}
		evidence.NativeCalled = true
		if run.native == nil {
			// An empty native RunID lazily describes the execution using this
			// context. Construct it inside admission, never in an identity getter.
			run.native = run.client.owner.native.GetWorkflow(work, run.GetID(), run.GetRunID())
		}
		err := run.native.GetWithOptions(work, result, options)
		run.observeIdentity()
		evidence.RunID = run.GetRunID()
		evidence.ResultObtained = err == nil
		return struct{}{}, err
	})
	return err
}

func (client *Executions) SignalWorkflow(ctx context.Context, correlation fault.Correlation, workflowID, runID, signal string, argument any) error {
	_, err := executeNative(ctx, client, correlation, Execution{Operation: "workflow.signal", WorkflowID: workflowID, RunID: runID}, func(work context.Context, evidence *Execution) (struct{}, error) {
		err := client.owner.native.SignalWorkflow(work, workflowID, runID, signal, argument)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

func (client *Executions) SignalWithStartWorkflow(ctx context.Context, correlation fault.Correlation, workflowID, signal string, signalArgument any, options sdk.StartWorkflowOptions, definition any, args ...any) (*WorkflowRun, error) {
	if workflowID == "" {
		workflowID = uuid.NewString()
	}
	return executeNative(ctx, client, correlation, Execution{Operation: "workflow.signal-start", WorkflowID: workflowID}, func(work context.Context, evidence *Execution) (*WorkflowRun, error) {
		native, err := client.owner.native.SignalWithStartWorkflow(work, workflowID, signal, signalArgument, options, definition, args...)
		if native == nil {
			return nil, err
		}
		evidence.WorkflowID = native.GetID()
		evidence.RunID = native.GetRunID()
		evidence.Accepted = err == nil
		return newWorkflowRun(client, native), err
	})
}

// QueryWorkflow includes native result decoding within the admitted call.
func (client *Executions) QueryWorkflow(ctx context.Context, correlation fault.Correlation, workflowID, runID, query string, result any, args ...any) error {
	_, err := executeNative(ctx, client, correlation, Execution{Operation: "workflow.query", WorkflowID: workflowID, RunID: runID}, func(work context.Context, evidence *Execution) (struct{}, error) {
		encoded, err := client.owner.native.QueryWorkflow(work, workflowID, runID, query, args...)
		if err == nil {
			err = encoded.Get(result)
			evidence.ResultObtained = err == nil
		}
		return struct{}{}, err
	})
	return err
}

// QueryWorkflowWithOptions preserves native headers and rejection conditions.
// A non-nil rejection is a native state rejection, not a decoded query result.
// Result decoding remains inside the admitted call; the rejection is caller-owned.
func (client *Executions) QueryWorkflowWithOptions(ctx context.Context, correlation fault.Correlation, request *sdk.QueryWorkflowWithOptionsRequest, result any) (*querypb.QueryRejected, error) {
	if request == nil {
		return nil, failure(ErrInput, "workflow-query")
	}
	return executeNative(ctx, client, correlation, Execution{Operation: "workflow.query", WorkflowID: request.WorkflowID, RunID: request.RunID}, func(work context.Context, evidence *Execution) (*querypb.QueryRejected, error) {
		response, err := client.owner.native.QueryWorkflowWithOptions(work, request)
		if err != nil {
			return nil, err
		}
		if response.QueryRejected != nil {
			return response.QueryRejected, nil
		}
		err = response.QueryResult.Get(result)
		evidence.ResultObtained = err == nil
		return nil, err
	})
}

// DescribeWorkflowExecution returns caller-owned native execution metadata.
func (client *Executions) DescribeWorkflowExecution(ctx context.Context, correlation fault.Correlation, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return executeNative(ctx, client, correlation, Execution{Operation: "workflow.describe", WorkflowID: workflowID, RunID: runID}, func(work context.Context, evidence *Execution) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		response, err := client.owner.native.DescribeWorkflowExecution(work, workflowID, runID)
		evidence.ResultObtained = err == nil
		return response, err
	})
}

func (client *Executions) CancelWorkflow(ctx context.Context, correlation fault.Correlation, options sdk.CancelWorkflowOptions) error {
	_, err := executeNative(ctx, client, correlation, Execution{Operation: "workflow.cancel", WorkflowID: options.WorkflowID, RunID: options.RunID}, func(work context.Context, evidence *Execution) (struct{}, error) {
		err := client.owner.native.CancelWorkflowWithOptions(work, options)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

func (client *Executions) TerminateWorkflow(ctx context.Context, correlation fault.Correlation, options sdk.TerminateWorkflowOptions) error {
	_, err := executeNative(ctx, client, correlation, Execution{Operation: "workflow.terminate", WorkflowID: options.WorkflowID, RunID: options.RunID}, func(work context.Context, evidence *Execution) (struct{}, error) {
		err := client.owner.native.TerminateWorkflowWithOptions(work, options)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

// WorkflowUpdate retains native Update identity and result retrieval semantics.
type WorkflowUpdate struct {
	private
	client *Executions
	native sdk.WorkflowUpdateHandle
	stage  enumspb.UpdateWorkflowExecutionLifecycleStage
}

func (update *WorkflowUpdate) WorkflowID() string {
	if update == nil || update.native == nil {
		return ""
	}
	return update.native.WorkflowID()
}

func (update *WorkflowUpdate) RunID() string {
	if update == nil || update.native == nil {
		return ""
	}
	return update.native.RunID()
}

func (update *WorkflowUpdate) UpdateID() string {
	if update == nil || update.native == nil {
		return ""
	}
	return update.native.UpdateID()
}

func (client *Executions) UpdateWorkflow(ctx context.Context, correlation fault.Correlation, options sdk.UpdateWorkflowOptions) (*WorkflowUpdate, error) {
	if options.UpdateID == "" {
		options.UpdateID = uuid.NewString()
	}
	return executeNative(ctx, client, correlation, Execution{Operation: "workflow.update", WorkflowID: options.WorkflowID, RunID: options.RunID, UpdateID: options.UpdateID}, func(work context.Context, evidence *Execution) (*WorkflowUpdate, error) {
		native, err := client.owner.native.UpdateWorkflow(work, options)
		if native == nil {
			return nil, err
		}
		evidence.Accepted = err == nil
		evidence.RunID = native.RunID()
		return &WorkflowUpdate{client: client, native: native, stage: evidence.UpdateStage}, err
	})
}

func (client *Executions) GetWorkflowUpdateHandle(options sdk.GetWorkflowUpdateHandleOptions) (*WorkflowUpdate, error) {
	if client == nil || client.owner == nil || !validText(options.WorkflowID, 1024) || !validText(options.UpdateID, 1024) || len(options.RunID) > 1024 {
		return nil, failure(ErrInput, "update-handle")
	}
	return &WorkflowUpdate{client: client, native: client.owner.native.GetWorkflowUpdateHandle(options)}, nil
}

func (update *WorkflowUpdate) Get(ctx context.Context, correlation fault.Correlation, result any) error {
	if update == nil || update.native == nil {
		return failure(ErrInput, "update-result")
	}
	_, err := executeNative(ctx, update.client, correlation, Execution{Operation: "workflow.update-result", WorkflowID: update.WorkflowID(), RunID: update.RunID(), UpdateID: update.UpdateID(), UpdateStage: update.stage}, func(work context.Context, evidence *Execution) (struct{}, error) {
		err := update.native.Get(work, result)
		evidence.ResultObtained = err == nil
		return struct{}{}, err
	})
	return err
}

// WithStartWorkflowOperation owns a single native combined-start intention, not
// a connection or background task. Its options and arguments are borrowed until
// UpdateWithStartWorkflow returns. Get can independently observe its start while
// the Update is still pending. As in the native SDK, when no start response has
// arrived Get waits until its context expires, even after an Update call failed.
// Do not copy an operation or use it with another source.
type WithStartWorkflowOperation struct {
	private
	client     *Executions
	native     sdk.WithStartWorkflowOperation
	workflowID string
	used       atomic.Bool
	runOnce    sync.Once
	run        *WorkflowRun
}

// NewWithStartWorkflowOperation retains the native validation and conflict-policy
// requirements. It does not issue an RPC. An omitted Workflow ID is generated
// here, before the intention is executed, and is available through WorkflowID.
func (client *Executions) NewWithStartWorkflowOperation(options sdk.StartWorkflowOptions, definition any, args ...any) (*WithStartWorkflowOperation, error) {
	if client == nil || client.owner == nil {
		return nil, failure(ErrInput, "workflow-start-operation")
	}
	if options.ID == "" {
		options.ID = uuid.NewString()
	}
	if !validText(options.ID, 1024) {
		return nil, failure(ErrInput, "workflow-start-operation")
	}
	return &WithStartWorkflowOperation{client: client, workflowID: options.ID, native: client.owner.native.NewWithStartWorkflowOperation(options, definition, args...)}, nil
}

func (operation *WithStartWorkflowOperation) WorkflowID() string {
	if operation == nil {
		return ""
	}
	return operation.workflowID
}

func (operation *WithStartWorkflowOperation) Get(ctx context.Context, correlation fault.Correlation) (*WorkflowRun, error) {
	if operation == nil || operation.native == nil {
		return nil, failure(ErrInput, "workflow-start-result")
	}
	return executeNative(ctx, operation.client, correlation, Execution{Operation: "workflow.start-result", WorkflowID: operation.workflowID}, func(work context.Context, evidence *Execution) (*WorkflowRun, error) {
		native, err := operation.native.Get(work)
		if native == nil {
			return nil, err
		}
		operation.runOnce.Do(func() { operation.run = newWorkflowRun(operation.client, native) })
		evidence.RunID = operation.run.GetRunID()
		evidence.StartAccepted = true
		return operation.run, err
	})
}

// UpdateWithStartWorkflow executes exactly one native combined operation. Once
// native execution begins, the operation cannot be reused, including after an
// uncertain failure. Retrying intentionally requires explicit native IDs/policies
// in a new operation. Rejection before invocation admission does not consume it.
func (client *Executions) UpdateWithStartWorkflow(ctx context.Context, correlation fault.Correlation, operation *WithStartWorkflowOperation, options sdk.UpdateWorkflowOptions) (*WorkflowUpdate, error) {
	if client == nil || operation == nil || operation.native == nil || operation.client.owner != client.owner {
		return nil, failure(ErrAuthority, "workflow-update-start")
	}
	if options.UpdateID == "" {
		options.UpdateID = uuid.NewString()
	}
	return executeNative(ctx, client, correlation, Execution{Operation: "workflow.update-start", WorkflowID: operation.workflowID, UpdateID: options.UpdateID}, func(work context.Context, evidence *Execution) (*WorkflowUpdate, error) {
		if !operation.used.CompareAndSwap(false, true) {
			evidence.NativeCalled = false
			return nil, failure(ErrInput, "workflow-start-operation-used")
		}
		native, err := client.owner.native.UpdateWithStartWorkflow(work, sdk.UpdateWithStartWorkflowOptions{StartWorkflowOperation: operation.native, UpdateOptions: options})
		if native == nil {
			return nil, err
		}
		evidence.RunID = native.RunID()
		evidence.Accepted = err == nil
		return &WorkflowUpdate{client: client, native: native, stage: evidence.UpdateStage}, err
	})
}

func (client *callbackClient) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	if request == nil {
		return nil, failure(ErrInput, "callback-request")
	}
	return callbackGranted(ctx, client, workflowServicePrefix+"ResetWorkflowExecution", Execution{Operation: "callback.resetworkflowexecution"},
		func(work context.Context, evidence *Execution) (*workflowservice.ResetWorkflowExecutionResponse, error) {
			if _, err := messageSize(work, request, client.binding.worker.client.owner.settings.MaxRequestBytes); err != nil {
				return nil, err
			}
			copy := proto.Clone(request).(*workflowservice.ResetWorkflowExecutionRequest)
			if copy.Namespace != "" && copy.Namespace != client.binding.worker.client.Namespace() {
				return nil, failure(ErrAuthority, "namespace")
			}
			value, err := client.nativeClient.ResetWorkflowExecution(work, copy)
			evidence.Accepted = err == nil
			return value, err
		})
}

func (client *callbackClient) UpdateWorkflowExecutionOptions(ctx context.Context, options sdk.UpdateWorkflowExecutionOptionsRequest) (sdk.WorkflowExecutionOptions, error) {
	return callbackGranted(ctx, client, workflowServicePrefix+"UpdateWorkflowExecutionOptions", Execution{Operation: "callback.updateworkflowexecutionoptions"},
		func(work context.Context, evidence *Execution) (sdk.WorkflowExecutionOptions, error) {
			value, err := client.nativeClient.UpdateWorkflowExecutionOptions(work, options)
			evidence.Accepted = err == nil
			return value, err
		})
}

func (client *callbackClient) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return callbackGranted(ctx, client, "", Execution{Operation: "callback.describeworkflowexecution"},
		func(work context.Context, evidence *Execution) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
			value, err := client.nativeClient.DescribeWorkflowExecution(work, workflowID, runID)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

type nativeWorkflowDescription = sdk.WorkflowExecutionDescription

// WorkflowDescription preserves native metadata and scopes its lazy memo/static
// metadata decoders. Borrowed payload storage must not be mutated during decoding.
type WorkflowDescription struct {
	private
	*nativeWorkflowDescription
	decoder           *sdk.WorkflowExecutionDescription
	scopeOwner        *sdk.FathomryScopeOwnerV1
	client            *Executions
	workflowID, runID string
	decoding          chan struct{}
}

func (*WorkflowDescription) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

func (client *Executions) DescribeWorkflow(ctx context.Context, correlation fault.Correlation, workflowID, runID string) (*WorkflowDescription, error) {
	return executeNative(ctx, client, correlation, Execution{Operation: "workflow.describe-metadata", WorkflowID: workflowID, RunID: runID}, func(work context.Context, evidence *Execution) (*WorkflowDescription, error) {
		native, err := client.owner.native.DescribeWorkflow(work, workflowID, runID)
		evidence.ResultObtained = err == nil
		if native == nil {
			return nil, err
		}
		authority := work.Value(nativeCallKey{}).(*nativeCall)
		return &WorkflowDescription{nativeWorkflowDescription: native, decoder: native, scopeOwner: authority.scopeOwner, client: client,
			workflowID: workflowID, runID: runID, decoding: make(chan struct{}, 1)}, err
	})
}

func decodeWorkflowDescription[T any](ctx context.Context, description *WorkflowDescription, correlation fault.Correlation, operation string, decode func() (T, error)) (T, error) {
	var zero T
	if description == nil || description.decoder == nil {
		return zero, failure(ErrInput, operation)
	}
	return executeNative(ctx, description.client, correlation, Execution{Operation: operation, WorkflowID: description.workflowID, RunID: description.runID}, func(work context.Context, evidence *Execution) (T, error) {
		evidence.NativeCalled = false
		select {
		case description.decoding <- struct{}{}:
			defer func() { <-description.decoding }()
		case <-work.Done():
			return zero, work.Err()
		}
		evidence.NativeCalled = true
		description.decoder = sdk.FathomryScopeWorkflowDescriptionV1(description.decoder, description.scopeOwner, func(decode func() error) error { return nativeBorrowGuard(work, description.client.owner, decode) })
		value, err := decode()
		evidence.ResultObtained = err == nil
		return value, err
	})
}

func (description *WorkflowDescription) GetStaticSummary(ctx context.Context, correlation fault.Correlation) (string, error) {
	return decodeWorkflowDescription(ctx, description, correlation, "workflow.static-summary", func() (string, error) { return description.decoder.GetStaticSummary() })
}

func (description *WorkflowDescription) GetStaticDetails(ctx context.Context, correlation fault.Correlation) (string, error) {
	return decodeWorkflowDescription(ctx, description, correlation, "workflow.static-details", func() (string, error) { return description.decoder.GetStaticDetails() })
}

func (description *WorkflowDescription) GetMemoValue(ctx context.Context, correlation fault.Correlation, key string, output any) error {
	_, err := decodeWorkflowDescription(ctx, description, correlation, "workflow.memo-value", func() (struct{}, error) { return struct{}{}, description.decoder.GetMemoValue(key, output) })
	return err
}

func (client *callbackClient) DescribeWorkflow(ctx context.Context, workflowID, runID string) (*sdk.WorkflowExecutionDescription, error) {
	identity := Execution{Operation: "callback.workflow.describe", WorkflowID: workflowID, RunID: runID}
	return callbackNative(ctx, client, "", identity, func(work context.Context, evidence *Execution) (*sdk.WorkflowExecutionDescription, error) {
		native, err := client.nativeClient.DescribeWorkflow(work, workflowID, runID)
		evidence.ResultObtained = err == nil
		if native == nil {
			return nil, err
		}
		authority := work.Value(nativeCallKey{}).(*nativeCall)
		identity.Operation = "callback.workflow.describe-decode"
		return sdk.FathomryScopeWorkflowDescriptionV1(native, authority.scopeOwner, func(decode func() error) error {
			_, err := callbackNative(context.WithoutCancel(ctx), client, "", identity, func(_ context.Context, evidence *Execution) (struct{}, error) {
				err := decode()
				evidence.ResultObtained = err == nil
				return struct{}{}, err
			})
			return err
		}), err
	})
}

func (client *callbackClient) SignalWorkflow(ctx context.Context, workflowID, runID, signal string, argument any) error {
	_, err := callbackNative(ctx, client, "", Execution{Operation: "callback.workflow.signal", WorkflowID: workflowID, RunID: runID}, func(work context.Context, evidence *Execution) (struct{}, error) {
		err := client.nativeClient.SignalWorkflow(work, workflowID, runID, signal, argument)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

func (client *callbackClient) SignalWithStartWorkflow(ctx context.Context, workflowID, signal string, argument any, options sdk.StartWorkflowOptions, definition any, args ...any) (sdk.WorkflowRun, error) {
	if workflowID == "" {
		workflowID = uuid.NewString()
	}
	return callbackNative(ctx, client, "", Execution{Operation: "callback.workflow.signal-start", WorkflowID: workflowID}, func(work context.Context, evidence *Execution) (sdk.WorkflowRun, error) {
		run, err := client.nativeClient.SignalWithStartWorkflow(work, workflowID, signal, argument, options, definition, args...)
		evidence.Accepted = err == nil
		if run != nil {
			evidence.WorkflowID, evidence.RunID = run.GetID(), run.GetRunID()
			return &callbackWorkflowRun{callbackWorkflowState: newWorkflowRun(client.binding.worker.client, run), borrower: client}, err
		}
		return nil, err
	})
}

func (client *callbackClient) TerminateWorkflow(ctx context.Context, workflowID, runID, reason string, details ...any) error {
	return client.TerminateWorkflowWithOptions(ctx, sdk.TerminateWorkflowOptions{WorkflowID: workflowID, RunID: runID, Reason: reason, Details: details})
}

func (client *callbackClient) TerminateWorkflowWithOptions(ctx context.Context, options sdk.TerminateWorkflowOptions) error {
	_, err := callbackNative(ctx, client, "", Execution{Operation: "callback.workflow.terminate", WorkflowID: options.WorkflowID, RunID: options.RunID}, func(work context.Context, evidence *Execution) (struct{}, error) {
		err := client.nativeClient.TerminateWorkflowWithOptions(work, options)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

type callbackWorkflowState = WorkflowRun

type callbackWorkflowRun struct {
	*callbackWorkflowState
	borrower *callbackClient
}

func (client *callbackClient) GetWorkflow(_ context.Context, workflowID, runID string) sdk.WorkflowRun {
	state := &WorkflowRun{waiting: make(chan struct{}, 1)}
	state.identity.Store(&workflowIdentity{workflowID: workflowID, runID: runID})
	return &callbackWorkflowRun{callbackWorkflowState: state, borrower: client}
}

func (run *callbackWorkflowRun) Get(ctx context.Context, output any) error {
	return run.GetWithOptions(ctx, output, sdk.WorkflowRunGetOptions{})
}

func (run *callbackWorkflowRun) GetWithOptions(ctx context.Context, output any, options sdk.WorkflowRunGetOptions) error {
	_, err := callbackNative(ctx, run.borrower, "", Execution{Operation: "callback.workflow.result", WorkflowID: run.GetID(), RunID: run.GetRunID()}, func(work context.Context, evidence *Execution) (struct{}, error) {
		evidence.NativeCalled = false
		select {
		case run.waiting <- struct{}{}:
			defer func() { <-run.waiting }()
		case <-work.Done():
			return struct{}{}, work.Err()
		}
		evidence.NativeCalled = true
		if run.native == nil {
			run.native = run.borrower.nativeClient.GetWorkflow(work, run.GetID(), run.GetRunID())
		}
		err := run.native.GetWithOptions(work, output, options)
		run.observeIdentity()
		evidence.RunID = run.GetRunID()
		evidence.ResultObtained = err == nil
		return struct{}{}, err
	})
	return err
}

type callbackValue struct {
	private
	native            converter.EncodedValue
	borrower          *callbackClient
	ctx               context.Context
	workflowID, runID string
}

func (value *callbackValue) HasValue() bool { return value.native.HasValue() }

func (value *callbackValue) Get(output any) error {
	_, err := callbackNative(value.ctx, value.borrower, "", Execution{Operation: "callback.workflow.query-result", WorkflowID: value.workflowID, RunID: value.runID}, func(_ context.Context, evidence *Execution) (struct{}, error) {
		err := value.native.Get(output)
		evidence.ResultObtained = err == nil
		return struct{}{}, err
	})
	return err
}

type callbackStartOperation struct {
	private
	borrower   *callbackClient
	native     sdk.WithStartWorkflowOperation
	workflowID string
}

func (client *callbackClient) NewWithStartWorkflowOperation(options sdk.StartWorkflowOptions, definition any, args ...any) sdk.WithStartWorkflowOperation {
	if options.ID == "" {
		options.ID = uuid.NewString()
	}
	return &callbackStartOperation{borrower: client, native: client.nativeClient.NewWithStartWorkflowOperation(options, definition, args...), workflowID: options.ID}
}

func (operation *callbackStartOperation) Get(ctx context.Context) (sdk.WorkflowRun, error) {
	return callbackNative(ctx, operation.borrower, "", Execution{Operation: "callback.workflow.start-result", WorkflowID: operation.workflowID},
		func(work context.Context, evidence *Execution) (sdk.WorkflowRun, error) {
			value, err := operation.native.Get(work)
			if value == nil {
				return nil, err
			}
			evidence.StartAccepted = true
			evidence.WorkflowID = value.GetID()
			evidence.RunID = value.GetRunID()
			return &callbackWorkflowRun{callbackWorkflowState: newWorkflowRun(operation.borrower.binding.worker.client, value), borrower: operation.borrower}, err
		})
}

func (client *callbackClient) UpdateWithStartWorkflow(ctx context.Context, options sdk.UpdateWithStartWorkflowOptions) (sdk.WorkflowUpdateHandle, error) {
	operation, ok := options.StartWorkflowOperation.(*callbackStartOperation)
	if !ok || operation == nil || operation.borrower.binding != client.binding {
		return nil, failure(ErrAuthority, "callback-start-owner")
	}
	if options.UpdateOptions.UpdateID == "" {
		options.UpdateOptions.UpdateID = uuid.NewString()
	}
	return callbackNative(ctx, client, "", Execution{Operation: "callback.workflow.update-start", WorkflowID: operation.workflowID, UpdateID: options.UpdateOptions.UpdateID},
		func(work context.Context, evidence *Execution) (sdk.WorkflowUpdateHandle, error) {
			options.StartWorkflowOperation = operation.native
			value, err := client.nativeClient.UpdateWithStartWorkflow(work, options)
			if value == nil {
				return nil, err
			}
			evidence.Accepted = err == nil
			evidence.WorkflowID = value.WorkflowID()
			evidence.RunID = value.RunID()
			evidence.UpdateID = value.UpdateID()
			handle, scopeErr := client.updateHandle(value, evidence.UpdateStage)
			if scopeErr != nil {
				return nil, scopeErr
			}
			return handle, err
		})
}

func (client *callbackClient) updateHandle(native sdk.WorkflowUpdateHandle, stage enumspb.UpdateWorkflowExecutionLifecycleStage) (sdk.WorkflowUpdateHandle, error) {
	identity := Execution{Operation: "callback.workflow.update-result", WorkflowID: native.WorkflowID(), RunID: native.RunID(), UpdateID: native.UpdateID(), UpdateStage: stage}
	return sdk.FathomryScopeWorkflowUpdateHandleV1(native, func(ctx context.Context, output any, next func(context.Context, any) error) error {
		_, err := callbackNative(ctx, client, "", identity, func(work context.Context, evidence *Execution) (struct{}, error) {
			err := next(work, output)
			evidence.ResultObtained = err == nil
			return struct{}{}, err
		})
		return err
	})
}

func (client *callbackClient) GetWorkflowUpdateHandle(options sdk.GetWorkflowUpdateHandleOptions) sdk.WorkflowUpdateHandle {
	value, err := client.updateHandle(client.nativeClient.GetWorkflowUpdateHandle(options), 0)
	if err != nil {
		panic(err)
	}
	return value
}

func (client *callbackClient) ExecuteWorkflow(ctx context.Context, options sdk.StartWorkflowOptions, definition any, args ...any) (sdk.WorkflowRun, error) {
	if options.ID == "" {
		options.ID = uuid.NewString()
	}
	return callbackNative(ctx, client, "", Execution{Operation: "callback.workflow.start", WorkflowID: options.ID}, func(work context.Context, evidence *Execution) (sdk.WorkflowRun, error) {
		run, err := client.nativeClient.ExecuteWorkflow(work, options, definition, args...)
		if run != nil {
			evidence.WorkflowID = run.GetID()
			evidence.RunID = run.GetRunID()
		}
		evidence.Accepted = err == nil
		if run != nil {
			return &callbackWorkflowRun{callbackWorkflowState: newWorkflowRun(client.binding.worker.client, run), borrower: client}, err
		}
		return nil, err
	})
}

func (client *callbackClient) UpdateWorkflow(ctx context.Context, options sdk.UpdateWorkflowOptions) (sdk.WorkflowUpdateHandle, error) {
	if options.UpdateID == "" {
		options.UpdateID = uuid.NewString()
	}
	return callbackNative(ctx, client, "", Execution{Operation: "callback.workflow.update", WorkflowID: options.WorkflowID, RunID: options.RunID, UpdateID: options.UpdateID}, func(work context.Context, evidence *Execution) (sdk.WorkflowUpdateHandle, error) {
		update, err := client.nativeClient.UpdateWorkflow(work, options)
		if update != nil {
			evidence.RunID = update.RunID()
		}
		evidence.Accepted = err == nil
		if err != nil || update == nil {
			return update, err
		}
		return client.updateHandle(update, evidence.UpdateStage)
	})
}

func (client *callbackClient) CancelWorkflow(ctx context.Context, workflowID, runID string) error {
	return client.CancelWorkflowWithOptions(ctx, sdk.CancelWorkflowOptions{WorkflowID: workflowID, RunID: runID})
}

func (client *callbackClient) CancelWorkflowWithOptions(ctx context.Context, options sdk.CancelWorkflowOptions) error {
	_, err := callbackNative(ctx, client, "", Execution{Operation: "callback.workflow.cancel", WorkflowID: options.WorkflowID, RunID: options.RunID}, func(work context.Context, evidence *Execution) (struct{}, error) {
		err := client.nativeClient.CancelWorkflowWithOptions(work, options)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

func (client *callbackClient) QueryWorkflow(ctx context.Context, workflowID, runID, query string, args ...any) (converter.EncodedValue, error) {
	return callbackNative(ctx, client, "", Execution{Operation: "callback.workflow.query", WorkflowID: workflowID, RunID: runID}, func(work context.Context, evidence *Execution) (converter.EncodedValue, error) {
		value, err := client.nativeClient.QueryWorkflow(work, workflowID, runID, query, args...)
		evidence.Accepted = err == nil
		if value != nil {
			value = &callbackValue{native: value, borrower: client, ctx: context.WithoutCancel(ctx), workflowID: workflowID, runID: runID}
		}
		return value, err
	})
}

func (client *callbackClient) QueryWorkflowWithOptions(ctx context.Context, options *sdk.QueryWorkflowWithOptionsRequest) (*sdk.QueryWorkflowWithOptionsResponse, error) {
	if options == nil {
		return nil, failure(ErrInput, "callback-query")
	}
	return callbackNative(ctx, client, "", Execution{Operation: "callback.workflow.query", WorkflowID: options.WorkflowID, RunID: options.RunID}, func(work context.Context, evidence *Execution) (*sdk.QueryWorkflowWithOptionsResponse, error) {
		value, err := client.nativeClient.QueryWorkflowWithOptions(work, options)
		evidence.Accepted = err == nil
		if value != nil && value.QueryResult != nil {
			value.QueryResult = &callbackValue{native: value.QueryResult, borrower: client, ctx: context.WithoutCancel(ctx), workflowID: options.WorkflowID, runID: options.RunID}
		}
		return value, err
	})
}
