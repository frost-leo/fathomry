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

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/nexus-rpc/sdk-go/nexus"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporalnexus"
)

type nexusInbound struct {
	interceptor.NexusOperationInboundInterceptorBase
	worker *Worker
}

type nexusOutboundBase = interceptor.NexusOperationOutboundInterceptorBase

type nexusOutbound[C sdk.Client] struct {
	nexusOutboundBase
	worker *Worker
}

func (guard *taskInterceptor) InterceptNexusOperation(_ context.Context, next interceptor.NexusOperationInboundInterceptor) interceptor.NexusOperationInboundInterceptor {
	return &nexusInbound{NexusOperationInboundInterceptorBase: interceptor.NexusOperationInboundInterceptorBase{Next: next}, worker: guard.worker}
}

func (guard *nexusInbound) Init(ctx context.Context, next interceptor.NexusOperationOutboundInterceptor) error {
	return guard.Next.Init(ctx, nexusClientAdapter(guard.worker, next, next.GetClient))
}

func nexusClientAdapter[C sdk.Client](worker *Worker, next interceptor.NexusOperationOutboundInterceptor, _ func(context.Context) C) *nexusOutbound[C] {
	return &nexusOutbound[C]{nexusOutboundBase: interceptor.NexusOperationOutboundInterceptorBase{Next: next}, worker: worker}
}

func (guard *nexusOutbound[C]) GetClient(ctx context.Context) C {
	return any(guard.worker.callbackClient(ctx)).(C)
}

func (guard *nexusInbound) StartOperation(ctx context.Context, input interceptor.NexusStartOperationInput) (result nexus.HandlerStartOperationResult[any], err error) {
	info := temporalnexus.GetOperationInfo(ctx)
	handler := nexus.ExtractHandlerInfo(ctx)
	evidence := TaskResult{Kind: "nexus-start", Namespace: info.Namespace, TaskQueue: info.TaskQueue,
		NexusService: handler.Service, NexusOperation: handler.Operation, NexusRequestID: input.Options.RequestID,
		NexusCallbackRequested: input.Options.CallbackURL != "", NexusRequestLinks: len(input.Options.Links)}
	call, binding, err := guard.worker.beginTask(ctx, evidence)
	if err != nil {
		return nil, err
	}
	defer func() {
		binding.closed.Store(true)
		primary := err
		recovered := recover()
		if recovered != nil {
			primary = failure(ErrTask, "nexus-panic")
		}
		if !evidence.HandlerReturned && primary == nil {
			primary = failure(ErrTask, "nexus-exit")
		}
		_, evidence.AsyncCompletion = result.(*nexus.HandlerStartOperationResultAsync)
		call.Complete(invocation.Outcome[TaskResult]{Value: evidence, Present: true, Primary: primary})
		<-guard.worker.handlers
		if recovered != nil {
			panic(recovered)
		}
	}()
	result, err = guard.Next.StartOperation(context.WithValue(ctx, taskBindingKey{}, binding), input)
	evidence.HandlerReturned = true
	return
}

func (guard *nexusInbound) CancelOperation(ctx context.Context, input interceptor.NexusCancelOperationInput) (err error) {
	info := temporalnexus.GetOperationInfo(ctx)
	handler := nexus.ExtractHandlerInfo(ctx)
	evidence := TaskResult{Kind: "nexus-cancel", Namespace: info.Namespace, TaskQueue: info.TaskQueue, NexusService: handler.Service, NexusOperation: handler.Operation}
	call, binding, err := guard.worker.beginTask(ctx, evidence)
	if err != nil {
		return err
	}
	defer func() {
		binding.closed.Store(true)
		primary := err
		recovered := recover()
		if recovered != nil {
			primary = failure(ErrTask, "nexus-panic")
		}
		if !evidence.HandlerReturned && primary == nil {
			primary = failure(ErrTask, "nexus-exit")
		}
		call.Complete(invocation.Outcome[TaskResult]{Value: evidence, Present: true, Primary: primary})
		<-guard.worker.handlers
		if recovered != nil {
			panic(recovered)
		}
	}()
	err = guard.Next.CancelOperation(context.WithValue(ctx, taskBindingKey{}, binding), input)
	evidence.HandlerReturned = true
	return
}

func (client *callbackClient) ListNexusOperations(ctx context.Context, options sdk.ListNexusOperationsOptions) (sdk.ListNexusOperationsResult, error) {
	if ctx == nil || client.binding == nil || client.binding.closed.Load() {
		return sdk.ListNexusOperationsResult{}, failure(ErrAuthority, "expired-callback")
	}
	return sdk.ListNexusOperationsResult{Results: func(yield func(*sdk.NexusOperationMetadata, error) bool) {
		deliveredError, stopped := false, false
		_, err := callbackNative(ctx, client, "", Execution{Operation: "callback.listnexusoperations"},
			func(work context.Context, evidence *Execution) (struct{}, error) {
				result, err := client.nativeClient.ListNexusOperations(work, options)
				if err != nil {
					return struct{}{}, err
				}
				for item, itemError := range result.Results {
					if itemError != nil {
						deliveredError = true
					}
					if !yield(item, itemError) {
						stopped = true
						return struct{}{}, itemError
					}
					if itemError != nil {
						return struct{}{}, itemError
					}
				}
				evidence.ResultObtained = true
				return struct{}{}, nil
			})
		if err != nil && !stopped && !deliveredError {
			yield(nil, err)
		}
	}}, nil
}

func (client *callbackClient) CountNexusOperations(ctx context.Context, options sdk.CountNexusOperationsOptions) (*sdk.CountNexusOperationsResult, error) {
	return callbackGranted(ctx, client, "", Execution{Operation: "callback.countnexusoperations"},
		func(work context.Context, evidence *Execution) (*sdk.CountNexusOperationsResult, error) {
			value, err := client.nativeClient.CountNexusOperations(work, options)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

// NexusRun is an experimental standalone Nexus handle, distinct from a Workflow
// Nexus future. Native cached result access is serialized within admission.
type NexusRun struct {
	private
	client       *Executions
	native       sdk.NexusOperationHandle
	id, runID    string
	waiting      chan struct{}
	initializing chan struct{}
}

func (*NexusRun) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

func (run *NexusRun) GetID() string {
	if run == nil {
		return ""
	}
	return run.id
}

func (run *NexusRun) GetRunID() string {
	if run == nil {
		return ""
	}
	return run.runID
}

func newNexusRun(client *Executions, native sdk.NexusOperationHandle) *NexusRun {
	return &NexusRun{client: client, native: native, id: native.GetID(), runID: native.GetRunID(), waiting: make(chan struct{}, 1), initializing: make(chan struct{}, 1)}
}

// ExecuteNexusOperation preserves native options and operation-reference typing.
// API availability is not namespace support; native capability errors remain
// errors. This operation enables no server setting or administrative authority.
func (client *Executions) ExecuteNexusOperation(ctx context.Context, correlation fault.Correlation, target sdk.NexusClientOptions, definition, input any, options sdk.StartNexusOperationOptions) (*NexusRun, error) {
	return executeNative(ctx, client, correlation, Execution{Operation: "nexus.start", NexusOperationID: options.ID}, func(work context.Context, evidence *Execution) (*NexusRun, error) {
		native, err := client.owner.native.NewNexusClient(target)
		if err != nil {
			return nil, err
		}
		run, err := native.ExecuteOperation(work, definition, input, options)
		evidence.Accepted = err == nil
		if run == nil {
			return nil, err
		}
		evidence.NexusOperationID, evidence.RunID = run.GetID(), run.GetRunID()
		return newNexusRun(client, run), err
	})
}

// GetNexusOperationHandle defers native interceptor entry to the admitted call.
func (client *Executions) GetNexusOperationHandle(options sdk.GetNexusOperationHandleOptions) (*NexusRun, error) {
	if client == nil || client.owner == nil || !validText(options.OperationID, 1024) || len(options.RunID) > 1024 {
		return nil, failure(ErrInput, "nexus-handle")
	}
	return &NexusRun{client: client, id: options.OperationID, runID: options.RunID, waiting: make(chan struct{}, 1), initializing: make(chan struct{}, 1)}, nil
}

func (run *NexusRun) nativeHandle(ctx context.Context) (sdk.NexusOperationHandle, error) {
	select {
	case run.initializing <- struct{}{}:
		defer func() { <-run.initializing }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if run.native == nil {
		run.native = run.client.owner.native.GetNexusOperationHandle(sdk.GetNexusOperationHandleOptions{OperationID: run.id, RunID: run.runID})
	}
	if nilRuntime(run.native) {
		return nil, failure(ErrExecution, "nexus-handle")
	}
	return run.native, nil
}

func nexusRunCall[T any](ctx context.Context, run *NexusRun, correlation fault.Correlation, operation string, call func(context.Context, sdk.NexusOperationHandle, *Execution) (T, error)) (T, error) {
	var zero T
	if run == nil || run.client == nil || run.waiting == nil {
		return zero, failure(ErrInput, operation)
	}
	return executeNative(ctx, run.client, correlation, Execution{Operation: operation, NexusOperationID: run.id, RunID: run.runID}, func(work context.Context, evidence *Execution) (T, error) {
		evidence.NativeCalled = false
		if operation == "nexus.result" {
			select {
			case run.waiting <- struct{}{}:
				defer func() { <-run.waiting }()
			case <-work.Done():
				return zero, work.Err()
			}
		}
		evidence.NativeCalled = true
		native, err := run.nativeHandle(work)
		if err != nil {
			return zero, err
		}
		return call(work, native, evidence)
	})
}

func (run *NexusRun) Get(ctx context.Context, correlation fault.Correlation, result any) error {
	_, err := nexusRunCall(ctx, run, correlation, "nexus.result", func(work context.Context, native sdk.NexusOperationHandle, evidence *Execution) (struct{}, error) {
		err := native.Get(work, result)
		evidence.ResultObtained = err == nil
		return struct{}{}, err
	})
	return err
}

func (run *NexusRun) Cancel(ctx context.Context, correlation fault.Correlation, options sdk.CancelNexusOperationOptions) error {
	_, err := nexusRunCall(ctx, run, correlation, "nexus.cancel", func(work context.Context, native sdk.NexusOperationHandle, evidence *Execution) (struct{}, error) {
		err := native.Cancel(work, options)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

func (run *NexusRun) Terminate(ctx context.Context, correlation fault.Correlation, options sdk.TerminateNexusOperationOptions) error {
	_, err := nexusRunCall(ctx, run, correlation, "nexus.terminate", func(work context.Context, native sdk.NexusOperationHandle, evidence *Execution) (struct{}, error) {
		err := native.Terminate(work, options)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

type nativeNexusDescription = sdk.NexusOperationExecutionDescription

type nativeNexusCancellation = sdk.NexusOperationCancellationInfo

// NexusDescription preserves caller-owned native metadata but controls each
// decoder, including the nested cancellation failure decoder. Native getters use
// background contexts; admitted ownership, not hard cancellation, protects them.
type NexusDescription struct {
	private
	*nativeNexusDescription
	CancellationInfo *NexusCancellation
	run              *NexusRun
	decoding         chan struct{}
	decoder          *sdk.NexusOperationExecutionDescription
	scopeOwner       *sdk.FathomryScopeOwnerV1
}

type NexusCancellation struct {
	private
	*nativeNexusCancellation
	description *NexusDescription
}

func (*NexusDescription) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

func (*NexusCancellation) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

func (run *NexusRun) Describe(ctx context.Context, correlation fault.Correlation, options sdk.DescribeNexusOperationOptions) (*NexusDescription, error) {
	return nexusRunCall(ctx, run, correlation, "nexus.describe", func(work context.Context, native sdk.NexusOperationHandle, evidence *Execution) (*NexusDescription, error) {
		value, err := native.Describe(work, options)
		evidence.ResultObtained = err == nil
		if value == nil {
			return nil, err
		}
		authority := work.Value(nativeCallKey{}).(*nativeCall)
		result := &NexusDescription{nativeNexusDescription: value, decoder: value, scopeOwner: authority.scopeOwner, run: run, decoding: make(chan struct{}, 1)}
		if value.CancellationInfo != nil {
			result.CancellationInfo = &NexusCancellation{nativeNexusCancellation: value.CancellationInfo, description: result}
		}
		return result, err
	})
}

func decodeNexusDescription[T any](ctx context.Context, description *NexusDescription, correlation fault.Correlation, operation string, decode func() (T, error)) (T, error) {
	var zero T
	if description == nil || description.nativeNexusDescription == nil {
		return zero, failure(ErrInput, operation)
	}
	return executeNative(ctx, description.run.client, correlation, Execution{Operation: operation, NexusOperationID: description.run.id, RunID: description.run.runID}, func(work context.Context, evidence *Execution) (T, error) {
		evidence.NativeCalled = false
		select {
		case description.decoding <- struct{}{}:
			defer func() { <-description.decoding }()
		case <-work.Done():
			return zero, work.Err()
		}
		evidence.NativeCalled = true
		description.decoder = sdk.FathomryScopeNexusDescriptionV1(description.decoder, description.scopeOwner, func(decode func() error) error { return nativeBorrowGuard(work, description.run.client.owner, decode) })
		value, err := decode()
		evidence.ResultObtained = err == nil
		return value, err
	})
}

func (description *NexusDescription) GetSummary(ctx context.Context, correlation fault.Correlation) (string, error) {
	return decodeNexusDescription(ctx, description, correlation, "nexus.describe-summary", func() (string, error) { return description.decoder.GetSummary() })
}

func (description *NexusDescription) GetLastAttemptFailure(ctx context.Context, correlation fault.Correlation) error {
	_, err := decodeNexusDescription(ctx, description, correlation, "nexus.describe-failure", func() (struct{}, error) {
		return struct{}{}, description.decoder.GetLastAttemptFailure()
	})
	return err
}

func (cancellation *NexusCancellation) GetLastAttemptFailure(ctx context.Context, correlation fault.Correlation) error {
	if cancellation == nil {
		return failure(ErrInput, "nexus.cancel-failure")
	}
	_, err := decodeNexusDescription(ctx, cancellation.description, correlation, "nexus.cancel-failure", func() (struct{}, error) {
		return struct{}{}, cancellation.description.decoder.CancellationInfo.GetLastAttemptFailure()
	})
	return err
}

// WalkNexusOperations owns the native experimental lazy sequence.
func (client *Executions) WalkNexusOperations(ctx context.Context, correlation fault.Correlation, options sdk.ListNexusOperationsOptions, visit func(context.Context, *sdk.NexusOperationMetadata) error) error {
	if visit == nil {
		return failure(ErrInput, "nexus-visitor")
	}
	_, err := executeNative(ctx, client, correlation, Execution{Operation: "nexus.list"}, func(work context.Context, evidence *Execution) (struct{}, error) {
		result, err := client.owner.native.ListNexusOperations(work, options)
		if err != nil {
			return struct{}{}, err
		}
		for entry, err := range result.Results {
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

func (client *Executions) CountNexusOperations(ctx context.Context, correlation fault.Correlation, options sdk.CountNexusOperationsOptions) (*sdk.CountNexusOperationsResult, error) {
	return executeNative(ctx, client, correlation, Execution{Operation: "nexus.count"}, func(work context.Context, evidence *Execution) (*sdk.CountNexusOperationsResult, error) {
		result, err := client.owner.native.CountNexusOperations(work, options)
		evidence.ResultObtained = err == nil
		return result, err
	})
}

type nexusHandleView struct {
	private
	id, runID             string
	native                sdk.NexusOperationHandle
	create                func(context.Context) (sdk.NexusOperationHandle, error)
	gate                  func(context.Context, string, func(context.Context, *Execution) error) error
	initializing, waiting chan struct{}
	factory               *clientFactory
	authority             *nativeCall
}

func (view *nexusHandleView) GetID() string { return view.id }

func (view *nexusHandleView) GetRunID() string { return view.runID }

func (view *nexusHandleView) load(ctx context.Context) (sdk.NexusOperationHandle, error) {
	select {
	case view.initializing <- struct{}{}:
		defer func() { <-view.initializing }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if view.native == nil {
		value, err := view.create(ctx)
		if err != nil {
			return nil, err
		}
		if nilRuntime(value) {
			return nil, failure(ErrExecution, "native-nexus-handle")
		}
		view.native = value
	}
	return view.native, nil
}

func (view *nexusHandleView) call(ctx context.Context, operation string, invoke func(context.Context, sdk.NexusOperationHandle, *Execution) error) error {
	return view.gate(ctx, operation, func(work context.Context, evidence *Execution) error {
		native, err := view.load(work)
		if err != nil {
			return err
		}
		return invoke(work, native, evidence)
	})
}

func (view *nexusHandleView) Get(ctx context.Context, output any) error {
	return view.call(ctx, "nexus.result", func(work context.Context, native sdk.NexusOperationHandle, evidence *Execution) error {
		select {
		case view.waiting <- struct{}{}:
			defer func() { <-view.waiting }()
		case <-work.Done():
			return work.Err()
		}
		err := native.Get(work, output)
		evidence.ResultObtained = err == nil
		return err
	})
}

func (view *nexusHandleView) Cancel(ctx context.Context, options sdk.CancelNexusOperationOptions) error {
	return view.call(ctx, "nexus.cancel", func(work context.Context, native sdk.NexusOperationHandle, evidence *Execution) error {
		err := native.Cancel(work, options)
		evidence.Accepted = err == nil
		return err
	})
}

func (view *nexusHandleView) Terminate(ctx context.Context, options sdk.TerminateNexusOperationOptions) error {
	return view.call(ctx, "nexus.terminate", func(work context.Context, native sdk.NexusOperationHandle, evidence *Execution) error {
		err := native.Terminate(work, options)
		evidence.Accepted = err == nil
		return err
	})
}

func (view *nexusHandleView) Describe(ctx context.Context, options sdk.DescribeNexusOperationOptions) (value *sdk.NexusOperationExecutionDescription, err error) {
	err = view.call(ctx, "nexus.describe", func(work context.Context, native sdk.NexusOperationHandle, evidence *Execution) error {
		var err error
		value, err = native.Describe(work, options)
		evidence.ResultObtained = err == nil
		if value != nil {
			authority := work.Value(nativeCallKey{}).(*nativeCall)
			value = sdk.FathomryScopeNexusDescriptionV1(value, authority.scopeOwner, func(decode func() error) error {
				return view.gate(context.WithoutCancel(work), "nexus.description-decode", func(_ context.Context, evidence *Execution) error {
					err := decode()
					evidence.ResultObtained = err == nil
					return err
				})
			})
		}
		return err
	})
	return
}

func (client *callbackClient) nexusHandle(native sdk.NexusOperationHandle, id, runID string) sdk.NexusOperationHandle {
	return &nexusHandleView{id: id, runID: runID, native: native, initializing: make(chan struct{}, 1), waiting: make(chan struct{}, 1),
		create: func(context.Context) (sdk.NexusOperationHandle, error) {
			return client.nativeClient.GetNexusOperationHandle(sdk.GetNexusOperationHandleOptions{OperationID: id, RunID: runID}), nil
		},
		gate: func(ctx context.Context, operation string, invoke func(context.Context, *Execution) error) error {
			_, err := callbackNative(ctx, client, "", Execution{Operation: "callback." + operation, NexusOperationID: id, RunID: runID},
				func(work context.Context, evidence *Execution) (struct{}, error) {
					return struct{}{}, invoke(work, evidence)
				})
			return err
		},
	}
}

func (client *callbackClient) GetNexusOperationHandle(options sdk.GetNexusOperationHandleOptions) sdk.NexusOperationHandle {
	return client.nexusHandle(nil, options.OperationID, options.RunID)
}

type callbackNexusClient struct {
	native   sdk.NexusClient
	borrower *callbackClient
}

func (client *callbackClient) NewNexusClient(options sdk.NexusClientOptions) (sdk.NexusClient, error) {
	native, err := client.nativeClient.NewNexusClient(options)
	if err != nil {
		return nil, err
	}
	return &callbackNexusClient{native: native, borrower: client}, nil
}

func (client *callbackNexusClient) ExecuteOperation(ctx context.Context, definition, input any, options sdk.StartNexusOperationOptions) (sdk.NexusOperationHandle, error) {
	return callbackNative(ctx, client.borrower, "", Execution{Operation: "callback.nexus.start", NexusOperationID: options.ID},
		func(work context.Context, evidence *Execution) (sdk.NexusOperationHandle, error) {
			native, err := client.native.ExecuteOperation(work, definition, input, options)
			evidence.Accepted = err == nil
			if native == nil {
				return nil, err
			}
			evidence.NexusOperationID, evidence.RunID = native.GetID(), native.GetRunID()
			return client.borrower.nexusHandle(native, native.GetID(), native.GetRunID()), err
		})
}

func (gate *clientContinuation[PollInput, PollOutput]) GetNexusOperationHandle(input *interceptor.ClientGetNexusOperationHandleInput) sdk.NexusOperationHandle {
	if input == nil {
		return nil
	}
	copy := *input
	return &nexusHandleView{id: copy.OperationID, runID: copy.RunID, initializing: make(chan struct{}, 1), waiting: make(chan struct{}, 1),
		create: func(context.Context) (sdk.NexusOperationHandle, error) {
			return gate.Next.GetNexusOperationHandle(&copy), nil
		},
		gate: func(ctx context.Context, operation string, invoke func(context.Context, *Execution) error) error {
			_, err := clientContinue(ctx, gate, func(work context.Context) (struct{}, error) {
				return struct{}{}, invoke(work, &Execution{Operation: operation, NexusOperationID: copy.OperationID, RunID: copy.RunID})
			})
			return err
		},
	}
}
