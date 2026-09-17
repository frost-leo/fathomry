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
	"sync"
	"sync/atomic"

	"github.com/nexus-rpc/sdk-go/nexus"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc/metadata"
)

type clientSetup struct {
	mu  sync.Mutex
	err error
}

func (setup *clientSetup) failed(err error) {
	setup.mu.Lock()
	defer setup.mu.Unlock()
	if setup.err == nil {
		setup.err = err
	}
}

func (setup *clientSetup) failure() error {
	setup.mu.Lock()
	defer setup.mu.Unlock()
	return setup.err
}

type clientFactory struct {
	interceptor.ClientInterceptorBase
	native interceptor.ClientInterceptor
	setup  *clientSetup
	owner  *connection
}

// The native factory has no error channel and runs after dialing but before
// returning an owned client. Record failure and finish construction so cleanup
// can close that client; no partially initialized connection is abandoned.
func (factory *clientFactory) InterceptClient(next interceptor.ClientOutboundInterceptor) (out interceptor.ClientOutboundInterceptor) {
	out = next
	defer func() {
		if recovered := recover(); recovered != nil {
			cause, _ := recovered.(error)
			factory.setup.failed(failure(ErrConnect, "client-interceptor-panic", cause))
			out = next
		}
	}()
	guardedNext := newClientContinuation(factory, next, next.PollWorkflowUpdate, true)
	out = factory.native.InterceptClient(guardedNext)
	if nilRuntime(out) {
		factory.setup.failed(failure(ErrConnect, "client-interceptor-result"))
		out = next
	}
	out = newClientContinuation(factory, out, out.PollWorkflowUpdate, false)
	return
}

type activityInboundDelegate = interceptor.ActivityInboundInterceptor

type factoryContextDelegate = context.Context

type activityFactoryContext struct {
	factoryContextDelegate
	worker *Worker
}

func (ctx activityFactoryContext) Value(key any) any {
	value := ctx.factoryContextDelegate.Value(key)
	// SDK factories run before Init replaces the Activity outbound interceptor.
	// Restrict its public interface without relying on unexported context keys.
	if outbound, ok := value.(interceptor.ActivityOutboundInterceptor); ok {
		return activityClientAdapter(ctx.worker, outbound, outbound.GetClient)
	}
	return value
}

type workerFactory struct {
	interceptor.WorkerInterceptorBase
	native interceptor.WorkerInterceptor
	worker *Worker
}

func (factory *workerFactory) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	initialization := &continuationFrame{closed: true}
	guarded := &activityInboundView{activityInboundDelegate: next, factory: factory, nextOnly: true, initialization: initialization}
	native := factory.native.InterceptActivity(activityFactoryContext{factoryContextDelegate: ctx, worker: factory.worker}, guarded)
	return &activityInboundView{activityInboundDelegate: native, factory: factory, initialization: initialization}
}

func (factory *workerFactory) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return factory.native.InterceptWorkflow(ctx, next)
}

func (factory *workerFactory) InterceptNexusOperation(ctx context.Context, next interceptor.NexusOperationInboundInterceptor) interceptor.NexusOperationInboundInterceptor {
	initialization := &continuationFrame{closed: true}
	guarded := &nexusInboundView{nexusInboundDelegate: next, factory: factory, nextOnly: true, initialization: initialization}
	native := factory.native.InterceptNexusOperation(ctx, guarded)
	return &nexusInboundView{nexusInboundDelegate: native, factory: factory, initialization: initialization}
}

// A continuation belongs to one interceptor invocation, not the lifetime of the
// interceptor object. Closing waits for an entered native tail without allowing
// retained continuations to reopen that invocation.
type continuationFrame struct {
	mu     sync.Mutex
	closed bool
	active chan struct{}
}

func (frame *continuationFrame) enter() (func(), error) {
	frame.mu.Lock()
	defer frame.mu.Unlock()
	if frame.closed {
		return nil, failure(ErrAuthority, "expired-continuation")
	}
	if frame.active != nil {
		return nil, failure(ErrLimit, "concurrent-continuation")
	}
	done := make(chan struct{})
	frame.active = done
	return func() {
		frame.mu.Lock()
		frame.active = nil
		close(done)
		frame.mu.Unlock()
	}, nil
}

func (frame *continuationFrame) seal() <-chan struct{} {
	frame.mu.Lock()
	defer frame.mu.Unlock()
	frame.closed = true
	return frame.active
}

func (frame *continuationFrame) close() {
	if active := frame.seal(); active != nil {
		<-active
	}
}

type clientOutboundDelegate = interceptor.ClientOutboundInterceptorBase

type nativePollOutput = struct {
	Result converter.EncodedValue
	Error  error
}

type clientContinuation[PollInput any, PollOutput ~nativePollOutput] struct {
	clientOutboundDelegate
	factory  *clientFactory
	nextOnly bool
	poll     func(context.Context, PollInput) (*PollOutput, error)
}

func newClientContinuation[PollInput any, PollOutput ~nativePollOutput](factory *clientFactory, next interceptor.ClientOutboundInterceptor, poll func(context.Context, PollInput) (*PollOutput, error), nextOnly bool) *clientContinuation[PollInput, PollOutput] {
	return &clientContinuation[PollInput, PollOutput]{clientOutboundDelegate: interceptor.ClientOutboundInterceptorBase{Next: next}, factory: factory, nextOnly: nextOnly, poll: poll}
}

func clientContinue[T, PollInput any, PollOutput ~nativePollOutput](ctx context.Context, gate *clientContinuation[PollInput, PollOutput], invoke func(context.Context) (T, error)) (T, error) {
	var zero T
	if ctx == nil {
		return zero, failure(ErrAuthority, "client-continuation-context")
	}
	authority, _ := ctx.Value(nativeCallKey{}).(*nativeCall)
	if authority == nil || authority.owner != gate.factory.owner || authority.closed.Load() {
		if !gate.nextOnly {
			if binding, ok := ctx.Value(taskBindingKey{}).(*taskBinding); ok && binding.worker.client.owner == gate.factory.owner {
				client := &callbackClient{nativeClient: gate.factory.owner.native, binding: binding}
				return callbackNative(ctx, client, "", Execution{Operation: "callback.interceptor"}, func(work context.Context, _ *Execution) (T, error) { return clientContinue(work, gate, invoke) })
			}
		}
		err := failure(ErrAuthority, "expired-client-continuation")
		if !gate.factory.owner.ready.Load() {
			gate.factory.setup.failed(err)
		}
		return zero, err
	}
	if gate.nextOnly {
		frame, _ := ctx.Value(gate.factory).(*continuationFrame)
		if frame == nil {
			return zero, failure(ErrAuthority, "client-continuation-frame")
		}
		leave, err := frame.enter()
		if err != nil {
			return zero, err
		}
		defer leave()
		return invoke(ctx)
	}
	frame := &continuationFrame{}
	defer frame.close()
	return invoke(context.WithValue(ctx, gate.factory, frame))
}

func nativeBorrowGuard(ctx context.Context, owner *connection, decode func() error) error {
	authority, _ := ctx.Value(nativeCallKey{}).(*nativeCall)
	if authority == nil || authority.owner != owner || authority.closed.Load() {
		return failure(ErrAuthority, "expired-native-value")
	}
	return decode()
}

func guardNativeError(ctx context.Context, owner *connection, err error) error {
	authority, _ := ctx.Value(nativeCallKey{}).(*nativeCall)
	if err == nil || authority == nil {
		return err
	}
	return sdk.FathomryScopeErrorV1(err, authority.scopeOwner, func(decode func() error) error { return nativeBorrowGuard(ctx, owner, decode) })
}

func (gate *clientContinuation[PollInput, PollOutput]) UpdateWorkflow(ctx context.Context, input *interceptor.ClientUpdateWorkflowInput) (sdk.WorkflowUpdateHandle, error) {
	return clientContinue(ctx, gate, func(work context.Context) (sdk.WorkflowUpdateHandle, error) {
		value, err := gate.Next.UpdateWorkflow(work, input)
		value, scopeErr := gate.resultUpdate(work, value)
		return value, gate.resultError(work, errors.Join(err, scopeErr))
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) PollWorkflowUpdate(ctx context.Context, input PollInput) (*PollOutput, error) {
	return clientContinue(ctx, gate, func(work context.Context) (*PollOutput, error) {
		value, err := gate.poll(work, input)
		if value != nil {
			copy := nativePollOutput(*value)
			copy.Result = gate.resultValue(work, copy.Result)
			copy.Error = gate.resultError(work, copy.Error)
			result := PollOutput(copy)
			value = &result
		}
		return value, gate.resultError(work, err)
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) UpdateWithStartWorkflow(ctx context.Context, input *interceptor.ClientUpdateWithStartWorkflowInput) (sdk.WorkflowUpdateHandle, error) {
	return clientContinue(ctx, gate, func(work context.Context) (sdk.WorkflowUpdateHandle, error) {
		value, err := gate.Next.UpdateWithStartWorkflow(work, input)
		value, scopeErr := gate.resultUpdate(work, value)
		return value, gate.resultError(work, errors.Join(err, scopeErr))
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) ExecuteWorkflow(ctx context.Context, input *interceptor.ClientExecuteWorkflowInput) (sdk.WorkflowRun, error) {
	return clientContinue(ctx, gate, func(work context.Context) (sdk.WorkflowRun, error) {
		value, err := gate.Next.ExecuteWorkflow(work, input)
		return gate.resultWorkflow(work, value), gate.resultError(work, err)
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) SignalWorkflow(ctx context.Context, input *interceptor.ClientSignalWorkflowInput) error {
	_, err := clientContinue(ctx, gate, func(work context.Context) (struct{}, error) {
		return struct{}{}, gate.resultError(work, gate.Next.SignalWorkflow(work, input))
	})
	return err
}

func (gate *clientContinuation[PollInput, PollOutput]) SignalWithStartWorkflow(ctx context.Context, input *interceptor.ClientSignalWithStartWorkflowInput) (sdk.WorkflowRun, error) {
	return clientContinue(ctx, gate, func(work context.Context) (sdk.WorkflowRun, error) {
		value, err := gate.Next.SignalWithStartWorkflow(work, input)
		return gate.resultWorkflow(work, value), gate.resultError(work, err)
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) CancelWorkflow(ctx context.Context, input *interceptor.ClientCancelWorkflowInput) error {
	_, err := clientContinue(ctx, gate, func(work context.Context) (struct{}, error) {
		return struct{}{}, gate.resultError(work, gate.Next.CancelWorkflow(work, input))
	})
	return err
}

func (gate *clientContinuation[PollInput, PollOutput]) TerminateWorkflow(ctx context.Context, input *interceptor.ClientTerminateWorkflowInput) error {
	_, err := clientContinue(ctx, gate, func(work context.Context) (struct{}, error) {
		return struct{}{}, gate.resultError(work, gate.Next.TerminateWorkflow(work, input))
	})
	return err
}

func (gate *clientContinuation[PollInput, PollOutput]) QueryWorkflow(ctx context.Context, input *interceptor.ClientQueryWorkflowInput) (converter.EncodedValue, error) {
	return clientContinue(ctx, gate, func(work context.Context) (converter.EncodedValue, error) {
		value, err := gate.Next.QueryWorkflow(work, input)
		return gate.resultValue(work, value), gate.resultError(work, err)
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) DescribeWorkflow(ctx context.Context, input *interceptor.ClientDescribeWorkflowInput) (*interceptor.ClientDescribeWorkflowOutput, error) {
	return clientContinue(ctx, gate, func(work context.Context) (*interceptor.ClientDescribeWorkflowOutput, error) {
		value, err := gate.Next.DescribeWorkflow(work, input)
		if value != nil {
			copy := *value
			copy.Response = sdk.FathomryScopeWorkflowDescriptionV1(value.Response, work.Value(nativeCallKey{}).(*nativeCall).scopeOwner, gate.resultGuard(work))
			value = &copy
		}
		return value, gate.resultError(work, err)
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) CreateSchedule(ctx context.Context, input *interceptor.ScheduleClientCreateInput) (sdk.ScheduleHandle, error) {
	return clientContinue(ctx, gate, func(work context.Context) (sdk.ScheduleHandle, error) {
		value, err := gate.Next.CreateSchedule(work, input)
		return gate.resultSchedule(work, value), gate.resultError(work, err)
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) ExecuteActivity(ctx context.Context, input *interceptor.ClientExecuteActivityInput) (sdk.ActivityHandle, error) {
	return clientContinue(ctx, gate, func(work context.Context) (sdk.ActivityHandle, error) {
		value, err := gate.Next.ExecuteActivity(work, input)
		return gate.resultActivity(work, value), gate.resultError(work, err)
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) CancelActivity(ctx context.Context, input *interceptor.ClientCancelActivityInput) error {
	_, err := clientContinue(ctx, gate, func(work context.Context) (struct{}, error) {
		return struct{}{}, gate.resultError(work, gate.Next.CancelActivity(work, input))
	})
	return err
}

func (gate *clientContinuation[PollInput, PollOutput]) TerminateActivity(ctx context.Context, input *interceptor.ClientTerminateActivityInput) error {
	_, err := clientContinue(ctx, gate, func(work context.Context) (struct{}, error) {
		return struct{}{}, gate.resultError(work, gate.Next.TerminateActivity(work, input))
	})
	return err
}

func (gate *clientContinuation[PollInput, PollOutput]) PauseActivity(ctx context.Context, input *interceptor.ClientPauseActivityInput) error {
	_, err := clientContinue(ctx, gate, func(work context.Context) (struct{}, error) {
		return struct{}{}, gate.resultError(work, gate.Next.PauseActivity(work, input))
	})
	return err
}

func (gate *clientContinuation[PollInput, PollOutput]) UnpauseActivity(ctx context.Context, input *interceptor.ClientUnpauseActivityInput) error {
	_, err := clientContinue(ctx, gate, func(work context.Context) (struct{}, error) {
		return struct{}{}, gate.resultError(work, gate.Next.UnpauseActivity(work, input))
	})
	return err
}

func (gate *clientContinuation[PollInput, PollOutput]) UpdateActivityOptions(ctx context.Context, input *interceptor.ClientUpdateActivityOptionsInput) (*interceptor.ClientUpdateActivityOptionsOutput, error) {
	return clientContinue(ctx, gate, func(work context.Context) (*interceptor.ClientUpdateActivityOptionsOutput, error) {
		value, err := gate.Next.UpdateActivityOptions(work, input)
		return value, gate.resultError(work, err)
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) DescribeActivity(ctx context.Context, input *interceptor.ClientDescribeActivityInput) (*interceptor.ClientDescribeActivityOutput, error) {
	return clientContinue(ctx, gate, func(work context.Context) (*interceptor.ClientDescribeActivityOutput, error) {
		value, err := gate.Next.DescribeActivity(work, input)
		if value != nil {
			copy := *value
			copy.Description = sdk.FathomryScopeActivityDescriptionV1(value.Description, work.Value(nativeCallKey{}).(*nativeCall).scopeOwner, gate.resultGuard(work))
			value = &copy
		}
		return value, gate.resultError(work, err)
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) PollActivityResult(ctx context.Context, input *interceptor.ClientPollActivityResultInput) (*interceptor.ClientPollActivityResultOutput, error) {
	return clientContinue(ctx, gate, func(work context.Context) (*interceptor.ClientPollActivityResultOutput, error) {
		value, err := gate.Next.PollActivityResult(work, input)
		if value != nil {
			copy := *value
			copy.Result, copy.Error = gate.resultValue(work, value.Result), gate.resultError(work, value.Error)
			value = &copy
		}
		return value, gate.resultError(work, err)
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) ExecuteNexusOperation(ctx context.Context, input *interceptor.ClientExecuteNexusOperationInput) (sdk.NexusOperationHandle, error) {
	return clientContinue(ctx, gate, func(work context.Context) (sdk.NexusOperationHandle, error) {
		value, err := gate.Next.ExecuteNexusOperation(work, input)
		return gate.resultNexus(work, value), gate.resultError(work, err)
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) CancelNexusOperation(ctx context.Context, input *interceptor.ClientCancelNexusOperationInput) error {
	_, err := clientContinue(ctx, gate, func(work context.Context) (struct{}, error) {
		return struct{}{}, gate.resultError(work, gate.Next.CancelNexusOperation(work, input))
	})
	return err
}

func (gate *clientContinuation[PollInput, PollOutput]) TerminateNexusOperation(ctx context.Context, input *interceptor.ClientTerminateNexusOperationInput) error {
	_, err := clientContinue(ctx, gate, func(work context.Context) (struct{}, error) {
		return struct{}{}, gate.resultError(work, gate.Next.TerminateNexusOperation(work, input))
	})
	return err
}

func (gate *clientContinuation[PollInput, PollOutput]) DescribeNexusOperation(ctx context.Context, input *interceptor.ClientDescribeNexusOperationInput) (*interceptor.ClientDescribeNexusOperationOutput, error) {
	return clientContinue(ctx, gate, func(work context.Context) (*interceptor.ClientDescribeNexusOperationOutput, error) {
		value, err := gate.Next.DescribeNexusOperation(work, input)
		if value != nil {
			copy := *value
			copy.Description = sdk.FathomryScopeNexusDescriptionV1(value.Description, work.Value(nativeCallKey{}).(*nativeCall).scopeOwner, gate.resultGuard(work))
			value = &copy
		}
		return value, gate.resultError(work, err)
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) PollNexusOperationResult(ctx context.Context, input *interceptor.ClientPollNexusOperationResultInput) (*interceptor.ClientPollNexusOperationResultOutput, error) {
	return clientContinue(ctx, gate, func(work context.Context) (*interceptor.ClientPollNexusOperationResultOutput, error) {
		value, err := gate.Next.PollNexusOperationResult(work, input)
		if value != nil {
			copy := *value
			copy.Result, copy.Error = gate.resultValue(work, value.Result), gate.resultError(work, value.Error)
			value = &copy
		}
		return value, gate.resultError(work, err)
	})
}

type interceptorValue struct {
	private
	native     converter.EncodedValue
	factory    *clientFactory
	authority  *nativeCall
	guard      func(func() error) error
	scopeError func(error) error
	present    bool
}

func (value *interceptorValue) HasValue() bool { return value.present }

func (value *interceptorValue) Get(output any) error {
	return value.guard(func() error { return value.scopeError(value.native.Get(output)) })
}

func (gate *clientContinuation[PollInput, PollOutput]) resultGuard(ctx context.Context) func(func() error) error {
	if !gate.nextOnly {
		return func(decode func() error) error { return decode() }
	}
	frame, _ := ctx.Value(gate.factory).(*continuationFrame)
	var guard func(func() error) error
	guard = func(decode func() error) error {
		if frame == nil {
			return failure(ErrAuthority, "native-value-frame")
		}
		leave, err := frame.enter()
		if err != nil {
			return err
		}
		defer leave()
		return nativeBorrowGuard(ctx, gate.factory.owner, func() error {
			return sdk.FathomryScopeErrorV1(decode(), ctx.Value(nativeCallKey{}).(*nativeCall).scopeOwner, guard)
		})
	}
	return guard
}

func (gate *clientContinuation[PollInput, PollOutput]) resultValue(ctx context.Context, value converter.EncodedValue) converter.EncodedValue {
	if value == nil {
		return nil
	}
	authority := ctx.Value(nativeCallKey{}).(*nativeCall)
	if !gate.nextOnly {
		if view, ok := value.(*interceptorValue); ok && view.factory == gate.factory && view.authority == authority {
			return view.native
		}
		return value
	}
	return &interceptorValue{native: value, factory: gate.factory, authority: authority, guard: gate.resultGuard(ctx), present: value.HasValue(), scopeError: func(err error) error { return gate.resultError(ctx, err) }}
}

func (gate *clientContinuation[PollInput, PollOutput]) resultError(ctx context.Context, value error) error {
	if value == nil {
		return nil
	}
	return sdk.FathomryScopeErrorV1(value, ctx.Value(nativeCallKey{}).(*nativeCall).scopeOwner, gate.resultGuard(ctx))
}

type interceptorUpdateTransfer struct {
	factory      *clientFactory
	view, native sdk.WorkflowUpdateHandle
}

func (gate *clientContinuation[PollInput, PollOutput]) resultUpdate(ctx context.Context, value sdk.WorkflowUpdateHandle) (sdk.WorkflowUpdateHandle, error) {
	if value == nil {
		return nil, nil
	}
	authority := ctx.Value(nativeCallKey{}).(*nativeCall)
	if !gate.nextOnly {
		authority.resultsMu.Lock()
		defer authority.resultsMu.Unlock()
		for _, item := range authority.updates {
			if item.factory == gate.factory && item.view == value {
				return item.native, nil
			}
		}
		return value, nil
	}
	guard := gate.resultGuard(ctx)
	view, err := sdk.FathomryScopeWorkflowUpdateHandleV1(value, func(work context.Context, output any, next func(context.Context, any) error) error {
		if work == nil {
			return failure(ErrInput, "update-result-context")
		}
		return guard(func() error {
			owned, release := borrowedResultContext(ctx, work)
			defer release()
			return gate.resultError(ctx, next(owned, output))
		})
	})
	if err != nil {
		return nil, err
	}
	authority.resultsMu.Lock()
	defer authority.resultsMu.Unlock()
	if len(authority.updates) >= 64 {
		return nil, failure(ErrLimit, "interceptor-update-values")
	}
	authority.updates = append(authority.updates, interceptorUpdateTransfer{factory: gate.factory, view: view, native: value})
	return view, nil
}

func borrowedResultContext(origin, supplied context.Context) (context.Context, func()) {
	work, release := mergeActivityContext(executionContext{supplied}, origin)
	work = metadata.NewOutgoingContext(work, nil)
	work = context.WithValue(work, nativeCallKey{}, origin.Value(nativeCallKey{}))
	if call := origin.Value(activeRPCKey{}); call != nil {
		work = context.WithValue(work, activeRPCKey{}, call)
	}
	if evidence := origin.Value(executionEvidenceKey{}); evidence != nil {
		work = context.WithValue(work, executionEvidenceKey{}, evidence)
	}
	return work, release
}

func (gate *clientContinuation[PollInput, PollOutput]) resultHandleGate(origin context.Context) func(context.Context, string, func(context.Context, *Execution) error) error {
	guard := gate.resultGuard(origin)
	return func(ctx context.Context, operation string, invoke func(context.Context, *Execution) error) error {
		if ctx == nil {
			return failure(ErrInput, "native-handle-context")
		}
		return guard(func() error {
			work, release := borrowedResultContext(origin, ctx)
			defer release()
			return gate.resultError(origin, invoke(work, &Execution{Operation: operation}))
		})
	}
}

func (gate *clientContinuation[PollInput, PollOutput]) resultActivity(ctx context.Context, value sdk.ActivityHandle) sdk.ActivityHandle {
	if value == nil {
		return nil
	}
	authority := ctx.Value(nativeCallKey{}).(*nativeCall)
	if !gate.nextOnly {
		if view, ok := value.(*activityHandleView); ok && view.factory == gate.factory && view.authority == authority {
			return view.native
		}
		return value
	}
	return &activityHandleView{id: value.GetID(), runID: value.GetRunID(), native: value, initializing: make(chan struct{}, 1), waiting: make(chan struct{}, 1),
		factory: gate.factory, authority: authority, gate: gate.resultHandleGate(ctx)}
}

func (gate *clientContinuation[PollInput, PollOutput]) resultNexus(ctx context.Context, value sdk.NexusOperationHandle) sdk.NexusOperationHandle {
	if value == nil {
		return nil
	}
	authority := ctx.Value(nativeCallKey{}).(*nativeCall)
	if !gate.nextOnly {
		if view, ok := value.(*nexusHandleView); ok && view.factory == gate.factory && view.authority == authority {
			return view.native
		}
		return value
	}
	return &nexusHandleView{id: value.GetID(), runID: value.GetRunID(), native: value, initializing: make(chan struct{}, 1), waiting: make(chan struct{}, 1),
		factory: gate.factory, authority: authority, gate: gate.resultHandleGate(ctx)}
}

func (gate *clientContinuation[PollInput, PollOutput]) resultSchedule(ctx context.Context, value sdk.ScheduleHandle) sdk.ScheduleHandle {
	if value == nil {
		return nil
	}
	authority := ctx.Value(nativeCallKey{}).(*nativeCall)
	if !gate.nextOnly {
		if view, ok := value.(*scheduleHandleView); ok && view.factory == gate.factory && view.authority == authority {
			return view.native
		}
		return value
	}
	return &scheduleHandleView{id: value.GetID(), native: value, factory: gate.factory, authority: authority, gate: gate.resultHandleGate(ctx)}
}

type interceptorWorkflowRun struct {
	*callbackWorkflowState
	factory   *clientFactory
	authority *nativeCall
	gate      func(context.Context, string, func(context.Context, *Execution) error) error
}

func (run *interceptorWorkflowRun) Get(ctx context.Context, output any) error {
	return run.GetWithOptions(ctx, output, sdk.WorkflowRunGetOptions{})
}

func (run *interceptorWorkflowRun) GetWithOptions(ctx context.Context, output any, options sdk.WorkflowRunGetOptions) error {
	return run.gate(ctx, "workflow.result", func(work context.Context, evidence *Execution) error {
		select {
		case run.waiting <- struct{}{}:
			defer func() { <-run.waiting }()
		case <-work.Done():
			return work.Err()
		}
		err := run.native.GetWithOptions(work, output, options)
		run.observeIdentity()
		evidence.ResultObtained = err == nil
		return err
	})
}

func (gate *clientContinuation[PollInput, PollOutput]) resultWorkflow(ctx context.Context, value sdk.WorkflowRun) sdk.WorkflowRun {
	if value == nil {
		return nil
	}
	authority := ctx.Value(nativeCallKey{}).(*nativeCall)
	if !gate.nextOnly {
		if view, ok := value.(*interceptorWorkflowRun); ok && view.factory == gate.factory && view.authority == authority {
			return view.native
		}
		return value
	}
	return &interceptorWorkflowRun{callbackWorkflowState: newWorkflowRun(nil, value), factory: gate.factory, authority: authority, gate: gate.resultHandleGate(ctx)}
}

type activityInboundView struct {
	activityInboundDelegate
	factory        *workerFactory
	nextOnly       bool
	initialization *continuationFrame
	initialized    atomic.Bool
}

func initializeContinuation(nextOnly bool, once *atomic.Bool, frame *continuationFrame, invoke func() error) error {
	if nextOnly {
		leave, err := frame.enter()
		if err != nil {
			return err
		}
		defer leave()
		return invoke()
	}
	if !once.CompareAndSwap(false, true) {
		return failure(ErrAuthority, "repeated-interceptor-init")
	}
	frame.mu.Lock()
	frame.closed = false
	frame.mu.Unlock()
	defer frame.close()
	return invoke()
}

func (view *activityInboundView) Init(next interceptor.ActivityOutboundInterceptor) error {
	return initializeContinuation(view.nextOnly, &view.initialized, view.initialization, func() error { return view.activityInboundDelegate.Init(next) })
}

func taskContinue[T any](ctx context.Context, factory *workerFactory, nextOnly bool, invoke func(context.Context) (T, error)) (T, error) {
	var zero T
	if ctx == nil {
		return zero, failure(ErrAuthority, "task-continuation-context")
	}
	binding, _ := ctx.Value(taskBindingKey{}).(*taskBinding)
	if binding == nil || binding.worker != factory.worker || binding.closed.Load() {
		return zero, failure(ErrAuthority, "expired-task-continuation")
	}
	if nextOnly {
		frame, _ := ctx.Value(factory).(*continuationFrame)
		if frame == nil {
			return zero, failure(ErrAuthority, "task-continuation-frame")
		}
		leave, err := frame.enter()
		if err != nil {
			return zero, err
		}
		defer leave()
		return invoke(ctx)
	}
	frame := &continuationFrame{}
	defer frame.close()
	return invoke(context.WithValue(ctx, factory, frame))
}

func (view *activityInboundView) ExecuteActivity(ctx context.Context, input *interceptor.ExecuteActivityInput) (any, error) {
	return taskContinue(ctx, view.factory, view.nextOnly, func(work context.Context) (any, error) {
		return view.activityInboundDelegate.ExecuteActivity(work, input)
	})
}

type nexusInboundDelegate = interceptor.NexusOperationInboundInterceptor

type nexusInboundView struct {
	nexusInboundDelegate
	factory        *workerFactory
	nextOnly       bool
	initialization *continuationFrame
	initialized    atomic.Bool
}

func (view *nexusInboundView) Init(ctx context.Context, next interceptor.NexusOperationOutboundInterceptor) error {
	return initializeContinuation(view.nextOnly, &view.initialized, view.initialization, func() error { return view.nexusInboundDelegate.Init(ctx, next) })
}

func (view *nexusInboundView) StartOperation(ctx context.Context, input interceptor.NexusStartOperationInput) (nexus.HandlerStartOperationResult[any], error) {
	return taskContinue(ctx, view.factory, view.nextOnly, func(work context.Context) (nexus.HandlerStartOperationResult[any], error) {
		return view.nexusInboundDelegate.StartOperation(work, input)
	})
}

func (view *nexusInboundView) CancelOperation(ctx context.Context, input interceptor.NexusCancelOperationInput) error {
	_, err := taskContinue(ctx, view.factory, view.nextOnly, func(work context.Context) (struct{}, error) {
		return struct{}{}, view.nexusInboundDelegate.CancelOperation(work, input)
	})
	return err
}
