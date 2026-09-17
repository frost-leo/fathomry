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
	"reflect"
	"slices"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
)

func (*ActivityRun) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

// ActivityRun is a Standalone Activity handle. No Workflow attribution is invented.
// Result waits are serialized because the native handle caches mutable results.
// Canceling a result wait does not cancel the remote execution. Do not copy it.
type ActivityRun struct {
	private
	client       *Executions
	native       sdk.ActivityHandle
	waiting      chan struct{}
	initializing chan struct{}
	id, runID    string
}

func (run *ActivityRun) GetID() string {
	if run == nil {
		return ""
	}
	return run.id
}

func (run *ActivityRun) GetRunID() string {
	if run == nil {
		return ""
	}
	return run.runID
}

func (client *Executions) ExecuteActivity(ctx context.Context, correlation fault.Correlation, options sdk.StartActivityOptions, definition any, args ...any) (*ActivityRun, error) {
	if options.ID == "" {
		options.ID = uuid.NewString()
	}
	return executeNative(ctx, client, correlation, Execution{Operation: "activity.start", ActivityID: options.ID}, func(work context.Context, evidence *Execution) (*ActivityRun, error) {
		native, err := client.owner.native.ExecuteActivity(work, options, definition, args...)
		if native == nil {
			return nil, err
		}
		evidence.ActivityID = native.GetID()
		evidence.RunID = native.GetRunID()
		evidence.Accepted = err == nil
		return &ActivityRun{client: client, native: native, waiting: make(chan struct{}, 1), initializing: make(chan struct{}, 1), id: native.GetID(), runID: native.GetRunID()}, err
	})
}

func (client *Executions) GetActivityHandle(options sdk.GetActivityHandleOptions) (*ActivityRun, error) {
	if client == nil || client.owner == nil || !validText(options.ActivityID, 1024) || len(options.RunID) > 1024 {
		return nil, failure(ErrInput, "activity-handle")
	}
	return &ActivityRun{client: client, waiting: make(chan struct{}, 1), initializing: make(chan struct{}, 1), id: options.ActivityID, runID: options.RunID}, nil
}

func (run *ActivityRun) initialize(ctx context.Context) error {
	select {
	case run.initializing <- struct{}{}:
		defer func() { <-run.initializing }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if run.native == nil {
		run.native = run.client.owner.native.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: run.id, RunID: run.runID})
	}
	if nilRuntime(run.native) {
		return failure(ErrExecution, "activity-handle")
	}
	return nil
}

func (run *ActivityRun) Get(ctx context.Context, correlation fault.Correlation, result any) error {
	if run == nil || run.client == nil || run.waiting == nil {
		return failure(ErrInput, "activity-result")
	}
	_, err := executeNative(ctx, run.client, correlation, Execution{Operation: "activity.result", ActivityID: run.GetID(), RunID: run.GetRunID()}, func(work context.Context, evidence *Execution) (struct{}, error) {
		evidence.NativeCalled = false
		select {
		case run.waiting <- struct{}{}:
			defer func() { <-run.waiting }()
		case <-work.Done():
			return struct{}{}, work.Err()
		}
		evidence.NativeCalled = true
		if err := run.initialize(work); err != nil {
			return struct{}{}, err
		}
		err := run.native.Get(work, result)
		evidence.ResultObtained = err == nil
		return struct{}{}, err
	})
	return err
}

func (run *ActivityRun) Cancel(ctx context.Context, correlation fault.Correlation, options sdk.CancelActivityOptions) error {
	if run == nil || run.client == nil {
		return failure(ErrInput, "activity-cancel")
	}
	_, err := executeNative(ctx, run.client, correlation, Execution{Operation: "activity.cancel", ActivityID: run.GetID(), RunID: run.GetRunID()}, func(work context.Context, evidence *Execution) (struct{}, error) {
		if err := run.initialize(work); err != nil {
			return struct{}{}, err
		}
		err := run.native.Cancel(work, options)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

func (run *ActivityRun) Terminate(ctx context.Context, correlation fault.Correlation, options sdk.TerminateActivityOptions) error {
	if run == nil || run.client == nil {
		return failure(ErrInput, "activity-terminate")
	}
	_, err := executeNative(ctx, run.client, correlation, Execution{Operation: "activity.terminate", ActivityID: run.GetID(), RunID: run.GetRunID()}, func(work context.Context, evidence *Execution) (struct{}, error) {
		if err := run.initialize(work); err != nil {
			return struct{}{}, err
		}
		err := run.native.Terminate(work, options)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

// CompleteActivity preserves native asynchronous completion and serialization
// context options. The task token is never copied into independent evidence.
func (client *Executions) CompleteActivity(ctx context.Context, correlation fault.Correlation, options sdk.CompleteActivityOptions) error {
	return activityCall(ctx, client, correlation, Execution{Operation: "activity.complete"}, options.Namespace, func(work context.Context) error {
		return client.owner.native.CompleteActivityWithOptions(work, options)
	})
}

// CompleteActivityByID completes a Workflow Activity by native IDs. Namespace is
// required by the SDK and must match this source. Empty RunID targets the latest
// Workflow run. IDs do not fence Activity attempts; keep attempt-specific tokens
// when stale completion must be rejected by the Server.
func (client *Executions) CompleteActivityByID(ctx context.Context, correlation fault.Correlation, options sdk.CompleteActivityByIDOptions) error {
	return activityCall(ctx, client, correlation, Execution{Operation: "activity.complete-id", WorkflowID: options.WorkflowID, RunID: options.RunID, ActivityID: options.ActivityID}, options.Namespace, func(work context.Context) error {
		return client.owner.native.CompleteActivityByIDWithOptions(work, options)
	})
}

// CompleteActivityByActivityID is the native Standalone Activity completion path.
// ActivityRunID is not a Workflow run; empty targets the latest Activity execution.
func (client *Executions) CompleteActivityByActivityID(ctx context.Context, correlation fault.Correlation, options sdk.CompleteActivityByActivityIDOptions) error {
	return activityCall(ctx, client, correlation, Execution{Operation: "activity.complete-standalone", RunID: options.ActivityRunID, ActivityID: options.ActivityID}, options.Namespace, func(work context.Context) error {
		return client.owner.native.CompleteActivityByActivityIDWithOptions(work, options)
	})
}

// RecordActivityHeartbeat preserves native cancellation/pause/reset errors.
// A successful heartbeat response is recorded separately from the SDK error.
// Tokens and heartbeat payloads are never included in Execution evidence.
func (client *Executions) RecordActivityHeartbeat(ctx context.Context, correlation fault.Correlation, options sdk.RecordActivityHeartbeatOptions) error {
	return activityCall(ctx, client, correlation, Execution{Operation: "activity.heartbeat"}, options.Namespace, func(work context.Context) error {
		return client.owner.native.RecordActivityHeartbeatWithOptions(work, options)
	})
}

// RecordActivityHeartbeatByID retains native by-ID targeting and error semantics.
// In SDK v1.49.0 this path converts cancellation, but not pause/reset responses,
// into an error. Independently observed response flags remain in the evidence.
func (client *Executions) RecordActivityHeartbeatByID(ctx context.Context, correlation fault.Correlation, options sdk.RecordActivityHeartbeatByIDOptions) error {
	return activityCall(ctx, client, correlation, Execution{Operation: "activity.heartbeat-id", WorkflowID: options.WorkflowID, RunID: options.RunID, ActivityID: options.ActivityID}, options.Namespace, func(work context.Context) error {
		return client.owner.native.RecordActivityHeartbeatByIDWithOptions(work, options)
	})
}

func activityCall(ctx context.Context, client *Executions, correlation fault.Correlation, evidence Execution, namespace string, run func(context.Context) error) error {
	_, err := executeNative(ctx, client, correlation, evidence, func(work context.Context, evidence *Execution) (struct{}, error) {
		if namespace != "" && namespace != client.Namespace() {
			evidence.NativeCalled = false
			return struct{}{}, failure(ErrAuthority, "activity-namespace")
		}
		err := run(work)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

// Pause is the experimental, namespace-gated native operational control. It
// requires an explicit PauseActivityExecution RPC grant; no grant is implied by
// binding a normal Activity handle. Server capability errors are preserved.
func (run *ActivityRun) Pause(ctx context.Context, correlation fault.Correlation, options sdk.PauseActivityOptions) error {
	return run.control(ctx, correlation, "activity.pause", "PauseActivityExecution", func(work context.Context, _ *Execution) error { return run.native.Pause(work, options) })
}

// Unpause preserves native jitter/time semantics and requires its exact RPC grant.
func (run *ActivityRun) Unpause(ctx context.Context, correlation fault.Correlation, options sdk.UnpauseActivityOptions) error {
	return run.control(ctx, correlation, "activity.unpause", "UnpauseActivityExecution", func(work context.Context, _ *Execution) error { return run.native.Unpause(work, options) })
}

func (run *ActivityRun) control(ctx context.Context, correlation fault.Correlation, operation, method string, invoke func(context.Context, *Execution) error) error {
	if run == nil || run.client == nil {
		return failure(ErrInput, operation)
	}
	_, err := executeNative(ctx, run.client, correlation, Execution{Operation: operation, ActivityID: run.GetID(), RunID: run.GetRunID()}, func(work context.Context, evidence *Execution) (struct{}, error) {
		if !slices.Contains(run.client.owner.settings.RPCs, "/temporal.api.workflowservice.v1.WorkflowService/"+method) {
			evidence.NativeCalled = false
			return struct{}{}, failure(ErrAuthority, "activity-control")
		}
		if err := run.initialize(work); err != nil {
			return struct{}{}, err
		}
		err := invoke(work, evidence)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

// UpdateOptions preserves native set/clear/no-change distinctions. This
// experimental operation requires an UpdateActivityExecutionOptions RPC grant.
func (run *ActivityRun) UpdateOptions(ctx context.Context, correlation fault.Correlation, options sdk.ActivityOptionsUpdate) (*sdk.ActivityExecutionOptions, error) {
	var result *sdk.ActivityExecutionOptions
	err := run.control(ctx, correlation, "activity.update-options", "UpdateActivityExecutionOptions", func(work context.Context, evidence *Execution) error {
		var err error
		result, err = run.native.UpdateOptions(work, options)
		evidence.ResultObtained = err == nil
		return err
	})
	return result, err
}

// RestoreOriginalOptions is a distinct native request, not a locally synthesized
// default configuration. The same experimental update grant is required.
func (run *ActivityRun) RestoreOriginalOptions(ctx context.Context, correlation fault.Correlation) (*sdk.ActivityExecutionOptions, error) {
	var result *sdk.ActivityExecutionOptions
	err := run.control(ctx, correlation, "activity.restore-options", "UpdateActivityExecutionOptions", func(work context.Context, evidence *Execution) error {
		var err error
		result, err = run.native.RestoreOriginalOptions(work)
		evidence.ResultObtained = err == nil
		return err
	})
	return result, err
}

type activityInbound struct {
	interceptor.ActivityInboundInterceptorBase
	worker *Worker
}

type activityOutboundBase = interceptor.ActivityOutboundInterceptorBase

type activityOutbound[C sdk.Client] struct {
	activityOutboundBase
	worker *Worker
}

func (guard *taskInterceptor) InterceptActivity(_ context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &activityInbound{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, worker: guard.worker}
}

func (guard *activityInbound) Init(next interceptor.ActivityOutboundInterceptor) error {
	return guard.Next.Init(activityClientAdapter(guard.worker, next, next.GetClient))
}

// Inference preserves the SDK's unexported Client return type without importing
// SDK internals or changing its interceptor method signature.
func activityClientAdapter[C sdk.Client](worker *Worker, next interceptor.ActivityOutboundInterceptor, _ func(context.Context) C) *activityOutbound[C] {
	return &activityOutbound[C]{activityOutboundBase: interceptor.ActivityOutboundInterceptorBase{Next: next}, worker: worker}
}

func (guard *activityOutbound[C]) GetClient(ctx context.Context) C {
	return any(guard.worker.callbackClient(ctx)).(C)
}

func (guard *activityOutbound[C]) RecordHeartbeat(ctx context.Context, details ...any) {
	client := guard.worker.callbackClient(ctx).(*callbackClient)
	_, err := callbackNative(context.WithoutCancel(ctx), client, "", Execution{Operation: "callback.activity.record-heartbeat"}, func(_ context.Context, _ *Execution) (struct{}, error) {
		guard.Next.RecordHeartbeat(ctx, details...)
		return struct{}{}, nil
	})
	if err != nil {
		panic(err)
	}
}

func (guard *activityOutbound[C]) GetHeartbeatDetails(ctx context.Context, output ...any) error {
	client := guard.worker.callbackClient(ctx).(*callbackClient)
	_, err := callbackNative(context.WithoutCancel(ctx), client, "", Execution{Operation: "callback.activity.heartbeat-details"}, func(_ context.Context, evidence *Execution) (struct{}, error) {
		err := guard.Next.GetHeartbeatDetails(ctx, output...)
		evidence.ResultObtained = err == nil
		return struct{}{}, err
	})
	return err
}

type callbackValues struct {
	private
	native  converter.EncodedValues
	client  *callbackClient
	ctx     context.Context
	present bool
}

// The selected SDK uses NewValues' concrete type for dynamic Activity inputs.
// Matching that native type, not its interface, leaves ordinary application
// arguments that happen to implement EncodedValues unchanged.
var nativeActivityValuesType = reflect.TypeOf(sdk.NewValues(nil))

func (values *callbackValues) HasValues() bool { return values.present }

func (values *callbackValues) Get(output ...any) error {
	_, err := callbackNative(values.ctx, values.client, "", Execution{Operation: "callback.activity.dynamic-input"}, func(_ context.Context, evidence *Execution) (struct{}, error) {
		err := values.native.Get(output...)
		evidence.ResultObtained = err == nil
		return struct{}{}, err
	})
	return err
}

func (guard *activityInbound) ExecuteActivity(ctx context.Context, input *interceptor.ExecuteActivityInput) (result any, err error) {
	info := activity.GetInfo(ctx)
	evidence := TaskResult{Kind: "activity", Namespace: info.Namespace, TaskQueue: info.TaskQueue,
		WorkflowID: info.WorkflowExecution.ID, RunID: info.WorkflowExecution.RunID, ActivityID: info.ActivityID, ActivityRunID: info.ActivityRunID, ActivityType: info.ActivityType.Name, Attempt: info.Attempt, Local: info.IsLocalActivity}
	call, binding, err := guard.worker.beginTask(ctx, evidence)
	if err != nil {
		return nil, err
	}
	defer func() {
		binding.closed.Store(true)
		primary := err
		recovered := recover()
		if recovered != nil {
			primary = failure(ErrTask, "activity-panic")
		}
		if !evidence.HandlerReturned && primary == nil {
			primary = failure(ErrTask, "activity-exit")
		}
		evidence.AsyncCompletion = errors.Is(err, activity.ErrResultPending)
		if evidence.AsyncCompletion {
			primary = nil
		}
		call.Complete(invocation.Outcome[TaskResult]{Value: evidence, Present: true, Primary: primary})
		<-guard.worker.handlers
		if recovered != nil {
			panic(recovered)
		}
	}()
	work := context.WithValue(ctx, taskBindingKey{}, binding)
	for _, argument := range input.Args {
		if reflect.TypeOf(argument) != nativeActivityValuesType {
			continue
		}
		copy := *input
		copy.Args = slices.Clone(input.Args)
		for index, argument := range copy.Args {
			if encoded, ok := argument.(converter.EncodedValues); ok && reflect.TypeOf(argument) == nativeActivityValuesType {
				copy.Args[index] = &callbackValues{native: encoded, client: guard.worker.callbackClient(work).(*callbackClient), ctx: context.WithoutCancel(work), present: encoded.HasValues()}
			}
		}
		input = &copy
		break
	}
	result, err = guard.Next.ExecuteActivity(work, input)
	evidence.HandlerReturned = true
	return
}

type nativeActivityDescription = sdk.ActivityExecutionDescription

// ActivityDescription preserves native metadata fields and presence methods,
// while admitting decoder calls separately. Metadata/payloads are caller-owned;
// do not mutate or read their storage concurrently with decoding. Decoder calls
// are serialized because native metadata getters cache results and visit payloads.
// The selected SDK's description getters use background contexts internally:
// cancellation bounds admission/waiting, but cannot forcibly interrupt a getter.
// Its lease remains held until the getter returns. Do not copy this handle.
type ActivityDescription struct {
	private
	*nativeActivityDescription
	client            *Executions
	activityID, runID string
	decoding          chan struct{}
	decoder           *sdk.ActivityExecutionDescription
	scopeOwner        *sdk.FathomryScopeOwnerV1
}

func (*ActivityDescription) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

// Describe requests the native opt-in payload fields without eagerly decoding
// them. The returned view has no connection or source-release authority.
func (run *ActivityRun) Describe(ctx context.Context, correlation fault.Correlation, options sdk.DescribeActivityOptions) (*ActivityDescription, error) {
	if run == nil || run.client == nil {
		return nil, failure(ErrInput, "activity.describe")
	}
	return executeNative(ctx, run.client, correlation, Execution{Operation: "activity.describe", ActivityID: run.GetID(), RunID: run.GetRunID()}, func(work context.Context, evidence *Execution) (*ActivityDescription, error) {
		if err := run.initialize(work); err != nil {
			return nil, err
		}
		description, err := run.native.Describe(work, options)
		evidence.ResultObtained = err == nil
		if description == nil {
			return nil, err
		}
		authority := work.Value(nativeCallKey{}).(*nativeCall)
		return &ActivityDescription{nativeActivityDescription: description, decoder: description, scopeOwner: authority.scopeOwner, client: run.client, activityID: run.GetID(), runID: run.GetRunID(), decoding: make(chan struct{}, 1)}, err
	})
}

func decodeActivityDescription[T any](ctx context.Context, description *ActivityDescription, correlation fault.Correlation, operation string, read func() (T, error)) (T, error) {
	var zero T
	if description == nil || description.nativeActivityDescription == nil || description.decoding == nil {
		return zero, failure(ErrInput, operation)
	}
	return executeNative(ctx, description.client, correlation, Execution{Operation: operation, ActivityID: description.activityID, RunID: description.runID}, func(work context.Context, evidence *Execution) (T, error) {
		evidence.NativeCalled = false
		select {
		case description.decoding <- struct{}{}:
			defer func() { <-description.decoding }()
		case <-work.Done():
			return zero, work.Err()
		}
		evidence.NativeCalled = true
		description.decoder = sdk.FathomryScopeActivityDescriptionV1(description.decoder, description.scopeOwner, func(decode func() error) error { return nativeBorrowGuard(work, description.client.owner, decode) })
		value, err := read()
		evidence.ResultObtained = err == nil
		return value, err
	})
}

func (description *ActivityDescription) GetInput(ctx context.Context, correlation fault.Correlation, values ...any) error {
	_, err := decodeActivityDescription(ctx, description, correlation, "activity.describe-input", func() (struct{}, error) {
		return struct{}{}, description.decoder.GetInput(values...)
	})
	return err
}

func (description *ActivityDescription) GetHeartbeatDetails(ctx context.Context, correlation fault.Correlation, values ...any) error {
	_, err := decodeActivityDescription(ctx, description, correlation, "activity.describe-heartbeat", func() (struct{}, error) {
		return struct{}{}, description.decoder.GetHeartbeatDetails(values...)
	})
	return err
}

func (description *ActivityDescription) GetResult(ctx context.Context, correlation fault.Correlation, value any) error {
	_, err := decodeActivityDescription(ctx, description, correlation, "activity.describe-result", func() (struct{}, error) {
		return struct{}{}, description.decoder.GetResult(value)
	})
	return err
}

// GetOutcomeFailure preserves the native accessor's error result. HasOutcomeFailure
// distinguishes absent/unrequested failure data; conversion errors remain errors.
func (description *ActivityDescription) GetOutcomeFailure(ctx context.Context, correlation fault.Correlation) error {
	_, err := decodeActivityDescription(ctx, description, correlation, "activity.describe-outcome", func() (struct{}, error) {
		return struct{}{}, description.decoder.GetOutcomeFailure()
	})
	return err
}

// GetLastFailure observes the most recent attempt failure, not necessarily the
// terminal execution outcome. It preserves the native error and presence semantics.
func (description *ActivityDescription) GetLastFailure(ctx context.Context, correlation fault.Correlation) error {
	_, err := decodeActivityDescription(ctx, description, correlation, "activity.describe-last-failure", func() (struct{}, error) {
		return struct{}{}, description.decoder.GetLastFailure()
	})
	return err
}

func (description *ActivityDescription) GetSummary(ctx context.Context, correlation fault.Correlation) (string, error) {
	return decodeActivityDescription(ctx, description, correlation, "activity.describe-summary", func() (string, error) {
		return description.decoder.GetSummary()
	})
}

func (description *ActivityDescription) GetStaticDetails(ctx context.Context, correlation fault.Correlation) (string, error) {
	return decodeActivityDescription(ctx, description, correlation, "activity.describe-details", func() (string, error) {
		return description.decoder.GetStaticDetails()
	})
}

// CountActivities preserves native visibility aggregation and approximate-group
// semantics. Returned data is caller-owned, not independent execution evidence.
func (client *Executions) CountActivities(ctx context.Context, correlation fault.Correlation, options sdk.CountActivitiesOptions) (*sdk.CountActivitiesResult, error) {
	return executeNative(ctx, client, correlation, Execution{Operation: "activity.count"}, func(work context.Context, evidence *Execution) (*sdk.CountActivitiesResult, error) {
		result, err := client.owner.native.CountActivities(work, options)
		evidence.ResultObtained = err == nil
		return result, err
	})
}

func (client *callbackClient) ListActivities(ctx context.Context, options sdk.ListActivitiesOptions) (sdk.ListActivitiesResult, error) {
	if ctx == nil || client.binding == nil || client.binding.closed.Load() {
		return sdk.ListActivitiesResult{}, failure(ErrAuthority, "expired-callback")
	}
	return sdk.ListActivitiesResult{Results: func(yield func(*sdk.ActivityExecutionInfo, error) bool) {
		deliveredError, stopped := false, false
		_, err := callbackNative(ctx, client, "", Execution{Operation: "callback.listactivities"},
			func(work context.Context, evidence *Execution) (struct{}, error) {
				result, err := client.nativeClient.ListActivities(work, options)
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

func (client *callbackClient) CountActivities(ctx context.Context, options sdk.CountActivitiesOptions) (*sdk.CountActivitiesResult, error) {
	return callbackGranted(ctx, client, "", Execution{Operation: "callback.countactivities"},
		func(work context.Context, evidence *Execution) (*sdk.CountActivitiesResult, error) {
			value, err := client.nativeClient.CountActivities(work, options)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackClient) activityCall(ctx context.Context, namespace string, evidence Execution, invoke func(context.Context) error) error {
	_, err := callbackNative(ctx, client, namespace, evidence, func(work context.Context, evidence *Execution) (struct{}, error) {
		err := invoke(work)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

func (client *callbackClient) CompleteActivity(ctx context.Context, token []byte, result any, cause error) error {
	return client.CompleteActivityWithOptions(ctx, sdk.CompleteActivityOptions{TaskToken: token, Result: result, Err: cause})
}

func (client *callbackClient) CompleteActivityWithOptions(ctx context.Context, options sdk.CompleteActivityOptions) error {
	return client.activityCall(ctx, options.Namespace, Execution{Operation: "callback.activity.complete"}, func(work context.Context) error {
		return client.nativeClient.CompleteActivityWithOptions(work, options)
	})
}

func (client *callbackClient) CompleteActivityByID(ctx context.Context, namespace, workflowID, runID, activityID string, result any, cause error) error {
	return client.CompleteActivityByIDWithOptions(ctx, sdk.CompleteActivityByIDOptions{Namespace: namespace, WorkflowID: workflowID, RunID: runID, ActivityID: activityID, Result: result, Err: cause})
}

func (client *callbackClient) CompleteActivityByIDWithOptions(ctx context.Context, options sdk.CompleteActivityByIDOptions) error {
	return client.activityCall(ctx, options.Namespace, Execution{Operation: "callback.activity.complete-id", WorkflowID: options.WorkflowID, RunID: options.RunID, ActivityID: options.ActivityID}, func(work context.Context) error {
		return client.nativeClient.CompleteActivityByIDWithOptions(work, options)
	})
}

func (client *callbackClient) CompleteActivityByActivityID(ctx context.Context, namespace, activityID, runID string, result any, cause error) error {
	return client.CompleteActivityByActivityIDWithOptions(ctx, sdk.CompleteActivityByActivityIDOptions{Namespace: namespace, ActivityID: activityID, ActivityRunID: runID, Result: result, Err: cause})
}

func (client *callbackClient) CompleteActivityByActivityIDWithOptions(ctx context.Context, options sdk.CompleteActivityByActivityIDOptions) error {
	return client.activityCall(ctx, options.Namespace, Execution{Operation: "callback.activity.complete-standalone", RunID: options.ActivityRunID, ActivityID: options.ActivityID}, func(work context.Context) error {
		return client.nativeClient.CompleteActivityByActivityIDWithOptions(work, options)
	})
}

func (client *callbackClient) RecordActivityHeartbeat(ctx context.Context, token []byte, details ...any) error {
	return client.RecordActivityHeartbeatWithOptions(ctx, sdk.RecordActivityHeartbeatOptions{TaskToken: token, Details: details})
}

func (client *callbackClient) RecordActivityHeartbeatWithOptions(ctx context.Context, options sdk.RecordActivityHeartbeatOptions) error {
	return client.activityCall(ctx, options.Namespace, Execution{Operation: "callback.activity.heartbeat"}, func(work context.Context) error {
		return client.nativeClient.RecordActivityHeartbeatWithOptions(work, options)
	})
}

func (client *callbackClient) RecordActivityHeartbeatByID(ctx context.Context, namespace, workflowID, runID, activityID string, details ...any) error {
	return client.RecordActivityHeartbeatByIDWithOptions(ctx, sdk.RecordActivityHeartbeatByIDOptions{Namespace: namespace, WorkflowID: workflowID, RunID: runID, ActivityID: activityID, Details: details})
}

func (client *callbackClient) RecordActivityHeartbeatByIDWithOptions(ctx context.Context, options sdk.RecordActivityHeartbeatByIDOptions) error {
	return client.activityCall(ctx, options.Namespace, Execution{Operation: "callback.activity.heartbeat-id", WorkflowID: options.WorkflowID, RunID: options.RunID, ActivityID: options.ActivityID}, func(work context.Context) error {
		return client.nativeClient.RecordActivityHeartbeatByIDWithOptions(work, options)
	})
}

// The SDK handle interface is preserved without exporting the native handle or
// running context-free interceptor factories outside an admitted operation.
type activityHandleView struct {
	private
	id, runID             string
	native                sdk.ActivityHandle
	create                func(context.Context) (sdk.ActivityHandle, error)
	gate                  func(context.Context, string, func(context.Context, *Execution) error) error
	initializing, waiting chan struct{}
	factory               *clientFactory
	authority             *nativeCall
}

func (view *activityHandleView) GetID() string { return view.id }

func (view *activityHandleView) GetRunID() string { return view.runID }

func (view *activityHandleView) load(ctx context.Context) (sdk.ActivityHandle, error) {
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
			return nil, failure(ErrExecution, "native-activity-handle")
		}
		view.native = value
	}
	return view.native, nil
}

func (view *activityHandleView) call(ctx context.Context, operation string, invoke func(context.Context, sdk.ActivityHandle, *Execution) error) error {
	return view.gate(ctx, operation, func(work context.Context, evidence *Execution) error {
		native, err := view.load(work)
		if err != nil {
			return err
		}
		return invoke(work, native, evidence)
	})
}

func (view *activityHandleView) Get(ctx context.Context, output any) error {
	return view.call(ctx, "activity.result", func(work context.Context, native sdk.ActivityHandle, evidence *Execution) error {
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

func (view *activityHandleView) Describe(ctx context.Context, options sdk.DescribeActivityOptions) (value *sdk.ActivityExecutionDescription, err error) {
	err = view.call(ctx, "activity.describe", func(work context.Context, native sdk.ActivityHandle, evidence *Execution) error {
		var err error
		value, err = native.Describe(work, options)
		evidence.ResultObtained = err == nil
		if value != nil {
			authority, _ := work.Value(nativeCallKey{}).(*nativeCall)
			if authority == nil {
				return failure(ErrAuthority, "activity-description-scope")
			}
			value = sdk.FathomryScopeActivityDescriptionV1(value, authority.scopeOwner, func(decode func() error) error {
				return view.gate(context.WithoutCancel(work), "activity.description-decode", func(_ context.Context, evidence *Execution) error {
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

func (client *callbackClient) activityHandle(native sdk.ActivityHandle, id, runID string) sdk.ActivityHandle {
	return &activityHandleView{id: id, runID: runID, native: native, initializing: make(chan struct{}, 1), waiting: make(chan struct{}, 1),
		create: func(context.Context) (sdk.ActivityHandle, error) {
			return client.nativeClient.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: id, RunID: runID}), nil
		},
		gate: func(ctx context.Context, operation string, invoke func(context.Context, *Execution) error) error {
			_, err := callbackNative(ctx, client, "", Execution{Operation: "callback." + operation, ActivityID: id, RunID: runID}, func(work context.Context, evidence *Execution) (struct{}, error) {
				return struct{}{}, invoke(work, evidence)
			})
			return err
		},
	}
}

func (client *callbackClient) GetActivityHandle(options sdk.GetActivityHandleOptions) sdk.ActivityHandle {
	return client.activityHandle(nil, options.ActivityID, options.RunID)
}

func (gate *clientContinuation[PollInput, PollOutput]) GetActivityHandle(input *interceptor.ClientGetActivityHandleInput) sdk.ActivityHandle {
	if input == nil {
		return nil
	}
	copy := *input
	return &activityHandleView{id: input.ActivityID, runID: input.RunID, initializing: make(chan struct{}, 1), waiting: make(chan struct{}, 1),
		create: func(context.Context) (sdk.ActivityHandle, error) { return gate.Next.GetActivityHandle(&copy), nil },
		gate: func(ctx context.Context, operation string, invoke func(context.Context, *Execution) error) error {
			_, err := clientContinue(ctx, gate, func(work context.Context) (struct{}, error) {
				evidence := &Execution{Operation: operation, ActivityID: copy.ActivityID, RunID: copy.RunID}
				return struct{}{}, invoke(work, evidence)
			})
			return err
		},
	}
}

func (view *activityHandleView) Cancel(ctx context.Context, options sdk.CancelActivityOptions) error {
	return view.call(ctx, "activity.cancel", func(work context.Context, native sdk.ActivityHandle, evidence *Execution) error {
		err := native.Cancel(work, options)
		evidence.Accepted = err == nil
		return err
	})
}

func (view *activityHandleView) Terminate(ctx context.Context, options sdk.TerminateActivityOptions) error {
	return view.call(ctx, "activity.terminate", func(work context.Context, native sdk.ActivityHandle, evidence *Execution) error {
		err := native.Terminate(work, options)
		evidence.Accepted = err == nil
		return err
	})
}

func (view *activityHandleView) Pause(ctx context.Context, options sdk.PauseActivityOptions) error {
	return view.call(ctx, "activity.pause", func(work context.Context, native sdk.ActivityHandle, evidence *Execution) error {
		err := native.Pause(work, options)
		evidence.Accepted = err == nil
		return err
	})
}

func (view *activityHandleView) Unpause(ctx context.Context, options sdk.UnpauseActivityOptions) error {
	return view.call(ctx, "activity.unpause", func(work context.Context, native sdk.ActivityHandle, evidence *Execution) error {
		err := native.Unpause(work, options)
		evidence.Accepted = err == nil
		return err
	})
}

func (view *activityHandleView) UpdateOptions(ctx context.Context, options sdk.ActivityOptionsUpdate) (value *sdk.ActivityExecutionOptions, err error) {
	err = view.call(ctx, "activity.update-options", func(work context.Context, native sdk.ActivityHandle, evidence *Execution) error {
		var err error
		value, err = native.UpdateOptions(work, options)
		evidence.Accepted = err == nil
		return err
	})
	return
}

func (view *activityHandleView) RestoreOriginalOptions(ctx context.Context) (value *sdk.ActivityExecutionOptions, err error) {
	err = view.call(ctx, "activity.restore-options", func(work context.Context, native sdk.ActivityHandle, evidence *Execution) error {
		var err error
		value, err = native.RestoreOriginalOptions(work)
		evidence.Accepted = err == nil
		return err
	})
	return
}

func (client *callbackClient) ExecuteActivity(ctx context.Context, options sdk.StartActivityOptions, definition any, args ...any) (sdk.ActivityHandle, error) {
	return callbackNative(ctx, client, "", Execution{Operation: "callback.activity.start", ActivityID: options.ID}, func(work context.Context, evidence *Execution) (sdk.ActivityHandle, error) {
		run, err := client.nativeClient.ExecuteActivity(work, options, definition, args...)
		if run != nil {
			evidence.ActivityID = run.GetID()
			evidence.RunID = run.GetRunID()
		}
		evidence.Accepted = err == nil
		if run != nil {
			return client.activityHandle(run, run.GetID(), run.GetRunID()), err
		}
		return nil, err
	})
}
