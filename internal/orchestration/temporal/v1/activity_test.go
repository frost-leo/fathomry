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

package temporal_test

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func activityPayload(value string) *commonpb.Payload {
	return &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(value)}
}

func activityPayloads(value string) *commonpb.Payloads {
	return &commonpb.Payloads{Payloads: []*commonpb.Payload{activityPayload(value)}}
}

func TestActivityHeartbeatNativeDirectivesAndEvidence(t *testing.T) {
	for _, test := range []struct {
		name                          string
		byID, canceled, paused, reset bool
		want                          error
	}{
		{name: "normal"}, {name: "canceled", canceled: true}, {name: "paused", paused: true, want: activity.ErrActivityPaused},
		{name: "reset", reset: true, want: activity.ErrActivityReset}, {name: "by-id-canceled", byID: true, canceled: true},
		{name: "by-id-pause-reset", byID: true, paused: true, reset: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var seen atomic.Int32
			fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
				peer.intercept = func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
					switch request := request.(type) {
					case *workflowservice.RecordActivityTaskHeartbeatRequest:
						seen.Add(1)
						if request.Namespace != "test" || string(request.TaskToken) != "sensitive-token" || string(request.Details.Payloads[0].Data) != "\"checkpoint\"" {
							return nil, status.Error(codes.InvalidArgument, "unexpected token heartbeat")
						}
						return &workflowservice.RecordActivityTaskHeartbeatResponse{CancelRequested: test.canceled, ActivityPaused: test.paused, ActivityReset: test.reset}, nil
					case *workflowservice.RecordActivityTaskHeartbeatByIdRequest:
						seen.Add(1)
						if request.Namespace != "test" || request.WorkflowId != "workflow" || request.RunId != "run" || request.ActivityId != "activity" {
							return nil, status.Error(codes.InvalidArgument, "unexpected ID heartbeat")
						}
						return &workflowservice.RecordActivityTaskHeartbeatByIdResponse{CancelRequested: test.canceled, ActivityPaused: test.paused, ActivityReset: test.reset}, nil
					}
					return next(ctx, request)
				}
			})
			executions, inbox := executionBinding(t, fixture)
			var err error
			if test.byID {
				err = executions.RecordActivityHeartbeatByID(context.Background(), fault.Correlation{Call: "heartbeat"}, sdk.RecordActivityHeartbeatByIDOptions{WorkflowID: "workflow", RunID: "run", ActivityID: "activity", Details: []any{"checkpoint"}})
			} else {
				err = executions.RecordActivityHeartbeat(context.Background(), fault.Correlation{Call: "heartbeat"}, sdk.RecordActivityHeartbeatOptions{TaskToken: []byte("sensitive-token"), Details: []any{"checkpoint"}})
			}
			var canceled *sdktemporal.CanceledError
			if test.canceled {
				if !errors.As(err, &canceled) {
					t.Fatal("native heartbeat cancellation was not preserved")
				}
			} else if test.want != nil {
				if !errors.Is(err, test.want) {
					t.Fatal("native heartbeat directive error changed")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			record := receiveExecution(t, inbox)
			value := record.Outcome.Value
			if seen.Load() != 1 || !value.HeartbeatAcknowledged || value.CancellationRequested != test.canceled || value.ActivityPaused != test.paused || value.ActivityReset != test.reset ||
				value.Accepted != (err == nil) {
				t.Fatal("native heartbeat response facts were erased or conflated with the SDK error")
			}
			if test.canceled && !errors.As(record.Err(), &canceled) || test.want != nil && !errors.Is(record.Err(), test.want) {
				t.Fatal("handled directive error disappeared")
			}
			conformance.Private(t, value, "sensitive-token", "checkpoint")
		})
	}
}

type activityMarshalProbe struct{ called *atomic.Int32 }

func (probe activityMarshalProbe) MarshalJSON() ([]byte, error) {
	probe.called.Add(1)
	return []byte("\"must-not-encode\""), nil
}

func TestActivityCompletionRoutingNamespaceAndUnknownAcceptance(t *testing.T) {
	var sent atomic.Int32
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			switch request := request.(type) {
			case *workflowservice.RespondActivityTaskCompletedRequest:
				sent.Add(1)
				if request.Namespace != "test" || string(request.Result.Payloads[0].Data) != "\"completed\"" {
					return nil, status.Error(codes.InvalidArgument, "unexpected completion")
				}
				if string(request.TaskToken) == "lost-response" {
					return nil, status.Error(codes.DeadlineExceeded, "accepted completion response lost")
				}
				return &workflowservice.RespondActivityTaskCompletedResponse{}, nil
			case *workflowservice.RespondActivityTaskCanceledByIdRequest:
				sent.Add(1)
				if request.Namespace != "test" || request.WorkflowId != "workflow" || request.RunId != "workflow-run" || request.ActivityId != "activity" ||
					string(request.Details.Payloads[0].Data) != "\"canceled-detail\"" {
					return nil, status.Error(codes.InvalidArgument, "unexpected canceled ID completion")
				}
				return &workflowservice.RespondActivityTaskCanceledByIdResponse{}, nil
			case *workflowservice.RespondActivityTaskFailedByIdRequest:
				sent.Add(1)
				if request.Namespace != "test" || request.WorkflowId != "" || request.RunId != "activity-run" || request.ActivityId != "standalone" ||
					request.Failure.GetApplicationFailureInfo().GetType() != "synthetic" {
					return nil, status.Error(codes.InvalidArgument, "unexpected standalone failure completion")
				}
				return &workflowservice.RespondActivityTaskFailedByIdResponse{}, nil
			}
			return next(ctx, request)
		}
	})
	executions, inbox := executionBinding(t, fixture)
	ctx := context.Background()
	if err := executions.CompleteActivity(ctx, fault.Correlation{Call: "token"}, sdk.CompleteActivityOptions{TaskToken: []byte("token"), Result: "completed"}); err != nil {
		t.Fatal(err)
	}
	if err := executions.CompleteActivityByID(ctx, fault.Correlation{Call: "by-id"}, sdk.CompleteActivityByIDOptions{Namespace: "test", WorkflowID: "workflow", RunID: "workflow-run", ActivityID: "activity", Err: sdktemporal.NewCanceledError("canceled-detail")}); err != nil {
		t.Fatal(err)
	}
	if err := executions.CompleteActivityByActivityID(ctx, fault.Correlation{Call: "standalone"}, sdk.CompleteActivityByActivityIDOptions{Namespace: "test", ActivityID: "standalone", ActivityRunID: "activity-run", WorkflowID: "serialization-hint", Err: sdktemporal.NewApplicationError("retry", "synthetic")}); err != nil {
		t.Fatal(err)
	}
	err := executions.CompleteActivity(ctx, fault.Correlation{Call: "unknown"}, sdk.CompleteActivityOptions{TaskToken: []byte("lost-response"), Result: "completed"})
	var deadline *serviceerror.DeadlineExceeded
	if !errors.As(err, &deadline) {
		t.Fatal("completion response loss was not preserved")
	}
	var encoded atomic.Int32
	err = executions.CompleteActivity(ctx, fault.Correlation{Call: "wrong-namespace"}, sdk.CompleteActivityOptions{TaskToken: []byte("token"), Namespace: "other", Result: activityMarshalProbe{called: &encoded}})
	if !errors.Is(err, temporal.ErrAuthority) || encoded.Load() != 0 || sent.Load() != 4 {
		t.Fatal("invalid serialization namespace entered native conversion or transport")
	}
	if err := executions.CompleteActivity(ctx, fault.Correlation{Call: "pending-no-report"}, sdk.CompleteActivityOptions{TaskToken: []byte("token"), Err: activity.ErrResultPending}); err != nil {
		t.Fatal(err)
	}
	for range 6 {
		record := receiveExecution(t, inbox)
		value := record.Outcome.Value
		switch record.Context.Correlation.Call {
		case "token":
			if !value.Accepted || value.WorkflowID != "" || value.ActivityID != "" {
				t.Fatal("opaque token invented an execution identity")
			}
		case "by-id":
			if value.WorkflowID != "workflow" || value.RunID != "workflow-run" || !value.Accepted {
				t.Fatal("workflow completion identity lost")
			}
		case "standalone":
			if value.WorkflowID != "" || value.ActivityID != "standalone" || value.RunID != "activity-run" || !value.Accepted {
				t.Fatal("standalone completion invented Workflow attribution")
			}
		case "unknown":
			if !value.NativeCalled || value.Accepted || !errors.As(record.Err(), &deadline) {
				t.Fatal("unknown completion was presented as accepted")
			}
		case "wrong-namespace":
			if value.NativeCalled || !errors.Is(record.Err(), temporal.ErrAuthority) {
				t.Fatal("namespace refusal lost evidence")
			}
		case "pending-no-report":
			if !value.NativeCalled || value.CompletionAcknowledged || record.Attempts.Observed != 0 {
				t.Fatal("native pending result invented a completion acknowledgement")
			}
		default:
			t.Fatal("unexpected completion record")
		}
	}
}

func TestActivityHandleSerializesResultAndDescriptionDecoders(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var polls atomic.Int32
	fixture := newFixture(t, 1, func(options *temporal.OptionsV1, limits *resource.Limits, peer *rpcServer) {
		options.MaxActive, limits.Active, limits.Bytes = 2, 2, 2*limits.Bytes
		peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			switch request := request.(type) {
			case *workflowservice.PollActivityExecutionRequest:
				polls.Add(1)
				once.Do(func() { close(entered) })
				select {
				case <-release:
				case <-ctx.Done():
					return nil, status.FromContextError(ctx.Err()).Err()
				}
				return &workflowservice.PollActivityExecutionResponse{Outcome: &activitypb.ActivityExecutionOutcome{Value: &activitypb.ActivityExecutionOutcome_Result{Result: activityPayloads("\"answer\"")}}}, nil
			case *workflowservice.DescribeActivityExecutionRequest:
				if !request.IncludeInput || !request.IncludeOutcome || !request.IncludeHeartbeatDetails {
					return nil, status.Error(codes.InvalidArgument, "payload options lost")
				}
				return &workflowservice.DescribeActivityExecutionResponse{
					Info: &activitypb.ActivityExecutionInfo{ActivityId: "activity", RunId: "activity-run", ActivityType: &commonpb.ActivityType{Name: "definition"},
						SearchAttributes: &commonpb.SearchAttributes{}, Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, HeartbeatDetails: activityPayloads("\"heartbeat\""),
						UserMetadata: &sdkpb.UserMetadata{Summary: activityPayload("\"summary\""), Details: activityPayload("\"details\"")}},
					Input: activityPayloads("\"input\""), Outcome: &activitypb.ActivityExecutionOutcome{Value: &activitypb.ActivityExecutionOutcome_Result{Result: activityPayloads("\"answer\"")}},
				}, nil
			case *workflowservice.CountActivityExecutionsRequest:
				if request.Namespace != "test" || request.Query != "native-query" {
					return nil, status.Error(codes.InvalidArgument, "count target changed")
				}
				return &workflowservice.CountActivityExecutionsResponse{Count: 7, Groups: []*workflowservice.CountActivityExecutionsResponse_AggregationGroup{{Count: 7, GroupValues: []*commonpb.Payload{activityPayload("\"group\"")}}}}, nil
			}
			return next(ctx, request)
		}
	})
	var released sync.Once
	defer released.Do(func() { close(release) })
	executions, inbox := executionBinding(t, fixture)
	run, err := executions.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "activity", RunID: "activity-run"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	var value string
	go func() { done <- run.Get(ctx, fault.Correlation{Call: "result"}, &value) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("activity result wait did not enter")
	}
	short, cancelWait := context.WithTimeout(ctx, 20*time.Millisecond)
	err = run.Get(short, fault.Correlation{Call: "serialized-wait"}, nil)
	cancelWait()
	if !errors.Is(err, context.DeadlineExceeded) || polls.Load() != 1 {
		t.Fatal("concurrent native result cache access was not bounded")
	}
	released.Do(func() { close(release) })
	if err := <-done; err != nil || value != "answer" {
		t.Fatal("native result changed")
	}
	if err := run.Get(ctx, fault.Correlation{Call: "cached"}, &value); err != nil || polls.Load() != 1 {
		t.Fatal("native result cache was lost")
	}
	description, err := run.Describe(ctx, fault.Correlation{Call: "describe"}, sdk.DescribeActivityOptions{IncludeInput: true, IncludeOutcome: true, IncludeHeartbeatDetails: true})
	if err != nil {
		t.Fatal(err)
	}
	if description.ActivityID != "activity" || description.ActivityRunID != "activity-run" || !description.HasResult() || !description.HasInput() || !description.HasHeartbeatDetails() {
		t.Fatal("native metadata/presence fields lost")
	}
	if err := description.GetInput(ctx, fault.Correlation{Call: "input"}, &value); err != nil || value != "input" {
		t.Fatal("native input decode failed")
	}
	if err := description.GetHeartbeatDetails(ctx, fault.Correlation{Call: "heartbeat"}, &value); err != nil || value != "heartbeat" {
		t.Fatal("heartbeat decode failed")
	}
	if err := description.GetResult(ctx, fault.Correlation{Call: "description-result"}, &value); err != nil || value != "answer" {
		t.Fatal("description result decode failed")
	}
	if summary, err := description.GetSummary(ctx, fault.Correlation{Call: "summary"}); err != nil || summary != "summary" {
		t.Fatal("native summary lost")
	}
	if details, err := description.GetStaticDetails(ctx, fault.Correlation{Call: "details"}); err != nil || details != "details" {
		t.Fatal("native details lost")
	}
	count, err := executions.CountActivities(ctx, fault.Correlation{Call: "count"}, sdk.CountActivitiesOptions{Query: "native-query"})
	if err != nil || count.Count != 7 || len(count.Groups) != 1 || count.Groups[0].GroupValues[0] != "group" {
		t.Fatal("native visibility aggregation changed")
	}
	for range 10 {
		receiveExecution(t, inbox)
	}
	if err := fixture.assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := run.Get(ctx, fault.Correlation{Call: "closed-cache"}, &value); err == nil {
		t.Fatal("cached result bypassed source lifetime")
	}
	if _, err := description.GetSummary(ctx, fault.Correlation{Call: "closed-summary"}); err == nil {
		t.Fatal("cached metadata decoder bypassed source lifetime")
	}
	conformance.Private(t, description, "answer", "input", "summary")
}

func TestActivityOperationalGrantsAndNativeOptionChanges(t *testing.T) {
	for _, granted := range []bool{false, true} {
		t.Run(map[bool]string{false: "ungranted", true: "granted"}[granted], func(t *testing.T) {
			var changes, pauses atomic.Int32
			fixture := newFixture(t, 1, func(options *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
				if granted {
					options.RPCs = append(options.RPCs, workflowPrefix+"PauseActivityExecution", workflowPrefix+"UnpauseActivityExecution", workflowPrefix+"UpdateActivityExecutionOptions")
				}
				peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
					switch request := request.(type) {
					case *workflowservice.PauseActivityExecutionRequest:
						pauses.Add(1)
						return nil, status.Error(codes.Unimplemented, "standalone operator capability disabled")
					case *workflowservice.UnpauseActivityExecutionRequest:
						if request.Jitter.AsDuration() != time.Second {
							return nil, status.Error(codes.InvalidArgument, "jitter changed")
						}
						return &workflowservice.UnpauseActivityExecutionResponse{}, nil
					case *workflowservice.UpdateActivityExecutionOptionsRequest:
						changes.Add(1)
						if request.RestoreOriginal {
							if len(request.UpdateMask.GetPaths()) != 0 {
								return nil, status.Error(codes.InvalidArgument, "restore also changed options")
							}
						} else if !slices.Equal(request.UpdateMask.GetPaths(), []string{"heartbeat_timeout"}) || request.ActivityOptions.GetHeartbeatTimeout() != nil {
							return nil, status.Error(codes.InvalidArgument, "native clear/no-change semantics lost")
						}
						return &workflowservice.UpdateActivityExecutionOptionsResponse{ActivityOptions: &activitypb.ActivityOptions{}}, nil
					}
					return next(ctx, request)
				}
			})
			executions, _ := executionBinding(t, fixture)
			run, err := executions.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "operational", RunID: "run"})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			err = run.Pause(ctx, fault.Correlation{Call: "pause"}, sdk.PauseActivityOptions{Reason: "fixture"})
			if !granted {
				if !errors.Is(err, temporal.ErrAuthority) || pauses.Load() != 0 {
					t.Fatal("operator authority was enabled implicitly")
				}
				return
			}
			var unimplemented *serviceerror.Unimplemented
			if !errors.As(err, &unimplemented) || pauses.Load() != 1 {
				t.Fatal("server capability refusal was hidden")
			}
			if err := run.Unpause(ctx, fault.Correlation{Call: "unpause"}, sdk.UnpauseActivityOptions{Jitter: time.Second}); err != nil {
				t.Fatal(err)
			}
			if _, err := run.UpdateOptions(ctx, fault.Correlation{Call: "clear"}, sdk.ActivityOptionsUpdate{HeartbeatTimeout: &sdk.ActivityOptionChange[time.Duration]{}}); err != nil {
				t.Fatal(err)
			}
			if _, err := run.RestoreOriginalOptions(ctx, fault.Correlation{Call: "restore"}); err != nil {
				t.Fatal(err)
			}
			if changes.Load() != 2 {
				t.Fatal("option update/restore did not use distinct native requests")
			}
		})
	}
}

type controlledActivityValue struct{ entered, release chan struct{} }

func (value controlledActivityValue) MarshalJSON() ([]byte, error) {
	close(value.entered)
	<-value.release
	return []byte("\"controlled\""), nil
}

type exitingActivityValue struct {
	cause  error
	goexit bool
}

func (value exitingActivityValue) MarshalJSON() ([]byte, error) {
	if value.goexit {
		runtime.Goexit()
	}
	panic(value.cause)
}

func TestActivityCompletionPanicAndGoexitReleaseEvidence(t *testing.T) {
	for _, goexit := range []bool{false, true} {
		t.Run(map[bool]string{false: "panic", true: "goexit"}[goexit], func(t *testing.T) {
			fixture := newFixture(t, 1)
			executions, inbox := executionBinding(t, fixture)
			original := errors.New("synthetic conversion panic")
			done := make(chan any, 1)
			go func() {
				defer func() { done <- recover() }()
				_ = executions.CompleteActivity(context.Background(), fault.Correlation{Call: "conversion-exit"}, sdk.CompleteActivityOptions{TaskToken: []byte("token"), Result: exitingActivityValue{cause: original, goexit: goexit}})
			}()
			select {
			case recovered := <-done:
				if !goexit && recovered != original {
					t.Fatal("native panic identity was changed")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("native conversion did not exit")
			}
			record := receiveExecution(t, inbox)
			if record.Err() == nil || !record.Released || !record.Outcome.Value.NativeCalled || record.Outcome.Value.CompletionAcknowledged {
				t.Fatal("abrupt native exit lost evidence or retained a live lease")
			}
			if !goexit && !errors.Is(record.Err(), original) {
				t.Fatal("panic cause was not independently retained")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := fixture.assembly.Close(ctx); err != nil {
				t.Fatal("abrupt callback leaked source ownership", err)
			}
		})
	}
}

func TestActivityCallbackAdmissionPrecedesConversionAndRetainsTail(t *testing.T) {
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.activityName = "callback-complete"
		peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			if _, ok := request.(*workflowservice.RecordActivityTaskHeartbeatRequest); ok {
				return &workflowservice.RecordActivityTaskHeartbeatResponse{}, nil
			}
			return next(ctx, request)
		}
	})
	inbox, err := invocation.NewInbox[temporal.Execution](1, temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	executions, err := temporal.BindExecutions(fixture.assembly, fixture.selection, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	workers, tasks := workerInboxes(t)
	full, proceed, entered, release := make(chan error, 1), make(chan struct{}), make(chan struct{}), make(chan struct{})
	completion := make(chan error, 1)
	var goOnce, releaseOnce sync.Once
	defer goOnce.Do(func() { close(proceed) })
	defer releaseOnce.Do(func() { close(release) })
	var borrowed sdk.Client
	var borrowedContext context.Context
	var encoded atomic.Int32
	body := func(ctx context.Context) error {
		borrowed = activity.GetClient(ctx)
		borrowedContext = context.WithoutCancel(ctx)
		if err := borrowed.CompleteActivityWithOptions(ctx, sdk.CompleteActivityOptions{Namespace: "other", TaskToken: []byte("other-token"), Result: activityMarshalProbe{called: &encoded}}); !errors.Is(err, temporal.ErrAuthority) || encoded.Load() != 0 {
			return errors.New("callback namespace validation did not precede conversion")
		}
		if err := borrowed.RecordActivityHeartbeat(ctx, []byte("other-token"), "checkpoint"); err != nil {
			return err
		}
		denied := borrowed.CompleteActivity(ctx, []byte("other-token"), activityMarshalProbe{called: &encoded}, nil)
		full <- denied
		<-proceed
		go func() {
			completion <- borrowed.CompleteActivity(borrowedContext, []byte("other-token"), controlledActivityValue{entered: entered, release: release}, nil)
		}()
		select {
		case <-entered:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "completion-worker"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1, WorkerStopTimeout: time.Millisecond},
		Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "callback-complete"}}},
	}, workers, tasks)
	if managed != nil {
		t.Cleanup(func() {
			goOnce.Do(func() { close(proceed) })
			releaseOnce.Do(func() { close(release) })
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := managed.Stop(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-full:
		if !errors.Is(err, invocation.ErrEvidence) || encoded.Load() != 0 {
			t.Fatal("evidence saturation reached native conversion")
		}
	case <-ctx.Done():
		t.Fatal("callback control did not execute")
	}
	receiveExecution(t, inbox)
	goOnce.Do(func() { close(proceed) })
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("owned callback conversion did not enter")
	}
	short, endWait := context.WithTimeout(ctx, 20*time.Millisecond)
	err = managed.Stop(short)
	endWait()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Worker released a live admitted conversion tail")
	}
	if err := fixture.assembly.Close(ctx); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("source released while callback conversion still owned it")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-completion:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("owned callback did not complete")
	}
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	record := receiveExecution(t, inbox)
	if !record.Nested || !record.Outcome.Value.CompletionAcknowledged || record.Err() != nil {
		t.Fatal("nested completion evidence was lost")
	}
	err = borrowed.CompleteActivity(borrowedContext, []byte("other-token"), activityMarshalProbe{called: &encoded}, nil)
	if !errors.Is(err, temporal.ErrAuthority) || encoded.Load() != 0 {
		t.Fatal("expired callback entered conversion")
	}
}
