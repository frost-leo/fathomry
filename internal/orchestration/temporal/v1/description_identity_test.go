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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
)

type descriptionIdentityTraffic struct{ target string }

func (hook descriptionIdentityTraffic) CheckCallAllowed(_ context.Context, _ string, request, _ any) error {
	switch request := request.(type) {
	case *workflowservice.QueryWorkflowRequest:
		request.Execution.WorkflowId, request.Execution.RunId = hook.target, "effective-run"
	case *workflowservice.DescribeWorkflowExecutionRequest:
		request.Execution.WorkflowId, request.Execution.RunId = hook.target, "effective-run"
	case *workflowservice.DescribeActivityExecutionRequest:
		request.ActivityId, request.RunId = hook.target, "effective-run"
	case *workflowservice.DescribeNexusOperationExecutionRequest:
		request.OperationId, request.RunId = hook.target, "effective-run"
	}
	return nil
}

func descriptionIdentityResponse(ctx context.Context, request any, next grpc.UnaryHandler) (any, error) {
	payloads := activityPayloads("\"detail\"")
	switch request := request.(type) {
	case *workflowservice.QueryWorkflowRequest:
		return &workflowservice.QueryWorkflowResponse{QueryResult: payloads}, nil
	case *workflowservice.DescribeWorkflowExecutionRequest:
		return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: request.Execution, SearchAttributes: &commonpb.SearchAttributes{}, Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{"payload": payloads.Payloads[0]}},
		}}, nil
	case *workflowservice.DescribeActivityExecutionRequest:
		return &workflowservice.DescribeActivityExecutionResponse{RunId: request.RunId, Info: &activitypb.ActivityExecutionInfo{
			ActivityId: request.ActivityId, RunId: request.RunId, ActivityType: &commonpb.ActivityType{Name: "fixture"}, SearchAttributes: &commonpb.SearchAttributes{},
		}, Input: payloads}, nil
	case *workflowservice.DescribeNexusOperationExecutionRequest:
		return &workflowservice.DescribeNexusOperationExecutionResponse{RunId: request.RunId, Info: &nexuspb.NexusOperationExecutionInfo{
			OperationId: request.OperationId, RunId: request.RunId, UserMetadata: &sdkpb.UserMetadata{Summary: payloads.Payloads[0]},
			CancellationInfo: &nexuspb.NexusOperationExecutionCancellationInfo{LastAttemptFailure: &failurepb.Failure{Message: "fixture", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "fixture", Details: payloads}}}},
		}}, nil
	}
	return next(ctx, request)
}

func describeIdentity(ctx context.Context, direct *temporal.Executions, callback sdk.Client, family string) (func() error, error) {
	var output string
	if strings.HasPrefix(family, "query") {
		if callback != nil {
			var value converter.EncodedValue
			var err error
			if family == "query-options" {
				var response *sdk.QueryWorkflowWithOptionsResponse
				response, err = callback.QueryWorkflowWithOptions(ctx, &sdk.QueryWorkflowWithOptionsRequest{WorkflowID: "intention", RunID: "run", QueryType: "query"})
				if response != nil {
					value = response.QueryResult
				}
			} else {
				value, err = callback.QueryWorkflow(ctx, "intention", "run", "query")
			}
			return func() error {
				if err := value.Get(&output); err != nil {
					return err
				}
				if converter.GetPayloads(value) == nil {
					return errors.New("native query payloads missing")
				}
				return nil
			}, err
		}
		var value *temporal.QueryValue
		var err error
		if family == "query-options" {
			value, _, err = direct.QueryWorkflowValueWithOptions(ctx, fault.Correlation{Call: "query"}, &sdk.QueryWorkflowWithOptionsRequest{WorkflowID: "intention", RunID: "run", QueryType: "query"})
		} else {
			value, err = direct.QueryWorkflowValue(ctx, fault.Correlation{Call: "query"}, "intention", "run", "query")
		}
		return func() error {
			if err := value.Get(ctx, fault.Correlation{Call: "decode"}, &output); err != nil {
				return err
			}
			_, _, err := value.RawPayloads(ctx, fault.Correlation{Call: "payloads"})
			return err
		}, err
	}
	if family == "workflow" {
		if callback != nil {
			value, err := callback.DescribeWorkflow(ctx, "intention", "run")
			return func() error { return value.GetMemoValue("payload", &output) }, err
		}
		value, err := direct.DescribeWorkflow(ctx, fault.Correlation{Call: "describe"}, "intention", "run")
		return func() error { return value.GetMemoValue(ctx, fault.Correlation{Call: "decode"}, "payload", &output) }, err
	}
	if family == "activity" {
		options := sdk.GetActivityHandleOptions{ActivityID: "intention", RunID: "run"}
		if callback != nil {
			value, err := callback.GetActivityHandle(options).Describe(ctx, sdk.DescribeActivityOptions{IncludeInput: true})
			return func() error { return value.GetInput(&output) }, err
		}
		run, err := direct.GetActivityHandle(options)
		if err != nil {
			return nil, err
		}
		value, err := run.Describe(ctx, fault.Correlation{Call: "describe"}, sdk.DescribeActivityOptions{IncludeInput: true})
		return func() error { return value.GetInput(ctx, fault.Correlation{Call: "decode"}, &output) }, err
	}
	options := sdk.GetNexusOperationHandleOptions{OperationID: "intention", RunID: "run"}
	var decode func() error
	var err error
	if callback != nil {
		var value *sdk.NexusOperationExecutionDescription
		value, err = callback.GetNexusOperationHandle(options).Describe(ctx, sdk.DescribeNexusOperationOptions{})
		if family == "cancellation" {
			decode = value.CancellationInfo.GetLastAttemptFailure
		} else {
			decode = func() error { _, err := value.GetSummary(); return err }
		}
	} else {
		run, handleErr := direct.GetNexusOperationHandle(options)
		if handleErr != nil {
			return nil, handleErr
		}
		var value *temporal.NexusDescription
		value, err = run.Describe(ctx, fault.Correlation{Call: "describe"}, sdk.DescribeNexusOperationOptions{})
		if family == "cancellation" {
			decode = func() error {
				return value.CancellationInfo.GetLastAttemptFailure(ctx, fault.Correlation{Call: "decode"})
			}
		} else {
			decode = func() error { _, err := value.GetSummary(ctx, fault.Correlation{Call: "decode"}); return err }
		}
	}
	if family == "cancellation" {
		failureDecode := decode
		decode = func() error {
			err := failureDecode()
			var native *sdktemporal.ApplicationError
			if !errors.As(err, &native) {
				return fmt.Errorf("native cancellation failure missing: %w", err)
			}
			return nil
		}
	}
	return decode, err
}

func TestDescriptionDelayedEvidencePreservesEffectiveAndOmittedIdentity(t *testing.T) {
	for _, family := range []string{"workflow", "activity", "nexus", "cancellation", "query", "query-options"} {
		for _, callback := range []bool{false, true} {
			for _, omitted := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/callback=%t/omitted=%t", family, callback, omitted), func(t *testing.T) {
					target := "effective"
					if omitted {
						target = strings.Repeat("x", 1025)
					}
					fixture := newRuntimeFixture(t, 4, temporal.RuntimeOptions{TrafficController: descriptionIdentityTraffic{target: target}, Interceptors: []interceptor.ClientInterceptor{&interceptor.ClientInterceptorBase{}}}, nil,
						func(options *temporal.OptionsV1, limits *resource.Limits, peer *rpcServer) {
							options.MaxRequestBytes, options.MaxResponseBytes = 4096, 4096
							limits.Bytes, limits.MaxLeases = 4096+4096+(16<<10), 32
							peer.activityName = "description-identity"
							peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
								return descriptionIdentityResponse(ctx, request, next)
							}
						})
					client, inbox := executionBinding(t, fixture)
					invoke := func(ctx context.Context, native sdk.Client) error {
						decode, err := describeIdentity(ctx, client, native, family)
						if omitted && !errors.Is(err, temporal.ErrLimit) || !omitted && err != nil {
							return fmt.Errorf("description identity control: %w", err)
						}
						return decode()
					}
					if callback {
						workers, tasks := workerInboxes(t)
						lifetime, cancel := context.WithCancel(context.Background())
						defer cancel()
						done := make(chan error, 1)
						body := func(ctx context.Context) error { err := invoke(ctx, activity.GetClient(ctx)); done <- err; return err }
						managed, err := client.StartWorker(context.Background(), lifetime, fault.Correlation{Call: "worker"}, temporal.WorkerSpec{TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
							Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1},
							Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "description-identity"}}}}, workers, tasks)
						if err != nil {
							t.Fatal(err)
						}
						select {
						case err := <-done:
							if err != nil {
								t.Error(err)
							}
						case <-time.After(5 * time.Second):
							t.Error("description callback did not complete")
						}
						if err := managed.Stop(context.Background()); err != nil {
							t.Fatal(err)
						}
					} else if err := invoke(context.Background(), nil); err != nil {
						t.Fatal(err)
					}
					records := 2
					if strings.HasPrefix(family, "query") {
						records = 3
					}
					for range records {
						record := receiveExecution(t, inbox).Outcome.Value
						identity := record.WorkflowID
						if family == "activity" {
							identity = record.ActivityID
						}
						if family == "nexus" || family == "cancellation" {
							identity = record.NexusOperationID
						}
						expectID, expectRun := "effective", "effective-run"
						if omitted {
							expectID, expectRun = "intention", "run"
						}
						if identity != expectID || record.RunID != expectRun || record.IdentityOmitted != omitted {
							t.Error("description/decode attribution changed", record.Operation, identity, record.RunID, record.IdentityOmitted)
						}
					}
				})
			}
		}
	}
}
