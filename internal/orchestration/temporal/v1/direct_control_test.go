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
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestResetRetainsEffectiveIdentityWithoutMutatingRequest(t *testing.T) {
	var observed string
	var calls atomic.Int32
	fixture := newFixture(t, 1, func(options *temporal.OptionsV1, _ *resource.Limits, server *rpcServer) {
		options.RPCs = append(options.RPCs, workflowPrefix+"ResetWorkflowExecution")
		server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			if reset, ok := request.(*workflowservice.ResetWorkflowExecutionRequest); ok {
				calls.Add(1)
				if reset.Namespace != "test" || reset.RequestId == "" || observed != "" && observed != reset.RequestId {
					return nil, status.Error(codes.InvalidArgument, "identity changed")
				}
				observed = reset.RequestId
				return nil, status.Error(codes.Unknown, "response lost after possible reset")
			}
			return next(ctx, request)
		}
	})
	client, inbox := executionBinding(t, fixture)
	request := &workflowservice.ResetWorkflowExecutionRequest{Namespace: "test", WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow", RunId: "run"}, WorkflowTaskFinishEventId: 8}
	before := proto.Clone(request)
	result, err := client.ResetWorkflowExecution(context.Background(), fault.Correlation{Call: "reset"}, request)
	if err == nil || result.RequestID == "" || result.RequestID != observed || result.Response != nil || calls.Load() < 1 || !proto.Equal(request, before) {
		t.Fatal("uncertain reset lost effective identity or mutated input", err)
	}
	delivery, err := inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	record, err := delivery.Receipt().WaitReleased(context.Background())
	if err != nil || record.Outcome.Value.RequestID != result.RequestID || record.Outcome.Value.Accepted {
		t.Fatal("reset evidence does not preserve uncertain identity", err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	request.RequestId = result.RequestID
	retry, err := client.ResetWorkflowExecution(context.Background(), fault.Correlation{Call: "reset-retry"}, request)
	if err == nil || retry.RequestID != result.RequestID || observed != result.RequestID {
		t.Fatal("explicit retry identity changed", err)
	}
}

func TestResetRetainsPostHookIdentityAndRefusesInvalidMutation(t *testing.T) {
	for _, targetID := range []string{"effective-id", "", strings.Repeat("x", 1025)} {
		name := "valid"
		if targetID == "" {
			name = "cleared"
		} else if len(targetID) > 1024 {
			name = "oversized"
		}
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			hook := reentrantServiceTraffic(func(_ context.Context, _ string, request, _ any) error {
				if reset, ok := request.(*workflowservice.ResetWorkflowExecutionRequest); ok {
					reset.RequestId = targetID
				}
				return nil
			})
			fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{TrafficController: hook}, nil, func(options *temporal.OptionsV1, _ *resource.Limits, server *rpcServer) {
				options.RPCs = append(options.RPCs, workflowPrefix+"ResetWorkflowExecution")
				server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
					if reset, ok := request.(*workflowservice.ResetWorkflowExecutionRequest); ok {
						requests.Add(1)
						if reset.RequestId != "effective-id" {
							return nil, status.Error(codes.InvalidArgument, "unexpected effective identity")
						}
						return nil, status.Error(codes.Unknown, "response lost")
					}
					return next(ctx, request)
				}
			})
			client, inbox := executionBinding(t, fixture)
			request := &workflowservice.ResetWorkflowExecutionRequest{Namespace: "test", WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow", RunId: "run"}}
			before := proto.Clone(request)
			result, err := client.ResetWorkflowExecution(context.Background(), fault.Correlation{Call: "reset-hook"}, request)
			if err == nil || !proto.Equal(request, before) {
				t.Fatal("unknown reset mutated caller request or erased error", err)
			}
			record := receiveExecution(t, inbox)
			if targetID == "effective-id" {
				if requests.Load() == 0 || result.RequestID != targetID || record.Outcome.Value.RequestID != targetID || record.Outcome.Value.Accepted {
					t.Fatal("post-hook unknown outcome identity was not retained")
				}
			} else if requests.Load() != 0 || result.RequestID != "" || record.Outcome.Value.RequestID != "" {
				t.Fatal("invalid effective identity transmitted or mislabeled")
			}
		})
	}
}

func TestCallbackResetPostHookIdentityRefusalsPreserveCallerInput(t *testing.T) {
	for _, targetID := range []string{"effective-id", "", strings.Repeat("x", 1025)} {
		name := "valid"
		if targetID == "" {
			name = "cleared"
		} else if len(targetID) > 1024 {
			name = "oversized"
		}
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			hook := reentrantServiceTraffic(func(_ context.Context, _ string, request, _ any) error {
				if reset, ok := request.(*workflowservice.ResetWorkflowExecutionRequest); ok {
					reset.RequestId = targetID
				}
				return nil
			})
			fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{TrafficController: hook}, nil, func(options *temporal.OptionsV1, _ *resource.Limits, server *rpcServer) {
				options.RPCs = append(options.RPCs, workflowPrefix+"ResetWorkflowExecution")
				server.activityName = "callback-reset"
				server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
					if reset, ok := request.(*workflowservice.ResetWorkflowExecutionRequest); ok {
						requests.Add(1)
						if reset.RequestId != "effective-id" {
							return nil, errors.New("unexpected reset identity")
						}
						return nil, status.Error(codes.Unknown, "response lost")
					}
					return next(ctx, request)
				}
			})
			client, inbox := executionBinding(t, fixture)
			workers, tasks := workerInboxes(t)
			done := make(chan error, 1)
			body := func(ctx context.Context) error {
				client := activity.GetClient(ctx)
				request := &workflowservice.ResetWorkflowExecutionRequest{Namespace: "test", WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow", RunId: "run"}}
				before := proto.Clone(request)
				_, err := client.ResetWorkflowExecution(ctx, request)
				if err == nil || !proto.Equal(request, before) {
					done <- errors.New("callback reset lost error or mutated caller input")
					return nil
				}
				if targetID != "effective-id" && requests.Load() != 0 {
					done <- errors.New("invalid callback reset transmitted")
					return nil
				}
				done <- nil
				return nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			lifetime, stop := context.WithCancel(context.Background())
			defer stop()
			managed, err := client.StartWorker(ctx, lifetime, fault.Correlation{Call: "reset-worker"}, temporal.WorkerSpec{
				TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
				Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1},
				Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "callback-reset"}}},
			}, workers, tasks)
			if managed != nil {
				t.Cleanup(func() { _ = managed.Stop(context.Background()) })
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("callback reset did not finish")
			}
			if err := managed.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			record := receiveExecution(t, inbox)
			wantID := targetID
			if len(wantID) > 1024 {
				wantID = ""
			}
			if record.Outcome.Value.RequestID != wantID || record.Outcome.Value.Accepted {
				t.Fatal("callback reset effective identity evidence differs")
			}
			releaseServiceEvidence(t, workers)
			releaseServiceEvidence(t, tasks)
			if targetID == "effective-id" && requests.Load() == 0 {
				t.Fatal("valid callback reset did not transmit")
			}
		})
	}
}

func TestDirectWorkflowControlNativeValidationAndConversion(t *testing.T) {
	var updates, scans atomic.Int32
	fixture := newFixture(t, 1, func(options *temporal.OptionsV1, _ *resource.Limits, server *rpcServer) {
		options.RPCs = append(options.RPCs, workflowPrefix+"UpdateWorkflowExecutionOptions")
		server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			switch request := request.(type) {
			case *workflowservice.UpdateWorkflowExecutionOptionsRequest:
				updates.Add(1)
				if request.Namespace != "test" || request.WorkflowExecution.WorkflowId != "workflow" ||
					!reflect.DeepEqual(request.UpdateMask.Paths, []string{"versioning_override"}) {
					return nil, status.Error(codes.InvalidArgument, "native conversion")
				}
				return &workflowservice.UpdateWorkflowExecutionOptionsResponse{WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{VersioningOverride: &workflowpb.VersioningOverride{Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE}}}, nil
			case *workflowservice.ScanWorkflowExecutionsRequest:
				scans.Add(1)
				if request.Namespace != "test" || request.Query != "fixture" {
					return nil, status.Error(codes.InvalidArgument, "scan")
				}
				return &workflowservice.ScanWorkflowExecutionsResponse{NextPageToken: []byte("next")}, nil
			}
			return next(ctx, request)
		}
	})
	client, _ := executionBinding(t, fixture)
	result, err := client.UpdateWorkflowExecutionOptions(context.Background(), fault.Correlation{Call: "options"}, sdk.UpdateWorkflowExecutionOptionsRequest{
		WorkflowId: "workflow", WorkflowExecutionOptionsChanges: sdk.WorkflowExecutionOptionsChanges{VersioningOverride: &sdk.VersioningOverrideChange{Value: &sdk.AutoUpgradeVersioningOverride{}}}})
	if _, ok := result.VersioningOverride.(*sdk.AutoUpgradeVersioningOverride); !ok || err != nil {
		t.Fatal("native versioning option response conversion lost", err)
	}
	if _, err := client.UpdateWorkflowExecutionOptions(context.Background(), fault.Correlation{Call: "invalid-options"}, sdk.UpdateWorkflowExecutionOptionsRequest{}); err == nil || updates.Load() != 1 {
		t.Fatal("native validation omitted", err)
	}
	request := &workflowservice.ScanWorkflowExecutionsRequest{Query: "fixture"}
	page, err := client.ScanWorkflow(context.Background(), fault.Correlation{Call: "scan"}, request)
	if err != nil || string(page.NextPageToken) != "next" || request.Namespace != "" || scans.Load() != 1 {
		t.Fatal("deprecated native scan defaulting/result changed", err)
	}
	request.Namespace = "other"
	if _, err := client.ScanWorkflow(context.Background(), fault.Correlation{Call: "scan-other"}, request); !errors.Is(err, temporal.ErrAuthority) || scans.Load() != 1 {
		t.Fatal("scan namespace boundary omitted", err)
	}
}

func TestDirectWorkerVersioningNativeConversionsAndValidation(t *testing.T) {
	var nativeCalls atomic.Int32
	fixture := newFixture(t, 1, func(options *temporal.OptionsV1, _ *resource.Limits, server *rpcServer) {
		for _, method := range []string{"GetWorkerBuildIdCompatibility", "GetWorkerTaskReachability", "GetWorkerVersioningRules", "UpdateWorkerBuildIdCompatibility", "UpdateWorkerVersioningRules"} {
			options.RPCs = append(options.RPCs, workflowPrefix+method)
		}
		server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			switch request := request.(type) {
			case *workflowservice.GetWorkerBuildIdCompatibilityRequest:
				nativeCalls.Add(1)
				if request.Namespace != "test" || request.MaxSets != 2 {
					return nil, status.Error(codes.InvalidArgument, "build request")
				}
				return &workflowservice.GetWorkerBuildIdCompatibilityResponse{MajorVersionSets: []*taskqueuepb.CompatibleVersionSet{{BuildIds: []string{"old", "default"}}}}, nil
			case *workflowservice.GetWorkerTaskReachabilityRequest:
				nativeCalls.Add(1)
				return &workflowservice.GetWorkerTaskReachabilityResponse{BuildIdReachability: []*taskqueuepb.BuildIdReachability{{BuildId: "build", TaskQueueReachability: []*taskqueuepb.TaskQueueReachability{
					{TaskQueue: "reachable", Reachability: []enumspb.TaskReachability{enumspb.TASK_REACHABILITY_OPEN_WORKFLOWS}},
					{TaskQueue: "not-retrieved", Reachability: []enumspb.TaskReachability{enumspb.TASK_REACHABILITY_UNSPECIFIED}},
				}}}}, nil
			case *workflowservice.GetWorkerVersioningRulesRequest:
				nativeCalls.Add(1)
				return &workflowservice.GetWorkerVersioningRulesResponse{ConflictToken: []byte("conflict"), AssignmentRules: []*taskqueuepb.TimestampedBuildIdAssignmentRule{{Rule: &taskqueuepb.BuildIdAssignmentRule{TargetBuildId: "build"}}}}, nil
			case *workflowservice.UpdateWorkerBuildIdCompatibilityRequest:
				nativeCalls.Add(1)
				if request.GetAddNewBuildIdInNewDefaultSet() != "build" {
					return nil, status.Error(codes.InvalidArgument, "native build conversion")
				}
				return &workflowservice.UpdateWorkerBuildIdCompatibilityResponse{}, nil
			case *workflowservice.UpdateWorkerVersioningRulesRequest:
				nativeCalls.Add(1)
				if string(request.ConflictToken) != "conflict" || request.GetInsertAssignmentRule().GetRule().GetTargetBuildId() != "build" {
					return nil, status.Error(codes.InvalidArgument, "native rule conversion")
				}
				return &workflowservice.UpdateWorkerVersioningRulesResponse{ConflictToken: []byte("next")}, nil
			}
			return next(ctx, request)
		}
	})
	client, _ := executionBinding(t, fixture)
	ctx := context.Background()
	sets, err := client.GetWorkerBuildIdCompatibility(ctx, fault.Correlation{Call: "get-build"}, &sdk.GetWorkerBuildIdCompatibilityOptions{TaskQueue: "queue", MaxSets: 2})
	if err != nil || sets.Default() != "default" {
		t.Fatal("version-set conversion", err)
	}
	reachability, err := client.GetWorkerTaskReachability(ctx, fault.Correlation{Call: "reachability"}, &sdk.GetWorkerTaskReachabilityOptions{BuildIDs: []string{"build"}})
	if err != nil || !reflect.DeepEqual(reachability.BuildIDReachability["build"].UnretrievedTaskQueues, []string{"not-retrieved"}) ||
		reachability.BuildIDReachability["build"].TaskQueueReachable["reachable"].TaskQueueReachability[0] != sdk.TaskReachabilityOpenWorkflows {
		t.Fatal("reachability conversion", err)
	}
	rules, err := client.GetWorkerVersioningRules(ctx, fault.Correlation{Call: "get-rules"}, sdk.GetWorkerVersioningOptions{TaskQueue: "queue"})
	if err != nil || rules.AssignmentRules[0].Rule.TargetBuildID != "build" {
		t.Fatal("rule conversion", err)
	}
	if err := client.UpdateWorkerBuildIdCompatibility(ctx, fault.Correlation{Call: "set-build"}, &sdk.UpdateWorkerBuildIdCompatibilityOptions{TaskQueue: "queue", Operation: &sdk.BuildIDOpAddNewIDInNewDefaultSet{BuildID: "build"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.UpdateWorkerVersioningRules(ctx, fault.Correlation{Call: "set-rules"}, sdk.UpdateWorkerVersioningRulesOptions{TaskQueue: "queue", ConflictToken: rules.ConflictToken,
		Operation: &sdk.VersioningOperationInsertAssignmentRule{Rule: sdk.VersioningAssignmentRule{TargetBuildID: "build"}}}); err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{
		func() error {
			_, err := client.GetWorkerBuildIdCompatibility(ctx, fault.Correlation{Call: "bad-build"}, &sdk.GetWorkerBuildIdCompatibilityOptions{MaxSets: -1})
			return err
		}(),
		func() error {
			_, err := client.GetWorkerVersioningRules(ctx, fault.Correlation{Call: "bad-rules"}, sdk.GetWorkerVersioningOptions{})
			return err
		}(),
		client.UpdateWorkerBuildIdCompatibility(ctx, fault.Correlation{Call: "bad-update-build"}, &sdk.UpdateWorkerBuildIdCompatibilityOptions{}),
		func() error {
			_, err := client.UpdateWorkerVersioningRules(ctx, fault.Correlation{Call: "bad-update-rules"}, sdk.UpdateWorkerVersioningRulesOptions{})
			return err
		}(),
	} {
		if err == nil {
			t.Fatal("native invalid-option control accepted")
		}
	}
	if nativeCalls.Load() != 5 {
		t.Fatalf("invalid options transmitted: %d", nativeCalls.Load())
	}
}

func TestDeprecatedDeploymentCompleteFamilyAndEmptyPages(t *testing.T) {
	deployment := &deploymentpb.Deployment{SeriesName: "series", BuildId: "build"}
	info := &deploymentpb.DeploymentInfo{Deployment: deployment, IsCurrent: true}
	var pages, calls atomic.Int32
	fixture := newFixture(t, 1, func(options *temporal.OptionsV1, _ *resource.Limits, server *rpcServer) {
		for _, method := range []string{"DescribeDeployment", "GetDeploymentReachability", "GetCurrentDeployment", "SetCurrentDeployment", "ListDeployments"} {
			options.RPCs = append(options.RPCs, workflowPrefix+method)
		}
		server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			switch request := request.(type) {
			case *workflowservice.DescribeDeploymentRequest:
				calls.Add(1)
				return &workflowservice.DescribeDeploymentResponse{DeploymentInfo: info}, nil
			case *workflowservice.GetDeploymentReachabilityRequest:
				calls.Add(1)
				return &workflowservice.GetDeploymentReachabilityResponse{DeploymentInfo: info, Reachability: enumspb.DEPLOYMENT_REACHABILITY_REACHABLE}, nil
			case *workflowservice.GetCurrentDeploymentRequest:
				calls.Add(1)
				return &workflowservice.GetCurrentDeploymentResponse{CurrentDeploymentInfo: info}, nil
			case *workflowservice.SetCurrentDeploymentRequest:
				calls.Add(1)
				if request.Identity != "fathomry" || request.UpdateMetadata.UpsertEntries["key"] == nil {
					return nil, status.Error(codes.InvalidArgument, "metadata")
				}
				return &workflowservice.SetCurrentDeploymentResponse{CurrentDeploymentInfo: info}, nil
			case *workflowservice.ListDeploymentsRequest:
				pages.Add(1)
				if len(request.NextPageToken) == 0 {
					return &workflowservice.ListDeploymentsResponse{NextPageToken: []byte("next")}, nil
				}
				return &workflowservice.ListDeploymentsResponse{Deployments: []*deploymentpb.DeploymentListInfo{{Deployment: deployment, IsCurrent: true}}}, nil
			}
			return next(ctx, request)
		}
	})
	executions, _ := executionBinding(t, fixture)
	client := executions.DeploymentClient()
	ctx := context.Background()
	id := sdk.Deployment{SeriesName: "series", BuildID: "build"}
	described, err := client.Describe(ctx, fault.Correlation{Call: "describe"}, sdk.DeploymentDescribeOptions{Deployment: id})
	if err != nil || described.DeploymentInfo.Deployment != id {
		t.Fatal("deployment describe conversion", err)
	}
	reachable, err := client.GetReachability(ctx, fault.Correlation{Call: "reachable"}, sdk.DeploymentGetReachabilityOptions{Deployment: id})
	if err != nil || reachable.DeploymentInfo.Deployment != id {
		t.Fatal("deployment reachability", err)
	}
	current, err := client.GetCurrent(ctx, fault.Correlation{Call: "current"}, sdk.DeploymentGetCurrentOptions{SeriesName: "series"})
	if err != nil || current.DeploymentInfo.Deployment != id {
		t.Fatal("deployment current", err)
	}
	updated, err := client.SetCurrent(ctx, fault.Correlation{Call: "set"}, sdk.DeploymentSetCurrentOptions{Deployment: id, MetadataUpdate: sdk.DeploymentMetadataUpdate{UpsertEntries: map[string]any{"key": "value"}}})
	if err != nil || updated.Current.Deployment != id {
		t.Fatal("deployment set", err)
	}
	visited := 0
	err = client.Walk(ctx, fault.Correlation{Call: "walk"}, sdk.DeploymentListOptions{PageSize: 1, SeriesName: "series"}, func(_ context.Context, entry *sdk.DeploymentListEntry) error {
		visited++
		if entry.Deployment != id {
			t.Fatal("deployment list conversion")
		}
		return nil
	})
	if err != nil || visited != 1 || pages.Load() != 2 {
		t.Fatal("empty continuation page lost", err)
	}
	if _, err := client.Describe(ctx, fault.Correlation{Call: "bad-describe"}, sdk.DeploymentDescribeOptions{}); err == nil || calls.Load() != 4 {
		t.Fatal("native legacy deployment validation bypassed", err)
	}
}

func TestDirectOperationalControlsRequireExactGrants(t *testing.T) {
	var transmitted atomic.Int32
	fixture := newFixture(t, 1, func(options *temporal.OptionsV1, _ *resource.Limits, server *rpcServer) {
		options.RPCs = nil
		server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			if _, discovery := request.(*workflowservice.GetSystemInfoRequest); !discovery {
				transmitted.Add(1)
			}
			return next(ctx, request)
		}
	})
	client, _ := executionBinding(t, fixture)
	ctx := context.Background()
	correlation := fault.Correlation{Call: "refused-control"}
	legacy := client.DeploymentClient()
	for _, invoke := range []func() error{
		func() error {
			_, err := client.ResetWorkflowExecution(ctx, correlation, &workflowservice.ResetWorkflowExecutionRequest{Namespace: "test"})
			return err
		},
		func() error {
			_, err := client.UpdateWorkflowExecutionOptions(ctx, correlation, sdk.UpdateWorkflowExecutionOptionsRequest{})
			return err
		},
		func() error {
			_, err := client.GetWorkerBuildIdCompatibility(ctx, correlation, &sdk.GetWorkerBuildIdCompatibilityOptions{})
			return err
		},
		func() error {
			_, err := client.GetWorkerTaskReachability(ctx, correlation, &sdk.GetWorkerTaskReachabilityOptions{})
			return err
		},
		func() error {
			_, err := client.GetWorkerVersioningRules(ctx, correlation, sdk.GetWorkerVersioningOptions{})
			return err
		},
		func() error {
			return client.UpdateWorkerBuildIdCompatibility(ctx, correlation, &sdk.UpdateWorkerBuildIdCompatibilityOptions{})
		},
		func() error {
			_, err := client.UpdateWorkerVersioningRules(ctx, correlation, sdk.UpdateWorkerVersioningRulesOptions{})
			return err
		},
		func() error { _, err := legacy.Describe(ctx, correlation, sdk.DeploymentDescribeOptions{}); return err },
		func() error {
			_, err := legacy.GetReachability(ctx, correlation, sdk.DeploymentGetReachabilityOptions{})
			return err
		},
		func() error {
			_, err := legacy.GetCurrent(ctx, correlation, sdk.DeploymentGetCurrentOptions{})
			return err
		},
		func() error {
			_, err := legacy.SetCurrent(ctx, correlation, sdk.DeploymentSetCurrentOptions{})
			return err
		},
		func() error {
			return legacy.Walk(ctx, correlation, sdk.DeploymentListOptions{}, func(context.Context, *sdk.DeploymentListEntry) error { return nil })
		},
	} {
		if err := invoke(); !errors.Is(err, temporal.ErrAuthority) || transmitted.Load() != 0 {
			t.Fatal("operational control bypassed exact grant", err)
		}
	}
}
