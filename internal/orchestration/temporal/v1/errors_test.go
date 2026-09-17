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
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
)

func TestAuditRetainedApplicationFailureDecoderAfterRelease(t *testing.T) {
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.history = func(context.Context, *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
			return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{
				EventId: 5, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
					Failure: &failurepb.Failure{Message: "fixture failure", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "fixture", Details: activityPayloads("\"detail\"")}}},
				}},
			}}}}, nil
		}
	})
	executions, inbox := executionBinding(t, fixture)
	run, err := executions.GetWorkflow("workflow", "run")
	if err != nil {
		t.Fatal(err)
	}
	err = run.Get(context.Background(), fault.Correlation{Call: "failure"}, nil)
	var application *sdktemporal.ApplicationError
	if !errors.As(err, &application) {
		t.Fatal("native typed failure missing", err)
	}
	var calls atomic.Int32
	if err := application.Details(&decodeCounter{calls: &calls}); err != nil || calls.Load() != 1 {
		t.Fatal("normal error detail control failed", err)
	}
	releaseServiceEvidence(t, inbox)
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	err = application.Details(&decodeCounter{calls: &calls})
	if err == nil || calls.Load() != 1 {
		t.Fatalf("retained error decoded after source release: err=%v calls=%d", err, calls.Load())
	}
}

func TestReviewNestedDescriptionFailureRetainsScope(t *testing.T) {
	extension := &reviewNestedDescriptionFailure{}
	reviewReleasedCallbackFixture(t, "DescribeActivityExecution", temporal.RuntimeOptions{Interceptors: []interceptor.ClientInterceptor{extension}},
		func(ctx context.Context, client sdk.Client) error {
			_, err := client.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "activity", RunID: "run"}).Describe(ctx, sdk.DescribeActivityOptions{IncludeLastFailure: true})
			return err
		}, func(ctx context.Context, request any, next grpc.UnaryHandler) (any, error) {
			if _, ok := request.(*workflowservice.DescribeActivityExecutionRequest); ok {
				return &workflowservice.DescribeActivityExecutionResponse{Info: &activitypb.ActivityExecutionInfo{
					ActivityId: "activity", RunId: "run", ActivityType: &commonpb.ActivityType{Name: "definition"},
					SearchAttributes: &commonpb.SearchAttributes{}, Status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
					LastFailure: &failurepb.Failure{Message: "synthetic failure", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "review", Details: activityPayloads("\"detail\"")},
					}},
				}}, nil
			}
			return next(ctx, request)
		})
	if extension.retained == nil || extension.decodes.Load() != 1 {
		t.Fatal("native live error decoder control failed")
	}
	err := extension.retained.Details(&decodeCounter{calls: &extension.decodes})
	if !errors.Is(err, temporal.ErrAuthority) || extension.decodes.Load() != 1 {
		t.Fatalf("retained native description error escaped after release: refusal=%v decodes=%d", errors.Is(err, temporal.ErrAuthority), extension.decodes.Load())
	}
}
