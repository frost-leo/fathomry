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
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
)

type errorIdentityTraffic struct{ target string }

func (hook errorIdentityTraffic) CheckCallAllowed(_ context.Context, _ string, request, _ any) error {
	if request, ok := request.(*workflowservice.GetWorkflowExecutionHistoryRequest); ok {
		request.Execution.WorkflowId, request.Execution.RunId = hook.target, "effective-run"
	}
	return nil
}

func TestLazyErrorDetailsPreserveEffectiveAndOmittedIdentity(t *testing.T) {
	for _, callback := range []bool{false, true} {
		for _, omitted := range []bool{false, true} {
			t.Run(fmt.Sprintf("callback=%t/omitted=%t", callback, omitted), func(t *testing.T) {
				target := "effective"
				if omitted {
					target = strings.Repeat("x", 1025)
				}
				wire := &failurepb.Failure{Message: "fixture failure", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "fixture", NonRetryable: true, Details: activityPayloads("\"detail\"")}}}
				fixture := newRuntimeFixture(t, 4, temporal.RuntimeOptions{TrafficController: errorIdentityTraffic{target: target}}, nil,
					func(options *temporal.OptionsV1, limits *resource.Limits, peer *rpcServer) {
						options.MaxRequestBytes, options.MaxResponseBytes = 4096, 4096
						limits.Bytes, limits.MaxLeases = 4096+4096+(16<<10), 32
						peer.activityName = "error-identity"
						peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
							if _, ok := request.(*workflowservice.GetWorkflowExecutionHistoryRequest); ok {
								return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{EventId: 1,
									EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
									Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{Failure: wire}},
								}}}}, nil
							}
							return next(ctx, request)
						}
					})
				client, inbox := executionBinding(t, fixture)
				invoke := func(ctx context.Context, native sdk.Client) error {
					var returned error
					if native != nil {
						returned = native.GetWorkflow(ctx, "intention", "run").Get(ctx, nil)
					} else {
						run, err := client.GetWorkflow("intention", "run")
						if err != nil {
							return err
						}
						returned = run.Get(ctx, fault.Correlation{Call: "failure"}, nil)
					}
					var application *sdktemporal.ApplicationError
					if !errors.As(returned, &application) {
						return fmt.Errorf("native ApplicationError missing: %w", returned)
					}
					var detail string
					if err := application.Details(&detail); err != nil {
						return err
					}
					if detail != "detail" {
						return errors.New("native error detail changed")
					}
					return nil
				}
				if callback {
					workers, tasks := workerInboxes(t)
					lifetime, cancel := context.WithCancel(context.Background())
					defer cancel()
					done := make(chan error, 1)
					body := func(ctx context.Context) error { err := invoke(ctx, activity.GetClient(ctx)); done <- err; return err }
					managed, err := client.StartWorker(context.Background(), lifetime, fault.Correlation{Call: "worker"}, temporal.WorkerSpec{
						TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
						Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1},
						Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "error-identity"}}},
					}, workers, tasks)
					if err != nil {
						t.Fatal(err)
					}
					select {
					case err := <-done:
						if err != nil {
							t.Error(err)
						}
					case <-time.After(5 * time.Second):
						t.Error("error callback did not complete")
					}
					if err := managed.Stop(context.Background()); err != nil {
						t.Fatal(err)
					}
				} else if err := invoke(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					result := receiveExecution(t, inbox).Outcome.Value
					wantID, wantRun := "effective", "effective-run"
					if omitted {
						wantID, wantRun = "intention", "run"
					}
					if result.WorkflowID != wantID || result.RunID != wantRun || result.IdentityOmitted != omitted {
						t.Error("lazy native error lost bounded final origin", result.Operation, result.WorkflowID, result.RunID, result.IdentityOmitted)
					}
				}
			})
		}
	}
}
