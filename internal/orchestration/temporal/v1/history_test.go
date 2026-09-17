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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
)

func TestHistoryWalkContinuesEmptyPagesAndKeepsVisitorOwner(t *testing.T) {
	var pages atomic.Int32
	fixture := newFixture(t, 1, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.history = func(_ context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
			pages.Add(1)
			if request.Namespace != "test" || request.Execution.WorkflowId != "history" || request.WaitNewEvent {
				t.Error("native history target/options changed")
			}
			switch string(request.NextPageToken) {
			case "":
				return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{}, NextPageToken: []byte("empty")}, nil
			case "empty":
				return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{}, NextPageToken: []byte("events")}, nil
			case "events":
				return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{EventId: 7, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}}}, NextPageToken: []byte("final")}, nil
			default:
				return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{}}, nil
			}
		}
	})
	executions, inbox := executionBinding(t, fixture)
	var ids []int64
	err := executions.WalkHistory(context.Background(), fault.Correlation{Call: "walk"}, "history", "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
		func(_ context.Context, event *historypb.HistoryEvent) error {
			ids = append(ids, event.EventId)
			return nil
		})
	if err != nil || pages.Load() != 4 || !reflect.DeepEqual(ids, []int64{7}) {
		t.Fatal("empty history page became false exhaustion", err)
	}
	receiveExecution(t, inbox)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	defer once.Do(func() { close(release) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		done <- executions.WalkHistory(ctx, fault.Correlation{Call: "visitor"}, "history", "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
			func(context.Context, *historypb.HistoryEvent) error { close(entered); <-release; return ctx.Err() })
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("history visitor never entered")
	}
	cancel()
	if err := fixture.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("canceled visitor released dependencies early")
	}
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("visitor cancellation lost", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("visitor did not finish")
	}
	receiveExecution(t, inbox)
}
