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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func executionBinding(t *testing.T, fixture *fixture) (*temporal.Executions, *invocation.Inbox[temporal.Execution]) {
	t.Helper()
	inbox, err := invocation.NewInbox[temporal.Execution](16, 16*temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	client, err := temporal.BindExecutions(fixture.assembly, fixture.selection, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	return client, inbox
}

func workerInboxes(t *testing.T) (*invocation.Inbox[temporal.WorkerResult], *invocation.Inbox[temporal.TaskResult]) {
	t.Helper()
	workers, err := invocation.NewInbox[temporal.WorkerResult](1, temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := invocation.NewInbox[temporal.TaskResult](4, 4*temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	return workers, tasks
}

func TestManagedWorkerFatalCallbackOwnership(t *testing.T) {
	if testing.Short() {
		t.Skip("native fatal polling retains its two-minute grace period")
	}
	for _, mode := range []string{"normal", "blocked", "panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			var polls, callbacks atomic.Int32
			fixture := newFixture(t, 2, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
				peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
					if _, ok := request.(*workflowservice.PollActivityTaskQueueRequest); ok {
						polls.Add(1)
						return nil, status.Error(codes.InvalidArgument, "controlled native fatal poll")
					}
					return next(ctx, request)
				}
			})
			executions, _ := executionBinding(t, fixture)
			workers, tasks := workerInboxes(t)
			entered, released, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			marker := errors.New("controlled fatal callback panic")
			callback := func(cause error) {
				callbacks.Add(1)
				defer close(returned)
				var invalid *serviceerror.InvalidArgument
				if !errors.As(cause, &invalid) {
					panic("fatal poll identity changed")
				}
				close(entered)
				switch mode {
				case "blocked":
					<-released
				case "panic":
					panic(marker)
				case "goexit":
					runtime.Goexit()
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			lifetime, stopLifetime := context.WithCancel(context.Background())
			defer stopLifetime()
			managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "fatal-review"}, temporal.WorkerSpec{
				TaskQueue: "fatal-review", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
				Options: worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1,
					MaxConcurrentActivityTaskPollers: 1, WorkerStopTimeout: time.Millisecond, OnFatalError: callback},
				Activities: []temporal.ActivityRegistration{{Definition: func() error { return nil }, Options: activity.RegisterOptions{Name: "unused"}}},
			}, workers, tasks)
			if managed != nil {
				t.Cleanup(func() {
					releaseOnce.Do(func() { close(released) })
					cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
					defer stop()
					_ = managed.Stop(cleanup)
					if !managed.Status().Joined {
						t.Error("fatal test Worker cleanup did not join")
					}
				})
			}
			if err != nil || managed == nil {
				t.Fatalf("startup failed before fatal poll: %v", err)
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatalf("native fatal poll did not invoke configured callback: polls=%d status=%+v", polls.Load(), managed.Status())
			}
			if mode == "blocked" {
				short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
				err := managed.Stop(short)
				stop()
				if !errors.Is(err, context.DeadlineExceeded) || managed.Status().Joined {
					t.Fatalf("Stop claimed join while fatal callback remained active: %v", err)
				}
				if err := fixture.assembly.Close(ctx); !errors.Is(err, resource.ErrIncomplete) {
					t.Fatalf("source released active fatal callback dependency: %v", err)
				}
			}
			releaseOnce.Do(func() { close(released) })
			err = managed.Stop(ctx)
			var invalid *serviceerror.InvalidArgument
			if !errors.As(err, &invalid) || invalid.Message != "controlled native fatal poll" {
				t.Fatalf("fatal poll cause lost from final outcome: %v", err)
			}
			if mode == "panic" && !errors.Is(err, marker) {
				t.Fatal("fatal callback panic lost from cleanup evidence")
			}
			if mode == "goexit" && !errors.Is(err, temporal.ErrWorker) {
				t.Fatal("fatal callback Goexit lost from cleanup evidence")
			}
			select {
			case <-returned:
			default:
				t.Fatal("final Stop preceded fatal callback exit")
			}
			if callbacks.Load() != 1 || polls.Load() < 1 || !managed.Status().Started || !managed.Status().NativeStopReturned || !managed.Status().Joined {
				t.Fatalf("unexpected native fatal lifecycle: callbacks=%d polls=%d status=%+v", callbacks.Load(), polls.Load(), managed.Status())
			}
			delivery, err := workers.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := delivery.Receipt().WaitReleased(ctx)
			if err != nil || !evidence.Outcome.Value.Joined || !errors.As(evidence.Err(), &invalid) {
				t.Fatalf("independent fatal outcome missing: %v", err)
			}
			if mode == "panic" && !errors.Is(evidence.Err(), marker) {
				t.Fatal("independent callback panic missing")
			}
			if err := delivery.Release(); err != nil {
				t.Fatal(err)
			}
			if err := fixture.assembly.Close(ctx); err != nil {
				t.Fatalf("final source cleanup: %v", err)
			}
		})
	}
}

func TestManagedWorkerRetainsDependenciesAndGuardsGetClient(t *testing.T) {
	fixture := newFixture(t, 4, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) { peer.activityName = "controlled" })
	executions, _ := executionBinding(t, fixture)
	workers, tasks := workerInboxes(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	var borrowed sdk.Client
	var borrowedContext context.Context
	body := func(ctx context.Context) error {
		borrowed = activity.GetClient(ctx)
		borrowedContext = context.WithoutCancel(ctx)
		var refusal any
		func() { defer func() { refusal = recover() }(); borrowed.Close() }()
		closeError, ok := refusal.(error)
		if !ok || !errors.Is(closeError, temporal.ErrAuthority) {
			return errors.New("borrowed client acquired close authority")
		}
		_, err := borrowed.WorkflowService().QueryWorkflow(ctx, &workflowservice.QueryWorkflowRequest{Namespace: "another"})
		if !errors.Is(err, temporal.ErrAuthority) {
			return errors.New("raw context client escaped namespace authority")
		}
		first, err := borrowed.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
		if err != nil || first.GetCount() != 7 {
			return errors.New("controlled context client was not usable")
		}
		close(entered)
		<-release
		last, err := borrowed.CountWorkflow(borrowedContext, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
		if err != nil || last.GetCount() != 7 {
			return errors.New("dependency was released before callback exit")
		}
		return nil
	}
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	defer cancelLifetime()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "worker"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1, WorkerStopTimeout: time.Millisecond},
		Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "controlled"}}},
	}, workers, tasks)
	if managed != nil {
		t.Cleanup(func() {
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
	case <-entered:
	case <-ctx.Done():
		t.Fatal("controlled activity did not enter")
	}
	short, stopWaiting := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = managed.Stop(short)
	stopWaiting()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Stop claimed quiescence while callback was blocked")
	}
	if err := fixture.assembly.Close(ctx); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("assembly released a live worker dependency")
	}
	if managed.Status().Joined {
		t.Fatal("snapshot invented a completed join")
	}
	releaseOnce.Do(func() { close(release) })
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	delivery, err := tasks.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := delivery.Receipt().WaitReleased(ctx)
	if err != nil || result.Err() != nil || !result.Outcome.Value.HandlerReturned {
		t.Fatal("task facts lost after callback cleanup")
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	_, err = borrowed.CountWorkflow(borrowedContext, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
	if !errors.Is(err, temporal.ErrAuthority) {
		t.Fatal("retained callback client outlived its scope")
	}
	if fixture.server.calls.Load() != 2 {
		t.Fatal("forbidden or premature client path reached the peer")
	}
	if err := fixture.assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestManagedRegistrationFailureKeepsCleanupOwnership(t *testing.T) {
	fixture := newFixture(t, 1)
	executions, _ := executionBinding(t, fixture)
	workers, tasks := workerInboxes(t)
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	defer cancelLifetime()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	definition := func(workflow.Context) error { return nil }
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "invalid-worker"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options: worker.Options{LocalActivityWorkerOnly: true},
		Workflows: []temporal.WorkflowRegistration{
			{Definition: definition, Options: workflow.RegisterOptions{Name: "duplicate"}},
			{Definition: definition, Options: workflow.RegisterOptions{Name: "duplicate"}},
		},
	}, workers, tasks)
	if managed == nil || err == nil {
		t.Fatal("registration failure lost acquired owner")
	}
	if err := managed.Stop(ctx); !errors.Is(err, temporal.ErrWorker) {
		t.Fatal("primary registration failure was discarded")
	}
	delivery, err := workers.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := delivery.Receipt().WaitReleased(ctx)
	if err != nil || !result.Released || result.Outcome.Value.Started || !result.Outcome.Value.Joined {
		t.Fatal("partial startup release facts are incorrect")
	}
	if !errors.Is(result.Err(), temporal.ErrWorker) {
		t.Fatal("independent worker evidence lost the primary failure")
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAuditRetainedWorkerContinuationAfterJoin(t *testing.T) {
	extension := &auditRetainedWorkerInterceptor{}
	fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{Interceptors: []interceptor.ClientInterceptor{extension}}, nil, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
		peer.activityName = "retained-worker"
	})
	executions, inbox := executionBinding(t, fixture)
	workers, tasks := workerInboxes(t)
	done := make(chan struct{}, 1)
	var bodies atomic.Int32
	body := func(context.Context) error { bodies.Add(1); done <- struct{}{}; return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "retained-worker"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1},
		Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "retained-worker"}}},
	}, workers, tasks)
	if managed != nil {
		t.Cleanup(func() {
			if !managed.Status().Joined {
				if err := managed.Stop(ctx); err != nil {
					t.Error(err)
				}
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("activity did not run")
	}
	if err := managed.Stop(ctx); err != nil || bodies.Load() != 1 {
		t.Fatal("normal activity did not join", err)
	}
	releaseServiceEvidence(t, inbox)
	releaseServiceEvidence(t, workers)
	releaseServiceEvidence(t, tasks)
	if err := fixture.assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = extension.next.ExecuteActivity(extension.ctx, extension.input)
	if !errors.Is(err, temporal.ErrAuthority) || bodies.Load() != 1 {
		t.Fatalf("retained native Activity executor ran after join+release: refused=%v bodies=%d", errors.Is(err, temporal.ErrAuthority), bodies.Load())
	}
}

func TestManagedWorkerEnablesNativeEagerStartAndRetiresIt(t *testing.T) {
	requests := make(chan bool, 3)
	fixture := newFixture(t, 1, func(options *temporal.OptionsV1, limits *resource.Limits, peer *rpcServer) {
		options.MaxActive = 2
		limits.Active = 2
		limits.Bytes *= 2
		peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			switch request.(type) {
			case *workflowservice.GetSystemInfoRequest:
				return &workflowservice.GetSystemInfoResponse{ServerVersion: "1.32.0", Capabilities: &workflowservice.GetSystemInfoResponse_Capabilities{EagerWorkflowStart: true}}, nil
			case *workflowservice.PollWorkflowTaskQueueRequest:
				<-ctx.Done()
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			return next(ctx, request)
		}
		peer.start = func(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
			requests <- request.RequestEagerExecution
			return &workflowservice.StartWorkflowExecutionResponse{RunId: "run"}, nil
		}
	})
	executions, inbox := executionBinding(t, fixture)
	workers, tasks := workerInboxes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := func(want bool) {
		t.Helper()
		_, err := executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "eager"}, sdk.StartWorkflowOptions{ID: "workflow", TaskQueue: "unit", EnableEagerStart: true}, "definition")
		if err != nil {
			t.Fatal(err)
		}
		if actual := <-requests; actual != want {
			t.Fatalf("eager request bit=%v want=%v", actual, want)
		}
		receiveExecution(t, inbox)
	}
	start(false)
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "eager-worker"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options:   worker.Options{LocalActivityWorkerOnly: true, MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2},
		Workflows: []temporal.WorkflowRegistration{{Definition: func(workflow.Context) error { return nil }, Options: workflow.RegisterOptions{Name: "definition"}}},
	}, workers, tasks)
	if managed != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			if err := managed.Stop(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	start(true)
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	start(false)
}
