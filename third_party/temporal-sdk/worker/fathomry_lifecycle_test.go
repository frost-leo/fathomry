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

package worker_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type service struct {
	workflowservice.UnimplementedWorkflowServiceServer
	runID                 string
	delivered             atomic.Bool
	workflowDelivered     atomic.Bool
	workflowCompleted     chan struct{}
	workflowCompletedOnce sync.Once
	taskCompleted         chan struct{}
	taskCompletedOnce     sync.Once
}

func (s *service) PollWorkflowTaskQueue(ctx context.Context, request *workflowservice.PollWorkflowTaskQueueRequest) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	if request.GetTaskQueue().GetKind() != enumspb.TASK_QUEUE_KIND_STICKY && s.workflowDelivered.CompareAndSwap(false, true) {
		taskQueue := &taskqueuepb.TaskQueue{Name: "probe"}
		return &workflowservice.PollWorkflowTaskQueueResponse{
			TaskToken: []byte("workflow-token"), WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow", RunId: s.runID},
			WorkflowType: &commonpb.WorkflowType{Name: "local-probe"}, StartedEventId: 3, Attempt: 1,
			WorkflowExecutionTaskQueue: taskQueue,
			History: &historypb.History{Events: []*historypb.HistoryEvent{
				{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, EventTime: timestamppb.Now(), Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType: &commonpb.WorkflowType{Name: "local-probe"}, TaskQueue: taskQueue, WorkflowTaskTimeout: durationpb.New(10 * time.Second), OriginalExecutionRunId: s.runID, FirstExecutionRunId: s.runID,
				}}},
				{EventId: 2, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, EventTime: timestamppb.Now(), Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{TaskQueue: taskQueue, StartToCloseTimeout: durationpb.New(10 * time.Second), Attempt: 1}}},
				{EventId: 3, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, EventTime: timestamppb.Now(), Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{ScheduledEventId: 2, Identity: "probe"}}},
			}},
		}, nil
	}
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func (s *service) RespondWorkflowTaskCompleted(_ context.Context, request *workflowservice.RespondWorkflowTaskCompletedRequest) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	s.taskCompletedOnce.Do(func() { close(s.taskCompleted) })
	for _, command := range request.Commands {
		if command.GetCommandType() == enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION {
			s.workflowCompletedOnce.Do(func() { close(s.workflowCompleted) })
		}
	}
	return &workflowservice.RespondWorkflowTaskCompletedResponse{}, nil
}

type observedConverter struct {
	converter.DataConverter
	converted chan struct{}
	once      sync.Once
}

func (c *observedConverter) ToPayloads(values ...any) (*commonpb.Payloads, error) {
	result, err := c.DataConverter.ToPayloads(values...)
	if len(values) == 1 && values[0] == "late-local-result" {
		c.once.Do(func() { close(c.converted) })
	}
	return result, err
}

func (*service) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (*service) DescribeNamespace(context.Context, *workflowservice.DescribeNamespaceRequest) (*workflowservice.DescribeNamespaceResponse, error) {
	return &workflowservice.DescribeNamespaceResponse{NamespaceInfo: &namespacepb.NamespaceInfo{Name: "probe"}}, nil
}

func (s *service) PollActivityTaskQueue(ctx context.Context, _ *workflowservice.PollActivityTaskQueueRequest) (*workflowservice.PollActivityTaskQueueResponse, error) {
	if s.delivered.CompareAndSwap(false, true) {
		return &workflowservice.PollActivityTaskQueueResponse{
			TaskToken: []byte("synthetic-token"), ActivityId: "probe", ActivityType: &commonpb.ActivityType{Name: "probe"},
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "probe", RunId: "probe-run"},
			ScheduledTime:     timestamppb.Now(), StartedTime: timestamppb.Now(),
			StartToCloseTimeout: durationpb.New(time.Minute), ScheduleToCloseTimeout: durationpb.New(time.Minute), Attempt: 1,
		}, nil
	}
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func (*service) RespondActivityTaskCompleted(context.Context, *workflowservice.RespondActivityTaskCompletedRequest) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	return &workflowservice.RespondActivityTaskCompletedResponse{}, nil
}

func (*service) ShutdownWorker(context.Context, *workflowservice.ShutdownWorkerRequest) (*workflowservice.ShutdownWorkerResponse, error) {
	return &workflowservice.ShutdownWorkerResponse{}, nil
}

type quietLogger struct{}

func (quietLogger) Debug(string, ...any) {}
func (quietLogger) Info(string, ...any)  {}
func (quietLogger) Warn(string, ...any)  {}
func (quietLogger) Error(string, ...any) {}

func connect(t *testing.T, configure ...func(*client.Options)) (client.Client, *service) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	s := &service{runID: "workflow-run", workflowCompleted: make(chan struct{}), taskCompleted: make(chan struct{})}
	workflowservice.RegisterWorkflowServiceServer(server, s)
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-joined })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := client.Options{HostPort: listener.Addr().String(), Namespace: "probe", Identity: "probe", Logger: quietLogger{}, DisableWorkerEnvironmentInfo: true}
	for _, apply := range configure {
		apply(&options)
	}
	native, err := client.DialContext(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(native.Close)
	return native, s
}

type observedSlots struct {
	worker.SlotSupplier
	mu        sync.Mutex
	used      map[*worker.SlotPermit]bool
	processed chan struct{}
	once      sync.Once
}

func (s *observedSlots) MarkSlotUsed(info worker.SlotMarkUsedInfo) {
	s.mu.Lock()
	s.used[info.Permit()] = true
	s.mu.Unlock()
	s.SlotSupplier.MarkSlotUsed(info)
}

func (s *observedSlots) ReleaseSlot(info worker.SlotReleaseInfo) {
	s.SlotSupplier.ReleaseSlot(info)
	s.mu.Lock()
	used := s.used[info.Permit()]
	delete(s.used, info.Permit())
	s.mu.Unlock()
	if used {
		s.once.Do(func() { close(s.processed) })
	}
}

func await(t *testing.T, done <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(message)
	}
}

func TestFathomryWaitJoinsActivity(t *testing.T) {
	for _, releaseBeforeStop := range []bool{true, false} {
		t.Run(map[bool]string{true: "cooperative-control", false: "noncooperative-counterexample"}[releaseBeforeStop], func(t *testing.T) {
			native, _ := connect(t)
			fixed, err := worker.NewFixedSizeSlotSupplier(2)
			if err != nil {
				t.Fatal(err)
			}
			slots := &observedSlots{SlotSupplier: fixed, used: map[*worker.SlotPermit]bool{}, processed: make(chan struct{})}
			tuner, err := worker.NewCompositeTuner(worker.CompositeTunerOptions{ActivitySlotSupplier: slots})
			if err != nil {
				t.Fatal(err)
			}
			entered, released, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce, stopOnce sync.Once
			w := worker.New(native, "probe", worker.Options{FathomryLifecycleV1: true, DisableWorkflowWorker: true, WorkerStopTimeout: 10 * time.Millisecond, Tuner: tuner})
			w.RegisterActivityWithOptions(func(context.Context) (string, error) {
				close(entered)
				<-released
				close(returned)
				return "finished", nil
			}, activity.RegisterOptions{Name: "probe"})
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(released) })
				stopOnce.Do(w.Stop)
				assertJoined(t, w)
				select {
				case <-entered:
					await(t, slots.processed, "native task did not finish cleanup")
				default:
				}
			})
			if err := w.Start(); err != nil {
				t.Fatal(err)
			}
			await(t, entered, "activity not entered")
			if releaseBeforeStop {
				releaseOnce.Do(func() { close(released) })
				await(t, slots.processed, "cooperative task not processed")
			}
			stopped := make(chan struct{})
			go func() { stopOnce.Do(w.Stop); close(stopped) }()
			await(t, stopped, "native stop did not return")
			if !releaseBeforeStop {
				assertWaiting(t, w)
				select {
				case <-returned:
					t.Fatal("counterexample unexpectedly returned")
				default:
				}
				select {
				case <-slots.processed:
					t.Fatal("slot released before native task returned")
				default:
				}
				t.Log("Stop returned while activity and its native task slot were still live")
				releaseOnce.Do(func() { close(released) })
				await(t, slots.processed, "late native task did not finish")
			}
		})
	}
}

func TestFathomryWaitJoinsLocalActivityTail(t *testing.T) {
	dataConverter := &observedConverter{DataConverter: converter.GetDefaultDataConverter(), converted: make(chan struct{})}
	native, s := connect(t, func(options *client.Options) { options.DataConverter = dataConverter })
	fixed, err := worker.NewFixedSizeSlotSupplier(1)
	if err != nil {
		t.Fatal(err)
	}
	slots := &observedSlots{SlotSupplier: fixed, used: map[*worker.SlotPermit]bool{}, processed: make(chan struct{})}
	workflowSlots, err := worker.NewFixedSizeSlotSupplier(2)
	if err != nil {
		t.Fatal(err)
	}
	tuner, err := worker.NewCompositeTuner(worker.CompositeTunerOptions{WorkflowSlotSupplier: workflowSlots, LocalActivitySlotSupplier: slots})
	if err != nil {
		t.Fatal(err)
	}
	entered, released, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce, stopOnce sync.Once
	w := worker.New(native, "probe", worker.Options{FathomryLifecycleV1: true, LocalActivityWorkerOnly: true, WorkerStopTimeout: 10 * time.Millisecond, Tuner: tuner})
	w.RegisterActivityWithOptions(func(context.Context) (string, error) {
		close(entered)
		<-released
		close(returned)
		return "late-local-result", nil
	}, activity.RegisterOptions{Name: "local"})
	w.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: 100 * time.Millisecond, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1}})
		var result string
		_ = workflow.ExecuteLocalActivity(ctx, "local").Get(ctx, &result)
		return nil
	}, workflow.RegisterOptions{Name: "local-probe"})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(released) })
		stopOnce.Do(w.Stop)
		assertJoined(t, w)
		select {
		case <-entered:
			await(t, dataConverter.converted, "late converter did not return")
		default:
		}
	})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	await(t, entered, "local activity did not enter")
	await(t, slots.processed, "local task did not release its slot after timeout")
	await(t, s.workflowCompleted, "workflow did not handle the timeout")
	stopped := make(chan struct{})
	go func() { stopOnce.Do(w.Stop); close(stopped) }()
	await(t, stopped, "native worker did not stop")
	select {
	case <-returned:
		t.Fatal("local callback unexpectedly finished")
	default:
	}
	assertWaiting(t, w)
	t.Log("native slot release and Worker.Stop returned, but managed join retained the local callback and converter")
	releaseOnce.Do(func() { close(released) })
	await(t, dataConverter.converted, "late result conversion not observed")
}

type evictionLogger struct {
	quietLogger
	entered chan struct{}
	release chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (l *evictionLogger) Info(message string, _ ...any) {
	if message == "probe-workflow-exit" {
		l.once.Do(func() {
			close(l.entered)
			<-l.release
			close(l.exited)
		})
	}
}

func assertWaiting(t *testing.T, w worker.Worker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := worker.FathomryWaitStoppedV1(ctx, w); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("join must retain live work, got %v", err)
	}
}

func assertJoined(t *testing.T, w worker.Worker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := worker.FathomryWaitStoppedV1(ctx, w); err != nil {
		t.Fatal(err)
	}
}

func TestFathomryWaitJoinsWorkflowEviction(t *testing.T) {
	logger := &evictionLogger{entered: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
	native, s := connect(t, func(options *client.Options) { options.Logger = logger })
	var releaseOnce, stopOnce sync.Once
	w := worker.New(native, "probe", worker.Options{FathomryLifecycleV1: true, LocalActivityWorkerOnly: true, WorkerStopTimeout: 10 * time.Millisecond, EnableLoggingInReplay: true})
	w.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		defer workflow.GetLogger(ctx).Info("probe-workflow-exit")
		return workflow.Await(ctx, func() bool { return false })
	}, workflow.RegisterOptions{Name: "local-probe"})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(logger.release) })
		stopOnce.Do(w.Stop)
		assertJoined(t, w)
	})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	await(t, s.taskCompleted, "suspended workflow did not complete a task")
	stopOnce.Do(w.Stop)
	select {
	case <-logger.entered:
		t.Fatal("Stop unexpectedly evicted workflow state")
	default:
	}
	assertWaiting(t, w)
	await(t, logger.entered, "managed join did not retire its workflow")
	select {
	case <-logger.exited:
		t.Fatal("cleanup unexpectedly finished")
	default:
	}
	releaseOnce.Do(func() { close(logger.release) })
	assertJoined(t, w)
	await(t, logger.exited, "workflow cleanup did not finish")
}

func TestFathomryRetirementPreservesLivePeerCache(t *testing.T) {
	var workers []worker.Worker
	var loggers []*evictionLogger
	for _, runID := range []string{"left-run", "right-run"} {
		logger := &evictionLogger{entered: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
		native, s := connect(t, func(options *client.Options) { options.Logger = logger })
		s.runID = runID
		w := worker.New(native, "probe", worker.Options{FathomryLifecycleV1: true, LocalActivityWorkerOnly: true, WorkerStopTimeout: 10 * time.Millisecond, EnableLoggingInReplay: true})
		w.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
			defer workflow.GetLogger(ctx).Info("probe-workflow-exit")
			return workflow.Await(ctx, func() bool { return false })
		}, workflow.RegisterOptions{Name: "local-probe"})
		var releaseOnce sync.Once
		t.Cleanup(func() {
			releaseOnce.Do(func() { close(logger.release) })
			w.Stop()
			assertJoined(t, w)
		})
		if err := w.Start(); err != nil {
			t.Fatal(err)
		}
		await(t, s.taskCompleted, "peer workflow not cached")
		workers = append(workers, w)
		loggers = append(loggers, logger)
	}
	workers[0].Stop()
	assertWaiting(t, workers[0])
	await(t, loggers[0].entered, "first worker did not begin retiring")
	select {
	case <-loggers[1].entered:
		t.Fatal("retiring one worker evicted its live peer")
	default:
	}
	// Cleanup releases both barriers and joins each native worker independently.
}
