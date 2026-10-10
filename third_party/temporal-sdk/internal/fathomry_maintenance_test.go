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

package internal

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	internallog "go.temporal.io/sdk/internal/log"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

func TestFathomryLocalActivityRetryPolicyReuse(t *testing.T) {
	policy := &RetryPolicy{MaximumAttempts: 3}
	var work sync.WaitGroup
	for range 8 {
		work.Go(func() {
			options := GetLocalActivityOptions(WithLocalActivityOptions(newTestWorkflowContext(), LocalActivityOptions{
				StartToCloseTimeout: time.Minute, RetryPolicy: policy,
			}))
			require.Equal(t, &RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2,
				MaximumInterval: 100 * time.Second, MaximumAttempts: 3}, options.RetryPolicy)
			require.NotSame(t, policy, options.RetryPolicy)
		})
	}
	work.Wait()
	require.Equal(t, &RetryPolicy{MaximumAttempts: 3}, policy)
	explicit := &RetryPolicy{InitialInterval: time.Minute, BackoffCoefficient: 3,
		MaximumInterval: 10 * time.Minute, MaximumAttempts: 5}
	require.Equal(t, explicit, applyRetryPolicyDefaultsForLocalActivity(explicit))
	require.NotSame(t, explicit, applyRetryPolicyDefaultsForLocalActivity(explicit))
}

func TestFathomryCacheGenerationAndConditionalRemoval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, lock := &sharedWorkerCache{}, &sync.Mutex{}
		first := newWorkerCache(store, lock, 10)
		peer := newWorkerCache(store, lock, 10)
		contextFor := func(owner *WorkerCache) *workflowExecutionContextImpl {
			return &workflowExecutionContextImpl{workflowInfo: &WorkflowInfo{WorkflowExecution: WorkflowExecution{RunID: "same-run"}},
				wth: &workflowTaskHandlerImpl{cache: owner, metricsHandler: metrics.NopHandler}, cached: true}
		}
		stale, replacement := contextFor(first), contextFor(peer)
		_, err := first.putWorkflowContext("same-run", stale)
		require.NoError(t, err)
		first.getWorkflowCache().Delete("same-run")
		synctest.Wait()
		_, err = peer.putWorkflowContext("same-run", replacement)
		require.NoError(t, err)
		stale.Lock()
		stale.Unlock(errors.New("late stale completion"))
		require.Same(t, replacement, peer.getWorkflowContext("same-run"))
		first.close(lock)
		first.close(lock)
		require.Equal(t, 1, store.workerRefcount)
		require.Same(t, replacement, peer.getWorkflowContext("same-run"))
		late, err := first.putWorkflowContext("late-run", contextFor(first))
		require.ErrorContains(t, err, "workflow cache handle is released")
		require.NotNil(t, late)
		require.Nil(t, peer.getWorkflowContext("late-run"))
		peer.close(lock)
		require.Zero(t, store.workerRefcount)
		require.Nil(t, store.workflowCache)
		require.Zero(t, peer.getWorkflowCache().Size())
		next := newWorkerCache(store, lock, 20)
		defer next.close(lock)
		require.NotSame(t, next.getWorkflowCache(), first.getWorkflowCache())
		require.Equal(t, 10, first.MaxWorkflowCacheSize())
		require.Equal(t, 20, next.MaxWorkflowCacheSize())
		require.Nil(t, first.getWorkflowContext("same-run"))
	})
}

type maintenanceService struct {
	workflowservice.WorkflowServiceClient
	poll     func(context.Context)
	shutdown func()
	nexus    atomic.Int32
}

func (*maintenanceService) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest, ...grpc.CallOption) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (*maintenanceService) DescribeNamespace(context.Context, *workflowservice.DescribeNamespaceRequest, ...grpc.CallOption) (*workflowservice.DescribeNamespaceResponse, error) {
	return &workflowservice.DescribeNamespaceResponse{NamespaceInfo: &namespacepb.NamespaceInfo{Name: "maintenance", State: enumspb.NAMESPACE_STATE_REGISTERED}}, nil
}

func (service *maintenanceService) PollActivityTaskQueue(ctx context.Context, _ *workflowservice.PollActivityTaskQueueRequest, _ ...grpc.CallOption) (*workflowservice.PollActivityTaskQueueResponse, error) {
	service.poll(ctx)
	return &workflowservice.PollActivityTaskQueueResponse{}, nil
}

func (service *maintenanceService) PollNexusTaskQueue(ctx context.Context, _ *workflowservice.PollNexusTaskQueueRequest, _ ...grpc.CallOption) (*workflowservice.PollNexusTaskQueueResponse, error) {
	service.nexus.Add(1)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (service *maintenanceService) ShutdownWorker(context.Context, *workflowservice.ShutdownWorkerRequest, ...grpc.CallOption) (*workflowservice.ShutdownWorkerResponse, error) {
	service.shutdown()
	return &workflowservice.ShutdownWorkerResponse{}, nil
}

func maintenanceWorker(t *testing.T, options WorkerOptions, service *maintenanceService) *AggregatedWorker {
	t.Helper()
	client := NewServiceClient(service, nil, ClientOptions{Namespace: "maintenance", Logger: internallog.NewNopLogger(), WorkerHeartbeatInterval: -1})
	options.DisableWorkflowWorker = true
	worker := NewAggregatedWorker(client, "maintenance", options)
	worker.RegisterActivityWithOptions(func(context.Context) error { return nil }, RegisterActivityOptions{Name: "maintenance-activity"})
	t.Cleanup(client.Close)
	return worker
}

func TestFathomryFatalNotificationAndStopOwnership(t *testing.T) {
	for _, managed := range []bool{false, true} {
		for _, autoscaling := range []bool{false, true} {
			name := "native"
			if managed {
				name = "managed"
			}
			if autoscaling {
				name += "-autoscaling"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					cause := errors.New("first fatal cause")
					hookEntered, releaseHook := make(chan struct{}), make(chan struct{})
					shutdownEntered, releaseShutdown := make(chan struct{}), make(chan struct{})
					var notifications, shutdowns atomic.Int32
					service := &maintenanceService{shutdown: func() { shutdowns.Add(1); close(shutdownEntered); <-releaseShutdown }}
					options := WorkerOptions{FathomryLifecycleV1: managed, WorkerStopTimeout: time.Minute,
						OnFatalError:               func(err error) { notifications.Add(1); assert.Same(t, cause, err); close(hookEntered); <-releaseHook },
						ActivityTaskPollerBehavior: NewPollerBehaviorSimpleMaximum(PollerBehaviorSimpleMaximumOptions{MaximumNumberOfPollers: 1})}
					if autoscaling {
						options.ActivityTaskPollerBehavior = NewPollerBehaviorAutoscaling(PollerBehaviorAutoscalingOptions{
							InitialNumberOfPollers: 1, MinimumNumberOfPollers: 1, MaximumNumberOfPollers: 1})
					}
					worker := maintenanceWorker(t, options, service)
					service.poll = func(context.Context) {
						worker.executionParams.WorkerFatalErrorCallback(cause)
						worker.executionParams.WorkerFatalErrorCallback(errors.New("later fatal cause"))
					}
					require.NoError(t, worker.Start())
					<-hookEntered
					retired := make(chan struct{})
					go func() { worker.activityWorker.worker.pollerWG.Wait(); close(retired) }()
					synctest.Wait()
					select {
					case <-retired:
					default:
						t.Error("fatal notification retained its reporting poller")
					}
					stopped := make(chan struct{}, 2)
					go func() { worker.Stop(); stopped <- struct{}{} }()
					<-shutdownEntered
					go func() { worker.Stop(); stopped <- struct{}{} }()
					synctest.Wait()
					assert.Empty(t, stopped, "concurrent Stop returned before shared cleanup")
					close(releaseShutdown)
					<-stopped
					<-stopped
					if managed {
						ctx, cancel := context.WithCancel(t.Context())
						cancel()
						require.ErrorIs(t, worker.FathomryWaitStoppedV1(ctx), context.Canceled)
					}
					close(releaseHook)
					synctest.Wait()
					if managed {
						require.NoError(t, worker.FathomryWaitStoppedV1(t.Context()))
					}
					assert.EqualValues(t, 1, notifications.Load())
					assert.EqualValues(t, 1, shutdowns.Load())
					assert.Same(t, cause, worker.fatalErr)
				})
			})
		}
	}
}

type maintenanceStartupTuner struct {
	WorkerTuner
	entered chan struct{}
	release chan struct{}
}

func (tuner *maintenanceStartupTuner) GetNexusSlotSupplier() SlotSupplier {
	close(tuner.entered)
	<-tuner.release
	return tuner.WorkerTuner.GetNexusSlotSupplier()
}

func TestFathomryFatalDuringStartupJoinsLateWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fixed, err := NewFixedSizeTuner(FixedSizeTunerOptions{NumWorkflowSlots: 2, NumActivitySlots: 2, NumLocalActivitySlots: 2, NumNexusSlots: 2})
		require.NoError(t, err)
		tuner := &maintenanceStartupTuner{WorkerTuner: fixed, entered: make(chan struct{}), release: make(chan struct{})}
		fatal := make(chan struct{})
		var shutdowns atomic.Int32
		service := &maintenanceService{shutdown: func() { shutdowns.Add(1) }}
		worker := maintenanceWorker(t, WorkerOptions{FathomryLifecycleV1: true, Tuner: tuner, WorkerStopTimeout: time.Minute,
			OnFatalError: func(error) { close(fatal) }}, service)
		service.poll = func(ctx context.Context) { <-ctx.Done() }
		nexusService := nexus.NewService("maintenance-service")
		require.NoError(t, nexusService.Register(nexus.NewSyncOperation("echo", func(context.Context, string, nexus.StartOperationOptions) (string, error) { return "", nil })))
		worker.RegisterNexusService(nexusService)
		started := make(chan error, 1)
		go func() { started <- worker.Start() }()
		<-tuner.entered
		go worker.executionParams.WorkerFatalErrorCallback(errors.New("fatal during Nexus construction"))
		<-fatal
		synctest.Wait()
		assert.Zero(t, shutdowns.Load(), "automatic Stop ran before startup published its owned workers")
		close(tuner.release)
		require.NoError(t, <-started)
		worker.Stop()
		require.NoError(t, worker.FathomryWaitStoppedV1(t.Context()))
		require.Zero(t, service.nexus.Load(), "late Nexus worker polled after the fatal error")
		require.EqualValues(t, 1, shutdowns.Load())
	})
}

type maintenanceStopPlugin struct {
	WorkerPluginBase
	entered chan struct{}
	release chan struct{}
}

func (*maintenanceStopPlugin) Name() string { return "maintenance-stop" }

func (plugin *maintenanceStopPlugin) StopWorker(ctx context.Context, options WorkerPluginStopWorkerOptions, next func(context.Context, WorkerPluginStopWorkerOptions)) {
	next(ctx, options)
	close(plugin.entered)
	<-plugin.release
}

func TestFathomryInterruptedRunRetainsFatalAndJoinsPlugin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cause := errors.New("first fatal cause")
		fatalEntered, releaseFatal := make(chan struct{}), make(chan struct{})
		shutdownEntered, releaseShutdown := make(chan struct{}), make(chan struct{})
		plugin := &maintenanceStopPlugin{entered: make(chan struct{}), release: make(chan struct{})}
		service := &maintenanceService{poll: func(ctx context.Context) { <-ctx.Done() }, shutdown: func() { close(shutdownEntered); <-releaseShutdown }}
		worker := maintenanceWorker(t, WorkerOptions{WorkerStopTimeout: time.Minute, Plugins: []WorkerPlugin{plugin},
			OnFatalError: func(error) { close(fatalEntered); <-releaseFatal }}, service)
		start := worker.memoizedStart
		startEntered, releaseStart := make(chan struct{}), make(chan struct{})
		worker.memoizedStart = func() error {
			err := start()
			close(startEntered)
			<-releaseStart
			return err
		}
		interrupt := make(chan any, 1)
		result := make(chan error, 1)
		go func() { result <- worker.Run(interrupt) }()
		<-startEntered
		go worker.executionParams.WorkerFatalErrorCallback(cause)
		<-fatalEntered
		interrupt <- "interrupted"
		close(releaseStart)
		<-shutdownEntered
		synctest.Wait()
		assert.Empty(t, result, "Run returned before native cleanup")
		close(releaseShutdown)
		<-plugin.entered
		second := make(chan struct{})
		go func() { worker.Stop(); close(second) }()
		synctest.Wait()
		assert.Empty(t, result, "Run returned before plugin cleanup")
		select {
		case <-second:
			t.Error("concurrent Stop returned before plugin cleanup")
		default:
		}
		close(plugin.release)
		assert.Same(t, cause, <-result)
		<-second
		close(releaseFatal)
	})
}

type maintenanceHeldSlotSupplier struct {
	SlotSupplier
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type maintenanceProbePoller struct{ calls atomic.Int32 }

func (poller *maintenanceProbePoller) PollTask() (taskForWorker, error) {
	poller.calls.Add(1)
	return nil, nil
}

func (supplier *maintenanceHeldSlotSupplier) ReserveSlot(ctx context.Context, info SlotReservationInfo) (*SlotPermit, error) {
	supplier.once.Do(func() { close(supplier.entered) })
	select {
	case <-supplier.release:
		return supplier.SlotSupplier.ReserveSlot(ctx, info)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestFathomryPollingStopReturnsHeldReservations(t *testing.T) {
	for _, autoscaling := range []bool{false, true} {
		for _, wait := range []string{"slot", "rate"} {
			name := "fixed-" + wait
			if autoscaling {
				name = "autoscaling-" + wait
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					recorded := &releaseRecordingSlotSupplier{released: make(chan SlotReleaseReason, 2)}
					supplier := &maintenanceHeldSlotSupplier{SlotSupplier: recorded, entered: make(chan struct{}), release: make(chan struct{})}
					producer := &maintenanceProbePoller{}
					var behavior PollerBehavior = NewPollerBehaviorSimpleMaximum(PollerBehaviorSimpleMaximumOptions{MaximumNumberOfPollers: 1})
					if autoscaling {
						behavior = NewPollerBehaviorAutoscaling(PollerBehaviorAutoscalingOptions{InitialNumberOfPollers: 1, MinimumNumberOfPollers: 1, MaximumNumberOfPollers: 1})
					}
					poller := newScalableTaskPoller(producer, internallog.NewNopLogger(), behavior, metrics.PollerTypeActivityTask, nil)
					base := newBaseWorker(baseWorkerOptions{slotSupplier: supplier, maxTaskPerSecond: 1000, taskPollers: []scalableTaskPoller{poller},
						taskProcessor: noopTaskProcessor{}, workerType: "maintenance", logger: internallog.NewNopLogger(), stopTimeout: time.Minute, metricsHandler: metrics.NopHandler})
					if wait == "rate" {
						base.pollLimiter = rate.NewLimiter(rate.Every(time.Second), 1)
						require.True(t, base.pollLimiter.Allow())
					}
					base.Start()
					defer base.Stop()
					<-supplier.entered
					if wait == "rate" {
						close(supplier.release)
						synctest.Wait()
						require.EqualValues(t, 1, base.slotSupplier.issuedSlotsAtomic.Load())
					}
					base.noRepoll.Store(true)
					if wait == "slot" {
						close(supplier.release)
					}
					retired := make(chan struct{})
					go func() { base.pollerWG.Wait(); close(retired) }()
					<-retired
					assert.Zero(t, producer.calls.Load(), "a stopped poll opened after its wait")
					assert.Zero(t, base.slotSupplier.issuedSlotsAtomic.Load())
					assert.Equal(t, SlotReleaseReasonUnused, <-recorded.released)
					assert.Empty(t, recorded.released, "the unused slot was returned twice")
					if autoscaling {
						poller.autoscalingRunner.activeMu.Lock()
						assert.Zero(t, poller.autoscalingRunner.active)
						poller.autoscalingRunner.activeMu.Unlock()
					}
				})
			})
		}
	}
}

func TestFathomryMTLSSetupReuse(t *testing.T) {
	certificate := tls.Certificate{Certificate: [][]byte{[]byte("client-certificate")}}
	backing := []tls.Certificate{{Certificate: [][]byte{[]byte("unused-certificate")}}}
	configuration := &tls.Config{ServerName: "test-domain", MinVersion: tls.VersionTLS12, Certificates: backing[:0]}
	credentials := NewMTLSCredentials(certificate)
	for range 2 {
		options := ConnectionOptions{TLS: configuration}
		require.NoError(t, credentials.applyToOptions(&options))
		require.NotSame(t, configuration, options.TLS)
		require.Equal(t, configuration.ServerName, options.TLS.ServerName)
		require.Equal(t, configuration.MinVersion, options.TLS.MinVersion)
		require.Equal(t, []tls.Certificate{certificate}, options.TLS.Certificates)
		require.Empty(t, configuration.Certificates)
		require.Equal(t, "unused-certificate", string(backing[0].Certificate[0]))
	}
	conflict := ConnectionOptions{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}}
	require.ErrorContains(t, credentials.applyToOptions(&conflict), "certificates already exist")
	require.Equal(t, []tls.Certificate{certificate}, conflict.TLS.Certificates)
	var empty ConnectionOptions
	require.NoError(t, credentials.applyToOptions(&empty))
	require.Equal(t, []tls.Certificate{certificate}, empty.TLS.Certificates)
}

func TestFathomryScheduleActionReuse(t *testing.T) {
	client := &WorkflowClient{namespace: "maintenance", dataConverter: converter.GetDefaultDataConverter(), registry: newRegistry()}
	for _, identity := range []string{"", "explicit-workflow"} {
		t.Run(identity, func(t *testing.T) {
			action := &ScheduleWorkflowAction{ID: identity, Workflow: "maintenance-workflow", TaskQueue: "maintenance"}
			first, err := convertToPBScheduleAction(contextWithNewHeader(t.Context()), client, action)
			require.NoError(t, err)
			second, err := convertToPBScheduleAction(contextWithNewHeader(t.Context()), client, action)
			require.NoError(t, err)
			require.Equal(t, identity, action.ID)
			if identity == "" {
				_, err = uuid.Parse(first.GetStartWorkflow().GetWorkflowId())
				require.NoError(t, err)
				require.NotEqual(t, first.GetStartWorkflow().GetWorkflowId(), second.GetStartWorkflow().GetWorkflowId())
			} else {
				require.Equal(t, identity, first.GetStartWorkflow().GetWorkflowId())
				require.Equal(t, identity, second.GetStartWorkflow().GetWorkflowId())
			}
		})
	}
}
