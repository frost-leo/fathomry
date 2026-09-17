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
	"crypto/sha256"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type controlSample struct {
	Value               int
	QueueWait, BodyTime time.Duration
}

func controlWorkflow(ctx workflow.Context) ([]controlSample, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 15 * time.Second, ScheduleToCloseTimeout: 30 * time.Second,
		RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	futures := make([]workflow.Future, 12)
	for index := range futures {
		futures[index] = workflow.ExecuteActivity(ctx, "controlled-load", index)
	}
	result := make([]controlSample, len(futures))
	for index, future := range futures {
		if err := future.Get(ctx, &result[index]); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func TestAuthorizedNativeTunerAndPollerControl(t *testing.T) {
	for _, autoscaling := range []bool{false, true} {
		name := "fixed-pollers"
		if autoscaling {
			name = "autoscaling-pollers"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			fixture := newExecutionServiceFixture(t, ctx, workflowPrefix+"GetWorkflowExecutionHistory", workflowPrefix+"DeleteWorkflowExecution")
			tasks, err := invocation.NewInbox[temporal.TaskResult](32, 32*temporal.ExecutionEvidenceBytes)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { releaseServiceEvidence(t, tasks) })
			release, full := make(chan struct{}), make(chan struct{})
			var releaseOnce, fullOnce sync.Once
			var active, peak, calls atomic.Int32
			body := func(ctx context.Context, index int) (controlSample, error) {
				entered := time.Now()
				current := active.Add(1)
				defer active.Add(-1)
				for previous := peak.Load(); current > previous; previous = peak.Load() {
					if peak.CompareAndSwap(previous, current) {
						break
					}
				}
				calls.Add(1)
				if current == 2 {
					fullOnce.Do(func() { close(full) })
				}
				select {
				case <-release:
				case <-ctx.Done():
					return controlSample{}, ctx.Err()
				}
				digest := sha256.Sum256([]byte("fixed-correctness-workload"))
				for range 4096 {
					digest = sha256.Sum256(digest[:])
				}
				info := activity.GetInfo(ctx)
				return controlSample{Value: index*2 + 1, QueueWait: info.StartedTime.Sub(info.ScheduledTime), BodyTime: time.Since(entered)}, nil
			}
			tuner, err := worker.NewFixedSizeTuner(worker.FixedSizeTunerOptions{NumWorkflowSlots: 4, NumActivitySlots: 2, NumLocalActivitySlots: 2, NumNexusSlots: 2})
			if err != nil {
				t.Fatal(err)
			}
			pollers := worker.NewPollerBehaviorSimpleMaximum(worker.PollerBehaviorSimpleMaximumOptions{MaximumNumberOfPollers: 2})
			if autoscaling {
				pollers = worker.NewPollerBehaviorAutoscaling(worker.PollerBehaviorAutoscalingOptions{InitialNumberOfPollers: 1, MinimumNumberOfPollers: 1, MaximumNumberOfPollers: 2})
			}
			lifetime, stopLifetime := context.WithCancel(context.Background())
			t.Cleanup(stopLifetime)
			managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "control-worker"}, temporal.WorkerSpec{
				TaskQueue: fixture.prefix, MaxHandlers: 2, Bytes: 4 * fixture.envelope,
				Options:    worker.Options{Tuner: tuner, MaxConcurrentWorkflowTaskPollers: 2, ActivityTaskPollerBehavior: pollers, WorkerStopTimeout: time.Second},
				Workflows:  []temporal.WorkflowRegistration{{Definition: controlWorkflow, Options: workflow.RegisterOptions{Name: "controlled-workflow"}}},
				Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "controlled-load"}}},
			}, fixture.workers, tasks)
			if managed != nil {
				t.Cleanup(func() {
					releaseOnce.Do(func() { close(release) })
					cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
					defer stop()
					if err := managed.Stop(cleanup); err != nil {
						t.Error(err)
					}
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			id := fixture.prefix + "-control"
			t.Cleanup(func() { fixture.cleanupWorkflow(t, id, "") })
			run, err := fixture.executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "control-start"}, sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: 45 * time.Second}, "controlled-workflow")
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-full:
			case <-ctx.Done():
				t.Fatal("native slots did not admit the normal two-task control")
			}
			if calls.Load() != 2 || active.Load() != 2 {
				t.Fatal("native fixed tuner overshot its saturated slots")
			}
			releaseOnce.Do(func() { close(release) })
			var samples []controlSample
			if err := run.Get(ctx, fault.Correlation{Call: "control-result"}, &samples); err != nil {
				t.Fatal(err)
			}
			if len(samples) != 12 || calls.Load() != 12 || peak.Load() != 2 || active.Load() != 0 {
				t.Fatal("native control lost work, repeated it, or exceeded slots")
			}
			queue, bodyTimes := make([]time.Duration, 12), make([]time.Duration, 12)
			for index, sample := range samples {
				if sample.Value != index*2+1 || sample.QueueWait < 0 {
					t.Fatal("independent result/queue-time control failed")
				}
				queue[index], bodyTimes[index] = sample.QueueWait, sample.BodyTime
			}
			history := fixture.history(t, ctx, id, run.GetRunID())
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflowWithOptions(controlWorkflow, workflow.RegisterOptions{Name: "controlled-workflow"})
			if err := replayer.ReplayWorkflowHistoryWithOptions(executionLogger{}, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: id, RunID: run.GetRunID()}}); err != nil {
				t.Fatal(err)
			}
			if err := managed.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			slices.Sort(queue)
			slices.Sort(bodyTimes)
			t.Logf("12 exact results; peak=2; queue p50=%s max=%s; body p50=%s max=%s; evidence outstanding=%d; JSON import/replay and Stop joined", queue[5], queue[11], bodyTimes[5], bodyTimes[11], tasks.Usage().Outstanding)
		})
	}
}

type pluginServiceKey struct{}

func pluginServiceWorkflow(ctx workflow.Context, input converter.EncodedValues) (string, error) {
	var value string
	if err := input.Get(&value); err != nil {
		return "", err
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Second, RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: 5 * time.Second, RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	var remote, local string
	if err := workflow.ExecuteActivity(ctx, "plugin-dynamic-activity", value).Get(ctx, &remote); err != nil {
		return "", err
	}
	if err := workflow.ExecuteLocalActivity(ctx, "plugin-local", value).Get(ctx, &local); err != nil {
		return "", err
	}
	return remote + "/" + local, nil
}

func TestAuthorizedWorkerPluginsRegisterAndOwnDynamicExecution(t *testing.T) {
	authorizedPluginExecution(t, false)
}

func TestAuthorizedCombinedClientWorkerPlugin(t *testing.T) {
	authorizedPluginExecution(t, true)
}

type combinedServicePlugin struct {
	sdk.PluginBase
	*controlledWorkerPlugin
	converter           converter.DataConverter
	configured, created atomic.Int32
}

func (*combinedServicePlugin) Name() string { return "combined-service-plugin" }

func (plugin *combinedServicePlugin) ConfigureClient(_ context.Context, options sdk.PluginConfigureClientOptions) error {
	plugin.configured.Add(1)
	options.ClientOptions.DataConverter = plugin.converter
	return nil
}

func (plugin *combinedServicePlugin) NewClient(ctx context.Context, options sdk.PluginNewClientOptions, next func(context.Context, sdk.PluginNewClientOptions) error) error {
	plugin.created.Add(1)
	return next(ctx, options)
}

func authorizedPluginExecution(t *testing.T, fromClient bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var configured, started, stopped atomic.Int32
	var retained converter.EncodedValues
	var decoded atomic.Int32
	background := context.WithValue(context.Background(), pluginServiceKey{}, "plugin-context")
	plugin := &controlledWorkerPlugin{
		configure: func(_ context.Context, options worker.PluginConfigureWorkerOptions) error {
			configured.Add(1)
			options.WorkerOptions.BackgroundActivityContext = background
			options.WorkerOptions.FathomryLifecycleV1 = false
			return nil
		},
		start: func(nextContext context.Context, options worker.PluginStartWorkerOptions, next func(context.Context, worker.PluginStartWorkerOptions) error) error {
			started.Add(1)
			options.WorkerRegistry.RegisterDynamicWorkflow(pluginServiceWorkflow, workflow.DynamicRegisterOptions{})
			options.WorkerRegistry.RegisterDynamicActivity(func(ctx context.Context, input converter.EncodedValues) (string, error) {
				retained = input
				var value string
				if err := input.Get(&value); err != nil {
					return "", err
				}
				decoded.Add(1)
				if ctx.Value(pluginServiceKey{}) != "plugin-context" {
					return "", errors.New("configured Activity context lost")
				}
				return value + ":remote", nil
			}, activity.DynamicRegisterOptions{})
			options.WorkerRegistry.RegisterActivityWithOptions(func(ctx context.Context, value string) (string, error) {
				if ctx.Value(pluginServiceKey{}) != "plugin-context" {
					return "", errors.New("configured Local Activity context lost")
				}
				return value + ":local", nil
			}, activity.RegisterOptions{Name: "plugin-local"})
			return next(nextContext, options)
		},
		stop: func(ctx context.Context, options worker.PluginStopWorkerOptions, next func(context.Context, worker.PluginStopWorkerOptions)) {
			next(ctx, options)
			stopped.Add(1)
		},
	}
	runtime := temporal.RuntimeOptions{Interceptors: []interceptor.ClientInterceptor{&interceptor.ClientInterceptorBase{}}}
	workerPlugins := []worker.Plugin{plugin}
	var combined *combinedServicePlugin
	var codec converter.DataConverter
	if fromClient {
		codec = converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), versionCodec{version: "2", legacy: true})
		combined = &combinedServicePlugin{controlledWorkerPlugin: plugin, converter: codec}
		runtime.Plugins = []sdk.Plugin{combined}
		workerPlugins = nil
	}
	fixture := newRuntimeExecutionServiceFixture(t, ctx, runtime, workflowPrefix+"GetWorkflowExecutionHistory", workflowPrefix+"DeleteWorkflowExecution")
	lifetime, stopLifetime := context.WithCancel(context.Background())
	t.Cleanup(stopLifetime)
	managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "plugin-service-worker"}, temporal.WorkerSpec{
		TaskQueue: fixture.prefix, MaxHandlers: 4, Bytes: 4 * fixture.envelope,
		Options: worker.Options{MaxConcurrentWorkflowTaskPollers: 2, MaxConcurrentWorkflowTaskExecutionSize: 4,
			MaxConcurrentActivityTaskPollers: 1, MaxConcurrentActivityExecutionSize: 2, Plugins: workerPlugins},
	}, fixture.workers, fixture.tasks)
	if managed != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if err := managed.Stop(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	id := fixture.prefix + "-plugin"
	t.Cleanup(func() { fixture.cleanupWorkflow(t, id, "") })
	run, err := fixture.executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "plugin-service-start"},
		sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: 30 * time.Second}, "plugin-dynamic-workflow", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	var result string
	if err := run.Get(ctx, fault.Correlation{Call: "plugin-service-result"}, &result); err != nil || result != "fixture:remote/fixture:local" {
		t.Fatal("plugin dynamic Workflow/Activity or Local Activity failed", err)
	}
	history := fixture.history(t, ctx, id, run.GetRunID())
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{DataConverter: codec})
	if err != nil {
		t.Fatal(err)
	}
	replayer.RegisterDynamicWorkflow(pluginServiceWorkflow, workflow.DynamicRegisterOptions{})
	if err := replayer.ReplayWorkflowHistoryWithOptions(executionLogger{}, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: id, RunID: run.GetRunID()}}); err != nil {
		t.Fatal("plugin-registered dynamic history replay failed", err)
	}
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if configured.Load() != 1 || started.Load() != 1 || stopped.Load() != 1 || decoded.Load() != 1 {
		t.Fatal("plugin lifecycle or callback was repeated")
	}
	if combined != nil && (combined.configured.Load() != 1 || combined.created.Load() != 1) {
		t.Fatal("polling client reran Client plugin initialization")
	}
	var value string
	if retained == nil || !errors.Is(retained.Get(&value), temporal.ErrAuthority) {
		t.Fatal("retained plugin-registered dynamic Activity input decoded after ownership ended")
	}
	t.Log("Real plugin configuration/start/stop, dynamic Workflow/Activity, Local Activity background values, retained decoder rejection and exact history replay passed")
}
