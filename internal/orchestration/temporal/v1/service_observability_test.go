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
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func metricsReplayWorkflow(ctx workflow.Context) (string, error) {
	workflow.GetMetricsHandler(ctx).Counter("gh61_replay").Inc(1)
	value, _ := ctx.Value(nativeContextKey{}).(string)
	if err := workflow.SetQueryHandler(ctx, "context", func() (string, error) { return value, nil }); err != nil {
		return "", err
	}
	var finish bool
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, &finish)
	return value, nil
}

func TestAuthorizedMetricsAndTracingAcrossWorkerRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, nativeContextKey{}, "restart-context")
	metrics := newNativeTestMetrics()
	tracer := &nativeTestTracer{}
	fixture := newRuntimeExecutionServiceFixture(t, ctx, temporal.RuntimeOptions{MetricsHandler: metrics,
		Interceptors:       []interceptor.ClientInterceptor{interceptor.NewTracingInterceptor(tracer)},
		ContextPropagators: []workflow.ContextPropagator{&nativeTestPropagator{}}}, workflowPrefix+"DeleteWorkflowExecution")
	lifetime, stopLifetime := context.WithCancel(context.Background())
	t.Cleanup(stopLifetime)
	start := func(id string) *temporal.Worker {
		t.Helper()
		managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: id}, temporal.WorkerSpec{
			TaskQueue: fixture.prefix, MaxHandlers: 1, Bytes: 4 * fixture.envelope,
			Options:   worker.Options{MaxConcurrentWorkflowTaskExecutionSize: 2, MaxConcurrentWorkflowTaskPollers: 2, WorkerStopTimeout: time.Second},
			Workflows: []temporal.WorkflowRegistration{{Definition: metricsReplayWorkflow, Options: workflow.RegisterOptions{Name: "metric-replay"}}},
		}, fixture.workers, fixture.tasks)
		if managed != nil {
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				if err := managed.Stop(cleanup); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		return managed
	}
	first := start("metrics-worker-first")
	t.Cleanup(func() { fixture.cleanupWorkflow(t, fixture.prefix, "") })
	run, err := fixture.executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "metrics-start"}, sdk.StartWorkflowOptions{
		ID: fixture.prefix, TaskQueue: fixture.prefix, WorkflowExecutionTimeout: 60 * time.Second}, "metric-replay")
	if err != nil {
		t.Fatal(err)
	}
	var result string
	if err := fixture.executions.QueryWorkflow(ctx, fault.Correlation{Call: "first-query"}, fixture.prefix, run.GetRunID(), "context", &result); err != nil || result != "restart-context" {
		t.Fatal("first worker did not observe the propagated context")
	}
	if metrics.sum("gh61_replay") != 1 {
		t.Fatal("normal execution did not reach the configured metrics handler")
	}
	if err := first.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	firstDelivery, err := fixture.workers.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstResult, err := firstDelivery.Receipt().WaitReleased(ctx)
	if err != nil || firstResult.Err() != nil || !firstResult.Outcome.Value.Joined {
		t.Fatal("first worker did not fully exit")
	}
	if err := firstDelivery.Release(); err != nil {
		t.Fatal(err)
	}
	second := start("metrics-worker-second")
	if err := fixture.executions.QueryWorkflow(ctx, fault.Correlation{Call: "replayed-query"}, fixture.prefix, run.GetRunID(), "context", &result); err != nil || result != "restart-context" {
		t.Fatal("replacement worker could not replay the context header")
	}
	runs := 0
	for _, span := range tracer.observations() {
		if span.operation == "RunWorkflow" && span.name == "metric-replay" {
			runs++
		}
	}
	if runs != 2 {
		t.Fatalf("replacement worker did not independently reenter the workflow: %d", runs)
	}
	if metrics.sum("gh61_replay") != 1 {
		t.Fatal("workflow replay duplicated a native metric")
	}
	if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "metrics-finish"}, fixture.prefix, run.GetRunID(), "finish", true); err != nil {
		t.Fatal(err)
	}
	if err := run.Get(ctx, fault.Correlation{Call: "metrics-result"}, &result); err != nil || result != "restart-context" {
		t.Fatal("replayed workflow did not complete")
	}
	if err := second.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if metrics.sum("gh61_replay") != 1 {
		t.Fatal("execution completion duplicated the initial metric")
	}
	var key string
	for _, span := range tracer.observations() {
		if !span.finished {
			t.Fatalf("native tracing tail survived worker exit: %s", span.operation)
		}
		if span.operation == "RunWorkflow" {
			if span.key == "" || key != "" && key != span.key {
				t.Fatal("native workflow span idempotency key changed on replay")
			}
			key = span.key
		}
	}
	t.Log("Two owned Workers reentered the same workflow, context headers survived replay, native workflow metrics emitted once, and native spans finished with stable idempotency keys")
}
