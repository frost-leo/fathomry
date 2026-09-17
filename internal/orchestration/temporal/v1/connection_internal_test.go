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

package temporal

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type setupPeer struct {
	workflowservice.UnimplementedWorkflowServiceServer
}

func (*setupPeer) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{ServerVersion: "1.32.0"}, nil
}

type setupMetrics struct {
	failed atomic.Bool
	cause  error
}

func (metrics *setupMetrics) WithTags(map[string]string) sdk.MetricsHandler { return metrics }

func (metrics *setupMetrics) Counter(name string) sdk.MetricsCounter {
	return setupCounter(func() {
		if name == "temporal_request" && metrics.failed.Load() {
			panic(metrics.cause)
		}
	})
}

func (*setupMetrics) Gauge(name string) sdk.MetricsGauge { return sdk.MetricsNopHandler.Gauge(name) }

func (*setupMetrics) Timer(name string) sdk.MetricsTimer { return sdk.MetricsNopHandler.Timer(name) }

type setupCounter func()

func (counter setupCounter) Inc(int64) { counter() }

func TestWorkerDialCallbackPanicClosesCapturedConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(server, &setupPeer{})
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-joined })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	original := errors.New("synthetic metric initialization panic")
	metrics := &setupMetrics{cause: original}
	selected, err := SelectWithRuntime(OptionsV1{Name: "setup", Endpoint: listener.Addr().String(), Namespace: "test", Plaintext: true,
		MaxActive: 1, MaxRequestBytes: 1024, MaxResponseBytes: 1024}, RuntimeOptions{MetricsHandler: metrics})
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, resource.Limits{Active: 1, Bytes: 18 << 10, MaxLeases: 8})
	assembly, err := resource.Assemble(ctx, ctx, "setup", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := assembly.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	executionInbox, err := invocation.NewInbox[Execution](1, ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	executions, err := BindExecutions(assembly, selected, executionInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	workerInbox, err := invocation.NewInbox[WorkerResult](1, ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	taskInbox, err := invocation.NewInbox[TaskResult](1, ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	metrics.failed.Store(true)
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "failed-worker"}, WorkerSpec{TaskQueue: "setup", MaxHandlers: 1, Bytes: 18 << 10}, workerInbox, taskInbox)
	if managed == nil {
		t.Fatal("partial initialization returned no owner")
	}
	if !errors.Is(err, original) {
		t.Fatal("startup panic did not retain its original cause")
	}
	if err := managed.Stop(ctx); !errors.Is(err, original) {
		t.Fatal("observing cleanup erased the startup failure")
	}
	if managed.connection == nil || managed.connection.GetState() != connectivity.Shutdown || !managed.Status().Joined {
		t.Fatal("failed initialization abandoned its actual private gRPC connection")
	}
	delivery, err := workerInbox.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := delivery.Receipt().WaitReleased(ctx)
	if err != nil || !errors.Is(result.Err(), original) || !result.Outcome.Value.Joined {
		t.Fatal("partial initialization evidence incomplete")
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
}
