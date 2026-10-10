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
	"net"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
	"github.com/google/uuid"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/interceptor"
	"google.golang.org/grpc"
)

type costInterceptor struct {
	interceptor.ClientInterceptorBase
	calls atomic.Int64
}

func (hook *costInterceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return &costOutbound{ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next}, hook: hook}
}

type costOutbound struct {
	interceptor.ClientOutboundInterceptorBase
	hook *costInterceptor
}

func (hook *costOutbound) SignalWorkflow(ctx context.Context, input *interceptor.ClientSignalWorkflowInput) error {
	hook.hook.calls.Add(1)
	return hook.Next.SignalWorkflow(ctx, input)
}

type costSample struct{ Value string }

func BenchmarkLoopbackSignalCost(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	peer := &testServer{}
	server := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(server, peer)
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	b.Cleanup(func() { server.Stop(); <-joined })
	for _, mode := range []string{"internal-borrow", "internal-equal-guarantees", "public-adapter"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			settings := Settings{Name: "cost", Endpoint: listener.Addr().String(), Namespace: "test", Plaintext: true,
				MaxActive: 1, MaxRequestBytes: 4096, MaxResponseBytes: 4096, InnerEvidenceCapacity: 64, FamilyLimit: 8}
			hook := &costInterceptor{}
			options := NativeOptions{Interceptors: []interceptor.ClientInterceptor{hook}}
			prepared, err := Prepare(settings, options)
			if err != nil {
				b.Fatal(err)
			}
			policy, err := prepared.Policy()
			if err != nil {
				b.Fatal(err)
			}
			lifetime, cancel := context.WithCancel(context.Background())
			b.Cleanup(cancel)
			var invoke func(context.Context) error
			var drain func(context.Context) error
			var closeSource func() error
			if mode == "internal-borrow" {
				selected := source.WithLimits(prepared.native.Selection(), policy.nativeLimits)
				assembly, err := source.Assemble(lifetime, context.Background(), "cost", selected)
				if err != nil {
					b.Fatal(err)
				}
				borrowed := source.Borrow("cost-use", assembly, selected)
				use, err := source.Assemble(lifetime, context.Background(), "cost-use", borrowed)
				if err != nil {
					b.Fatal(err)
				}
				executions, err := invocation.NewInbox[native.Execution](settings.InnerEvidenceCapacity, int64(settings.InnerEvidenceCapacity)*prepared.metadata.EvidenceBytes)
				if err != nil {
					b.Fatal(err)
				}
				rpcs, err := invocation.NewInbox[native.RPCResult](settings.InnerEvidenceCapacity, int64(settings.InnerEvidenceCapacity)*prepared.metadata.RPCEvidenceBytes)
				if err != nil {
					b.Fatal(err)
				}
				client, err := native.BindExecutions(use, borrowed, executions, nil)
				if err != nil {
					b.Fatal(err)
				}
				raw, err := native.Bind(use, borrowed, rpcs, nil)
				if err != nil {
					b.Fatal(err)
				}
				client, _, err = native.WithWorkEnvelope(client, raw, policy.NativeWorkBytes)
				if err != nil {
					b.Fatal(err)
				}
				invoke = func(ctx context.Context) error {
					return client.SignalWorkflow(ctx, fault.Correlation{Call: uuid.NewString()}, "workflow", "run", "signal", costSample{Value: "same-payload"})
				}
				drain = func(ctx context.Context) error {
					delivery, err := executions.Next(ctx)
					if err != nil {
						return err
					}
					result, err := delivery.Receipt().WaitReleased(ctx)
					if err != nil {
						return err
					}
					if err := result.Err(); err != nil {
						return err
					}
					return delivery.Release()
				}
				closeSource = func() error {
					if err := use.Close(context.Background()); err != nil {
						return err
					}
					return assembly.Close(context.Background())
				}
			} else {
				operationRuntime, err := adapters.New(lifetime, policy.Runtime)
				if err != nil {
					b.Fatal(err)
				}
				evidence, err := adapters.NewInbox[Result](policy.Evidence)
				if err != nil {
					b.Fatal(err)
				}
				workers, err := adapters.NewInbox[WorkerResult](policy.Workers)
				if err != nil {
					b.Fatal(err)
				}
				tasks, err := adapters.NewInbox[TaskResult](policy.Tasks)
				if err != nil {
					b.Fatal(err)
				}
				owner, err := prepared.Open(lifetime, Dependencies{Runtime: operationRuntime, Evidence: evidence, Workers: workers, Tasks: tasks})
				if err != nil {
					b.Fatal(err)
				}
				client := owner.Client()
				invoke = func(ctx context.Context) error {
					return client.SignalWorkflow(ctx, "workflow", "run", "signal", costSample{Value: "same-payload"})
				}
				if mode == "internal-equal-guarantees" {
					// Reuse the admitted use and error boundary to isolate forwarding,
					// not the cost of the stronger contract over bare Internal use.
					invoke = func(ctx context.Context) error {
						return translate(client.native.SignalWorkflow(ctx, client.correlation(), "workflow", "run", "signal", costSample{Value: "same-payload"}), "workflow-signal")
					}
				}
				drain = func(ctx context.Context) error {
					delivery, err := evidence.NextReleased(ctx)
					if err != nil {
						return err
					}
					receipt, err := delivery.Receipt()
					if err != nil {
						return err
					}
					result, err := receipt.WaitReleased(ctx)
					if err != nil {
						return err
					}
					if err := result.Err(); err != nil {
						return err
					}
					return delivery.Ack()
				}
				closeSource = func() error {
					if err := owner.Close(context.Background()); err != nil {
						return err
					}
					if err := drain(context.Background()); err != nil {
						return err
					}
					return operationRuntime.Close(context.Background())
				}
			}
			b.Cleanup(func() {
				if err := closeSource(); err != nil {
					b.Error(err)
				}
			})
			samples := make([]int64, min(b.N, 8192))
			stride := max(1, b.N/max(1, len(samples)))
			sampled := 0
			runtime.GC()
			var resident runtime.MemStats
			runtime.ReadMemStats(&resident)
			goroutines := runtime.NumGoroutine()
			startedCalls := peer.signals.Load()
			observedCalls := hook.calls.Load()
			b.ResetTimer()
			for index := range b.N {
				started := time.Now()
				if err := invoke(lifetime); err != nil {
					b.Fatal(err)
				}
				if err := drain(lifetime); err != nil {
					b.Fatal(err)
				}
				if index%stride == 0 && sampled < len(samples) {
					samples[sampled] = time.Since(started).Nanoseconds()
					sampled++
				}
			}
			b.StopTimer()
			if peer.signals.Load()-startedCalls != int32(b.N) || hook.calls.Load()-observedCalls != int64(b.N) {
				b.Fatal("comparison bypassed native interceptor, serialization or successful transport")
			}
			slices.Sort(samples[:sampled])
			for _, percentile := range []struct {
				fraction float64
				name     string
			}{{0.50, "p50-ns/op"}, {0.95, "p95-ns/op"}, {0.99, "p99-ns/op"}} {
				position := min(sampled-1, int(float64(sampled)*percentile.fraction))
				if position >= 0 {
					b.ReportMetric(float64(samples[position]), percentile.name)
				}
			}
			b.ReportMetric(float64(resident.HeapAlloc), "resident-heap-bytes")
			b.ReportMetric(float64(resident.HeapObjects), "resident-heap-objects")
			b.ReportMetric(float64(goroutines), "resident-goroutines")
			b.ReportMetric(float64(policy.NativeWorkBytes), "native-envelope-bytes")
			b.ReportMetric(float64(hook.calls.Load()-observedCalls)/float64(b.N), "nativecalls/op")
			b.ReportMetric(1, "records/op")
		})
	}
}
