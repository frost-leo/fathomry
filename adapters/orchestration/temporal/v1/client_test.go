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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

func newCapabilityFixture(t *testing.T, peer workflowservice.WorkflowServiceServer, options NativeOptions, grants ...string) testFixture {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(server, peer)
	if health, ok := peer.(healthpb.HealthServer); ok {
		healthpb.RegisterHealthServer(server, health)
	}
	if operator, ok := peer.(operatorservice.OperatorServiceServer); ok {
		operatorservice.RegisterOperatorServiceServer(server, operator)
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() { server.Stop(); <-stopped })
	methods := make([]string, len(grants))
	for index, grant := range grants {
		methods[index] = grant
		if !strings.HasPrefix(grant, "/") {
			methods[index] = "/temporal.api.workflowservice.v1.WorkflowService/" + grant
		}
	}
	prepared, err := Prepare(Settings{Name: "capability", Endpoint: listener.Addr().String(), Namespace: "test",
		Plaintext: true, MaxActive: 4, InnerEvidenceCapacity: 128, FamilyLimit: 16,
		MaxRequestBytes: 64 << 10, MaxResponseBytes: 64 << 10, RPCs: methods}, options)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	runtime, err := adapters.New(lifetime, policy.Runtime)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	evidence, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	workers, err := adapters.NewInbox[WorkerResult](policy.Workers)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	tasks, err := adapters.NewInbox[TaskResult](policy.Tasks)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	dependencies := Dependencies{Runtime: runtime, Evidence: evidence, Workers: workers, Tasks: tasks}
	owner, err := prepared.Open(lifetime, dependencies)
	t.Cleanup(func() {
		cancel()
		wait, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if owner != nil {
			if err := owner.Close(wait); err != nil {
				t.Error("capability source cleanup", err)
			}
		}
		if err := runtime.Close(wait); err != nil {
			t.Error("capability runtime cleanup", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return testFixture{owner: owner, dependencies: dependencies, ctx: lifetime}
}

func capabilityEvidence(t *testing.T, fixture testFixture, operation string) Result {
	t.Helper()
	wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	delivery, err := fixture.dependencies.Evidence.NextReleased(wait)
	if err != nil {
		t.Fatal("missing independent capability evidence", operation, err)
	}
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(wait)
	if err != nil {
		t.Fatal(err)
	}
	value, present := snapshot.ValueCopy()
	if !present || value.Execution.Operation != operation {
		t.Fatalf("capability evidence operation=%q, want %q", value.Execution.Operation, operation)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestPublicClientCopiesPreserveUseWhileBorrowKeepsIndependentAuthority(t *testing.T) {
	fixture := newCapabilityFixture(t, &testServer{}, NativeOptions{})
	client := fixture.owner.Client()
	copied, err := client.WithID("copied-facade")
	if err != nil {
		t.Fatal(err)
	}
	borrowed, err := client.Borrow(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if copied.Attribution() != client.Attribution() || borrowed.Attribution().UseID == client.Attribution().UseID {
		t.Fatal("facade copy and retained Borrow identity were conflated")
	}
	if err := copied.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.SignalWorkflow(fixture.ctx, "workflow", "run", "signal", nil); !errors.Is(err, ErrState) {
		t.Fatal("closing a facade copy left the same use live", err)
	}
	if err := borrowed.SignalWorkflow(fixture.ctx, "workflow", "run", "signal", nil); err != nil {
		t.Fatal("closing one use revoked a peer", err)
	}
	value := capabilityEvidence(t, fixture, "workflow.signal")
	if value.Source != borrowed.Attribution() || !value.Execution.Accepted {
		t.Fatal("peer evidence lost its exact retained identity")
	}
	if err := borrowed.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
}

type clientCapabilityServer struct {
	*testServer
	operatorservice.UnimplementedOperatorServiceServer
	healthpb.UnimplementedHealthServer
	healthCalls, operatorCalls atomic.Int32
}

func (peer *clientCapabilityServer) Check(_ context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	peer.healthCalls.Add(1)
	if request.Service != "temporal.api.workflowservice.v1.WorkflowService" {
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVICE_UNKNOWN}, nil
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}
func (peer *clientCapabilityServer) ListNexusEndpoints(ctx context.Context, request *operatorservice.ListNexusEndpointsRequest) (*operatorservice.ListNexusEndpointsResponse, error) {
	peer.operatorCalls.Add(1)
	_ = grpc.SetHeader(ctx, metadata.Pairs("fixture-header", "present"))
	grpc.SetTrailer(ctx, metadata.Pairs("fixture-trailer", "present"))
	return &operatorservice.ListNexusEndpointsResponse{NextPageToken: append([]byte(nil), request.NextPageToken...)}, nil
}

func TestPublicClientHealthAndRawViewsKeepExplicitAuthorityAndOptions(t *testing.T) {
	peer := &clientCapabilityServer{testServer: &testServer{}}
	fixture := newCapabilityFixture(t, peer, NativeOptions{},
		"GetSystemInfo", "/temporal.api.operatorservice.v1.OperatorService/ListNexusEndpoints")
	client := fixture.owner.Client()
	if client.Namespace() != "test" {
		t.Fatal("Client namespace is not the prepared source")
	}
	response, err := client.CheckHealth(fixture.ctx, &sdk.CheckHealthRequest{})
	if err != nil || response == nil || peer.healthCalls.Load() != 1 {
		t.Fatal("native health contract changed", err)
	}
	if result := capabilityEvidence(t, fixture, "health"); !result.Execution.ResultObtained {
		t.Fatal("health observation lost independent evidence")
	}
	if _, err := client.WorkflowService().GetSystemInfo(fixture.ctx, &workflowservice.GetSystemInfoRequest{}); err != nil {
		t.Fatal(err)
	}
	if record := capabilityEvidence(t, fixture, ""); !record.RPC.Invoked || !record.RPC.Acknowledged || !strings.HasSuffix(record.RPC.Method, "/GetSystemInfo") {
		t.Fatal("raw workflow service evidence changed")
	}
	var headers, trailers metadata.MD
	result, err := client.OperatorService().ListNexusEndpoints(fixture.ctx, &operatorservice.ListNexusEndpointsRequest{NextPageToken: []byte("token")},
		grpc.Header(&headers), grpc.Trailer(&trailers), grpc.WaitForReady(true))
	if err != nil || string(result.NextPageToken) != "token" || len(headers.Get("fixture-header")) != 1 || len(trailers.Get("fixture-trailer")) != 1 {
		t.Fatal("raw operator call options/results changed", err)
	}
	if record := capabilityEvidence(t, fixture, ""); !record.RPC.Acknowledged || !strings.HasSuffix(record.RPC.Method, "/ListNexusEndpoints") {
		t.Fatal("raw operator evidence missing")
	}
	before, err := fixture.dependencies.Evidence.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.OperatorService().ListNexusEndpoints(fixture.ctx, &operatorservice.ListNexusEndpointsRequest{}, grpc.Header(nil)); !errors.Is(err, ErrInput) || peer.operatorCalls.Load() != 1 {
		t.Fatal("nil output call option reached transport", err)
	}
	after, err := fixture.dependencies.Evidence.Inspect()
	if err != nil || after.Outstanding != before.Outstanding {
		t.Fatal("pre-admission call-option refusal invented accepted evidence", err)
	}
	if _, err := client.WorkflowService().PollWorkflowTaskQueue(fixture.ctx, &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "test"}); !errors.Is(err, ErrAuthority) {
		t.Fatal("raw view gained implicit Worker polling", err)
	}
	if err := client.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.OperatorService().ListNexusEndpoints(fixture.ctx, &operatorservice.ListNexusEndpointsRequest{}); err == nil || peer.operatorCalls.Load() != 1 {
		t.Fatal("closed raw view retained operator authority", err)
	}
	var zero *Client
	if _, err := zero.WorkflowService().GetSystemInfo(context.Background(), &workflowservice.GetSystemInfoRequest{}); !errors.Is(err, ErrInput) {
		t.Fatal("nil workflow view leaked Internal error", err)
	}
	if _, err := zero.OperatorService().ListNexusEndpoints(context.Background(), &operatorservice.ListNexusEndpointsRequest{}); !errors.Is(err, ErrInput) {
		t.Fatal("nil operator view leaked Internal error", err)
	}
}
