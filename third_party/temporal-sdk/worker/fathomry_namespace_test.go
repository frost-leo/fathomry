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
	"net"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type namespaceCompletion struct{ namespace, value, token string }

type namespacePeer struct {
	workflowservice.UnimplementedWorkflowServiceServer
	fixtures    map[string]*service
	completions chan namespaceCompletion
	betaPolled  chan struct{}
	betaRelease chan struct{}
	betaOnce    sync.Once
}

func (*namespacePeer) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (peer *namespacePeer) DescribeNamespace(_ context.Context, request *workflowservice.DescribeNamespaceRequest) (*workflowservice.DescribeNamespaceResponse, error) {
	if peer.fixtures[request.Namespace] == nil {
		return nil, status.Error(codes.NotFound, "namespace")
	}
	return &workflowservice.DescribeNamespaceResponse{NamespaceInfo: &namespacepb.NamespaceInfo{Name: request.Namespace}}, nil
}

func (peer *namespacePeer) PollWorkflowTaskQueue(ctx context.Context, request *workflowservice.PollWorkflowTaskQueueRequest) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	fixture := peer.fixtures[request.Namespace]
	if fixture == nil {
		return nil, status.Error(codes.NotFound, "namespace")
	}
	if request.TaskQueue.GetName() != "probe" && request.TaskQueue.GetKind() != enumspb.TASK_QUEUE_KIND_STICKY {
		return nil, status.Error(codes.InvalidArgument, "task queue")
	}
	if request.Namespace == "beta" {
		peer.betaOnce.Do(func() { close(peer.betaPolled) })
		select {
		case <-peer.betaRelease:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	task, err := fixture.PollWorkflowTaskQueue(ctx, request)
	if task != nil {
		task.TaskToken = []byte(request.Namespace + "-token")
	}
	return task, err
}

func (peer *namespacePeer) RespondWorkflowTaskCompleted(_ context.Context, request *workflowservice.RespondWorkflowTaskCompletedRequest) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	for _, command := range request.Commands {
		if command.GetCommandType() != enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION {
			continue
		}
		var value string
		if err := converter.GetDefaultDataConverter().FromPayloads(command.GetCompleteWorkflowExecutionCommandAttributes().GetResult(), &value); err != nil {
			return nil, status.Error(codes.InvalidArgument, "result")
		}
		peer.completions <- namespaceCompletion{namespace: request.Namespace, value: value, token: string(request.TaskToken)}
	}
	return &workflowservice.RespondWorkflowTaskCompletedResponse{}, nil
}

func (*namespacePeer) ShutdownWorker(context.Context, *workflowservice.ShutdownWorkerRequest) (*workflowservice.ShutdownWorkerResponse, error) {
	return &workflowservice.ShutdownWorkerResponse{}, nil
}

func namespaceWorkflow(ctx workflow.Context) (string, error) {
	return workflow.GetInfo(ctx).Namespace, nil
}

func TestFathomryMultipleNamespacesShareBinaryAndConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer := &namespacePeer{fixtures: map[string]*service{
		"alpha": {runID: "alpha-run"}, "beta": {runID: "beta-run"},
	}, completions: make(chan namespaceCompletion, 4), betaPolled: make(chan struct{}), betaRelease: make(chan struct{})}
	server := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(server, peer)
	served := make(chan struct{})
	go func() { defer close(served); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-served })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	alpha, err := client.DialContext(ctx, client.Options{HostPort: listener.Addr().String(), Namespace: "alpha", Identity: "namespace-test", Logger: quietLogger{}, WorkerHeartbeatInterval: -1, DisableWorkerEnvironmentInfo: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(alpha.Close)
	beta, err := client.NewClientFromExistingWithContext(ctx, alpha, client.Options{Namespace: "beta", Identity: "namespace-test", Logger: quietLogger{}, WorkerHeartbeatInterval: -1, DisableWorkerEnvironmentInfo: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(beta.Close)
	var workers []worker.Worker
	for _, native := range []client.Client{alpha, beta} {
		w := worker.New(native, "probe", worker.Options{FathomryLifecycleV1: true, LocalActivityWorkerOnly: true, WorkerStopTimeout: 10 * time.Millisecond,
			MaxConcurrentWorkflowTaskExecutionSize: 2, MaxConcurrentWorkflowTaskPollers: 2, MaxConcurrentLocalActivityExecutionSize: 1})
		w.RegisterWorkflowWithOptions(namespaceWorkflow, workflow.RegisterOptions{Name: "local-probe"})
		t.Cleanup(func() { w.Stop(); assertJoined(t, w) })
		if err := w.Start(); err != nil {
			t.Fatal(err)
		}
		workers = append(workers, w)
	}
	await(t, peer.betaPolled, "second namespace did not poll")
	select {
	case result := <-peer.completions:
		if result != (namespaceCompletion{namespace: "alpha", value: "alpha", token: "alpha-token"}) {
			t.Fatal("first namespace execution/response crossed its binding")
		}
	case <-ctx.Done():
		t.Fatal("first namespace workflow did not complete")
	}
	workers[0].Stop()
	assertJoined(t, workers[0])
	alpha.Close()
	close(peer.betaRelease)
	select {
	case result := <-peer.completions:
		if result != (namespaceCompletion{namespace: "beta", value: "beta", token: "beta-token"}) {
			t.Fatal("second namespace execution/response crossed its binding")
		}
	case <-ctx.Done():
		t.Fatal("closing the first client/worker disrupted the shared connection")
	}
}
