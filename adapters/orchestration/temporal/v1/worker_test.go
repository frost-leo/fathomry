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
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	nativeworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
)

type categoryWorkerPeer struct {
	testServer
	descriptions   atomic.Int32
	polls          atomic.Int32
	remoteControls atomic.Int32
	startupError   error
	activityPoll   chan *workflowservice.PollActivityTaskQueueRequest
}

func (peer *categoryWorkerPeer) DescribeNamespace(context.Context, *workflowservice.DescribeNamespaceRequest) (*workflowservice.DescribeNamespaceResponse, error) {
	peer.descriptions.Add(1)
	if peer.startupError != nil {
		return nil, serviceerror.ToStatus(peer.startupError).Err()
	}
	return &workflowservice.DescribeNamespaceResponse{NamespaceInfo: &namespacepb.NamespaceInfo{Name: "test"}}, nil
}
func (peer *categoryWorkerPeer) PollWorkflowTaskQueue(ctx context.Context, _ *workflowservice.PollWorkflowTaskQueueRequest) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	peer.polls.Add(1)
	<-ctx.Done()
	return nil, ctx.Err()
}
func (peer *categoryWorkerPeer) PollActivityTaskQueue(ctx context.Context, request *workflowservice.PollActivityTaskQueueRequest) (*workflowservice.PollActivityTaskQueueResponse, error) {
	peer.polls.Add(1)
	if peer.activityPoll != nil {
		select {
		case peer.activityPoll <- proto.Clone(request).(*workflowservice.PollActivityTaskQueueRequest):
		default:
		}
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (peer *categoryWorkerPeer) PollNexusTaskQueue(ctx context.Context, _ *workflowservice.PollNexusTaskQueueRequest) (*workflowservice.PollNexusTaskQueueResponse, error) {
	peer.polls.Add(1)
	<-ctx.Done()
	return nil, ctx.Err()
}
func (peer *categoryWorkerPeer) RequestCancelWorkflowExecution(context.Context, *workflowservice.RequestCancelWorkflowExecutionRequest) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
	peer.remoteControls.Add(1)
	return &workflowservice.RequestCancelWorkflowExecutionResponse{}, nil
}
func (peer *categoryWorkerPeer) TerminateWorkflowExecution(context.Context, *workflowservice.TerminateWorkflowExecutionRequest) (*workflowservice.TerminateWorkflowExecutionResponse, error) {
	peer.remoteControls.Add(1)
	return &workflowservice.TerminateWorkflowExecutionResponse{}, nil
}

type categoryWorkerPlugin struct {
	nativeworker.PluginBase
	mu                       sync.Mutex
	registrations            []string
	starts, stops            atomic.Int32
	startCause               error
	stopEntered, stopRelease chan struct{}
}

func (*categoryWorkerPlugin) Name() string { return "public-category-worker" }
func (plugin *categoryWorkerPlugin) registered(name string) {
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	plugin.registrations = append(plugin.registrations, name)
}
func (plugin *categoryWorkerPlugin) ConfigureWorker(_ context.Context, options nativeworker.PluginConfigureWorkerOptions) error {
	options.WorkerRegistryOptions.OnRegisterWorkflow = func(_ any, options workflow.RegisterOptions) { plugin.registered("workflow:" + options.Name) }
	options.WorkerRegistryOptions.OnRegisterActivity = func(_ any, options activity.RegisterOptions) { plugin.registered("activity:" + options.Name) }
	options.WorkerRegistryOptions.OnRegisterDynamicWorkflow = func(any, workflow.DynamicRegisterOptions) { plugin.registered("dynamic-workflow") }
	options.WorkerRegistryOptions.OnRegisterDynamicActivity = func(any, activity.DynamicRegisterOptions) { plugin.registered("dynamic-activity") }
	options.WorkerRegistryOptions.OnRegisterNexusService = func(*nexus.Service) { plugin.registered("nexus") }
	return nil
}
func (plugin *categoryWorkerPlugin) StartWorker(ctx context.Context, options nativeworker.PluginStartWorkerOptions, next func(context.Context, nativeworker.PluginStartWorkerOptions) error) error {
	plugin.starts.Add(1)
	if err := next(ctx, options); err != nil {
		return err
	}
	return plugin.startCause
}
func (plugin *categoryWorkerPlugin) StopWorker(ctx context.Context, options nativeworker.PluginStopWorkerOptions, next func(context.Context, nativeworker.PluginStopWorkerOptions)) {
	plugin.stops.Add(1)
	if plugin.stopEntered != nil {
		close(plugin.stopEntered)
		<-plugin.stopRelease
	}
	next(ctx, options)
}

func categoryWorkerSpec(plugin *categoryWorkerPlugin) WorkerSpec {
	return WorkerSpec{TaskQueue: "public-worker", MaxHandlers: 2,
		Options: nativeworker.Options{MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2,
			MaxConcurrentActivityTaskPollers: 1, MaxConcurrentNexusTaskPollers: 1, WorkerStopTimeout: time.Millisecond, Plugins: []nativeworker.Plugin{plugin}},
		Workflows:  []WorkflowRegistration{{Definition: func(workflow.Context) (string, error) { return "workflow", nil }, Options: workflow.RegisterOptions{Name: "workflow"}}},
		Activities: []ActivityRegistration{{Definition: func(context.Context) (string, error) { return "activity", nil }, Options: activity.RegisterOptions{Name: "activity"}}}}
}

func categoryWorkerCleanup(t *testing.T, worker *Worker, release func()) {
	t.Helper()
	t.Cleanup(func() {
		if release != nil {
			release()
		}
		if worker == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = worker.Stop(ctx)
		if !worker.Status().Joined {
			t.Error("public Worker cleanup did not join")
		}
	})
}

func categoryWorkerEvidence(t *testing.T, fixture testFixture) (WorkerResult, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	delivery, err := fixture.dependencies.Workers.NextReleased(ctx)
	if err != nil {
		t.Fatal("missing independently released Worker record", err)
	}
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	value, present := snapshot.ValueCopy()
	if !present {
		t.Fatal("Worker evidence had no value")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	return value, snapshot.Err()
}

func TestPublicWorkerRegistrationAndFacadeParentOwnership(t *testing.T) {
	peer := &categoryWorkerPeer{activityPoll: make(chan *workflowservice.PollActivityTaskQueueRequest, 1)}
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	parent, err := fixture.owner.Client().Borrow(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	copy, err := parent.WithID("worker-facade")
	if err != nil {
		t.Fatal(err)
	}
	peerUse, err := fixture.owner.Client().Borrow(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	plugin := &categoryWorkerPlugin{}
	spec := categoryWorkerSpec(plugin)
	spec.DynamicWorkflow = func(workflow.Context, converter.EncodedValues) (string, error) { return "dynamic-workflow", nil }
	spec.DynamicActivity = func(context.Context, converter.EncodedValues) (string, error) { return "dynamic-activity", nil }
	service := nexus.NewService("public-worker-service")
	if err := service.Register(nexus.NewSyncOperation("operation", func(_ context.Context, input string, _ nexus.StartOperationOptions) (string, error) {
		return input, nil
	})); err != nil {
		t.Fatal(err)
	}
	spec.NexusServices = []*nexus.Service{service}
	worker, err := copy.StartWorker(ctx, fixture.ctx, spec)
	categoryWorkerCleanup(t, worker, nil)
	if err != nil || worker == nil || !worker.Status().Started {
		t.Fatal("registered public Worker did not start", err)
	}
	select {
	case request := <-peer.activityPoll:
		if request.Namespace != "test" || request.GetTaskQueue().GetName() != spec.TaskQueue {
			t.Fatal("Worker registration did not poll the selected native namespace/task queue")
		}
	case <-ctx.Done():
		t.Fatal("accepted public Worker never reached actual native polling")
	}
	plugin.mu.Lock()
	registered := slices.Clone(plugin.registrations)
	plugin.mu.Unlock()
	slices.Sort(registered)
	if !reflect.DeepEqual(registered, []string{"activity:activity", "dynamic-activity", "dynamic-workflow", "nexus", "workflow:workflow"}) {
		t.Fatal("public Worker registration families did not reach native registry", registered)
	}
	status := worker.Status()
	if status.Source.SourceID != parent.Attribution().SourceID || status.Source.UseID == parent.Attribution().UseID || status.Source.UseID == 0 || status.Namespace != "test" || status.TaskQueue != spec.TaskQueue {
		t.Fatal("Worker failed to acquire an independent use of its facade parent's exact source")
	}
	if err := peerUse.Close(ctx); err != nil || worker.Status().Joined {
		t.Fatal("closing an unrelated retained use stopped the Worker", err)
	}
	if err := parent.Close(ctx); err != nil || !copy.Closed() || !worker.Status().Joined {
		t.Fatal("facade-parent close did not join its independent Worker child", err)
	}
	final, err := categoryWorkerEvidence(t, fixture)
	if err != nil || !final.Started || !final.NativeStopReturned || !final.Joined || final.Source != status.Source || plugin.starts.Load() != 1 || plugin.stops.Load() != 1 {
		t.Fatal("registered Worker lifecycle evidence changed", err)
	}
	if peer.remoteControls.Load() != 0 {
		t.Fatal("local Worker shutdown sent remote Workflow cancel/terminate")
	}
	if err := fixture.owner.Client().SignalWorkflow(ctx, "workflow", "run", "still-live", nil); err != nil {
		t.Fatal("Worker parent closure revoked the source owner's peer", err)
	}
	capabilityEvidence(t, fixture, "workflow.signal")
}

func TestPublicWorkerPartialNativeStartRetainsOwnedHandleAndError(t *testing.T) {
	for _, mode := range []string{"native-namespace", "plugin-after-start"} {
		t.Run(mode, func(t *testing.T) {
			marker := serviceerror.NewPermissionDenied("controlled Worker startup", "")
			peer := &categoryWorkerPeer{}
			plugin := &categoryWorkerPlugin{}
			if mode == "native-namespace" {
				peer.startupError = marker
			} else {
				plugin.startCause = marker
			}
			fixture := newCapabilityFixture(t, peer, NativeOptions{})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			worker, err := fixture.owner.Client().StartWorker(ctx, fixture.ctx, categoryWorkerSpec(plugin))
			categoryWorkerCleanup(t, worker, nil)
			var denied *serviceerror.PermissionDenied
			if worker == nil || !errors.As(err, &denied) || denied.Message != "controlled Worker startup" {
				t.Fatal("partial startup lost its non-nil owner or native error", err)
			}
			if mode == "plugin-after-start" && !errors.Is(err, marker) {
				t.Fatal("post-start plugin error identity changed")
			}
			if peer.descriptions.Load() == 0 {
				t.Fatal("partial startup did not enter native namespace validation")
			}
			err = worker.Stop(ctx)
			if !errors.As(err, &denied) || !worker.Status().Joined || !worker.Status().NativeStopReturned {
				t.Fatal("failed startup was mistaken for completed cleanup", err)
			}
			value, evidenceErr := categoryWorkerEvidence(t, fixture)
			if !value.Joined || !errors.As(evidenceErr, &denied) || plugin.starts.Load() != 1 || plugin.stops.Load() != 1 {
				t.Fatal("failed Worker evidence dropped its startup error or actual join", evidenceErr)
			}
			if peer.remoteControls.Load() != 0 {
				t.Fatal("partial startup cleanup mutated remote Workflows")
			}
		})
	}
}

func TestPublicWorkerCanceledStopCannotClaimNativeJoin(t *testing.T) {
	peer := &categoryWorkerPeer{}
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	plugin := &categoryWorkerPlugin{stopEntered: make(chan struct{}), stopRelease: make(chan struct{})}
	release := sync.OnceFunc(func() { close(plugin.stopRelease) })
	worker, err := fixture.owner.Client().StartWorker(ctx, fixture.ctx, categoryWorkerSpec(plugin))
	categoryWorkerCleanup(t, worker, release)
	if err != nil {
		t.Fatal(err)
	}
	wait, stopWait := context.WithCancel(ctx)
	returned := make(chan error, 1)
	go func() { returned <- worker.Stop(wait) }()
	select {
	case <-plugin.stopEntered:
	case <-ctx.Done():
		t.Fatal("native Worker cleanup did not enter its stop plugin")
	}
	stopWait()
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("Stop did not retain canceled observation", err)
		}
	case <-ctx.Done():
		t.Fatal("Stop cancellation did not return")
	}
	if worker.Status().Joined || worker.Status().NativeStopReturned {
		t.Fatal("canceled Stop claimed join before its native stop continuation")
	}
	state, err := fixture.dependencies.Workers.Inspect()
	if err != nil || state.Outstanding != 1 {
		t.Fatal("unfinished Worker evidence custody was prematurely released", err)
	}
	if err := fixture.owner.Client().SignalWorkflow(ctx, "workflow", "run", "peer", nil); err != nil {
		t.Fatal("canceled Stop revoked independent source access", err)
	}
	capabilityEvidence(t, fixture, "workflow.signal")
	release()
	if err := worker.Stop(ctx); err != nil || !worker.Status().Joined {
		t.Fatal("later Stop did not resume exact native cleanup", err)
	}
	result, err := categoryWorkerEvidence(t, fixture)
	if err != nil || !result.Joined || !result.NativeStopReturned || plugin.stops.Load() != 1 || peer.remoteControls.Load() != 0 {
		t.Fatal("late Worker join evidence was incorrect", err)
	}
}

func TestPublicWorkerRejectsZeroInvalidAndClosedOrigins(t *testing.T) {
	peer := &categoryWorkerPeer{}
	fixture := newCapabilityFixture(t, peer, NativeOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var zero Worker
	var absent *Worker
	if !reflect.DeepEqual(zero.Status(), WorkerResult{}) || !reflect.DeepEqual(absent.Status(), WorkerResult{}) {
		t.Fatal("zero Worker status invented lifecycle facts")
	}
	for _, err := range []error{zero.Stop(ctx), absent.Stop(ctx), zero.Stop(nil)} {
		if !errors.Is(err, ErrInput) {
			t.Fatal("zero Worker Stop was not refused", err)
		}
	}
	var empty Client
	var nilClient *Client
	for _, client := range []*Client{&empty, nilClient} {
		if worker, err := client.StartWorker(ctx, fixture.ctx, WorkerSpec{}); worker != nil || !errors.Is(err, ErrInput) {
			t.Fatal("zero Client started a Worker", err)
		}
	}
	client := fixture.owner.Client()
	for _, scenario := range []struct {
		start, lifetime context.Context
		spec            WorkerSpec
		expected        error
	}{
		{nil, fixture.ctx, WorkerSpec{TaskQueue: "queue", MaxHandlers: 1}, ErrInput},
		{ctx, nil, WorkerSpec{TaskQueue: "queue", MaxHandlers: 1}, ErrInput},
		{ctx, context.Background(), WorkerSpec{TaskQueue: "queue", MaxHandlers: 1}, ErrInput},
		{ctx, fixture.ctx, WorkerSpec{MaxHandlers: 1}, ErrInput},
		{ctx, fixture.ctx, WorkerSpec{TaskQueue: "queue"}, ErrInput},
		{ctx, fixture.ctx, WorkerSpec{TaskQueue: "queue", MaxHandlers: 1, Bytes: 1}, ErrLimit},
	} {
		worker, err := client.StartWorker(scenario.start, scenario.lifetime, scenario.spec)
		if worker != nil || !errors.Is(err, scenario.expected) {
			t.Fatal("invalid Worker arguments acquired native ownership", err)
		}
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if worker, err := client.StartWorker(ctx, fixture.ctx, WorkerSpec{TaskQueue: "queue", MaxHandlers: 1}); worker != nil || !errors.Is(err, ErrState) {
		t.Fatal("closed origin started a Worker", err)
	}
	state, err := fixture.dependencies.Workers.Inspect()
	if err != nil || state.Outstanding != 0 || peer.descriptions.Load() != 0 || peer.polls.Load() != 0 {
		t.Fatal("invalid Worker input entered the native lifecycle", err)
	}
}
