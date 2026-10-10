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
	native "github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/activity"
	nativeworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// WorkflowRegistration passes deterministic Workflow code unchanged to the SDK.
type WorkflowRegistration struct {
	Definition any
	Options    workflow.RegisterOptions
}
type ActivityRegistration struct {
	Definition any
	Options    activity.RegisterOptions
}

// WorkerSpec is borrowed runtime authority, never loadable or durable data.
// Registration slices are copied; definitions, services and callbacks must stay
// valid until actual join. Bytes declares native work, not arbitrary Go heap.
type WorkerSpec struct {
	private
	TaskQueue              string
	Options                nativeworker.Options
	Workflows              []WorkflowRegistration
	Activities             []ActivityRegistration
	DynamicWorkflow        any
	DynamicWorkflowOptions workflow.DynamicRegisterOptions
	DynamicActivity        any
	DynamicActivityOptions activity.DynamicRegisterOptions
	NexusServices          []*nexus.Service
	MaxHandlers            int
	Bytes                  int64
}

// Worker owns a separate retained source use. Stop requests local shutdown and
// waits for actual SDK join; it never sends Workflow cancel/terminate commands.
type Worker struct {
	private
	native     *native.Worker
	client     *Client
	stopParent func() bool
}

func (client *Client) StartWorker(ctx, lifetime context.Context, spec WorkerSpec) (*Worker, error) {
	if client == nil || client.use == nil || ctx == nil || lifetime == nil || lifetime.Done() == nil {
		return nil, fail(ErrInput, "worker-start")
	}
	if err := client.use.enter(); err != nil {
		return nil, err
	}
	defer client.use.leave()
	policy := client.use.owner.policy
	if spec.Bytes == 0 {
		spec.Bytes = policy.NativeWorkerBytes
	}
	if spec.Bytes < policy.NativeWorkBytes || spec.Bytes > policy.NativeWorkerBytes {
		return nil, fail(ErrLimit, "worker-envelope")
	}
	owned, cancel := context.WithCancel(lifetime)
	stopParent := context.AfterFunc(client.use.context, cancel)
	if client.use.context.Err() != nil {
		cancel()
	}
	if err := client.use.generation.hold(); err != nil {
		stopParent()
		cancel()
		return nil, err
	}
	workerClient, err := client.use.owner.borrow(owned, client.use.endpoints, client.use.attribution.Generation, client.use.generation, func() error { stopParent(); cancel(); return nil }, client.use)
	if err != nil {
		_ = client.use.generation.drop()
		stopParent()
		cancel()
		return nil, err
	}
	workerClient = bindClient(workerClient.use, workerClient.native, workerClient.raw, client.id)
	nativeSpec := native.WorkerSpec{TaskQueue: spec.TaskQueue, Options: spec.Options,
		DynamicWorkflow: spec.DynamicWorkflow, DynamicWorkflowOptions: spec.DynamicWorkflowOptions,
		DynamicActivity: spec.DynamicActivity, DynamicActivityOptions: spec.DynamicActivityOptions,
		NexusServices: spec.NexusServices, MaxHandlers: spec.MaxHandlers, Bytes: spec.Bytes}
	for _, entry := range spec.Workflows {
		nativeSpec.Workflows = append(nativeSpec.Workflows, native.WorkflowRegistration{Definition: entry.Definition, Options: entry.Options})
	}
	for _, entry := range spec.Activities {
		nativeSpec.Activities = append(nativeSpec.Activities, native.ActivityRegistration{Definition: entry.Definition, Options: entry.Options})
	}
	value, err := workerClient.native.StartWorker(ctx, workerClient.use.context, client.correlation(), nativeSpec, client.use.owner.workers, client.use.owner.tasks)
	if value == nil {
		_ = workerClient.Close(context.Background())
		return nil, translate(err, "worker-start")
	}
	result := &Worker{native: value, client: workerClient, stopParent: stopParent}
	go func() {
		_, _ = value.Receipt().WaitReleased(context.Background())
		_ = workerClient.Close(context.Background())
	}()
	return result, translate(err, "worker-start")
}
func (worker *Worker) Status() WorkerResult {
	if worker == nil || worker.native == nil {
		return WorkerResult{}
	}
	return workerResult(worker.native.Status(), worker.client.Attribution())
}
func (worker *Worker) Stop(ctx context.Context) error {
	if worker == nil || worker.native == nil || ctx == nil {
		return fail(ErrInput, "worker-stop")
	}
	err := worker.native.Stop(ctx)
	if !worker.native.Status().Joined {
		return translate(err, "worker-stop")
	}
	cleanup := worker.client.Close(ctx)
	if err != nil {
		return translate(err, "worker-stop")
	}
	return cleanup
}
