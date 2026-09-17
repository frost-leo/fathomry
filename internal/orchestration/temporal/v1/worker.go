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
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	nativeworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// WorkflowRegistration preserves native names and per-definition version routing.
type WorkflowRegistration struct {
	Definition any
	Options    workflow.RegisterOptions
}

// ActivityRegistration accepts native functions or activity implementation structs.
type ActivityRegistration struct {
	Definition any
	Options    activity.RegisterOptions
}

// WorkerSpec contains runtime registrations/options, never durable configuration.
// Definitions, services, interceptors and tuner callbacks are borrowed for the
// whole Worker lifetime and must not be mutated while it is live. Applications
// must join goroutines they start outside SDK-managed execution.
//
// Workflow code is registered unchanged; no process admission/evidence code runs
// inside deterministic Workflow functions. Native Worker plugins receive scoped
// registration and single-use lifecycle continuations. Stop's continuation joins
// native work before returning; plugin teardown must release dependencies after
// that continuation and stop any producers that it owns before invoking it.
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
	// MaxHandlers bounds actual Activity/Local Activity/Nexus callback entries,
	// including Local Activities still running after native slot release.
	// Saturation refuses new handler entry; it never blocks task completion.
	MaxHandlers int
	// Bytes is the caller-declared native working envelope, at least one source
	// RPC envelope. It is not a limit on arbitrary Go application heap or RSS.
	Bytes int64
}

// WorkerResult distinguishes native startup, Stop return and confirmed local
// termination. None establishes remote Workflow completion or effect reversal.
type WorkerResult struct {
	private
	Namespace          string
	TaskQueue          string
	Started            bool
	NativeStopReturned bool
	Joined             bool
}

// TaskResult describes a process-side user callback, not an entire native task,
// its later conversion/response, a Workflow result, or an external business effect.
type TaskResult struct {
	private
	Kind                   string
	Namespace              string
	TaskQueue              string
	WorkflowID             string
	RunID                  string
	ActivityID             string
	ActivityRunID          string
	ActivityType           string
	NexusService           string
	NexusOperation         string
	NexusRequestID         string
	NexusCallbackRequested bool
	NexusRequestLinks      int
	Attempt                int32
	Local                  bool
	HandlerReturned        bool
	AsyncCompletion        bool
}

// Worker owns one accepted long-lived execution and its native dependencies.
// Composition calls Stop before closing the source assembly. Stop can also be
// called after Assembly.Close reports outstanding work; it requires no new lease.
type Worker struct {
	private
	client            *Executions
	call              *invocation.Call[WorkerResult]
	tasks             *invocation.Inbox[TaskResult]
	handlers          chan struct{}
	lifetime          context.Context
	cancel            context.CancelFunc
	spec              WorkerSpec
	correlation       fault.Correlation
	stopOnce          sync.Once
	stopRequested     chan struct{}
	started           chan struct{}
	done              chan struct{}
	mu                sync.Mutex
	status            WorkerResult
	startError        error
	fatalError        error
	cleanupError      error
	transportOnce     sync.Once
	connection        *grpc.ClientConn
	transportLifetime transportLifetime
}

func (*Worker) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

func (*WorkerSpec) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

// StartWorker admits a Worker lifetime before dialing, registering or polling.
// lifetime must be caller-owned and cancellable. ctx bounds startup observation;
// a canceled startup wait requests Stop but returns the non-nil owned Worker.
// Even startup errors can have started work: retain the handle and observe Stop.
func (client *Executions) StartWorker(ctx, lifetime context.Context, correlation fault.Correlation, spec WorkerSpec, inbox *invocation.Inbox[WorkerResult], tasks *invocation.Inbox[TaskResult]) (*Worker, error) {
	if client == nil || client.owner == nil || ctx == nil || lifetime == nil || lifetime.Done() == nil || inbox == nil || tasks == nil ||
		!validText(spec.TaskQueue, 255) || spec.MaxHandlers < 1 || spec.MaxHandlers > 1024 || spec.Bytes < client.owner.settings.reservation() ||
		len(spec.Workflows)+len(spec.Activities) > 256 || len(spec.NexusServices) > 32 || client.access.Limits().MaxLeases < 3 {
		return nil, failure(ErrInput, "worker-spec")
	}
	if err := validateWorkerOptions(spec.Options); err != nil {
		return nil, err
	}
	combined := client.owner.combinedWorkerPlugins()
	if len(combined)+len(spec.Options.Plugins) > 32 {
		return nil, failure(ErrInput, "worker-plugin-count")
	}
	spec.Options.Plugins = append(combined, spec.Options.Plugins...)
	if lifetime.Err() != nil {
		return nil, failure(ErrWorker, "worker-lifetime", lifetime.Err(), context.Cause(lifetime))
	}
	spec.Workflows = slices.Clone(spec.Workflows)
	spec.Activities = slices.Clone(spec.Activities)
	spec.NexusServices = slices.Clone(spec.NexusServices)
	spec.Options = copyWorkerOptions(spec.Options)
	call, err := invocation.Begin(ctx, client.access, invocation.Request{Name: "worker.run", Correlation: correlation, Shape: invocation.Session,
		Bytes: spec.Bytes, EvidenceBytes: ExecutionEvidenceBytes, Admission: invocation.Budget{Limit: client.owner.settings.AdmissionTimeout}}, inbox, client.observer)
	if err != nil {
		return nil, err
	}
	owned, cancel := context.WithCancel(lifetime)
	worker := &Worker{client: client, call: call, tasks: tasks, handlers: make(chan struct{}, spec.MaxHandlers),
		lifetime: owned, cancel: cancel, spec: spec, correlation: correlation, stopRequested: make(chan struct{}), started: make(chan struct{}), done: make(chan struct{}),
		status: WorkerResult{Namespace: client.Namespace(), TaskQueue: spec.TaskQueue}}
	go worker.run(ctx)
	select {
	case <-worker.started:
		worker.mu.Lock()
		err := worker.startError
		worker.mu.Unlock()
		return worker, err
	case <-ctx.Done():
		worker.requestStop()
		return worker, failure(ErrWorker, "worker-start-wait", ctx.Err(), context.Cause(ctx))
	}
}

// Status returns a value snapshot; it does not expose mutable native handles.
func (worker *Worker) Status() WorkerResult {
	if worker == nil {
		return WorkerResult{}
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.status
}

func (worker *Worker) Receipt() *invocation.Receipt[WorkerResult] {
	if worker == nil {
		return nil
	}
	return worker.call.Receipt()
}

func (worker *Worker) requestStop() { worker.stopOnce.Do(func() { close(worker.stopRequested) }) }

// Stop requests only this Worker's shutdown, then observes its owned join.
// Cancellation stops waiting, not cleanup. Dependencies remain leased until
// native task, callback and cache cleanup is positively observed.
func (worker *Worker) Stop(ctx context.Context) error {
	if worker == nil || ctx == nil {
		return failure(ErrInput, "worker-stop")
	}
	worker.requestStop()
	select {
	case <-worker.done:
		result, err := worker.call.Receipt().WaitReleased(ctx)
		if err != nil {
			return err
		}
		return result.Err()
	case <-ctx.Done():
		return failure(ErrCleanup, "worker-stop-wait", ctx.Err(), context.Cause(ctx))
	}
}

func contained(operation string, run func() error) (err error) {
	done := make(chan error, 1)
	go func() {
		returned := false
		var err error
		defer func() {
			if !returned {
				value := recover()
				cause, _ := value.(error)
				err = failure(ErrWorker, operation, cause)
			}
			done <- err
		}()
		err = run()
		returned = true
	}()
	return <-done
}

func (worker *Worker) run(startContext context.Context) {
	defer close(worker.done)
	defer worker.cancel()
	var native nativeworker.Worker
	var nativeClient sdk.Client
	plugins := &workerPlugins{worker: worker}
	startErr := contained("worker-construct", func() error {
		// Client interception belongs to the admitted source client, not this
		// private polling connection. Its Worker halves are installed below.
		options := worker.client.owner.pollingOptions(worker.transport, &worker.transportLifetime)
		startup, cancel, err := (invocation.Budget{Limit: worker.client.owner.settings.ConnectTimeout}).Context(startContext, invocation.Establish)
		if err != nil {
			return err
		}
		defer cancel()
		nativeClient, err = sdk.DialContext(startup, options)
		if err != nil {
			return err
		}
		optionsWorker, err := plugins.prepare(worker.spec.Options)
		if err != nil {
			return err
		}
		native, err = nativeworker.FathomryNewWithEagerClientV1(nativeClient, worker.client.owner.native, worker.spec.TaskQueue, optionsWorker)
		if err != nil {
			return err
		}
		registry := &workerRegistry{owner: plugins, native: native}
		defer registry.close()
		for _, registration := range worker.spec.Workflows {
			registry.RegisterWorkflowWithOptions(registration.Definition, registration.Options)
		}
		for _, registration := range worker.spec.Activities {
			registry.RegisterActivityWithOptions(registration.Definition, registration.Options)
		}
		if worker.spec.DynamicWorkflow != nil {
			registry.RegisterDynamicWorkflow(worker.spec.DynamicWorkflow, worker.spec.DynamicWorkflowOptions)
		}
		if worker.spec.DynamicActivity != nil {
			registry.RegisterDynamicActivity(worker.spec.DynamicActivity, worker.spec.DynamicActivityOptions)
		}
		for _, service := range worker.spec.NexusServices {
			registry.RegisterNexusService(service)
		}
		return plugins.start(0, context.Background(), native)
	})
	worker.mu.Lock()
	worker.startError = startErr
	worker.mu.Unlock()
	close(worker.started)
	if startErr == nil {
		select {
		case <-worker.stopRequested:
		case <-worker.lifetime.Done():
		}
	}
	if native != nil {
		var joinErr error
		stopErr := plugins.stop(0, context.Background(), func() {
			nativeErr := contained("worker-stop", func() error { native.Stop(); return nil })
			worker.mu.Lock()
			worker.cleanupError = errors.Join(worker.cleanupError, nativeErr)
			worker.status.NativeStopReturned = nativeErr == nil
			worker.mu.Unlock()
			// The supervisor owns this wait independently of canceled observers
			// and the Activity cancellation context. Plugin teardown follows it.
			joinErr = nativeworker.FathomryWaitStoppedV1(context.Background(), native)
		})
		worker.mu.Lock()
		worker.cleanupError = errors.Join(worker.cleanupError, stopErr)
		worker.mu.Unlock()
		if joinErr != nil {
			worker.mu.Lock()
			worker.cleanupError = errors.Join(worker.cleanupError, joinErr)
			worker.mu.Unlock()
			worker.call.Resolve(invocation.Outcome[WorkerResult]{Value: worker.Status(), Present: true, Primary: startErr, Cleanup: worker.cleanupError})
			worker.call.Finish(nil)
			return
		}
	}
	if plugins.release != nil {
		plugins.release()
	}
	worker.transportLifetime.close(func() {
		if nativeClient != nil {
			nativeClient.Close()
		} else if worker.connection != nil {
			_ = worker.connection.Close()
		}
	})
	worker.mu.Lock()
	worker.status.Joined = true
	status, primary, cleanup := worker.status, errors.Join(worker.startError, worker.fatalError), worker.cleanupError
	worker.mu.Unlock()
	worker.call.Complete(invocation.Outcome[WorkerResult]{Value: status, Present: true, Primary: primary, Cleanup: cleanup})
}

func (worker *Worker) transport(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, next grpc.UnaryInvoker, options ...grpc.CallOption) error {
	worker.transportOnce.Do(func() { worker.connection = connection })
	ctx = metadata.NewOutgoingContext(ctx, nil)
	return boundedNativeRPC(ctx, worker.client.owner.settings, method, request, reply, connection, next, options...)
}

type taskBindingKey struct{}

type taskRawKey struct{}

type taskBinding struct {
	worker      *Worker
	scope       invocation.Scope
	correlation fault.Correlation
	closed      atomic.Bool
}
