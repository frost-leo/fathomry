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
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	nativeworker "go.temporal.io/sdk/worker"
)

func TestMergeActivityContextRetainsValuesAndBothCancellationSources(t *testing.T) {
	type key struct{}
	for _, source := range []string{"configured", "lifetime"} {
		t.Run(source, func(t *testing.T) {
			configured, cancelConfigured := context.WithCancelCause(context.WithValue(context.Background(), key{}, "configured-value"))
			defer cancelConfigured(nil)
			lifetime, cancelLifetime := context.WithCancelCause(context.Background())
			defer cancelLifetime(nil)
			merged, release := mergeActivityContext(configured, lifetime)
			defer release()
			if merged.Value(key{}) != "configured-value" {
				t.Fatal("configured value lost")
			}
			cause := errors.New("owned cancellation")
			if source == "configured" {
				cancelConfigured(cause)
			} else {
				cancelLifetime(cause)
			}
			select {
			case <-merged.Done():
			case <-time.After(time.Second):
				t.Fatal("merged context did not cancel")
			}
			if !errors.Is(context.Cause(merged), cause) {
				t.Fatal("cancellation cause lost")
			}
		})
	}
}

func TestMergeActivityContextPreservesEarlierDeadlineAndReleasesCallback(t *testing.T) {
	for _, source := range []string{"configured", "lifetime"} {
		t.Run(source, func(t *testing.T) {
			earlier := time.Now().Add(time.Hour)
			later := earlier.Add(time.Hour)
			first, second := earlier, later
			if source == "lifetime" {
				first, second = second, first
			}
			configured, cancelConfigured := context.WithDeadline(context.Background(), first)
			defer cancelConfigured()
			lifetime, cancelLifetime := context.WithDeadline(context.Background(), second)
			defer cancelLifetime()
			merged, release := mergeActivityContext(configured, lifetime)
			if deadline, ok := merged.Deadline(); !ok || !deadline.Equal(earlier) {
				t.Fatal("earliest deadline lost")
			}
			release()
			if !errors.Is(merged.Err(), context.Canceled) {
				t.Fatal("owned merged context was not released")
			}
			if lifetime.Err() != nil || configured.Err() != nil {
				t.Fatal("borrowed contexts were canceled")
			}
			cancelLifetime()
		})
	}
}

type reviewWorkerPlugin struct {
	nativeworker.PluginBase
	configure func(context.Context, nativeworker.PluginConfigureWorkerOptions) error
	start     func(context.Context, nativeworker.PluginStartWorkerOptions, func(context.Context, nativeworker.PluginStartWorkerOptions) error) error
}

func (*reviewWorkerPlugin) Name() string { return "independent-review" }

func (plugin *reviewWorkerPlugin) ConfigureWorker(ctx context.Context, options nativeworker.PluginConfigureWorkerOptions) error {
	return plugin.configure(ctx, options)
}

func (plugin *reviewWorkerPlugin) StartWorker(ctx context.Context, options nativeworker.PluginStartWorkerOptions, next func(context.Context, nativeworker.PluginStartWorkerOptions) error) error {
	if plugin.start != nil {
		return plugin.start(ctx, options, next)
	}
	return next(ctx, options)
}

func TestReviewWorkerConfigureKeepsCallbackSnapshotDetached(t *testing.T) {
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	managed := &Worker{lifetime: lifetime, stopRequested: make(chan struct{}), client: &Executions{owner: &connection{}}}
	owner := &workerPlugins{worker: managed}
	var retained *nativeworker.Options
	var calls int
	plugin := &reviewWorkerPlugin{configure: func(_ context.Context, options nativeworker.PluginConfigureWorkerOptions) error {
		calls++
		if retained == nil {
			retained = options.WorkerOptions
		}
		return nil
	}}
	supplied, err := owner.prepare(nativeworker.Options{Plugins: []nativeworker.Plugin{plugin}})
	if err != nil {
		t.Fatal(err)
	}
	hooks := nativeworker.PluginConfigureWorkerRegistryOptions{}
	if err := supplied.Plugins[0].ConfigureWorker(context.Background(), nativeworker.PluginConfigureWorkerOptions{
		WorkerInstanceKey: "review-key", TaskQueue: "review", WorkerOptions: &supplied, WorkerRegistryOptions: &hooks,
	}); err != nil {
		t.Fatal(err)
	}
	if owner.release != nil {
		defer owner.release()
	}
	if len(supplied.Interceptors) != 1 || supplied.OnFatalError == nil {
		t.Fatal("normal finalization control did not install guards")
	}
	if len(retained.Interceptors) != 0 || retained.OnFatalError != nil || len(retained.Plugins) != 0 {
		t.Errorf("callback snapshot acquired internal capabilities: interceptors=%d fatal=%t adapters=%d", len(retained.Interceptors), retained.OnFatalError != nil, len(retained.Plugins))
	}
	if len(retained.Plugins) != 0 {
		escaped := retained.Plugins[0]
		cancel()
		empty := nativeworker.Options{Plugins: []nativeworker.Plugin{escaped}}
		emptyHooks := nativeworker.PluginConfigureWorkerRegistryOptions{}
		_ = escaped.ConfigureWorker(context.Background(), nativeworker.PluginConfigureWorkerOptions{
			WorkerInstanceKey: "late", TaskQueue: "review", WorkerOptions: &empty, WorkerRegistryOptions: &emptyHooks,
		})
		if calls != 1 {
			t.Errorf("escaped native adapter reopened ConfigureWorker after lifetime cancellation: calls=%d", calls)
		}
	}
}

type reviewStaticArgument struct{ Value int }

func (reviewStaticArgument) HasValues() bool { return true }

func (argument reviewStaticArgument) Get(output ...any) error {
	if len(output) == 1 {
		if target, ok := output[0].(*int); ok {
			*target = argument.Value
			return nil
		}
	}
	return errors.New("wrong test output")
}

func reviewTypedActivity(_ context.Context, input reviewStaticArgument) (int, error) {
	return input.Value, nil
}

func TestReviewStaticEncodedValuesArgumentKeepsConcreteType(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	control := suite.NewTestActivityEnvironment()
	control.RegisterActivity(reviewTypedActivity)
	encoded, err := control.ExecuteActivity(reviewTypedActivity, reviewStaticArgument{Value: 73})
	if err != nil {
		t.Fatalf("native static-argument control: %v", err)
	}
	var value int
	if err := encoded.Get(&value); err != nil || value != 73 {
		t.Fatal("native control result changed")
	}

	prepared, err := resource.Prepare(resource.Schema[struct{}]{Format: 1},
		resource.Input{Identity: resource.Identity{Provider: "review.local", Name: "scope"}, Format: 1})
	if err != nil {
		t.Fatal(err)
	}
	selected := resource.WithLimits(resource.Select(prepared, func(context.Context, struct{}) (resource.Resource[struct{}], error) {
		return resource.Resource[struct{}]{Acquired: true, Capability: struct{}{}, Release: func(context.Context) resource.ReleaseResult {
			return resource.ReleaseResult{Quiescent: true, Released: true}
		}}, nil
	}), resource.Limits{Active: 1, Bytes: 4096, MaxLeases: 8})
	ctx := context.Background()
	assembly, err := resource.Assemble(ctx, ctx, "review", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := assembly.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	workers, _ := invocation.NewInbox[WorkerResult](1, ExecutionEvidenceBytes)
	tasks, _ := invocation.NewInbox[TaskResult](1, ExecutionEvidenceBytes)
	call, err := invocation.Begin(ctx, access, invocation.Request{Name: "worker.review", Shape: invocation.Session,
		Bytes: 4096, EvidenceBytes: ExecutionEvidenceBytes, Correlation: fault.Correlation{Call: "review-worker"},
		Admission: invocation.Budget{Limit: time.Second}}, workers, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer call.Complete(invocation.Outcome[WorkerResult]{Present: true})
	managed := &Worker{call: call, tasks: tasks, handlers: make(chan struct{}, 1), correlation: fault.Correlation{Call: "review-worker"},
		client: &Executions{owner: &connection{settings: settings{Namespace: "review", AdmissionTimeout: time.Second}}}}
	guarded := suite.NewTestActivityEnvironment()
	guarded.SetWorkerOptions(nativeworker.Options{Interceptors: []interceptor.WorkerInterceptor{&taskInterceptor{worker: managed}}})
	guarded.RegisterActivity(reviewTypedActivity)
	encoded, err = guarded.ExecuteActivity(reviewTypedActivity, reviewStaticArgument{Value: 73})
	if err != nil {
		t.Fatalf("managed guard rejected a valid native concrete Activity input: %v", err)
	}
	value = 0
	if err := encoded.Get(&value); err != nil || value != 73 {
		t.Fatal("managed result changed")
	}
}

type reviewNativeWorker struct {
	nativeworker.Worker
	starts, registrations int
}

func (native *reviewNativeWorker) Start() error { native.starts++; return nil }

func (native *reviewNativeWorker) RegisterActivityWithOptions(any, activity.RegisterOptions) {
	native.registrations++
}

func TestReviewHandledRegistryHookFailureLeavesEvidence(t *testing.T) {
	marker := errors.New("controlled registration hook failure")
	managed := &Worker{}
	owner := &workerPlugins{worker: managed, key: "review-key",
		hooks: nativeworker.PluginConfigureWorkerRegistryOptions{OnRegisterActivity: func(any, activity.RegisterOptions) { panic(marker) }}}
	plugin := &reviewWorkerPlugin{start: func(ctx context.Context, options nativeworker.PluginStartWorkerOptions, next func(context.Context, nativeworker.PluginStartWorkerOptions) error) error {
		func() {
			defer func() { _ = recover() }()
			options.WorkerRegistry.RegisterActivityWithOptions(func() error { return nil }, activity.RegisterOptions{Name: "review"})
		}()
		return next(ctx, options)
	}}
	owner.entries = []*workerPluginAdapter{{owner: owner, native: plugin}}
	native := &reviewNativeWorker{}
	err := owner.start(0, context.Background(), native)
	if native.starts != 1 || native.registrations != 0 {
		t.Fatal("registration rejection control did not exercise expected branch")
	}
	if err == nil && managed.startError == nil && managed.cleanupError == nil {
		t.Error("handled registration hook failure disappeared from direct and independent Worker evidence")
	}
}

type reviewBlockingNativeWorker struct {
	reviewNativeWorker
	entered  chan struct{}
	released chan struct{}
}

func (native *reviewBlockingNativeWorker) Start() error {
	close(native.entered)
	<-native.released
	return nil
}

func TestReviewActivityContextPreservesLifetimeDeadlineCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		marker := errors.New("owner lifetime deadline cause")
		lifetime, cancel := context.WithTimeoutCause(context.Background(), time.Second, marker)
		defer cancel()
		merged, release := mergeActivityContext(context.Background(), lifetime)
		defer release()
		<-merged.Done()
		synctest.Wait()
		if !errors.Is(context.Cause(merged), marker) {
			t.Errorf("merged background context discarded owner deadline cause: %v", context.Cause(merged))
		}
	})
}

func TestReviewRegistrySealsBeforeAwaitingAsyncStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		owner := &workerPlugins{worker: &Worker{}, key: "review-key"}
		native := &reviewBlockingNativeWorker{entered: make(chan struct{}), released: make(chan struct{})}
		var captured nativeworker.PluginStartWorkerOptions
		returned, nextDone, startDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		plugin := &reviewWorkerPlugin{start: func(ctx context.Context, options nativeworker.PluginStartWorkerOptions, next func(context.Context, nativeworker.PluginStartWorkerOptions) error) error {
			captured = options
			go func() { defer close(nextDone); _ = next(ctx, options) }()
			<-native.entered
			close(returned)
			return nil
		}}
		owner.entries = []*workerPluginAdapter{{owner: owner, native: plugin}}
		go func() { startDone <- owner.start(0, context.Background(), native) }()
		<-returned
		synctest.Wait()
		var refused any
		func() {
			defer func() { refused = recover() }()
			captured.WorkerRegistry.RegisterActivityWithOptions(func() error { return nil }, activity.RegisterOptions{Name: "late"})
		}()
		close(native.released)
		<-nextDone
		<-startDone
		if err, ok := refused.(error); !ok || !errors.Is(err, ErrAuthority) || native.registrations != 0 {
			t.Errorf("registry admitted work after plugin returned while next was joining: refusal=%v registrations=%d", refused, native.registrations)
		}
	})
}
