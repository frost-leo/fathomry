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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nativeworker "go.temporal.io/sdk/worker"
)

func TestFollowBorrowPeersRetainExactRetiredGeneration(t *testing.T) {
	first := newTestFixture(t, NativeOptions{}, nil)
	second := newTestFixture(t, NativeOptions{}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	scope, err := resource.New(context.Background(), resource.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := scope.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	ref, err := resource.Bind(scope, resource.Binding[int, Handle]{
		Name: "temporal", Policy: resource.Follow,
		Select: func(view settings.View) (int, error) {
			snapshot, err := settings.As[int](view)
			if err != nil {
				return 0, err
			}
			return snapshot.ValueCopy()
		},
		Clone: func(value int) int { return value },
		Equal: func(left, right int) bool { return left == right },
		Build: func(_ context.Context, generation int) (*resource.Instance[Handle], error) {
			owner := first.owner
			if generation == 2 {
				owner = second.owner
			}
			return &resource.Instance[Handle]{Value: owner.Handle(), Release: owner.Release}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	apply := func(value int) {
		snapshot, err := settings.New(value, func(value int) int { return value })
		if err != nil {
			t.Fatal(err)
		}
		update, err := scope.Apply(ctx, snapshot.View())
		if err != nil {
			t.Fatal(err)
		}
		if err := update.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	apply(1)
	binding, err := Using(context.Background(), ref, first.owner.state.policy.Budget, first.dependencies)
	if err != nil {
		t.Fatal(err)
	}
	original, err := binding.Retain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close(context.Background())
	peer, err := original.Borrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close(context.Background())
	apply(2)
	current, err := binding.Retain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close(context.Background())
	if original.Attribution().Generation != 1 || peer.Attribution().Generation != 1 || current.Attribution().Generation != 2 {
		t.Fatal("retention did not bind an exact generation")
	}
	if err := original.Close(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := ref.Inspect()
	if err != nil || status.Borrowers != 2 || status.Retiring != 1 {
		t.Fatal("closing original retention dropped its live peer's generation", status, err)
	}
	if err := peer.SignalWorkflow(ctx, "workflow", "run", "old", nil); err != nil {
		t.Fatal("retired peer was revoked or silently followed", err)
	}
	if err := current.SignalWorkflow(ctx, "workflow", "run", "new", nil); err != nil {
		t.Fatal("new retention did not use current generation", err)
	}
	if first.server.signals.Load() != 1 || second.server.signals.Load() != 1 || first.owner.state.call.Context().Err() != nil {
		t.Fatal("Follow retargeted a peer or started old-owner shutdown prematurely")
	}
	if err := peer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := first.owner.state.call.Receipt().WaitReleased(ctx)
	if err != nil {
		t.Fatal("last old-generation use did not permit exact source cleanup", err)
	}
	result, present := snapshot.ValueCopy()
	if !present || !result.SourceReleased {
		t.Fatal("old generation release lacked physical-source evidence")
	}
}

type lifetimeWorkerPlugin struct {
	nativeworker.PluginBase
	entered chan struct{}
	release chan struct{}
	cause   error
}

func (*lifetimeWorkerPlugin) Name() string { return "lifetime-test" }

func (plugin *lifetimeWorkerPlugin) StartWorker(context.Context, nativeworker.PluginStartWorkerOptions, func(context.Context, nativeworker.PluginStartWorkerOptions) error) error {
	close(plugin.entered)
	<-plugin.release
	return plugin.cause
}

func TestClientCloseJoinsIndependentWorkerAliasAfterStartupWaitEnds(t *testing.T) {
	fixture := newTestFixture(t, NativeOptions{}, nil)
	parent, err := fixture.owner.Client().Borrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	plugin := &lifetimeWorkerPlugin{entered: make(chan struct{}), release: make(chan struct{}), cause: errors.New("intentional stop before polling")}
	unblock := sync.OnceFunc(func() { close(plugin.release) })
	defer unblock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	startContext, stopStart := context.WithCancel(ctx)
	type started struct {
		worker *Worker
		err    error
	}
	startResult := make(chan started, 1)
	go func() {
		worker, err := parent.StartWorker(startContext, lifetime, WorkerSpec{TaskQueue: "lifetime", MaxHandlers: 1, Options: nativeworker.Options{Plugins: []nativeworker.Plugin{plugin}}})
		startResult <- started{worker, err}
	}()
	select {
	case <-plugin.entered:
	case <-ctx.Done():
		t.Fatal("Worker plugin did not enter", ctx.Err())
	}
	stopStart()
	var result started
	select {
	case result = <-startResult:
	case <-ctx.Done():
		t.Fatal("canceled startup observation did not return", ctx.Err())
	}
	if result.worker == nil || !errors.Is(result.err, context.Canceled) {
		t.Fatal("partial Worker lost its independent owner", result.err)
	}
	wait, stopWait := context.WithTimeout(ctx, 10*time.Millisecond)
	err = parent.Close(wait)
	stopWait()
	if !errors.Is(err, context.DeadlineExceeded) || parent.Closed() || result.worker.Status().Joined {
		t.Fatal("parent declared completion before its Worker alias joined", err)
	}
	unblock()
	if err := result.worker.Stop(ctx); !errors.Is(err, plugin.cause) || !result.worker.Status().Joined {
		t.Fatal("native partial-start failure or join was lost", err)
	}
	if err := parent.Close(ctx); err != nil || !parent.Closed() {
		t.Fatal("repeat parent cleanup did not join its completed Worker", err)
	}
	if err := fixture.owner.Client().SignalWorkflow(ctx, "workflow", "run", "peer", nil); err != nil {
		t.Fatal("closing Worker parent revoked an independent source peer", err)
	}
}

func TestCleanupCompletionWaitsForItsErrorPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempt cleanupAttempt
		var complete atomic.Bool
		var calls atomic.Int32
		entered, release := make(chan struct{}), make(chan struct{})
		original := errors.New("exact cleanup failure")
		work := func() error {
			calls.Add(1)
			complete.Store(true)
			close(entered)
			<-release
			return original
		}
		first, second := make(chan error, 1), make(chan error, 1)
		go func() { first <- attempt.run(context.Background(), complete.Load, work) }()
		<-entered
		go func() { second <- attempt.run(context.Background(), complete.Load, work) }()
		synctest.Wait()
		select {
		case err := <-second:
			t.Fatal("complete flag hid unpublished cleanup error", err)
		default:
		}
		close(release)
		if err := <-first; !errors.Is(err, original) {
			t.Fatal("first cleanup lost cause", err)
		}
		if err := <-second; !errors.Is(err, original) {
			t.Fatal("concurrent cleanup lost cause", err)
		}
		if err := attempt.run(context.Background(), complete.Load, work); !errors.Is(err, original) || calls.Load() != 1 {
			t.Fatal("repeat completed cleanup changed its exact result", err)
		}
	})
}

func TestWithIDPreservesCorrelationWithoutCollidingWithNestedNativeCall(t *testing.T) {
	for _, test := range []struct{ name, id string }{
		{name: "ascii", id: "owner-call"},
		{name: "wide-public-id", id: "request / " + strings.Repeat("\u56de", 80)},
	} {
		t.Run(test.name, func(t *testing.T) { checkNestedCorrelation(t, test.id) })
	}
}

func checkNestedCorrelation(t *testing.T, id string) {
	t.Helper()
	fixture := newTestFixture(t, NativeOptions{}, nil)
	client, err := fixture.owner.Client().WithID(id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	visits := 0
	err = client.WalkHistory(ctx, "workflow", "run", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT, func(work context.Context, _ *historypb.HistoryEvent) error {
		visits++
		return client.SignalWorkflow(work, "workflow", "run", "nested", nil)
	})
	if err != nil || visits != 1 || fixture.server.signals.Load() != 1 {
		t.Fatal("one correlation view collided with its nested native parent", err)
	}
	var parent uint64
	for index := range 2 {
		delivery, err := fixture.dependencies.Evidence.NextReleased(ctx)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := delivery.Receipt()
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := receipt.WaitReleased(ctx)
		if err != nil || snapshot.Err() != nil || snapshot.Info().ID != id {
			t.Fatal("native correlation repair lost caller-facing attribution", err)
		}
		if index == 0 {
			parent = snapshot.Info().Sequence
		} else if snapshot.Info().Parent != parent {
			t.Fatal("visitor child lost actual operation parent")
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSourceSetupAdmissionDoesNotBecomeItsAcceptedLifetime(t *testing.T) {
	fixture := newTestFixture(t, NativeOptions{}, func(settings *Settings, _ *Policy) {
		if settings != nil {
			settings.MaxActive = 1
			settings.QueuedCalls = 1
			settings.AdmissionTimeout = 20 * time.Millisecond
		}
	})
	blockers, _ := adapters.NewInbox[struct{}](adapters.EvidenceOptions{Capacity: 1, MaxBytes: 1})
	endpoint, err := adapters.Bind(fixture.dependencies.Runtime, adapters.Declaration[struct{}]{Evidence: blockers, Copy: func(value struct{}) struct{} { return value }})
	if err != nil {
		t.Fatal(err)
	}
	var held *adapters.Call[struct{}]
	var guard adapters.Guard
	_, err = endpoint.Run(context.Background(), adapters.Request{Operation: "test.block", WorkBytes: 1, EvidenceBytes: 1}, func(call *adapters.Call[struct{}]) {
		held = call
		guard, err = call.Hold()
	})
	if err != nil {
		t.Fatal(err)
	}
	unblock := sync.OnceFunc(func() { _ = held.Resolve(adapters.Outcome[struct{}]{Present: true}); _ = guard.Release() })
	defer unblock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	owner, err := fixture.owner.state.prepared.OpenFrom(ctx, fixture.owner.Client(), fixture.dependencies)
	if owner != nil {
		_ = owner.Close(context.Background())
	}
	if owner != nil || !errors.Is(err, context.DeadlineExceeded) || time.Since(started) >= time.Second || ctx.Err() != nil {
		t.Fatal("source setup queue ignored its own admission phase", err)
	}
	unblock()
	owner, err = fixture.owner.state.prepared.OpenFrom(ctx, fixture.owner.Client(), fixture.dependencies)
	if err != nil || owner == nil {
		t.Fatal("released source admission did not permit native construction", err)
	}
	defer owner.Close(context.Background())
	deadline, present := owner.state.call.Context().Deadline()
	expected, expectedPresent := ctx.Deadline()
	if !present || !expectedPresent || !deadline.Equal(expected) {
		t.Fatal("source admission timeout leaked into accepted lifetime")
	}
}
