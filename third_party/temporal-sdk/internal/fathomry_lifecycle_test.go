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

package internal

import (
	"context"
	"testing"
	"testing/synctest"

	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/internal/common/metrics"
	internallog "go.temporal.io/sdk/internal/log"
	"google.golang.org/grpc"
)

func TestFathomryLateEagerResponseReleasesPermit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lifetime := newFathomryWorkerLifetime()
		slots, err := NewFixedSizeSlotSupplier(1)
		if err != nil {
			t.Fatal(err)
		}
		base := newBaseWorker(baseWorkerOptions{
			fathomryLifetime: lifetime, slotSupplier: slots, maxTaskPerSecond: 1,
			logger: internallog.NewNopLogger(), metricsHandler: metrics.NopHandler,
		})
		defer base.limiterContextCancel()
		defer base.taskLimiterContextCancel()
		permit := base.tryReserveSlot()
		if permit == nil {
			t.Fatal("eager reservation was rejected")
		}
		lifetime.closeExternal()
		close(base.stopCh)
		joined := make(chan struct{})
		go func() { lifetime.work.Wait(); close(joined) }()
		synctest.Wait()
		select {
		case <-joined:
			t.Fatal("reserved eager response lost ownership")
		default:
		}
		base.pushEagerTask(eagerTask{permit: permit})
		synctest.Wait()
		select {
		case <-joined:
		default:
			t.Fatal("late eager response did not release its permit")
		}
		if base.slotSupplier.issuedSlotsAtomic.Load() != 0 {
			t.Fatal("late slot remained issued")
		}
		if base.tryReserveSlot() != nil {
			t.Fatal("retired eager admission reopened")
		}
		if len(base.eagerTaskQueueCh) != 0 {
			t.Fatal("late task was abandoned in stopped queue")
		}
	})
}

type fathomryHeartbeatService struct {
	workflowservice.WorkflowServiceClient
}

func (fathomryHeartbeatService) RecordWorkerHeartbeat(context.Context, *workflowservice.RecordWorkerHeartbeatRequest, ...grpc.CallOption) (*workflowservice.RecordWorkerHeartbeatResponse, error) {
	return &workflowservice.RecordWorkerHeartbeatResponse{}, nil
}

func TestFathomryHeartbeatSnapshotRetainsRemovedWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lifetime := newFathomryWorkerLifetime()
		entered, released, sent := make(chan struct{}), make(chan struct{}), make(chan struct{})
		shared := &sharedNamespaceWorker{
			client:    &WorkflowClient{workflowService: fathomryHeartbeatService{}, capabilities: &workflowservice.GetSystemInfoResponse_Capabilities{}},
			namespace: "test", workerCtx: t.Context(), logger: internallog.NewNopLogger(),
			callbacks: map[string]func() *workerpb.WorkerHeartbeat{
				"retiring": func() *workerpb.WorkerHeartbeat { close(entered); <-released; return &workerpb.WorkerHeartbeat{} },
				"peer":     func() *workerpb.WorkerHeartbeat { return &workerpb.WorkerHeartbeat{} },
			},
			fathomryLifetimes: map[string]*fathomryWorkerLifetime{"retiring": lifetime},
		}
		manager := &heartbeatManager{workers: map[string]*sharedNamespaceWorker{"test": shared}}
		go func() {
			defer close(sent)
			if err := shared.sendHeartbeats(); err != nil {
				t.Error(err)
			}
		}()
		<-entered
		manager.unregisterWorker(&AggregatedWorker{workerInstanceKey: "retiring", executionParams: workerExecutionParameters{Namespace: "test"}})
		joined := make(chan struct{})
		go func() { lifetime.work.Wait(); close(joined) }()
		synctest.Wait()
		select {
		case <-joined:
			t.Fatal("copied heartbeat callback lost retired worker ownership")
		default:
		}
		if len(shared.callbacks) != 1 {
			t.Fatal("live peer heartbeat callback was removed")
		}
		close(released)
		synctest.Wait()
		<-sent
		select {
		case <-joined:
		default:
			t.Fatal("completed heartbeat batch retained ownership")
		}
	})
}
