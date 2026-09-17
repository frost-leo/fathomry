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
	"errors"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	internallog "go.temporal.io/sdk/internal/log"
	"google.golang.org/grpc"
)

var eagerReviewStoreError = errors.New("controlled outbound storage failure")

type eagerReviewStorage struct {
	fail   bool
	stores int
}

func (*eagerReviewStorage) Name() string { return "eager-review" }
func (*eagerReviewStorage) Type() string { return "memory-test" }
func (driver *eagerReviewStorage) Store(_ converter.StorageDriverStoreContext, payloads []*commonpb.Payload) ([]converter.StorageDriverClaim, error) {
	driver.stores++
	if driver.fail {
		return nil, eagerReviewStoreError
	}
	claims := make([]converter.StorageDriverClaim, len(payloads))
	for index := range claims {
		claims[index] = converter.StorageDriverClaim{ClaimData: map[string]string{"test": "stored"}}
	}
	return claims, nil
}
func (*eagerReviewStorage) Retrieve(converter.StorageDriverRetrieveContext, []converter.StorageDriverClaim) ([]*commonpb.Payload, error) {
	return nil, errors.New("unexpected test retrieval")
}

type eagerReviewPeer struct {
	workflowservice.WorkflowServiceClient
	starts int
	eager  bool
}

func (peer *eagerReviewPeer) StartWorkflowExecution(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
	peer.starts++
	peer.eager = request.GetRequestEagerExecution()
	return &workflowservice.StartWorkflowExecutionResponse{RunId: "review-run"}, nil
}
func (*eagerReviewPeer) ShutdownWorker(context.Context, *workflowservice.ShutdownWorkerRequest, ...grpc.CallOption) (*workflowservice.ShutdownWorkerResponse, error) {
	return &workflowservice.ShutdownWorkerResponse{}, nil
}

type eagerReviewSlots struct {
	SlotSupplier
	permit   *SlotPermit
	reserved int
	released int
}

func (supplier *eagerReviewSlots) TryReserveSlot(info SlotReservationInfo) *SlotPermit {
	permit := supplier.SlotSupplier.TryReserveSlot(info)
	if permit != nil {
		supplier.permit = permit
		supplier.reserved++
	}
	return permit
}
func (supplier *eagerReviewSlots) ReleaseSlot(info SlotReleaseInfo) {
	supplier.released++
	supplier.SlotSupplier.ReleaseSlot(info)
}

func TestFathomryReviewEagerStorageFailureOwnership(t *testing.T) {
	for _, scenario := range []struct {
		name        string
		eager, fail bool
	}{
		{"non-eager-storage-failure", false, true},
		{"eager-storage-success", true, false},
		{"eager-storage-failure", true, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			driver := &eagerReviewStorage{fail: scenario.fail}
			peer := &eagerReviewPeer{}
			native := NewServiceClient(peer, nil, ClientOptions{
				Namespace: "review", Logger: internallog.NewNopLogger(),
				WorkerHeartbeatInterval: -1, DisableWorkerEnvironmentInfo: true,
				ExternalStorage: converter.ExternalStorage{Drivers: []converter.StorageDriver{driver}, PayloadSizeThreshold: 1},
			})
			native.capabilities = &workflowservice.GetSystemInfoResponse_Capabilities{EagerWorkflowStart: true}
			fixed, err := NewFixedSizeSlotSupplier(2)
			if err != nil {
				t.Fatal(err)
			}
			slots := &eagerReviewSlots{SlotSupplier: fixed}
			tuner, err := NewCompositeTuner(CompositeTunerOptions{WorkflowSlotSupplier: slots})
			if err != nil {
				t.Fatal(err)
			}
			managed := NewAggregatedWorker(native, "review", WorkerOptions{
				FathomryLifecycleV1: true, LocalActivityWorkerOnly: true, Tuner: tuner,
			})
			// Unit fixture registers only the eligible eager worker; no network or poll goroutines.
			native.eagerDispatcher.registerWorker(managed.workflowWorker)
			defer func() {
				// Repair only the observed leaked permit so a failing assertion cannot leak its join goroutine.
				if slots.reserved != slots.released {
					managed.workflowWorker.worker.releaseSlot(slots.permit, SlotReleaseReasonUnused)
				}
				managed.Stop()
				cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := managed.FathomryWaitStoppedV1(cleanup); err != nil {
					t.Errorf("fixture cleanup: %v", err)
				}
			}()
			_, err = native.ExecuteWorkflow(t.Context(), StartWorkflowOptions{
				ID: "review-workflow", TaskQueue: "review", EnableEagerStart: scenario.eager,
			}, "review-definition", "payload that must visit storage")
			if scenario.fail && !errors.Is(err, eagerReviewStoreError) {
				t.Fatalf("wrong storage error: %v", err)
			}
			if !scenario.fail && err != nil {
				t.Fatal(err)
			}
			if driver.stores != 1 {
				t.Fatalf("storage calls: got %d want 1", driver.stores)
			}
			expectedStarts := 1
			if scenario.fail {
				expectedStarts = 0
			}
			if peer.starts != expectedStarts {
				t.Fatalf("start RPC count: got %d want %d", peer.starts, expectedStarts)
			}
			if !scenario.fail && !peer.eager {
				t.Fatal("positive eager control did not request eager execution")
			}
			expectedReservations := 0
			if scenario.eager {
				expectedReservations = 1
			}
			if slots.reserved != expectedReservations {
				t.Fatalf("reservation count: got %d want %d", slots.reserved, expectedReservations)
			}
			if slots.released != expectedReservations {
				t.Errorf("eager permit leaked: reservations=%d releases=%d", slots.reserved, slots.released)
			}
			managed.Stop()
			observation, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			if err := managed.FathomryWaitStoppedV1(observation); err != nil {
				t.Errorf("native join cannot complete after returned start: %v", err)
			}
		})
	}
}
