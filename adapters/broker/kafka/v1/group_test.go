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

package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func waitAssignment(t testing.TB, group *Group, inbox *adapters.Inbox[Result], count int) {
	t.Helper()
	for {
		snapshot := group.Snapshot()
		if snapshot.Ready && len(snapshot.AssignmentsCopy()) == count {
			return
		}
		if snapshot.Err != nil {
			t.Fatal(snapshot.Err)
		}
		if _, err := group.Wait(testContext(t), snapshot.Revision); err != nil {
			t.Fatal(err)
		}
		ack(t, inbox)
	}
}

func TestPublicGroupChildrenDoNotQueueBehindRoot(t *testing.T) {
	peer := publicPeer(t)
	options := peerSettings(peer)
	options.MaxRecords = 1
	deps, inbox := testDependencies(t, options)
	owner := testOpen(t, options, deps)
	send(t, owner.Client(), Message{Topic: "records", Value: []byte("group")})
	ack(t, inbox)
	group, err := owner.Client().ConsumeGroup(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	waitAssignment(t, group, inbox, 2)
	result, err := group.Next(testContext(t))
	if err != nil || len(result.GroupBatch().Page().RecordsCopy()) != 1 {
		t.Fatal(err)
	}
	ack(t, inbox)
	if snapshot, resolved := group.Receipt().Snapshot(); resolved || snapshot.Info().Released {
		t.Fatal("live group root finalized")
	}
	commit, err := group.CommitBatch(testContext(t), result.GroupBatch())
	if err != nil || commit.CheckpointsCopy()[0].State != CheckpointCommitted {
		t.Fatal(err)
	}
	ack(t, inbox)
	if _, err := group.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
}

func TestSaturatedFinalizationUsesExistingReservations(t *testing.T) {
	peer := publicPeer(t)
	options := peerSettings(peer)
	options.MaxActive = 1
	options.MaxGroupSessions = 1
	policy, err := Recommend(options)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](adapters.EvidenceOptions{Capacity: 2, MaxBytes: policy.Evidence.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	owner := testOpen(t, options, Dependencies{Runtime: runtime, Evidence: inbox})
	group, err := owner.Client().ConsumeGroup(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := group.Next(testContext(t)); !errors.Is(err, adapters.ErrEvidence) {
		t.Fatal("saturated child admitted", err)
	}
	if _, err := group.Close(testContext(t)); err != nil {
		t.Fatal("group close needed a slot", err)
	}
	if err := owner.Close(testContext(t)); err != nil {
		t.Fatal("source close needed a slot", err)
	}
	if err := runtime.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, testContext(t), inbox)
}

func TestPublicLeaveFailureRemainsInIndependentRootEvidence(t *testing.T) {
	peer := publicPeer(t)
	options := peerSettings(peer)
	deps, inbox := testDependencies(t, options)
	owner := testOpen(t, options, deps)
	group, err := owner.Client().ConsumeGroup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waitAssignment(t, group, inbox, 2)
	peer.ControlKey(int16(kmsg.LeaveGroup), func(request kmsg.Request) (kmsg.Response, error, bool) {
		response := request.ResponseKind().(*kmsg.LeaveGroupResponse)
		response.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return response, nil, true
	})
	if _, err := group.Close(testContext(t)); !errors.Is(err, ErrCleanup) || !errors.Is(err, kerr.GroupAuthorizationFailed) {
		t.Fatal("public Close did not preserve native leave failure", err)
	}
	snapshot, err := group.Receipt().WaitReleased(testContext(t))
	if err != nil || snapshot.Primary() != nil || !errors.Is(snapshot.Cleanup(), ErrCleanup) || !errors.Is(snapshot.Cleanup(), kerr.GroupAuthorizationFailed) {
		t.Fatal("native leave cleanup was collapsed into primary or discarded", err)
	}
	delivery, err := inbox.NextReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	defer delivery.Retry()
	if err := owner.Close(testContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("remote leave refusal prevented confirmed local source release", err)
	}
	if root, err := delivery.Receipt(); err != nil {
		t.Fatal(err)
	} else if result, _ := root.Snapshot(); !errors.Is(result.Cleanup(), kerr.GroupAuthorizationFailed) {
		t.Fatal("independent inbox lost the closed group's remote uncertainty")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestPublicRetainedLatestEOFDoesNotSkipArrivalOnRebalance(t *testing.T) {
	peer := publicPeer(t)
	options := peerSettings(peer)
	options.InitialOffset = "latest"
	deps, inbox := testDependencies(t, options)
	owner := testOpen(t, options, deps)
	first, err := owner.Client().ConsumeGroup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(testContext(t))
	waitAssignment(t, first, inbox, 2)
	for range 2 {
		if _, err := first.Next(testContext(t)); !errors.Is(err, ErrUnavailable) {
			t.Fatal("empty partition did not establish its initial latest cursor", err)
		}
		ack(t, inbox)
	}
	send(t, owner.Client(), Message{Topic: "records", Partition: 0, Value: []byte("retained-latest")}, Message{Topic: "records", Partition: 1, Value: []byte("retained-latest")})
	ack(t, inbox)
	second, err := owner.Client().ConsumeGroup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(testContext(t))
	waitAssignment(t, second, inbox, 1)
	waitAssignment(t, first, inbox, 1)
	page, err := first.Next(testContext(t))
	ack(t, inbox)
	if err != nil || len(page.GroupBatch().Page().RecordsCopy()) != 1 || page.GroupBatch().Start().Offset != 0 {
		t.Fatal("cooperative retention reran latest and skipped arrival after the selected EOF", err)
	}
}
