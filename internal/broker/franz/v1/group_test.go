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

package franz

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func groupOptions(clusterAddresses []string) OptionsV1 {
	return OptionsV1{Name: "groups", Brokers: clusterAddresses, ClusterID: "fathomry-kafka-test",
		Topics: []string{"records"}, Plaintext: true, ConsumerGroup: "gh-105-group", InitialOffset: "earliest", ResetOffset: "error",
		MaxGroupSessions: 2, MaxRecords: 1}
}
func releaseEvidence(t testing.TB, fixture fixture) {
	t.Helper()
	delivery, err := fixture.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := delivery.Receipt().WaitReleased(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
}
func openGroup(t *testing.T, fixture fixture) *Group {
	t.Helper()
	group, err := fixture.client.ConsumeGroup(deadline(t), correlation("group-root"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := fixture.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		group.Close()
		if _, err := root.Receipt().WaitReleased(deadline(t)); err != nil {
			t.Error(err)
			return
		}
		if err := root.Release(); err != nil {
			t.Error(err)
		}
	})
	return group
}
func waitGroup(t *testing.T, fixture fixture, group *Group, count int) GroupSnapshot {
	t.Helper()
	ctx := deadline(t)
	for {
		snapshot := group.Snapshot()
		if snapshot.Ready && len(snapshot.AssignmentsCopy()) == count {
			return snapshot
		}
		if snapshot.Err != nil {
			logFixtureCauses(t, snapshot.Err)
			t.Fatal(snapshot.Err)
		}
		receipt, err := group.Wait(ctx, correlation("assignment"), snapshot.Revision)
		result := settle(t, receipt, err)
		releaseEvidence(t, fixture)
		if result.Err() != nil {
			logFixtureCauses(t, result.Err())
			t.Fatal(result.Err())
		}
	}
}

func waitGroupInitialOffsets(t *testing.T, group *Group) {
	t.Helper()
	ctx := deadline(t)
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		group.mu.Lock()
		ready := group.ready && !group.closing && len(group.partitions) > 0
		pending, problem := len(group.expectedOffsets), group.primary
		group.mu.Unlock()
		if problem != nil {
			logFixtureCauses(t, problem)
			t.Fatal("group failed before initial offsets completed")
		}
		if ready && pending == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("initial OffsetFetch did not complete", ctx.Err())
		case <-poll.C:
		}
	}
}
func nextBatch(t *testing.T, fixture fixture, group *Group) GroupBatch {
	t.Helper()
	for range 8 {
		receipt, err := group.Next(deadline(t), correlation("group-page"))
		result := settle(t, receipt, err)
		releaseEvidence(t, fixture)
		if errors.Is(result.Err(), ErrUnavailable) {
			continue
		}
		if result.Err() != nil {
			logFixtureCauses(t, result.Err())
			t.Fatal(result.Err())
		}
		batch := result.Outcome.Value.GroupBatch()
		if batch.token == nil {
			t.Fatal("successful group page has no token")
		}
		return batch
	}
	t.Fatal("no group data")
	return GroupBatch{}
}

func TestClassicGroupOwnsPagesAndCommitIdentity(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, groupOptions(cluster.ListenAddrs()), 8)
	native := nativeClient(t, cluster.ListenAddrs())
	nativeProduce(t, native, &kgo.Record{Topic: "records", Partition: 0, Value: []byte("first")},
		&kgo.Record{Topic: "records", Partition: 0, Value: []byte("second")},
		&kgo.Record{Topic: "records", Partition: 1, Value: []byte("other")})
	var fetches, commits atomic.Int32
	cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		fetches.Add(1)
		return nil, nil, false
	})
	var wireMu sync.Mutex
	var captured *kmsg.OffsetCommitRequest
	cluster.ControlKey(int16(kmsg.OffsetCommit), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		commits.Add(1)
		wireMu.Lock()
		captured = request.(*kmsg.OffsetCommitRequest)
		wireMu.Unlock()
		return nil, nil, false
	})
	first := openGroup(t, fixture)
	waitGroup(t, fixture, first, 2)
	if fetches.Load() != 0 {
		t.Fatal("membership-only session fetched record data")
	}
	batch := nextBatch(t, fixture, first)
	if len(batch.Page().RecordsCopy()) != 1 || batch.Start().Offset != 0 {
		t.Fatal("page or initial prefix changed")
	}
	other := nextBatch(t, fixture, first)
	if other.Start().Partition == batch.Start().Partition {
		t.Fatal("unprocessed partition advanced")
	}
	receipt, err := first.Next(deadline(t), correlation("gap"))
	result := settle(t, receipt, err)
	releaseEvidence(t, fixture)
	if !errors.Is(result.Err(), ErrState) {
		t.Fatal("unprocessed gap was crossed")
	}
	injected := false
	ctx := kgo.PreCommitFnContext(deadline(t), func(*kmsg.OffsetCommitRequest) error { injected = true; return errors.New("caller hook") })
	receipt, err = first.CommitBatch(ctx, correlation("commit"), batch)
	result = settle(t, receipt, err)
	releaseEvidence(t, fixture)
	if result.Err() != nil {
		logFixtureCauses(t, result.Err())
		t.Fatal(result.Err())
	}
	if injected {
		t.Fatal("caller native hook was not stripped")
	}
	wireMu.Lock()
	if captured == nil || captured.Generation < 0 || captured.MemberID == "" || captured.Version != 10 ||
		captured.Topics[0].Partitions[0].Offset != batch.Page().Next() {
		t.Fatal("wire membership or prefix mismatch")
	}
	wireMu.Unlock()
	if result.Outcome.Value.CheckpointsCopy()[0].State != CheckpointCommitted {
		t.Fatal("ACK lost")
	}
	if _, err := first.CommitBatch(deadline(t), correlation("repeated"), batch); !errors.Is(err, ErrState) {
		t.Fatal("reused token accepted")
	}
	second := openGroup(t, fixture)
	waitGroup(t, fixture, second, 1)
	waitGroup(t, fixture, first, 1)
	before := commits.Load()
	if _, err := first.CommitBatch(deadline(t), correlation("stale"), other); !errors.Is(err, ErrState) {
		t.Fatal("stale token accepted")
	}
	if commits.Load() != before {
		t.Fatal("stale commit reached broker")
	}
	first.Close()
	second.Close()
	if _, err := first.Receipt().WaitReleased(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Receipt().WaitReleased(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if commits.Load() != before {
		t.Fatal("close implicitly committed")
	}
	// Independent coordinator lookup sees exactly the explicitly declared prefix.
	request := kmsg.NewPtrOffsetFetchRequest()
	request.Groups = []kmsg.OffsetFetchRequestGroup{{Group: fixture.client.owner.settings.ConsumerGroup,
		Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: "records", TopicID: batch.Start().TopicID, Partitions: []int32{batch.Start().Partition}}}}}
	response, err := request.RequestWith(deadline(t), native)
	if err != nil {
		t.Fatal(err)
	}
	if response.Groups[0].Topics[0].Partitions[0].Offset != batch.Page().Next() {
		t.Fatal("independent stored checkpoint mismatch")
	}
}

func TestGroupAbsentOffsetIsNotAnImplicitReset(t *testing.T) {
	cluster := localCluster(t)
	options := groupOptions(cluster.ListenAddrs())
	options.InitialOffset = "error"
	fixture := bindFixture(t, options, 4)
	group := openGroup(t, fixture)
	waitGroup(t, fixture, group, 2)
	receipt, err := group.Next(deadline(t), correlation("absent"))
	result := settle(t, receipt, err)
	releaseEvidence(t, fixture)
	if !errors.Is(result.Err(), ErrOffsets) || result.Outcome.Value.CheckpointsCopy()[0].State != CheckpointAbsent {
		t.Fatal("missing initial offset was silently reset", result.Err())
	}
}

func TestGroupCanceledCommitRetainsUnknownAndCloses(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, groupOptions(cluster.ListenAddrs()), 5)
	nativeProduce(t, nativeClient(t, cluster.ListenAddrs()), &kgo.Record{Topic: "records", Value: []byte("pending")})
	group := openGroup(t, fixture)
	waitGroup(t, fixture, group, 2)
	batch := nextBatch(t, fixture, group)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	cluster.ControlKey(int16(kmsg.OffsetCommit), func(kmsg.Request) (kmsg.Response, error, bool) {
		close(entered)
		cluster.SleepControl(func() { <-release })
		return nil, nil, false
	})
	ctx, cancel := context.WithCancel(deadline(t))
	defer cancel()
	done := make(chan invocation.Result[Result], 1)
	go func() {
		receipt, err := group.CommitBatch(ctx, correlation("unknown"), batch)
		if err != nil {
			done <- invocation.Result[Result]{}
			return
		}
		result, _ := receipt.WaitReleased(deadline(t))
		done <- result
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("commit not dispatched")
	}
	cancel()
	var result invocation.Result[Result]
	select {
	case result = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not retain cancellation")
	}
	if !errors.Is(result.Err(), context.Canceled) || result.Outcome.Value.CheckpointsCopy()[0].State != CheckpointUnknown {
		t.Fatal("canceled sent commit was reported absent or successful", result.Err())
	}
	unblock()
	releaseEvidence(t, fixture)
	if _, err := group.Receipt().WaitReleased(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if group.Snapshot().Ready {
		t.Fatal("unknown commit left session usable")
	}
}

func TestGroupGracefulCloseDispatchesLeaveWithLiveNativeContext(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, groupOptions(cluster.ListenAddrs()), 4)
	var leaves, beta atomic.Int32
	cluster.ControlKey(int16(kmsg.LeaveGroup), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		leaves.Add(1)
		return nil, nil, false
	})
	cluster.ControlKey(int16(kmsg.ConsumerGroupHeartbeat), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		beta.Add(1)
		return nil, nil, false
	})
	ctx := context.WithValue(deadline(t), "opt_in_kafka_next_gen_balancer_beta", true)
	group, err := fixture.client.ConsumeGroup(ctx, correlation("isolated-context"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := fixture.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { group.Close(); _, _ = root.Receipt().WaitReleased(deadline(t)); _ = root.Release() }()
	waitGroup(t, fixture, group, 2)
	result := settle(t, group.Close(), nil)
	if result.Outcome.Primary != nil || leaves.Load() != 1 || beta.Load() != 0 {
		t.Fatal("close failed to drive classic LeaveGroup before native cancellation", result.Err(), leaves.Load(), beta.Load())
	}
	if result.Outcome.Cleanup != nil && !errors.Is(result.Outcome.Cleanup, ErrCleanup) {
		t.Fatal("native leave uncertainty lost its separate cleanup evidence")
	}
}

func TestGroupCommitGuardRejectsRebalanceAfterInitialValidation(t *testing.T) {
	cluster := localCluster(t)
	options := groupOptions(cluster.ListenAddrs())
	options.Retries = 2
	fixture := bindFixture(t, options, 8)
	nativeProduce(t, nativeClient(t, cluster.ListenAddrs()), &kgo.Record{Topic: "records", Value: []byte("guard")})
	group := openGroup(t, fixture)
	waitGroup(t, fixture, group, 2)
	batch := nextBatch(t, fixture, group)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var metadataBlocked atomic.Bool
	cluster.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) {
		if !metadataBlocked.CompareAndSwap(false, true) {
			return nil, nil, false
		}
		close(entered)
		cluster.SleepControl(func() { <-release })
		return nil, nil, false
	})
	var commits atomic.Int32
	cluster.ControlKey(int16(kmsg.OffsetCommit), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		commits.Add(1)
		return nil, nil, false
	})
	resultCh := make(chan invocation.Result[Result], 1)
	go func() {
		receipt, err := group.CommitBatch(deadline(t), correlation("guard"), batch)
		if err != nil {
			resultCh <- invocation.Result[Result]{}
			return
		}
		value, _ := receipt.WaitReleased(deadline(t))
		resultCh <- value
	}()
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("commit did not reach metadata preflight")
	}
	child, err := fixture.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := child.Receipt().WaitReleased(deadline(t)); err != nil {
			t.Error(err)
			return
		}
		if err := child.Release(); err != nil {
			t.Error(err)
		}
	})
	second := openGroup(t, fixture)
	wait := time.NewTimer(5 * time.Second)
	defer wait.Stop()
	for {
		snapshot := group.Snapshot()
		if snapshot.Ready && len(snapshot.AssignmentsCopy()) == 1 {
			break
		}
		select {
		case <-wait.C:
			t.Fatal("rebalance did not progress around blocked commit")
		case <-time.After(time.Millisecond):
		}
	}
	_ = second
	unblock()
	var result invocation.Result[Result]
	select {
	case result = <-resultCh:
	case <-deadline(t).Done():
		t.Fatal("commit guard did not finish")
	}
	if !errors.Is(result.Err(), ErrState) || commits.Load() != 0 || result.Outcome.Value.CheckpointsCopy()[0].State != CheckpointUnobserved {
		logFixtureCauses(t, result.Err())
		logFixtureCauses(t, group.Snapshot().Err)
		t.Logf("commit requests=%d checkpoint state=%d", commits.Load(), result.Outcome.Value.CheckpointsCopy()[0].State)
		t.Fatal("stale pre-commit guard reached wire", result.Err())
	}
}

func TestGroupLossAndAssignmentBoundsTerminateWithoutNativeFetch(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		t.Run(fmt.Sprint(bounded), func(t *testing.T) {
			cluster := localCluster(t)
			options := groupOptions(cluster.ListenAddrs())
			if bounded {
				options.MaxAssignments = 1
			}
			fixture := bindFixture(t, options, 4)
			var fetches atomic.Int32
			cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
				cluster.KeepControl()
				fetches.Add(1)
				return nil, nil, false
			})
			group := openGroup(t, fixture)
			var pending GroupBatch
			var expectedFetches int32
			if !bounded {
				waitGroup(t, fixture, group, 2)
				if fetches.Load() != 0 {
					t.Fatal("membership fetched before an explicit read")
				}
				nativeProduce(t, nativeClient(t, cluster.ListenAddrs()), &kgo.Record{Topic: "records", Value: []byte("pending-loss")})
				pending = nextBatch(t, fixture, group)
				expectedFetches = fetches.Load()
				cluster.ControlKey(int16(kmsg.JoinGroup), func(request kmsg.Request) (kmsg.Response, error, bool) {
					response := kmsg.NewPtrJoinGroupResponse()
					response.SetVersion(request.GetVersion())
					response.ErrorCode = kerr.GroupAuthorizationFailed.Code
					return response, nil, true
				})
				group.native.ForceRebalance()
			}
			result := settle(t, group.Receipt(), nil)
			if fetches.Load() != expectedFetches {
				t.Fatal("membership allocated native records")
			}
			if bounded && !errors.Is(result.Err(), ErrLimit) {
				t.Fatal("assignment cap not enforced", result.Err())
			}
			if !bounded && !errors.Is(result.Err(), kerr.GroupAuthorizationFailed) {
				logFixtureCauses(t, result.Err())
				t.Fatal("native loss cause missing", result.Err())
			}
			if !bounded {
				if _, err := group.CommitBatch(deadline(t), correlation("lost-processing"), pending); !errors.Is(err, ErrState) {
					t.Fatal("loss retained commit authority for an unprocessed page")
				}
				if string(pending.Page().RecordsCopy()[0].ValueCopy()) != "pending-loss" {
					t.Fatal("loss discarded retained page evidence")
				}
			}
			if group.Snapshot().Ready || group.Snapshot().Event != GroupClosed {
				t.Fatal("lost session remained usable")
			}
			if _, err := group.Next(deadline(t), correlation("after-loss")); !errors.Is(err, ErrState) {
				t.Fatal("lost assignment used")
			}
		})
	}
}

func TestGroupLifetimeCancellationKeepsCanonicalAndCustomCauses(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, groupOptions(cluster.ListenAddrs()), 4)
	lifetime, cancel := context.WithCancelCause(deadline(t))
	group, err := fixture.client.ConsumeGroup(lifetime, correlation("lifetime"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := fixture.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	waitGroup(t, fixture, group, 2)
	cause := errors.New("private cancellation cause")
	cancel(cause)
	group.Close()
	result := settle(t, group.Receipt(), nil)
	if !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), cause) || !errors.Is(group.Snapshot().Err, cause) {
		t.Fatal("lifetime cancellation was erased by close")
	}
	if err := root.Release(); err != nil {
		t.Fatal(err)
	}
}
func FuzzGroupTokenOwnership(f *testing.F) {
	f.Add(uint64(9), int32(7), false, false)
	f.Add(uint64(8), int32(7), true, true)
	f.Fuzz(func(t *testing.T, revision uint64, generation int32, foreign, closed bool) {
		start := Position{Topic: "records", Partition: 0}
		group := &Group{lifetime: context.Background(), identity: &groupIdentity{}, ready: true, member: "bounded-member", generation: 7, revision: 9,
			closing: closed, partitions: make(map[groupKey]*groupPartition)}
		token := &groupToken{identity: group.identity, revision: revision, member: group.member, generation: generation, start: start, next: 1}
		group.partitions[groupKey{"records", 0}] = &groupPartition{position: start, pending: token}
		if foreign {
			token.identity = &groupIdentity{}
		}
		err := group.validToken(token)
		valid := revision == 9 && generation == 7 && !foreign && !closed
		if (err == nil) != valid {
			t.Fatal("token authorization disagreed with owned identity")
		}
	})
}

func TestGroupLeaveMemberFailureRetained(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, groupOptions(cluster.ListenAddrs()), 8)
	group := openGroup(t, fixture)
	waitGroup(t, fixture, group, 2)
	// Ready reports assignment, not completion of the native initial OffsetFetch.
	// Its cancellation can retire the coordinator connection before Leave is read.
	waitGroupInitialOffsets(t, group)
	var leaves atomic.Int32
	cluster.ControlKey(int16(kmsg.LeaveGroup), func(request kmsg.Request) (kmsg.Response, error, bool) {
		leaves.Add(1)
		leave := request.(*kmsg.LeaveGroupRequest)
		if leave.Version != 2 {
			t.Error("dynamic group leave profile must be v2")
		}
		response := request.ResponseKind().(*kmsg.LeaveGroupResponse)
		if leave.Version < 3 {
			response.ErrorCode = kerr.UnknownServerError.Code
		} else {
			if len(leave.Members) != 1 {
				t.Error("probe requires one member in LeaveGroup v3+")
			}
			response.Members = []kmsg.LeaveGroupResponseMember{{MemberID: leave.Members[0].MemberID, ErrorCode: kerr.UnknownServerError.Code}}
		}
		return response, nil, true
	})
	result := settle(t, group.Close(), nil)
	if errors.Is(result.Outcome.Cleanup, kerr.UnknownServerError) && leaves.Load() == 1 {
		return
	}
	logFixtureCauses(t, result.Outcome.Cleanup)
	request := kmsg.NewPtrDescribeGroupsRequest()
	request.Groups = []string{fixture.client.owner.settings.ConsumerGroup}
	response, err := request.RequestWith(deadline(t), nativeClient(t, cluster.ListenAddrs()))
	if err != nil || len(response.Groups) != 1 {
		t.Fatal("independent membership observation failed")
	}
	t.Fatalf("member-specific LeaveGroup error discarded: primary=%v cleanup=%v remote members=%d", result.Outcome.Primary, result.Outcome.Cleanup, len(response.Groups[0].Members))
}

func TestGroupCanceledNativeOffsetFetchResumesRetainedAssignments(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, groupOptions(cluster.ListenAddrs()), 8)
	entered, release := make(chan struct{}), make(chan struct{})
	var started atomic.Bool
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	cluster.ControlKey(int16(kmsg.OffsetFetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		if started.CompareAndSwap(false, true) {
			close(entered)
			cluster.SleepControl(func() { <-release })
		}
		return nil, nil, false
	})
	group := openGroup(t, fixture)
	t.Cleanup(unblock)
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("native OffsetFetch was not dispatched")
	}
	before := group.Snapshot().Generation
	group.native.ForceRebalance()
	wait := time.NewTimer(5 * time.Second)
	defer wait.Stop()
	for {
		snapshot := group.Snapshot()
		if snapshot.Err != nil {
			logFixtureCauses(t, snapshot.Err)
			t.Fatalf("legitimate resumed OffsetFetch terminated the session: event=%d revision=%d", snapshot.Event, snapshot.Revision)
		}
		if snapshot.Ready && snapshot.Generation > before {
			break
		}
		select {
		case <-wait.C:
			t.Fatal("rebalance did not complete")
		case <-time.After(time.Millisecond):
		}
	}
	unblock()
	select {
	case <-group.done:
		snapshot := group.Snapshot()
		logFixtureCauses(t, snapshot.Err)
		t.Fatalf("legitimate resumed OffsetFetch terminated the session: event=%d revision=%d", snapshot.Event, snapshot.Revision)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestGroupOffsetExpectationsKeepRejectingControls(t *testing.T) {
	for _, name := range []string{"complete", "canceled", "duplicate", "foreign", "omitted", "partition-error"} {
		t.Run(name, func(t *testing.T) {
			lifetime, cancel := context.WithCancel(context.Background())
			defer cancel()
			topicID := [16]byte{1}
			group := &Group{
				client:   &Client{owner: &connection{settings: settings{ConsumerGroup: "review"}}},
				lifetime: lifetime, cancel: cancel, changed: make(chan struct{}),
				expectedOffsets: map[checkpointKey]bool{{topicID, 0}: true, {topicID, 1}: true},
			}
			response := kmsg.NewPtrOffsetFetchResponse()
			response.Version = 10
			response.Groups = []kmsg.OffsetFetchResponseGroup{{Group: "review", Topics: []kmsg.OffsetFetchResponseGroupTopic{{
				TopicID: topicID, Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{{Partition: 0, Offset: -1}, {Partition: 1, Offset: -1}},
			}}}}
			work, stop := context.WithCancel(context.Background())
			defer stop()
			switch name {
			case "canceled":
				stop()
			case "duplicate":
				response.Groups[0].Topics[0].Partitions[1].Partition = 0
			case "foreign":
				response.Groups[0].Topics[0].TopicID = [16]byte{2}
			case "omitted":
				response.Groups[0].Topics[0].Partitions = response.Groups[0].Topics[0].Partitions[:1]
			case "partition-error":
				response.Groups[0].Topics[0].Partitions[0].ErrorCode = kerr.TopicAuthorizationFailed.Code
			}
			err := group.offsetsFetched(work, nil, response)
			if name == "complete" {
				if err != nil || len(group.expectedOffsets) != 0 {
					t.Fatal("complete OffsetFetch did not release its pending expectations")
				}
				return
			}
			if err == nil || len(group.expectedOffsets) != 2 {
				t.Fatal("failed OffsetFetch accepted or erased pending expectations")
			}
			if name == "canceled" {
				if !errors.Is(err, context.Canceled) || group.closing {
					t.Fatal("canceled generation failed the entire session")
				}
			} else if !group.closing {
				t.Fatal("invalid response did not fence the session")
			}
			if name == "partition-error" && !errors.Is(err, kerr.TopicAuthorizationFailed) {
				t.Fatal("partition failure lost its original cause")
			}
		})
	}
}
