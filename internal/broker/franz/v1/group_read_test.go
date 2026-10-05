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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestGroupLatestPolicyFreezesSelectedOffsetAtAnEmptyPage(t *testing.T) {
	for _, reset := range []bool{false, true} {
		t.Run(fmt.Sprint(reset), func(t *testing.T) {
			cluster := localCluster(t)
			options := groupOptions(cluster.ListenAddrs())
			options.InitialOffset = "latest"
			if reset {
				options.InitialOffset = "earliest"
				options.ResetOffset = "latest"
			}
			fixture := bindFixture(t, options, 8)
			producer := nativeClient(t, cluster.ListenAddrs())
			nativeProduce(t, producer, &kgo.Record{Topic: "records", Value: []byte("prior")})
			if reset {
				request := kmsg.NewPtrOffsetCommitRequest()
				request.Group = options.ConsumerGroup
				request.Generation = -1
				partition := kmsg.NewOffsetCommitRequestTopicPartition()
				partition.Partition = 0
				partition.Offset = 99
				request.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "records", TopicID: fixture.client.owner.topics["records"].ID, Partitions: []kmsg.OffsetCommitRequestTopicPartition{partition}}}
				response, err := request.RequestWith(deadline(t), producer)
				if err != nil || len(response.Topics) != 1 || response.Topics[0].Partitions[0].ErrorCode != 0 {
					t.Fatal("reset checkpoint fixture failed")
				}
			}
			group := openGroup(t, fixture)
			waitGroup(t, fixture, group, 2)
			receipt, err := group.Next(deadline(t), correlation("initial-tip"))
			initial := settle(t, receipt, err)
			releaseEvidence(t, fixture)
			if !errors.Is(initial.Err(), ErrUnavailable) || initial.Outcome.Value.Page().Next() != 1 {
				t.Fatal("initial latest selection was not explicit", initial.Err())
			}
			nativeProduce(t, producer, &kgo.Record{Topic: "records", Value: []byte("arrived")})
			for range 3 {
				receipt, err := group.Next(deadline(t), correlation("after-arrival"))
				result := settle(t, receipt, err)
				releaseEvidence(t, fixture)
				if errors.Is(result.Err(), ErrUnavailable) {
					continue
				}
				if result.Err() != nil {
					t.Fatal(result.Err())
				}
				batch := result.Outcome.Value.GroupBatch()
				if batch.Start().Offset != 1 || string(batch.Page().RecordsCopy()[0].ValueCopy()) != "arrived" {
					t.Fatal("selected starting position changed")
				}
				return
			}
			t.Fatal("latest policy followed the moving end instead of retaining its selected offset")
		})
	}
}

func TestGroupRetainedLatestDoesNotSkipUncommittedPage(t *testing.T) {
	for _, reset := range []bool{false, true} {
		t.Run(fmt.Sprintf("reset=%t", reset), func(t *testing.T) {
			checkGroupRetainedLatest(t, reset)
		})
	}
}

func checkGroupRetainedLatest(t *testing.T, reset bool) {
	cluster := localCluster(t)
	options := groupOptions(cluster.ListenAddrs())
	options.InitialOffset = "latest"
	if reset {
		options.InitialOffset = "earliest"
		options.ResetOffset = "latest"
	}
	fixture := bindFixture(t, options, 8)
	producer := nativeClient(t, cluster.ListenAddrs())
	nativeProduce(t, producer, &kgo.Record{Topic: "records", Partition: 0, Value: []byte("prior")})
	if reset {
		request := kmsg.NewPtrOffsetCommitRequest()
		request.Group = options.ConsumerGroup
		request.Generation = -1
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition, partition.Offset = 0, 99
		request.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "records", TopicID: fixture.client.owner.topics["records"].ID, Partitions: []kmsg.OffsetCommitRequestTopicPartition{partition}}}
		response, err := request.RequestWith(deadline(t), producer)
		if err != nil || len(response.Topics) != 1 || response.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatal("failed to create an out-of-range checkpoint")
		}
	}
	group := openGroup(t, fixture)
	waitGroup(t, fixture, group, 2)
	receipt, err := group.Next(deadline(t), correlation("select-tip"))
	initial := settle(t, receipt, err)
	releaseEvidence(t, fixture)
	if !errors.Is(initial.Err(), ErrUnavailable) || initial.Outcome.Value.Page().Next() != 1 {
		t.Fatal("failed to select starting point")
	}
	nativeProduce(t, producer, &kgo.Record{Topic: "records", Partition: 0, Value: []byte("uncommitted")})
	batch := nextBatch(t, fixture, group)
	if batch.Start().Partition != 0 || batch.Start().Offset != 1 {
		t.Fatal("did not obtain the expected uncommitted page")
	}
	before := group.Snapshot().Revision
	group.native.ForceRebalance()
	wait := time.NewTimer(5 * time.Second)
	defer wait.Stop()
	for {
		snapshot := group.Snapshot()
		if snapshot.Ready && snapshot.Revision > before {
			break
		}
		select {
		case <-wait.C:
			t.Fatal("rebalance did not complete")
		case <-time.After(time.Millisecond):
		}
	}
	for range 2 {
		receipt, err := group.Next(deadline(t), correlation("after-rebalance"))
		result := settle(t, receipt, err)
		releaseEvidence(t, fixture)
		if result.Err() == nil {
			replayed := result.Outcome.Value.GroupBatch()
			if replayed.Start().Partition == 0 && replayed.Start().Offset == batch.Start().Offset {
				return
			}
		}
		if result.Err() != nil && !errors.Is(result.Err(), ErrUnavailable) {
			t.Fatal(result.Err())
		}
	}
	group.mu.Lock()
	position := group.partitions[groupKey{"records", 0}].position.Offset
	group.mu.Unlock()
	t.Fatalf("retained partition skipped uncommitted page: old start=%d, new cursor=%d, no explicit commit", batch.Start().Offset, position)
}
