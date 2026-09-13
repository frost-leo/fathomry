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
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func checkpointFor(position Position, next int64) Checkpoint {
	return Checkpoint{Topic: position.Topic, TopicID: position.TopicID, Partition: position.Partition, Next: next}
}
func TestManualCheckpointDoesNotProveProcessedPrefix(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.OffsetGroup = "manual-checkpoint"
	fixture := bindFixture(t, options, 8)
	writes := produce(t, fixture.client, "source", Message{Topic: "records", Value: []byte("unfinished-first")},
		Message{Topic: "records", Value: []byte("unfinished-second")}, Message{Topic: "records", Value: []byte("third")}).WritesCopy()
	checkpoint := checkpointFor(writes[2].Position, 3)
	receipt, err := fixture.client.FetchOffsets(deadline(t), correlation("absent"), []Checkpoint{checkpoint})
	initial := settle(t, receipt, err)
	if initial.Err() != nil && !errors.Is(initial.Err(), kerr.GroupIDNotFound) || initial.Outcome.Value.CheckpointsCopy()[0].State != CheckpointAbsent {
		logFixtureCauses(t, initial.Err())
		t.Fatal("missing checkpoint not explicit", initial.Err())
	}
	receipt, err = fixture.client.CommitOffsets(deadline(t), correlation("commit"), []Checkpoint{checkpoint})
	committed := settle(t, receipt, err)
	if committed.Err() != nil || committed.Outcome.Value.CheckpointsCopy()[0].State != CheckpointCommitted {
		t.Fatal("checkpoint commit failed", committed.Err())
	}
	receipt, err = fixture.client.FetchOffsets(deadline(t), correlation("fetch"), []Checkpoint{checkpoint})
	fetched := settle(t, receipt, err)
	if fetched.Err() != nil || fetched.Outcome.Value.CheckpointsCopy()[0].Checkpoint.Next != 3 {
		t.Fatal("checkpoint readback failed", fetched.Err())
	}
	// Independent native observation: committing the third offset stored next=3
	// even though no application processing happened for the first two records.
	native := nativeClient(t, cluster.ListenAddrs())
	request := kmsg.NewPtrOffsetFetchRequest()
	request.RequireStable = true
	group := kmsg.NewOffsetFetchRequestGroup()
	group.Group = options.OffsetGroup
	group.Topics = []kmsg.OffsetFetchRequestGroupTopic{{TopicID: checkpoint.TopicID, Partitions: []int32{0}}}
	request.Groups = []kmsg.OffsetFetchRequestGroup{group}
	response, err := request.RequestWith(deadline(t), native)
	if err != nil || len(response.Groups) != 1 || response.Groups[0].ErrorCode != 0 || response.Groups[0].Topics[0].Partitions[0].Offset != 3 {
		t.Fatal("independent checkpoint observation failed")
	}
	for _, write := range writes[:2] {
		if readResult(t, fixture.client, "still-present-"+string(rune('a'+write.Position.Offset)), write.Position).State != ReadFound {
			t.Fatal("consumer checkpoint erased physical data")
		}
	}
}
func TestCheckpointConfigurationAndPartialError(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	fixture := bindFixture(t, options, 8)
	position := produce(t, fixture.client, "record", Message{Topic: "records"}).WritesCopy()[0].Position
	checkpoint := checkpointFor(position, 1)
	if _, err := fixture.client.CommitOffsets(deadline(t), correlation("disabled"), []Checkpoint{checkpoint}); !errors.Is(err, ErrUnsupported) {
		t.Fatal("unconfigured group accepted")
	}
	options.OffsetGroup = "partial-checkpoint"
	second := bindFixture(t, options, 8)
	duplicate := []Checkpoint{checkpoint, checkpoint}
	if _, err := second.client.CommitOffsets(deadline(t), correlation("duplicate"), duplicate); !errors.Is(err, ErrInput) {
		t.Fatal("duplicate checkpoints accepted")
	}
	invalid := checkpoint
	invalid.Next = -1
	if _, err := second.client.CommitOffsets(deadline(t), correlation("negative"), []Checkpoint{invalid}); !errors.Is(err, ErrInput) {
		t.Fatal("negative checkpoint accepted")
	}
	other := checkpoint
	other.Partition = 1
	cluster.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
		response := req.ResponseKind().(*kmsg.OffsetCommitResponse)
		response.Topics = []kmsg.OffsetCommitResponseTopic{{TopicID: checkpoint.TopicID, Partitions: []kmsg.OffsetCommitResponseTopicPartition{
			{Partition: 0}, {Partition: 1, ErrorCode: kerr.NotCoordinator.Code}}}}
		return response, nil, true
	})
	receipt, err := second.client.CommitOffsets(deadline(t), correlation("partial"), []Checkpoint{checkpoint, other})
	result := settle(t, receipt, err)
	parts := result.Outcome.Value.CheckpointsCopy()
	if result.Err() == nil || parts[0].State != CheckpointCommitted || parts[1].State != CheckpointUnknown || !errors.Is(parts[1].Err, kerr.NotCoordinator) {
		t.Fatal("partial checkpoint evidence collapsed")
	}
}
