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
	"math"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Checkpoint declares the NEXT physical offset for a manually assigned partition.
// It is not a record address or proof that preceding business work completed.
// TopicID is mandatory. Next=0 is an explicit rewind; negative values reject
// for commits and are ignored for lookups. Leader-epoch truncation checks are
// not supplied by this profile.
type Checkpoint struct {
	private
	Topic     string
	TopicID   [16]byte
	Partition int32
	Next      int64
}

// CheckpointState separates unattempted/unknown commits from confirmed commits,
// observed offsets and absence. It does not certify a processed business prefix.
type CheckpointState uint8

const (
	CheckpointUnobserved CheckpointState = iota
	CheckpointUnknown
	CheckpointCommitted
	CheckpointObserved
	CheckpointAbsent
)

// CheckpointResult preserves each partition's commit/read observation. Absent
// means no currently stored offset (-1), not that business work never executed.
type CheckpointResult struct {
	private
	Checkpoint Checkpoint
	State      CheckpointState
	Err        error
}

func (result Result) CheckpointsCopy() []CheckpointResult {
	if result.data == nil {
		return nil
	}
	return append([]CheckpointResult{}, result.data.checkpoints...)
}

type checkpointKey struct {
	topic     [16]byte
	partition int32
}

func (client *Client) validCheckpoints(checkpoints []Checkpoint, commit bool) error {
	if client == nil || client.owner == nil {
		return failure(ErrInput, "offsets")
	}
	if client.owner.settings.OffsetGroup == "" {
		return failure(ErrUnsupported, "offset-group")
	}
	if len(checkpoints) == 0 || len(checkpoints) > client.owner.settings.MaxRecords {
		return failure(ErrLimit, "checkpoints")
	}
	seen := make(map[checkpointKey]bool)
	for _, checkpoint := range checkpoints {
		topic, ok := client.owner.topics[checkpoint.Topic]
		if !ok || checkpoint.TopicID != topic.ID {
			return failure(ErrIdentity, "checkpoint-topic")
		}
		if checkpoint.Partition < 0 || int(checkpoint.Partition) >= topic.Partitions || commit && (checkpoint.Next < 0 || checkpoint.Next == math.MaxInt64) {
			return failure(ErrInput, "checkpoint")
		}
		key := checkpointKey{checkpoint.TopicID, checkpoint.Partition}
		if seen[key] {
			return failure(ErrInput, "duplicate-checkpoint")
		}
		seen[key] = true
	}
	return nil
}

// CommitOffsets records explicit technical checkpoints in the configured,
// exclusively owned OffsetGroup, using generation=-1 (no membership).
// It neither tracks processed records nor prevents a caller from crossing an
// unfinished gap or rewinding. The outer framework owns that decision/ledger.
// Calls may run concurrently; there is no CAS/monotonic or cross-partition atomic
// guarantee. No automatic commit, rebalance participation or group EOS is implied.
func (client *Client) CommitOffsets(ctx context.Context, id fault.Correlation, checkpoints []Checkpoint) (*invocation.Receipt[Result], error) {
	return client.checkpoints(ctx, id, checkpoints, true, nil)
}

// FetchOffsets reads only explicitly selected partitions in OffsetGroup.
// Kafka offset retention can remove an earlier checkpoint; absence is explicit.
func (client *Client) FetchOffsets(ctx context.Context, id fault.Correlation, checkpoints []Checkpoint) (*invocation.Receipt[Result], error) {
	return client.checkpoints(ctx, id, checkpoints, false, nil)
}
func (client *Client) checkpoints(ctx context.Context, id fault.Correlation, checkpoints []Checkpoint, commit bool, parent *invocation.Call[Result]) (*invocation.Receipt[Result], error) {
	if err := client.validCheckpoints(checkpoints, commit); err != nil {
		return nil, err
	}
	name := "fetch-offsets"
	if commit {
		name = "commit-offsets"
	}
	var call *invocation.Call[Result]
	var err error
	if parent == nil {
		call, err = client.begin(ctx, id, name, invocation.Finite)
	} else {
		call, err = client.beginWithin(ctx, id, name, parent)
	}
	if err != nil {
		return nil, err
	}
	_ = call.Execute(ctx, invocation.Budget{Limit: client.owner.settings.Timeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Result] {
		data := &resultData{checkpoints: make([]CheckpointResult, len(checkpoints))}
		indices := make(map[checkpointKey]int, len(checkpoints))
		for index, checkpoint := range checkpoints {
			checkpoint.Topic = strings.Clone(checkpoint.Topic)
			data.checkpoints[index] = CheckpointResult{Checkpoint: checkpoint}
			indices[checkpointKey{checkpoint.TopicID, checkpoint.Partition}] = index
		}
		outcome := invocation.Outcome[Result]{Present: true, Value: Result{data: data}}
		if err := client.owner.checkIdentity(work); err != nil {
			outcome.Primary = err
			return outcome
		}
		if work.Err() != nil {
			outcome.Primary = failure(ErrOffsets, name, work.Err(), context.Cause(work))
			return outcome
		}
		for index := range data.checkpoints {
			data.checkpoints[index].State = CheckpointUnknown
		}
		if commit {
			outcome.Primary = client.commitCheckpoints(work, data, indices)
		} else {
			outcome.Primary = client.fetchCheckpoints(work, data, indices)
		}
		return outcome
	})
	return call.Receipt(), nil
}
func (client *Client) commitCheckpoints(ctx context.Context, data *resultData, indices map[checkpointKey]int) error {
	request := kmsg.NewPtrOffsetCommitRequest()
	request.Group = client.owner.settings.OffsetGroup
	topics := make(map[[16]byte]int)
	for _, item := range data.checkpoints {
		checkpoint := item.Checkpoint
		topic, ok := topics[checkpoint.TopicID]
		if !ok {
			topic = len(request.Topics)
			topics[checkpoint.TopicID] = topic
			request.Topics = append(request.Topics, kmsg.OffsetCommitRequestTopic{TopicID: checkpoint.TopicID})
		}
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = checkpoint.Partition
		partition.Offset = checkpoint.Next
		request.Topics[topic].Partitions = append(request.Topics[topic].Partitions, partition)
	}
	response, err := request.RequestWith(nativeContext{context.WithoutCancel(ctx)}, client.owner.native)
	if err != nil {
		return nativeFailure(ErrOffsets, "commit-offsets", ctx, err)
	}
	if response.Version != 10 {
		return failure(ErrUnsupported, "offset-version")
	}
	var failures []error
	seen := make(map[checkpointKey]bool)
	for _, topic := range response.Topics {
		for _, partition := range topic.Partitions {
			key := checkpointKey{topic.TopicID, partition.Partition}
			index, ok := indices[key]
			if !ok || seen[key] {
				return failure(ErrIdentity, "commit-response")
			}
			seen[key] = true
			item := &data.checkpoints[index]
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				item.Err = failure(ErrOffsets, "commit-partition", err)
				failures = append(failures, item.Err)
			} else {
				item.State = CheckpointCommitted
			}
		}
	}
	if len(seen) != len(indices) {
		failures = append(failures, failure(ErrOffsets, "commit-incomplete"))
	}
	return joinFailures(ErrOffsets, "commit-offsets", failures)
}
func (client *Client) fetchCheckpoints(ctx context.Context, data *resultData, indices map[checkpointKey]int) error {
	request := kmsg.NewPtrOffsetFetchRequest()
	request.RequireStable = true
	group := kmsg.NewOffsetFetchRequestGroup()
	group.Group = client.owner.settings.OffsetGroup
	topics := make(map[[16]byte]int)
	for _, item := range data.checkpoints {
		checkpoint := item.Checkpoint
		topic, ok := topics[checkpoint.TopicID]
		if !ok {
			topic = len(group.Topics)
			topics[checkpoint.TopicID] = topic
			group.Topics = append(group.Topics, kmsg.OffsetFetchRequestGroupTopic{TopicID: checkpoint.TopicID})
		}
		group.Topics[topic].Partitions = append(group.Topics[topic].Partitions, checkpoint.Partition)
	}
	request.Groups = []kmsg.OffsetFetchRequestGroup{group}
	response, err := request.RequestWith(nativeContext{context.WithoutCancel(ctx)}, client.owner.native)
	if err != nil {
		return nativeFailure(ErrOffsets, "fetch-offsets", ctx, err)
	}
	if response.Version != 10 {
		return failure(ErrUnsupported, "offset-version")
	}
	if len(response.Groups) != 1 || response.Groups[0].Group != group.Group {
		return failure(ErrIdentity, "offset-group")
	}
	if err := kerr.ErrorForCode(response.Groups[0].ErrorCode); err != nil {
		if errors.Is(err, kerr.GroupIDNotFound) {
			for index := range data.checkpoints {
				data.checkpoints[index].State = CheckpointAbsent
				data.checkpoints[index].Checkpoint.Next = -1
				data.checkpoints[index].Err = failure(ErrOffsets, "group-absent", err)
			}
		}
		return failure(ErrOffsets, "offset-group", err)
	}
	var failures []error
	seen := make(map[checkpointKey]bool)
	for _, topic := range response.Groups[0].Topics {
		for _, partition := range topic.Partitions {
			key := checkpointKey{topic.TopicID, partition.Partition}
			index, ok := indices[key]
			if !ok || seen[key] {
				return failure(ErrIdentity, "fetch-offset-response")
			}
			seen[key] = true
			item := &data.checkpoints[index]
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				item.Err = failure(ErrOffsets, "fetch-offset-partition", err)
				failures = append(failures, item.Err)
			} else if partition.Offset < -1 {
				failures = append(failures, failure(ErrOffsets, "invalid-offset"))
			} else {
				item.Checkpoint.Next = partition.Offset
				item.State = CheckpointObserved
				if partition.Offset == -1 {
					item.State = CheckpointAbsent
				}
			}
		}
	}
	if len(seen) != len(indices) {
		failures = append(failures, failure(ErrOffsets, "fetch-offset-incomplete"))
	}
	if err := errors.Join(failures...); err != nil {
		return failure(ErrOffsets, "fetch-offsets", err)
	}
	return nil
}
