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

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Next scans one available partition fairly, with at most one uncommitted page
// per partition. ErrUnavailable is a finite empty/stability observation; it does
// not trigger an internal poll loop. Failed/stale pages never advance progress.
func (group *Group) Next(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	if ctx == nil {
		return nil, failure(ErrInput, "group-next")
	}
	if err := group.enter(); err != nil {
		return nil, err
	}
	defer group.leave()
	ctx, cancel := group.operationContext(ctx)
	defer cancel()
	call, err := group.client.beginWithin(ctx, id, "group-next", group.call)
	if err != nil {
		return nil, err
	}
	_ = call.Execute(ctx, invocation.Budget{Limit: group.client.owner.settings.Timeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Result] {
		data, err := group.next(work)
		return invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: err}
	})
	return call.Receipt(), nil
}
func (group *Group) next(ctx context.Context) (*resultData, error) {
	group.mu.Lock()
	snapshot := group.snapshotLocked()
	data := &resultData{group: snapshot}
	if !snapshot.Ready || len(snapshot.assignments) == 0 {
		group.mu.Unlock()
		return data, failure(ErrUnavailable, "group-assignment")
	}
	var current *groupPartition
	for range len(snapshot.assignments) {
		index := group.roundRobin % len(snapshot.assignments)
		group.roundRobin = (index + 1) % len(snapshot.assignments)
		position := snapshot.assignments[index].Position
		candidate := group.partitions[groupKey{position.Topic, position.Partition}]
		if candidate.pending == nil {
			current = candidate
			break
		}
	}
	if current == nil {
		group.mu.Unlock()
		return data, failure(ErrState, "group-unprocessed")
	}
	position, initialized := current.position, current.initialized
	group.mu.Unlock()
	if !initialized {
		checkpoint := Checkpoint{Topic: position.Topic, TopicID: position.TopicID, Partition: position.Partition}
		data.checkpoints = []CheckpointResult{{Checkpoint: checkpoint, State: CheckpointUnknown}}
		if err := group.client.owner.checkIdentity(ctx); err != nil {
			return data, err
		}
		if err := group.client.fetchCheckpointsFor(ctx, group.client.owner.settings.ConsumerGroup, data,
			map[checkpointKey]int{{topic: position.TopicID, partition: position.Partition}: 0}); err != nil {
			return data, err
		}
		observed := data.checkpoints[0]
		bounds, err := group.bounds(ctx, position)
		if err != nil {
			return data, err
		}
		if observed.State == CheckpointAbsent {
			position.Offset, err = group.reset(group.client.owner.settings.InitialOffset, bounds, ErrOffsets)
		} else {
			position.Offset = observed.Checkpoint.Next
			if position.Offset < bounds.logStart || position.Offset > bounds.highWatermark {
				position.Offset, err = group.reset(group.client.owner.settings.ResetOffset, bounds, ErrExpired)
			}
		}
		if err != nil {
			return data, err
		}
		if !group.selectOffset(snapshot.Revision, position) {
			return data, failure(ErrState, "group-stale")
		}
	}
	if !group.current(snapshot.Revision, position) {
		return data, failure(ErrState, "group-stale")
	}
	page, err := group.client.fetchPage(ctx, Range{Start: position, End: math.MaxInt64})
	if errors.Is(err, ErrExpired) || errors.Is(err, ErrUnavailable) {
		bounds, boundErr := group.bounds(ctx, position)
		if boundErr != nil {
			return data, boundErr
		}
		if position.Offset < bounds.logStart || position.Offset > bounds.highWatermark {
			position.Offset, err = group.reset(group.client.owner.settings.ResetOffset, bounds, ErrExpired)
			if err == nil {
				if !group.selectOffset(snapshot.Revision, position) {
					return data, failure(ErrState, "group-stale")
				}
				page, err = group.client.fetchPage(ctx, Range{Start: position, End: math.MaxInt64})
			}
		}
	}
	data.page = page
	if err != nil {
		return data, err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if !group.currentLocked(snapshot.Revision, position) {
		return data, failure(ErrState, "group-stale")
	}
	token := &groupToken{identity: group.identity, revision: snapshot.Revision, member: snapshot.MemberID, generation: snapshot.Generation,
		start: position, next: page.Next()}
	partition := group.partitions[groupKey{position.Topic, position.Partition}]
	partition.position, partition.initialized, partition.pending = position, true, token
	data.batch = GroupBatch{page: page, token: token}
	return data, nil
}
func (group *Group) currentLocked(revision uint64, position Position) bool {
	return !group.closing && group.ready && group.lifetime.Err() == nil && group.revision == revision &&
		group.partitions[groupKey{position.Topic, position.Partition}] != nil
}
func (group *Group) current(revision uint64, position Position) bool {
	group.mu.Lock()
	defer group.mu.Unlock()
	return group.currentLocked(revision, position)
}

// Selecting an explicit initial/reset point is not processed-page progress.
// Retain it even at EOF, or "latest" would chase the moving end forever.
func (group *Group) selectOffset(revision uint64, position Position) bool {
	group.mu.Lock()
	defer group.mu.Unlock()
	if !group.currentLocked(revision, position) {
		return false
	}
	partition := group.partitions[groupKey{position.Topic, position.Partition}]
	partition.position, partition.initialized = position, true
	return true
}
func (group *Group) reset(policy string, bounds Page, kind fault.Kind) (int64, error) {
	switch policy {
	case "earliest":
		return bounds.logStart, nil
	case "latest":
		return bounds.stable, nil
	default:
		return 0, failure(kind, "group-offset-policy")
	}
}

// bounds confirms name-only ListOffsets against an ID-addressed Fetch. No data
// is decoded; even a reset cannot adopt a recreated topic's offsets.
func (group *Group) bounds(ctx context.Context, position Position) (Page, error) {
	topics, err := group.client.owner.metadata(ctx)
	if err != nil {
		return Page{}, err
	}
	topic := topics[position.Topic]
	if topic.ID != position.TopicID || position.Partition < 0 || int(position.Partition) >= len(topic.leaders) {
		return Page{}, failure(ErrIdentity, "group-bounds")
	}
	if ctx.Err() != nil {
		return Page{}, failure(ErrRead, "group-bounds", ctx.Err(), context.Cause(ctx))
	}
	request := kmsg.NewPtrListOffsetsRequest()
	request.IsolationLevel = 1
	partition := kmsg.NewListOffsetsRequestTopicPartition()
	partition.Partition, partition.Timestamp = position.Partition, -2
	request.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: position.Topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{partition}}}
	response, err := group.client.owner.native.Broker(int(topic.leaders[position.Partition])).Request(nativeContext{context.WithoutCancel(ctx)}, request)
	if err != nil {
		return Page{}, nativeFailure(ErrRead, "group-bounds", ctx, err)
	}
	offsets := response.(*kmsg.ListOffsetsResponse)
	if len(offsets.Topics) != 1 || offsets.Topics[0].Topic != position.Topic || len(offsets.Topics[0].Partitions) != 1 {
		return Page{}, failure(ErrIdentity, "group-bounds")
	}
	observed := offsets.Topics[0].Partitions[0]
	if observed.Partition != position.Partition || observed.Offset < 0 || observed.ErrorCode != 0 {
		return Page{}, failure(ErrRead, "group-bounds", kerr.ErrorForCode(observed.ErrorCode))
	}
	position.Offset = observed.Offset
	raw, err := group.client.fetchPartition(ctx, position, topic.leaders[position.Partition])
	if err != nil {
		return Page{}, err
	}
	if raw.ErrorCode != 0 || raw.LogStartOffset < 0 || raw.LastStableOffset < raw.LogStartOffset || raw.HighWatermark < raw.LastStableOffset {
		return Page{}, failure(ErrRead, "group-bounds", kerr.ErrorForCode(raw.ErrorCode))
	}
	return Page{observed: true, logStart: raw.LogStartOffset, highWatermark: raw.HighWatermark, stable: raw.LastStableOffset}, nil
}
