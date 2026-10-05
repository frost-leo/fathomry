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

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CommitBatch declares the complete supplied page processed; it does not infer
// business completion. It rejects foreign, stale, forged and already committed
// tokens. A sent commit with an unknown outcome permanently closes this session
// rather than allowing a later commit to race a delayed earlier write.
func (group *Group) CommitBatch(ctx context.Context, id fault.Correlation, batch GroupBatch) (*invocation.Receipt[Result], error) {
	if ctx == nil {
		return nil, failure(ErrInput, "group-commit")
	}
	if err := group.enter(); err != nil {
		return nil, err
	}
	defer group.leave()
	if err := group.validToken(batch.token); err != nil {
		return nil, err
	}
	ctx, cancel := group.operationContext(ctx)
	defer cancel()
	call, err := group.client.beginWithin(ctx, id, "group-commit", group.call)
	if err != nil {
		return nil, err
	}
	_ = call.Execute(ctx, invocation.Budget{Limit: group.client.owner.settings.Timeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Result] {
		data, err := group.commit(work, batch.token)
		return invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: err}
	})
	return call.Receipt(), nil
}
func (group *Group) validToken(token *groupToken) error {
	group.mu.Lock()
	defer group.mu.Unlock()
	if token == nil || token.identity == nil || token.identity != group.identity || !group.currentLocked(token.revision, token.start) {
		return failure(ErrState, "group-stale")
	}
	partition := group.partitions[groupKey{token.start.Topic, token.start.Partition}]
	if partition.pending != token || token.member != group.member || token.generation != group.generation {
		return failure(ErrState, "group-stale")
	}
	return nil
}
func (group *Group) commit(ctx context.Context, token *groupToken) (*resultData, error) {
	checkpoint := Checkpoint{Topic: token.start.Topic, TopicID: token.start.TopicID, Partition: token.start.Partition, Next: token.next}
	data := &resultData{group: group.Snapshot(), checkpoints: []CheckpointResult{{Checkpoint: checkpoint}}}
	if err := group.client.owner.checkIdentity(ctx); err != nil {
		return data, err
	}
	// Install the private guard AFTER stripping caller values, including KIP-848
	// and caller-injected pre-commit functions. The native request is still checked.
	guarded := kgo.PreCommitFnContext(nativeContext{ctx}, func(request *kmsg.OffsetCommitRequest) error {
		if err := group.validToken(token); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return failure(ErrOffsets, "group-commit", ctx.Err(), context.Cause(ctx))
		}
		if request.Group != group.client.owner.settings.ConsumerGroup || request.MemberID != token.member || request.Generation != token.generation ||
			request.InstanceID != nil || len(request.Topics) != 1 {
			return failure(ErrIdentity, "group-commit-request")
		}
		topic := request.Topics[0]
		if topic.Topic != checkpoint.Topic || topic.TopicID != checkpoint.TopicID || len(topic.Partitions) != 1 ||
			topic.Partitions[0].Partition != checkpoint.Partition || topic.Partitions[0].Offset != checkpoint.Next {
			return failure(ErrIdentity, "group-commit-request")
		}
		data.checkpoints[0].State = CheckpointUnknown
		return nil
	})
	var response *kmsg.OffsetCommitResponse
	var commitErr error
	group.native.CommitOffsetsSync(guarded, map[string]map[int32]kgo.EpochOffset{
		checkpoint.Topic: {checkpoint.Partition: {Epoch: -1, Offset: checkpoint.Next}},
	}, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, reply *kmsg.OffsetCommitResponse, err error) {
		response, commitErr = reply, err
	})
	if commitErr == nil {
		if response == nil || response.Version != 10 || len(response.Topics) != 1 || response.Topics[0].TopicID != checkpoint.TopicID ||
			len(response.Topics[0].Partitions) != 1 || response.Topics[0].Partitions[0].Partition != checkpoint.Partition {
			commitErr = failure(ErrIdentity, "group-commit-response")
		} else if cause := kerr.ErrorForCode(response.Topics[0].Partitions[0].ErrorCode); cause != nil {
			commitErr = failure(ErrOffsets, "group-commit-partition", cause)
		} else {
			data.checkpoints[0].State = CheckpointCommitted
			group.mu.Lock()
			if group.currentLocked(token.revision, token.start) {
				partition := group.partitions[groupKey{token.start.Topic, token.start.Partition}]
				partition.position.Offset, partition.pending = token.next, nil
			}
			group.mu.Unlock()
		}
	}
	if commitErr != nil {
		commitErr = nativeFailure(ErrOffsets, "group-commit", ctx, commitErr)
		data.checkpoints[0].Err = commitErr
		if data.checkpoints[0].State == CheckpointUnknown {
			group.fail(commitErr)
		}
	}
	return data, commitErr
}
