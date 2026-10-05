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

	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
)

// Checkpoint is the NEXT offset. Standalone commits declare caller intent and
// do not prove a processed prefix; managed groups instead require GroupBatch.
type Checkpoint struct {
	private
	Topic     string
	TopicID   [16]byte
	Partition int32
	Next      int64
}

// CheckpointState separates unknown commits, ACKs, observations and absence.
type CheckpointState uint8

const (
	CheckpointUnobserved CheckpointState = iota
	CheckpointUnknown
	CheckpointCommitted
	CheckpointObserved
	CheckpointAbsent
)

// CheckpointResult preserves each partition's independent technical evidence.
type CheckpointResult struct {
	private
	Checkpoint Checkpoint
	State      CheckpointState
	Err        error
}

func (value Result) CheckpointsCopy() []CheckpointResult {
	input := value.native.CheckpointsCopy()
	if input == nil {
		return nil
	}
	output := make([]CheckpointResult, len(input))
	for index, item := range input {
		output[index] = CheckpointResult{Checkpoint: Checkpoint{Topic: item.Checkpoint.Topic, TopicID: item.Checkpoint.TopicID,
			Partition: item.Checkpoint.Partition, Next: item.Checkpoint.Next}, State: CheckpointState(item.State), Err: translate(item.Err, "offsets")}
	}
	return output
}

// CommitOffsets writes standalone generation=-1 checkpoints, not group progress.
func (client *Client) CommitOffsets(ctx context.Context, checkpoints []Checkpoint) (Result, error) {
	return client.checkpoints(ctx, checkpoints, true)
}

// FetchOffsets observes only the explicitly selected standalone partitions.
func (client *Client) FetchOffsets(ctx context.Context, checkpoints []Checkpoint) (Result, error) {
	return client.checkpoints(ctx, checkpoints, false)
}

func (client *Client) checkpoints(ctx context.Context, checkpoints []Checkpoint, commit bool) (Result, error) {
	name := "fetch-offsets"
	if commit {
		name = "commit-offsets"
	}
	return result(client.dispatch(ctx, name, func(op *operation, bound *native.Client) {
		if len(checkpoints) == 0 || len(checkpoints) > op.state.maxRecords {
			op.finish(nil, fail(ErrLimit, "checkpoints"))
			return
		}
		input := make([]native.Checkpoint, len(checkpoints))
		for index, value := range checkpoints {
			input[index] = native.Checkpoint{Topic: value.Topic, TopicID: value.TopicID, Partition: value.Partition, Next: value.Next}
		}
		invoke := bound.FetchOffsets
		if commit {
			invoke = bound.CommitOffsets
		}
		receipt, err := invoke(op.lifetime, op.correlation(), input)
		op.finish(receipt, err)
	}))
}
