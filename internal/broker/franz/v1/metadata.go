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
	"slices"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Topic is copied metadata, not an authorization grant or retention guarantee.
type Topic struct {
	private
	Name       string
	ID         [16]byte
	Partitions int
	leaders    []int32
}

func (owner *connection) metadata(ctx context.Context) (map[string]Topic, error) {
	return owner.metadataWith(ctx, owner.native)
}
func (owner *connection) metadataWith(ctx context.Context, native *kgo.Client) (map[string]Topic, error) {
	request := kmsg.NewPtrMetadataRequest()
	request.AllowAutoTopicCreation = false
	for _, name := range owner.settings.Topics {
		request.Topics = append(request.Topics, kmsg.MetadataRequestTopic{Topic: kmsg.StringPtr(name)})
	}
	if ctx.Err() != nil {
		return nil, failure(ErrConnect, "metadata", ctx.Err(), context.Cause(ctx))
	}
	response, err := request.RequestWith(nativeContext{context.WithoutCancel(ctx)}, native)
	if err != nil {
		return nil, nativeFailure(ErrConnect, "metadata", ctx, err)
	}
	if response.ClusterID == nil || *response.ClusterID != owner.settings.ClusterID {
		return nil, failure(ErrIdentity, "cluster")
	}
	if len(response.Brokers) != len(owner.settings.Brokers) {
		return nil, failure(ErrAuthority, "broker-set")
	}
	if len(response.Topics) != len(owner.settings.Topics) {
		return nil, failure(ErrIdentity, "metadata-topics")
	}
	seenBrokers := make(map[string]bool)
	seenNodes := make(map[int32]bool)
	for _, broker := range response.Brokers {
		address := brokerAddress(broker.Host, broker.Port)
		if !owner.settings.allowed(address) || seenBrokers[address] || broker.NodeID < 0 || seenNodes[broker.NodeID] {
			return nil, failure(ErrAuthority, "metadata")
		}
		seenBrokers[address] = true
		seenNodes[broker.NodeID] = true
	}
	topics := make(map[string]Topic, len(response.Topics))
	totalPartitions := 0
	for _, topic := range response.Topics {
		if topic.Topic == nil || !slices.Contains(owner.settings.Topics, *topic.Topic) {
			return nil, failure(ErrIdentity, "topic")
		}
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			return nil, failure(ErrConnect, "topic", err)
		}
		if topic.TopicID == ([16]byte{}) {
			return nil, failure(ErrUnsupported, "topic-id")
		}
		totalPartitions += len(topic.Partitions)
		if len(topic.Partitions) == 0 || totalPartitions > 4096 {
			return nil, failure(ErrLimit, "partitions")
		}
		leaders := make([]int32, len(topic.Partitions))
		seen := make([]bool, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				return nil, failure(ErrConnect, "partition", err)
			}
			if partition.Partition < 0 || int(partition.Partition) >= len(topic.Partitions) || seen[partition.Partition] || !seenNodes[partition.Leader] {
				return nil, failure(ErrUnsupported, "partitions")
			}
			seen[partition.Partition] = true
			leaders[partition.Partition] = partition.Leader
		}
		if _, duplicate := topics[*topic.Topic]; duplicate {
			return nil, failure(ErrIdentity, "topic")
		}
		topics[*topic.Topic] = Topic{Name: *topic.Topic, ID: topic.TopicID, Partitions: len(topic.Partitions), leaders: leaders}
	}
	return topics, nil
}
func (owner *connection) checkIdentity(ctx context.Context) error {
	return owner.checkIdentityWith(ctx, owner.native)
}
func (owner *connection) checkIdentityWith(ctx context.Context, native *kgo.Client) error {
	current, err := owner.metadataWith(ctx, native)
	if err != nil {
		return err
	}
	for name, expected := range owner.topics {
		if current[name].ID != expected.ID {
			return failure(ErrIdentity, "topic-incarnation")
		}
	}
	return nil
}

// Metadata performs an explicit controlled metadata check against the frozen
// cluster/topic identities. Success is not produce authorization, transaction
// readiness, retention compliance or business acceptance.
func (client *Client) Metadata(ctx context.Context, correlation fault.Correlation) (*invocation.Receipt[Result], error) {
	call, err := client.begin(ctx, correlation, "metadata", invocation.Finite)
	if err != nil {
		return nil, err
	}
	_ = call.Execute(ctx, invocation.Budget{Limit: client.owner.settings.Timeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Result] {
		current, err := client.owner.metadata(work)
		data := &resultData{}
		if err == nil {
			for _, name := range client.owner.settings.Topics {
				topic := current[name]
				topic.leaders = nil
				data.topics = append(data.topics, topic)
				if topic.ID != client.owner.topics[name].ID {
					err = failure(ErrIdentity, "topic-incarnation")
				}
			}
		}
		return invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: err}
	})
	return call.Receipt(), nil
}
