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
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// GroupEvent is the last membership transition. Revisions expose coalesced gaps;
// the current assignment snapshot, not event delivery, is authoritative.
type GroupEvent uint8

const (
	GroupOpening GroupEvent = iota
	GroupAssigned
	GroupRevoked
	GroupLost
	GroupClosed
)

// Assignment describes current physical ownership, not processing completion.
type Assignment struct {
	private
	Position Position
}

// GroupSnapshot is immutable process-local membership evidence. Loss is terminal;
// a caller may explicitly open a new session rather than silently resume old work.
type GroupSnapshot struct {
	private
	Revision    uint64
	Event       GroupEvent
	MemberID    string
	Generation  int32
	Ready       bool
	Err         error
	assignments []Assignment
}

func (value GroupSnapshot) AssignmentsCopy() []Assignment {
	return append([]Assignment{}, value.assignments...)
}
func (result Result) Group() GroupSnapshot {
	if result.data == nil {
		return GroupSnapshot{}
	}
	return result.data.group
}

// GroupBatch owns a validated page and an unforgeable, session-bound commit token.
// CommitBatch explicitly declares the entire page processed. A higher page cannot
// bypass an uncommitted page of the same partition. No business effect is inferred.
type GroupBatch struct {
	private
	page  Page
	token *groupToken
}

func (batch GroupBatch) Page() Page { return batch.page }
func (batch GroupBatch) Start() Position {
	if batch.token == nil {
		return Position{}
	}
	return batch.token.start
}
func (result Result) GroupBatch() GroupBatch {
	if result.data == nil {
		return GroupBatch{}
	}
	return result.data.batch
}

type groupToken struct {
	identity   *groupIdentity
	revision   uint64
	member     string
	generation int32
	start      Position
	next       int64
}

// A nonzero-sized identity does not retain native/source owners in batch evidence.
type groupIdentity struct{ marker byte }

type groupPartition struct {
	position    Position
	initialized bool
	pending     *groupToken
}
type groupKey struct {
	topic     string
	partition int32
}

// Group owns one classic cooperative-sticky membership client and one source
// root. It never calls native Poll or commits automatically. Child calls reject
// concurrent use. Close fences new work, cancels children and joins native leave;
// canceled waits never relinquish that ownership.
type Group struct {
	private
	client              *Client
	identity            *groupIdentity
	call                *invocation.Call[Result]
	native              *kgo.Client
	lifetime            context.Context
	cancel              context.CancelFunc
	mu                  sync.Mutex
	partitions          map[groupKey]*groupPartition
	revision            uint64
	event               GroupEvent
	member              string
	generation          int32
	ready               bool
	active              bool
	closing             bool
	closeRequested      bool
	expectedOffsets     map[checkpointKey]bool
	primary             error
	nativeErrorCaptured bool
	changed             chan struct{}
	idle                chan struct{}
	done                chan struct{}
	roundRobin          int
}

func validOffsetPolicy(value string) bool {
	return value == "error" || value == "earliest" || value == "latest"
}

// ConsumeGroup starts membership for the explicitly configured ConsumerGroup and
// Topics. It requires a cancellable lifetime. Opening is not assignment readiness.
// OffsetGroup remains an independent, non-member checkpoint namespace.
func (client *Client) ConsumeGroup(ctx context.Context, id fault.Correlation) (*Group, error) {
	if client == nil || client.owner == nil || ctx == nil {
		return nil, failure(ErrInput, "group")
	}
	value := client.owner.settings
	if value.ConsumerGroup == "" {
		return nil, failure(ErrUnsupported, "group")
	}
	lifetime, cancel, err := (invocation.Budget{}).Context(ctx, invocation.Lifetime)
	if err != nil {
		return nil, err
	}
	owner := client.owner
	owner.groupMu.Lock()
	if owner.groups >= value.MaxGroupSessions {
		owner.groupMu.Unlock()
		cancel()
		return nil, failure(ErrLimit, "group-sessions")
	}
	owner.groups++
	owner.groupMu.Unlock()
	call, err := client.begin(ctx, id, "group", invocation.Stream)
	if err != nil {
		owner.groupMu.Lock()
		owner.groups--
		owner.groupMu.Unlock()
		cancel()
		return nil, err
	}
	idle := make(chan struct{})
	close(idle)
	group := &Group{client: client, identity: &groupIdentity{}, call: call, lifetime: lifetime, cancel: cancel,
		partitions: make(map[groupKey]*groupPartition), generation: -1,
		expectedOffsets: make(map[checkpointKey]bool),
		changed:         make(chan struct{}), idle: idle, done: make(chan struct{})}
	group.native, err = owner.newClient(false,
		kgo.ConsumerGroup(value.ConsumerGroup),
		kgo.ConsumeTopics(value.Topics...), kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()), kgo.MaxConcurrentFetches(0),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.OnPartitionsAssigned(group.assigned), kgo.OnPartitionsRevoked(group.revoked),
		kgo.OnPartitionsLost(group.lost), kgo.OnOffsetsFetched(group.offsetsFetched),
		kgo.AdjustFetchOffsetsFn(func(context.Context, map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
			// Membership is native; no native data cursor, reset lookup or record parser.
			return nil, nil
		}), kgo.WithHooks(group))
	if err != nil {
		group.fail(err)
	}
	go group.run()
	return group, err
}
func (group *Group) notifyLocked(event GroupEvent) {
	if group.revision == math.MaxUint64 {
		group.closing, group.ready = true, false
		if group.primary == nil {
			group.primary = failure(ErrLimit, "group-revision")
		}
		group.cancel()
	} else {
		group.revision++
	}
	group.event = event
	close(group.changed)
	group.changed = make(chan struct{})
}
func (group *Group) assigned(_ context.Context, native *kgo.Client, added map[string][]int32) {
	member, generation := native.GroupMetadata()
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.closing {
		return
	}
	if !validText(member, 512) || generation < 0 {
		group.failLocked(failure(ErrIdentity, "group-member"))
		return
	}
	for topic, partitions := range added {
		expected, ok := group.client.owner.topics[topic]
		for _, partition := range partitions {
			if !ok || partition < 0 || int(partition) >= expected.Partitions {
				group.failLocked(failure(ErrIdentity, "group-assignment"))
				return
			}
			key := groupKey{topic, partition}
			group.expectedOffsets[checkpointKey{expected.ID, partition}] = true
			if _, exists := group.partitions[key]; !exists {
				if len(group.partitions) == group.client.owner.settings.MaxAssignments {
					group.failLocked(failure(ErrLimit, "group-assignments"))
					return
				}
				group.partitions[key] = &groupPartition{position: Position{ClusterID: group.client.owner.settings.ClusterID,
					Topic: topic, TopicID: expected.ID, Partition: partition}}
			}
		}
	}
	// A retained partition keeps its selected cursor, even without a checkpoint.
	// Reapplying latest here could skip an uncommitted page. Tokens still expire.
	for _, partition := range group.partitions {
		partition.pending = nil
	}
	group.member, group.generation, group.ready = strings.Clone(member), generation, true
	group.notifyLocked(GroupAssigned)
}
func (group *Group) revoked(_ context.Context, _ *kgo.Client, removed map[string][]int32) {
	group.mu.Lock()
	defer group.mu.Unlock()
	for topic, partitions := range removed {
		for _, partition := range partitions {
			delete(group.partitions, groupKey{topic, partition})
			delete(group.expectedOffsets, checkpointKey{group.client.owner.topics[topic].ID, partition})
		}
	}
	group.ready = false
	group.notifyLocked(GroupRevoked)
}
func (group *Group) lost(_ context.Context, _ *kgo.Client, _ map[string][]int32) {
	group.mu.Lock()
	defer group.mu.Unlock()
	clear(group.partitions)
	clear(group.expectedOffsets)
	group.failLocked(failure(ErrState, "group-lost"))
}

// OnGroupManageError is a code-owned native hook; it only retains one cause.
func (group *Group) OnGroupManageError(err error) {
	group.mu.Lock()
	if !group.nativeErrorCaptured {
		group.primary = failure(ErrState, "group-lost", group.primary, err)
		group.nativeErrorCaptured = true
	}
	group.mu.Unlock()
}
func (group *Group) offsetsFetched(ctx context.Context, _ *kgo.Client, response *kmsg.OffsetFetchResponse) error {
	group.mu.Lock()
	defer group.mu.Unlock()
	if ctx.Err() != nil || group.closing {
		return context.Canceled
	}
	var err error
	if response.Version != 10 || len(response.Groups) != 1 || response.Groups[0].Group != group.client.owner.settings.ConsumerGroup {
		err = failure(ErrIdentity, "group-offsets")
	} else {
		seen := make(map[checkpointKey]bool)
		for _, topic := range response.Groups[0].Topics {
			for _, partition := range topic.Partitions {
				key := checkpointKey{topic.TopicID, partition.Partition}
				if !group.expectedOffsets[key] || seen[key] {
					err = failure(ErrIdentity, "group-offsets")
					break
				}
				seen[key] = true
				if cause := kerr.ErrorForCode(partition.ErrorCode); cause != nil {
					err = failure(ErrOffsets, "group-offsets", cause)
					break
				}
			}
			if err != nil {
				break
			}
		}
		if err == nil && len(seen) != len(group.expectedOffsets) {
			err = failure(ErrOffsets, "group-offsets-incomplete")
		}
	}
	if err != nil {
		group.failLocked(err)
	} else {
		clear(group.expectedOffsets)
	}
	return err
}
func (group *Group) failLocked(err error) {
	if group.primary == nil {
		group.primary = err
	}
	group.closing, group.ready = true, false
	group.notifyLocked(GroupLost)
	group.cancel()
}
func (group *Group) fail(err error) { group.mu.Lock(); group.failLocked(err); group.mu.Unlock() }
func (group *Group) run() {
	defer close(group.done)
	<-group.lifetime.Done()
	group.mu.Lock()
	group.closing, group.ready = true, false
	idle := group.idle
	group.mu.Unlock()
	// Drive native shutdown before waiting for a child that may itself need it.
	// The native client has its own context: canceling its parent before Leave
	// would prevent the coordinator request and strand remote membership.
	var cleanup error
	if group.native != nil {
		ctx, cancel := context.WithTimeout(context.Background(), group.client.owner.settings.CleanupTimeout)
		if err := group.native.LeaveGroupContext(nativeContext{ctx}); err != nil {
			cleanup = failure(ErrCleanup, "group-leave", err)
		}
		cancel()
		group.native.Close()
	}
	<-idle
	group.mu.Lock()
	primary := group.primary
	if primary == nil && !group.closeRequested {
		primary = failure(ErrState, "group-lifetime", group.lifetime.Err(), context.Cause(group.lifetime))
	}
	group.primary = primary
	clear(group.partitions)
	group.ready = false
	group.notifyLocked(GroupClosed)
	snapshot := group.snapshotLocked()
	group.mu.Unlock()
	group.client.owner.groupMu.Lock()
	group.client.owner.groups--
	group.client.owner.groupMu.Unlock()
	group.call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: &resultData{group: snapshot}}, Primary: primary, Cleanup: cleanup})
}
func (group *Group) snapshotLocked() GroupSnapshot {
	snapshot := GroupSnapshot{Revision: group.revision, Event: group.event, MemberID: group.member,
		Generation: group.generation, Ready: group.ready && !group.closing, Err: group.primary}
	for _, partition := range group.partitions {
		snapshot.assignments = append(snapshot.assignments, Assignment{Position: partition.position})
	}
	sort.Slice(snapshot.assignments, func(a, b int) bool {
		left, right := snapshot.assignments[a].Position, snapshot.assignments[b].Position
		if left.Topic != right.Topic {
			return left.Topic < right.Topic
		}
		return left.Partition < right.Partition
	})
	return snapshot
}

// Snapshot copies authoritative bounded membership state without network I/O.
// A revision jump greater than one means notifications were coalesced.
func (group *Group) Snapshot() GroupSnapshot {
	if group == nil {
		return GroupSnapshot{}
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	return group.snapshotLocked()
}
func (group *Group) Receipt() *invocation.Receipt[Result] {
	if group == nil || group.call == nil {
		return nil
	}
	return group.call.Receipt()
}

// Close never commits. Its root receipt remains independently observable.
func (group *Group) Close() *invocation.Receipt[Result] {
	if group == nil || group.call == nil {
		return nil
	}
	group.mu.Lock()
	group.closing, group.ready = true, false
	if group.lifetime.Err() == nil {
		group.closeRequested = true
	}
	group.mu.Unlock()
	group.cancel()
	return group.Receipt()
}
func (group *Group) enter() error {
	if group == nil || group.call == nil {
		return failure(ErrInput, "group")
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.closing || group.active || group.lifetime.Err() != nil {
		return failure(ErrState, "group", group.primary)
	}
	group.active = true
	group.idle = make(chan struct{})
	return nil
}
func (group *Group) leave() {
	group.mu.Lock()
	group.active = false
	close(group.idle)
	group.mu.Unlock()
}
func (group *Group) operationContext(ctx context.Context) (context.Context, func()) {
	child, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(group.lifetime, func() { cancel(context.Cause(group.lifetime)) })
	if group.lifetime.Err() != nil {
		cancel(context.Cause(group.lifetime))
	}
	return child, func() { stop(); cancel(nil) }
}

// Wait waits for a membership revision after the supplied revision. Initial
// assignment can be observed with after=0. Cancellation never leaves the group.
func (group *Group) Wait(ctx context.Context, id fault.Correlation, after uint64) (*invocation.Receipt[Result], error) {
	if ctx == nil {
		return nil, failure(ErrInput, "group-wait")
	}
	if err := group.enter(); err != nil {
		return nil, err
	}
	defer group.leave()
	ctx, cancel := group.operationContext(ctx)
	defer cancel()
	call, err := group.client.beginWithin(ctx, id, "group-wait", group.call)
	if err != nil {
		return nil, err
	}
	_ = call.Execute(ctx, invocation.Budget{Limit: group.client.owner.settings.Timeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Result] {
		for {
			group.mu.Lock()
			snapshot, changed := group.snapshotLocked(), group.changed
			group.mu.Unlock()
			if snapshot.Revision > after {
				return invocation.Outcome[Result]{Present: true, Value: Result{data: &resultData{group: snapshot}}}
			}
			select {
			case <-changed:
			case <-work.Done():
				return invocation.Outcome[Result]{Primary: failure(ErrState, "group-wait", work.Err(), context.Cause(work))}
			}
		}
	})
	return call.Receipt(), nil
}
func (*Group) LogValue() slog.Value         { return slog.StringValue("kafka[restricted]") }
func (*GroupSnapshot) LogValue() slog.Value { return slog.StringValue("kafka[restricted]") }
func (*GroupBatch) LogValue() slog.Value    { return slog.StringValue("kafka[restricted]") }
func (*Assignment) LogValue() slog.Value    { return slog.StringValue("kafka[restricted]") }
