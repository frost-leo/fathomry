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

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// GroupEvent identifies the last coalesced membership transition.
type GroupEvent uint8

const (
	GroupOpening GroupEvent = iota
	GroupAssigned
	GroupRevoked
	GroupLost
	GroupClosed
)

// Assignment describes currently observed physical ownership.
type Assignment struct {
	private
	Position Position
}

// GroupSnapshot is authoritative current state. Revision gaps indicate coalesced
// events; they never grant stale assignment authority.
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
func groupSnapshot(value native.GroupSnapshot) GroupSnapshot {
	result := GroupSnapshot{Revision: value.Revision, Event: GroupEvent(value.Event), MemberID: value.MemberID, Generation: value.Generation, Ready: value.Ready, Err: translate(value.Err, "group")}
	for _, item := range value.AssignmentsCopy() {
		result.assignments = append(result.assignments, Assignment{Position: position(item.Position)})
	}
	return result
}
func (value Result) Group() GroupSnapshot { return groupSnapshot(value.native.Group()) }

// GroupBatch retains one unforgeable assignment-bound whole-page commit token.
// It does not expose arbitrary offset mutation or native session authority.
type GroupBatch struct {
	private
	value native.GroupBatch
}

func (value GroupBatch) Page() Page         { return Page{value: value.value.Page()} }
func (value GroupBatch) Start() Position    { return position(value.value.Start()) }
func (value Result) GroupBatch() GroupBatch { return GroupBatch{value: value.native.GroupBatch()} }

// Group owns classic membership and retained assignment-bound batch authority.
// Loss or unknown commit terminates it. Explicitly open another session to rejoin;
// neither source replacement nor evidence Retry migrates this session.
type Group struct {
	private
	operation *operation
	native    *native.Group
}

// ConsumeGroup starts explicit classic membership, not assignment readiness.
// It retains one source generation and never uses the standalone OffsetGroup.
func (client *Client) ConsumeGroup(lifetime context.Context) (*Group, error) {
	var group *Group
	var setup error
	receipt, err := client.dispatch(lifetime, "group", func(op *operation, bound *native.Client) {
		session, err := bound.ConsumeGroup(op.lifetime, op.correlation())
		setup = translate(err, "group")
		if session == nil {
			op.finish(nil, err)
			return
		}
		group = &Group{operation: op, native: session}
		if keepErr := op.keep(session.Receipt(), err, func() { session.Close() }); keepErr != nil {
			session.Close()
			op.finish(nil, keepErr)
		}
	})
	if group == nil {
		_, err = result(receipt, err)
	}
	return group, combineErrors(err, setup)
}
func (group *Group) Receipt() *adapters.Receipt[Result] {
	if group == nil || group.operation == nil {
		return nil
	}
	return group.operation.call.Receipt()
}

// Snapshot returns bounded current state without network I/O or another slot.
func (group *Group) Snapshot() GroupSnapshot {
	if group == nil || group.native == nil {
		return GroupSnapshot{}
	}
	return groupSnapshot(group.native.Snapshot())
}

// Wait observes a revision newer than after; cancellation stops only this wait.
func (group *Group) Wait(ctx context.Context, after uint64) (Result, error) {
	if group == nil || group.native == nil {
		return Result{}, fail(ErrInput, "group-wait")
	}
	return group.operation.child(ctx, "group-wait", func(ctx context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return group.native.Wait(ctx, id, after)
	})
}

// Next fairly reads one eligible partition, retaining a whole-page commit token.
// ErrUnavailable is a finite observation, not a hidden polling retry.
func (group *Group) Next(ctx context.Context) (Result, error) {
	if group == nil || group.native == nil {
		return Result{}, fail(ErrInput, "group-next")
	}
	return group.operation.child(ctx, "group-next", group.native.Next)
}

// CommitBatch declares the complete page processed; stale or foreign tokens refuse.
// It never infers earlier processing from an arbitrary higher offset.
func (group *Group) CommitBatch(ctx context.Context, batch GroupBatch) (Result, error) {
	if group == nil || group.native == nil {
		return Result{}, fail(ErrInput, "group-commit")
	}
	return group.operation.child(ctx, "group-commit", func(ctx context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return group.native.CommitBatch(ctx, id, batch.value)
	})
}

// Close drives native leave without another admission slot or automatic commit.
// A canceled wait retains the root receipt and continued cleanup.
func (group *Group) Close(ctx context.Context) (Result, error) {
	if group == nil || group.native == nil || ctx == nil {
		return Result{}, fail(ErrInput, "close")
	}
	group.native.Close()
	return waitResult(ctx, group.Receipt())
}
