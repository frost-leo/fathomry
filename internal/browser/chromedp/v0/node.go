/*
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

package chromedp

import (
	"context"
	"slices"
	"sync/atomic"

	"github.com/chromedp/cdproto/cdp"
	sdk "github.com/chromedp/chromedp"
)

// Node borrows one native DOM node from its page's document cache. It owns no
// remote object or independent producer. Identity and metadata are frozen, and
// the original page/context/source and native document invalidation revoke use.
type Node struct {
	private
	session     *Session
	native      *sdk.Node
	id          cdp.NodeID
	epoch       uint64
	frame       *nodeFrame
	frameEpoch  uint64
	invalidated <-chan struct{}
	data        []byte
}

type nodeFrame struct {
	epoch atomic.Uint64
	polls map[*framePoll]struct{}
}

type framePoll struct{ cancel context.CancelCauseFunc }

func (origin *objectOrigin) invalidateNodeFrame(id cdp.FrameID, all bool) {
	origin.nodesMu.Lock()
	defer origin.nodesMu.Unlock()
	origin.nodeRevision++
	if all {
		for _, frame := range origin.nodeFrames {
			frame.epoch.Add(1)
			for poll := range frame.polls {
				poll.cancel(failure(ErrState, "poll-frame-retired"))
			}
		}
		clear(origin.nodeFrames)
	} else if frame := origin.nodeFrames[id]; frame != nil {
		frame.epoch.Add(1)
		for poll := range frame.polls {
			poll.cancel(failure(ErrState, "poll-frame-retired"))
		}
	}
}

// DataCopy deliberately exposes a detached native protocol-node snapshot, not
// chromedp's mutable node tree or its locks.
func (node *Node) DataCopy() []byte {
	if node == nil {
		return nil
	}
	return append([]byte(nil), node.data...)
}
func (node *Node) valid(session *Session) error {
	if node == nil || node.native == nil || node.session == nil || node.id == 0 {
		return failure(ErrInput, "node")
	}
	if node.session.sessionState != session.sessionState {
		return failure(ErrInput, "node-origin")
	}
	if session.work.Err() != nil || session.objectOrigin == nil || node.epoch != session.objectOrigin.epoch.Load() || node.frame != nil && node.frameEpoch != node.frame.epoch.Load() {
		return failure(ErrState, "node-document")
	}
	select {
	case <-node.invalidated:
		return failure(ErrState, "node-invalidated")
	default:
	}
	return nil
}

// FromNode retains native subtree/readiness behavior, including the SDK's
// same-process iframe handling. Nil keeps native root-selection semantics.
func FromNode(node *Node) QueryOption {
	if node == nil {
		return QueryOption{native: sdk.FromNode(nil)}
	}
	if node.native == nil {
		return QueryOption{}
	}
	return QueryOption{native: sdk.FromNode(node.native), node: node}
}

// SelectNodes replaces selector lookup with native NodeIDs lookup from managed
// node origins. The adapter-owned accessor runs only after count admission.
// It borrows its input through action completion and never receives a Target.
func SelectNodes(count int, nodeAt func(int) *Node) QueryOption {
	if count < 0 || nodeAt == nil {
		return QueryOption{}
	}
	return QueryOption{nodes: count, nodeAt: nodeAt}
}

func (session *Session) queryWork(ctx context.Context, options []QueryOption) (context.Context, []sdk.QueryOption, error) {
	values, check, err := session.prepareQuery(options)
	if err != nil {
		return nil, nil, err
	}
	return session.commandWork(ctx, true, check), values, nil
}

func (session *Session) prepareQuery(options []QueryOption) ([]sdk.QueryOption, func() error, error) {
	if len(options) > 64 {
		return nil, nil, failure(ErrInput, "query-options")
	}
	options = slices.Clone(options)
	var selected []*Node
	for index, option := range options {
		if option.nodeAt == nil {
			continue
		}
		if int64(option.nodes)+int64(len(selected)) > session.owner.settings.MaxCommandBytes/32 {
			return nil, nil, failure(ErrLimit, "node-selector")
		}
		identities := make(sdk.NodeIDs, option.nodes)
		for position := range option.nodes {
			node := option.nodeAt(position)
			if err := node.valid(session); err != nil {
				return nil, nil, err
			}
			selected = append(selected, node)
			identities[position] = node.id
		}
		options[index].native = sdk.ByFunc(func(ctx context.Context, target *sdk.Target, _ *sdk.Node) ([]cdp.NodeID, error) {
			return sdk.QueryNodeIDs(identities, sdk.AtLeast(0))(ctx, target)
		})
	}
	check := func() error {
		for _, option := range options {
			if option.node != nil {
				if err := option.node.valid(session); err != nil {
					return err
				}
			}
		}
		for _, node := range selected {
			if err := node.valid(session); err != nil {
				return err
			}
		}
		return nil
	}
	if err := check(); err != nil {
		return nil, nil, err
	}
	return queryOptions(options), check, nil
}

func deferredQuery[T any](size int64, options []QueryOption, build func([]sdk.QueryOption) sdk.Action[T]) Action[T] {
	return Action[T]{bytes: size, local: func(ctx context.Context, session *Session) (value T, err error) {
		work, native, err := session.queryWork(ctx, options)
		if err != nil {
			return value, err
		}
		return sdk.Run(work, build(native))
	}}
}

// QueryNodes uses the original selector/readiness implementation. Returned
// metadata/capabilities are bounded before retention. No native owner escapes.
func QueryNodes[S TextSelector](ctx context.Context, session *Session, selector S, options ...QueryOption) ([]*Node, error) {
	options = cloneQueryOptions(options)
	return RunAction(ctx, session, Action[[]*Node]{bytes: int64(len(selector)), local: func(ctx context.Context, session *Session) ([]*Node, error) {
		work, values, err := session.queryWork(ctx, options)
		if err != nil {
			return nil, err
		}
		if err := session.startObjectOrigin(); err != nil {
			return nil, err
		}
		epoch := session.objectOrigin.epoch.Load()
		origin := session.objectOrigin
		origin.nodesMu.Lock()
		revision := origin.nodeRevision
		origin.nodesMu.Unlock()
		selected, err := sdk.Run(work, sdk.Nodes(selector, values...))
		if err != nil {
			return nil, err
		}
		record := session.accounting()
		record.mu.Lock()
		remaining := session.owner.settings.MaxResultBytes - record.delivered
		record.mu.Unlock()
		if int64(len(selected)) > remaining/32 {
			return nil, failure(ErrLimit, "node-results")
		}
		nodes := make([]*Node, 0, len(selected))
		var used int64
		for _, value := range selected {
			if value == nil {
				return nil, failure(ErrNative, "node-result")
			}
			buffer := boundedBuffer{limit: remaining - used}
			value.RLock()
			if value.Node == nil || value.Invalidated == nil {
				value.RUnlock()
				return nil, failure(ErrNative, "node-document-identity")
			}
			err := marshalBounded(&buffer, value.Node)
			node := &Node{session: session, native: value, id: value.NodeID, epoch: epoch, invalidated: value.Invalidated, data: buffer.data}
			value.RUnlock()
			if err != nil {
				return nil, err
			}
			frameID, err := enclosingNodeFrame(value, session.owner.settings.MaxCommandBytes/8)
			if err != nil {
				return nil, err
			}
			origin.nodesMu.Lock()
			if revision != origin.nodeRevision {
				origin.nodesMu.Unlock()
				return nil, failure(ErrState, "node-document-changed")
			}
			if origin.nodeFrames == nil {
				origin.nodeFrames = make(map[cdp.FrameID]*nodeFrame)
			}
			frame := origin.nodeFrames[frameID]
			if frame == nil {
				frame = &nodeFrame{}
				origin.nodeFrames[frameID] = frame
			}
			node.frame, node.frameEpoch = frame, frame.epoch.Load()
			origin.nodesMu.Unlock()
			if err := node.valid(session); err != nil {
				return nil, err
			}
			nodes = append(nodes, node)
			used += int64(len(buffer.data))
		}
		record.mu.Lock()
		defer record.mu.Unlock()
		if used > session.owner.settings.MaxResultBytes-record.delivered {
			return nil, failure(ErrLimit, "node-output")
		}
		record.delivered += used
		return nodes, nil
	}})
}

func enclosingNodeFrame(node *sdk.Node, limit int64) (cdp.FrameID, error) {
	for range limit {
		if node == nil {
			return "", failure(ErrNative, "node-frame-identity")
		}
		node.RLock()
		id, parent := node.FrameID, node.Parent
		node.RUnlock()
		if id != "" {
			return id, nil
		}
		node = parent
	}
	return "", failure(ErrLimit, "node-ancestry")
}
