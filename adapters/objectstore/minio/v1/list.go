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

package minio

import (
	"context"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
)

// ListRequest is recursive. StartAfter is lexical, never a version continuation.
// Enumerate retains native continuation; List is the original bounded finite path.
type ListRequest struct {
	private
	Prefix, StartAfter string
	Versions           bool
}

func listRequest(value ListRequest) native.ListRequest {
	return native.ListRequest{Prefix: value.Prefix, StartAfter: value.StartAfter, Versions: value.Versions}
}

// List preserves the finite legacy limit behavior; use Enumerate for continuation.
func (client *Client) List(ctx context.Context, request ListRequest) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "list", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.List(group.lifetime, group.rootCorrelation(), listRequest(request))
	})
}

// Cursor owns contextual object/version continuation, not a snapshot or durable token.
type Cursor struct {
	private
	group  *family
	native *native.Cursor
}

func (client *Client) Enumerate(ctx, lifetime context.Context, request ListRequest) (*Cursor, *adapters.Receipt[Result], error) {
	var cursor *Cursor
	receipt, err := client.dispatch(ctx, lifetime, "enumerate", func(group *family) {
		setup, stop := joinContexts(ctx, group.lifetime)
		owned, nativeReceipt, err := group.native.Enumerate(setup, group.lifetime, group.rootCorrelation(), listRequest(request))
		stop()
		if owned != nil {
			cursor = &Cursor{group: group, native: owned}
		}
		group.attach(group.call, group.guard, nativeReceipt, err, true, nil)
	})
	return cursor, receipt, err
}

func (cursor *Cursor) Receipt() *adapters.Receipt[Result] {
	if cursor == nil || cursor.group == nil {
		return nil
	}
	return cursor.group.call.Receipt()
}

// Next emits one bounded result; empty with Complete means actual native EOF.
// HTTP attempts are counted at the root. A canceled fetch terminates this cursor.
func (cursor *Cursor) Next(ctx context.Context) (*adapters.Receipt[Result], error) {
	if cursor == nil || cursor.native == nil {
		return nil, fail(ErrState, "next")
	}
	return cursor.group.child(ctx, "next", func(work context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return cursor.native.Next(work, id)
	})
}

// Close stops the iterator without a new admission/evidence slot.
func (cursor *Cursor) Close(ctx context.Context) error {
	if cursor == nil || cursor.native == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	_ = cursor.native.Close(ctx)
	value, err := cursor.Receipt().WaitReleased(ctx)
	if err != nil {
		return err
	}
	return value.Err()
}
