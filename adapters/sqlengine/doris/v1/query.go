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

package doris

import (
	"context"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/sqlengine/doris/v1"
)

// Query retains one finite bounded result. It is not read-only authorization.
// A non-nil receipt owns accepted outcome evidence, including partial failures.
func (client *Client) Query(ctx context.Context, sql string) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "query", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.Query(group.lifetime, group.rootCorrelation(), sql)
	})
}

// Exec dispatches one authorized text command without mutation retries.
// SQL acknowledgement is neither commit nor visibility nor per-Item success.
func (client *Client) Exec(ctx context.Context, sql string) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "exec", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.Exec(group.lifetime, group.rootCorrelation(), sql)
	})
}

// Batch is one strict labeled native-table JSON array. JSON is borrowed without
// concurrent mutation until StreamLoad returns. Label reconciliation is caller-owned.
type Batch struct {
	private
	Table, Label string
	JSON         []byte
}

func (client *Client) StreamLoad(ctx context.Context, batch Batch) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "stream-load", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.StreamLoad(group.lifetime, group.rootCorrelation(), native.Batch{Table: batch.Table, Label: batch.Label, JSON: batch.JSON})
	})
}

// InspectLabel observes once, without polling/replay. A visible label is not
// proof of this payload's identity or row quality.
func (client *Client) InspectLabel(ctx context.Context, label string) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "inspect-label", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.InspectLabel(group.lifetime, group.rootCorrelation(), label)
	})
}

// Cursor retains the original source generation and full work envelope.
// Each page has independently reserved evidence. It is not a durable SQL token.
type Cursor struct {
	private
	group  *family
	native *native.Cursor
}

// QueryCursor uses ctx for setup and lifetime for accepted use. Ending successful
// setup does not end the result. Root evidence is terminal and holds no whole-result
// buffer. Always Close abandoned cursors, even when a page reported an error.
func (client *Client) QueryCursor(ctx, lifetime context.Context, sql string) (*Cursor, *adapters.Receipt[Result], error) {
	var cursor *Cursor
	receipt, err := client.dispatch(ctx, lifetime, "query-cursor", func(group *family) {
		setup, stop := joinContexts(ctx, group.lifetime)
		owned, nativeReceipt, err := group.native.QueryCursor(setup, group.lifetime, group.rootCorrelation(), sql)
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

// Next emits a bounded page. Complete means actual EOF, not merely a short page.
// An admitted canceled read ends the cursor; evidence/admission refusal does not.
func (cursor *Cursor) Next(ctx context.Context) (*adapters.Receipt[Result], error) {
	if cursor == nil || cursor.native == nil {
		return nil, fail(ErrState, "next")
	}
	return cursor.group.child(ctx, "cursor-next", func(work context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return cursor.native.Next(work, id)
	})
}

// Close needs no fresh operation/evidence capacity. ctx bounds waiting, not the
// actual release obligation. The terminal receipt retains cleanup separately.
func (cursor *Cursor) Close(ctx context.Context) error {
	if cursor == nil || cursor.native == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	wait, cancel := context.WithTimeout(ctx, cursor.group.timeout)
	defer cancel()
	_ = cursor.native.Close(wait)
	value, err := cursor.Receipt().WaitReleased(wait)
	if err != nil {
		return err
	}
	return value.Err()
}
