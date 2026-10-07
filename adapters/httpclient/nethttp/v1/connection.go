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

package nethttp

import (
	"context"
	"errors"
	"net/http"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	native "github.com/frost-leo/fathomry/internal/httpclient/nethttp/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Connection owns one controlled, source-bounded direct connection, not its
// source. One response child may be outstanding; descendants retain this root's
// actual generation and route. Cross-authority redirects are refused.
type Connection struct {
	private
	native *native.Connection
	group  *family
}

// Connect establishes one connection independent of the native transport pool.
// ctx owns the connection lifetime. Close is mandatory; source/ctx cancellation
// also initiates cleanup. No ClientConn/Reserve/Release authority escapes.
func (client *Client) Connect(ctx context.Context, scheme, address string, options ...RequestOptions) (*Connection, *adapters.Receipt[Result], error) {
	if len(options) > 1 {
		return nil, nil, fail(ErrInput, "connect")
	}
	var connection *Connection
	var nativeErr error
	receipt, err := client.dispatch(ctx, ctx, "connect", func(group *family) {
		value, receipt, err := group.native.Connect(group.lifetime, group.rootCorrelation(), scheme, address, requestOptions(options)...)
		nativeErr = translate(err, "connect")
		var cleanup func(context.Context) error
		if value != nil {
			connection = &Connection{native: value, group: group}
			cleanup = value.Close
		}
		group.attach(group.call, group.guard, receipt, err, true, nil, group.lifetime, cleanup)
	})
	return connection, receipt, errors.Join(err, nativeErr)
}
func (connection *Connection) Receipt() *adapters.Receipt[Result] {
	if connection == nil || connection.group == nil {
		return nil
	}
	return connection.group.call.Receipt()
}
func (connection *Connection) Close(ctx context.Context) error {
	if connection == nil || connection.native == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	return translate(connection.native.Close(ctx), "close")
}

// Do retains an independently evidenced child response on this exact connection.
func (connection *Connection) Do(ctx, cleanupCtx context.Context, input *http.Request) (*adapters.Receipt[Result], error) {
	if connection == nil || connection.native == nil || cleanupCtx == nil {
		return nil, fail(ErrInput, "do")
	}
	var nativeErr error
	receipt, err := connection.group.child(ctx, "do", func(live context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error, func(context.Context) error) {
		receipt, err := connection.native.Do(live, cleanupCtx, id, input)
		nativeErr = translate(err, "do")
		return receipt, err, nil
	})
	return receipt, errors.Join(err, nativeErr)
}

// Open preserves child method-context authority and the parent's cancellation.
func (connection *Connection) Open(ctx context.Context, input *http.Request) (*Stream, *adapters.Receipt[Result], error) {
	if connection == nil || connection.native == nil {
		return nil, nil, fail(ErrInput, "open-response")
	}
	var stream *Stream
	var nativeErr error
	receipt, err := connection.group.child(ctx, "open-response", func(live context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error, func(context.Context) error) {
		value, receipt, err := connection.native.Open(live, id, input)
		nativeErr = translate(err, "open-response")
		if value == nil {
			return receipt, err, nil
		}
		stream = &Stream{native: value}
		return receipt, err, value.Close
	})
	if stream != nil {
		stream.receipt = receipt
	}
	return stream, receipt, errors.Join(err, nativeErr)
}
