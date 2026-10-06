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

package nacos

import (
	"context"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Evidence preserves observed facts independently of the caller's result.
// MutationPresent separates a mutation result from unrelated read/lifetime facts.
// FailedIndex is -1 when no failed batch slot was identified. No payload is logged.
type Evidence struct {
	Documents       int
	FailedIndex     int
	MutationPresent bool
	Mutation        MutationResult
}

// Client is a concurrent-safe operation facade, with no Close authority.
// Direct facades use their Owner; Using facades borrow a generation per operation.
type Client struct {
	private
	endpoint adapters.Endpoint[Evidence]
	direct   Handle
	source   *resource.Ref[Handle]
}

func bind(dependencies Dependencies) (adapters.Endpoint[Evidence], error) {
	return adapters.Bind(dependencies.Runtime, adapters.Declaration[Evidence]{Evidence: dependencies.Evidence, Observer: dependencies.Observer, Copy: func(value Evidence) Evidence { return value }})
}

// Using constructs a resource-backed facade without borrowing yet. Each operation
// borrows once; a subscription guard retains that lease until native cleanup.
func Using(source resource.Ref[Handle], dependencies Dependencies) (*Client, error) {
	if _, err := source.Inspect(); err != nil {
		return nil, err
	}
	endpoint, err := bind(dependencies)
	if err != nil {
		return nil, err
	}
	return &Client{endpoint: endpoint, source: &source}, nil
}

func request(operation string, workBytes int64) adapters.Request {
	return adapters.Request{Operation: "config.nacos." + operation, WorkBytes: workBytes, EvidenceBytes: 64 << 10}
}
func (client *Client) dispatch(ctx context.Context, operation string, workBytes int64, work func(*adapters.Call[Evidence], *ownerState)) (*adapters.Receipt[Evidence], error) {
	if client == nil {
		return nil, fail(ErrInput, operation)
	}
	run := func(call *adapters.Call[Evidence], handle Handle) {
		if handle.state == nil || handle.state.native == nil {
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: fail(ErrInput, operation)})
			return
		}
		if handle.state.call.Context().Err() != nil {
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: fail(ErrClosed, operation, handle.state.call.Context().Err(), context.Cause(handle.state.call.Context()))})
			return
		}
		work(call, handle.state)
	}
	if client.source != nil {
		return adapters.Using(ctx, client.endpoint, *client.source, request(operation, workBytes), run)
	}
	return client.endpoint.Run(ctx, request(operation, workBytes), func(call *adapters.Call[Evidence]) { run(call, client.direct) })
}
func (client *Client) run(ctx context.Context, operation string, work func(context.Context, *native.Client) (Evidence, error)) error {
	receipt, err := client.dispatch(ctx, operation, 2*MaxWireBytes, func(call *adapters.Call[Evidence], state *ownerState) {
		evidence, err := work(call.Context(), state.native)
		_ = call.Resolve(adapters.Outcome[Evidence]{Value: evidence, Present: true, Primary: translate(err, operation)})
	})
	if err != nil {
		return err
	}
	value, _ := receipt.Snapshot()
	return value.Err()
}
