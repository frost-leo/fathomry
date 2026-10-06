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

package viper

import (
	"context"

	"github.com/frost-leo/fathomry/adapters/v1"
)

// Evidence contains bounded non-payload facts. Lifecycle records resolve at
// actual subscription cleanup. Errors remain in the independent common receipt.
type Evidence struct {
	Documents int
	Missing   bool
}

// Client is a non-owning concurrent-safe capability entry. It owns no transport
// and cannot close the common runtime. Copies share the same binding.
type Client struct {
	private
	endpoint adapters.Endpoint[Evidence]
}

// New performs no source I/O or implicit global lookup.
func New(dependencies Dependencies) (*Client, error) {
	endpoint, err := adapters.Bind(dependencies.Runtime, adapters.Declaration[Evidence]{Evidence: dependencies.Evidence, Observer: dependencies.Observer, Copy: func(value Evidence) Evidence { return value }})
	if err != nil {
		return nil, err
	}
	return &Client{endpoint: endpoint}, nil
}

func (client *Client) run(ctx context.Context, operation string, work func(context.Context) (Evidence, error)) error {
	if client == nil {
		return fail(ErrInput, operation)
	}
	receipt, err := client.endpoint.Run(ctx, request(operation), func(call *adapters.Call[Evidence]) {
		facts, err := work(call.Context())
		_ = call.Resolve(adapters.Outcome[Evidence]{Value: facts, Present: true, Primary: err})
	})
	if err != nil {
		return err
	}
	snapshot, _ := receipt.Snapshot()
	return snapshot.Err()
}
func request(operation string) adapters.Request {
	return adapters.Request{Operation: "config.viper." + operation, WorkBytes: 8 << 20, EvidenceBytes: 64 << 10}
}
