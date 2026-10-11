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

package iceberg

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/adapters/tableformat/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Client is non-owning. Each Use retains one Fixed/Follow generation for its
// whole callback, including native continuations and cleanup.
type Client struct {
	private
	endpoint adapters.Endpoint[Result]
	direct   Handle
	source   *resource.Ref[Handle]
	lifetime context.Context
	budget   tableformat.Budget
	id       string
}
type Session struct {
	private
	native *native.ManagedSession
}

func Using(lifetime context.Context, ref resource.Ref[Handle], budget tableformat.Budget, d Dependencies) (*Client, error) {
	if lifetime == nil || budget.WorkBytes < 1 || budget.EvidenceBytes < 1 {
		return nil, fail(ErrInput, "using")
	}
	if _, err := ref.Inspect(); err != nil {
		return nil, err
	}
	endpoint, err := bind(d)
	if err != nil {
		return nil, err
	}
	return &Client{endpoint: endpoint, source: &ref, lifetime: lifetime, budget: budget}, nil
}
func (c *Client) WithID(id string) (*Client, error) {
	if c == nil || c.lifetime == nil || len(id) > 256 || !utf8.ValidString(id) || strings.ContainsFunc(id, func(r rune) bool { return r < 32 || r == 127 }) {
		return nil, fail(ErrInput, "id")
	}
	copy := *c
	copy.id = strings.Clone(id)
	return &copy, nil
}
func request(name, id string, work, evidence int64) adapters.Request {
	return adapters.Request{Operation: "tableformat.iceberg." + name, ID: id, WorkBytes: work, EvidenceBytes: evidence}
}

// Use is a synchronous admitted callback family. A receipt denotes accepted
// work, not success; inspect its outcome and independently drain Evidence.
// Captured Session/Table/Transaction/Scan handles are revoked before return.
func (c *Client) Use(ctx context.Context, work func(*Session) error) (*adapters.Receipt[Result], error) {
	if c == nil || c.lifetime == nil || ctx == nil || work == nil {
		return nil, fail(ErrInput, "use")
	}
	live, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := context.AfterFunc(c.lifetime, func() { cancel(errors.Join(c.lifetime.Err(), context.Cause(c.lifetime))) })
	defer stop()
	if c.lifetime.Err() != nil {
		cancel(errors.Join(c.lifetime.Err(), context.Cause(c.lifetime)))
	}
	run := func(call *adapters.Call[Result], handle Handle) {
		state := handle.state
		reject := func(err error) { _ = call.Resolve(adapters.Outcome[Result]{Primary: translate(err, "use")}) }
		if state == nil {
			reject(fail(ErrState, "source"))
			return
		}
		if state.policy.Budget.WorkBytes > c.budget.WorkBytes || state.policy.Budget.EvidenceBytes > c.budget.EvidenceBytes {
			reject(fail(ErrLimit, "generation-budget"))
			return
		}
		release, err := state.use()
		if err != nil {
			reject(err)
			return
		}
		defer release()
		owned, stopWork := context.WithCancelCause(call.Context())
		defer stopWork(nil)
		stopOwner := context.AfterFunc(state.call.Context(), func() { stopWork(context.Cause(state.call.Context())) })
		defer stopOwner()
		if state.call.Context().Err() != nil {
			stopWork(context.Cause(state.call.Context()))
		}
		b := state.prepared.native.Budget()
		inbox, err := invocation.NewInbox[native.ManagedResult](1, b.EvidenceBytes)
		if err != nil {
			reject(err)
			return
		}
		client, err := native.BindManaged(state.assembly, state.selection, inbox)
		if err != nil {
			reject(err)
			return
		}
		snapshot, _ := call.Receipt().Snapshot()
		id := fault.Correlation{Call: "iceberg-" + strconv.FormatUint(snapshot.Info().Sequence, 10)}
		receipt, err := client.Use(owned, id, func(session *native.ManagedSession) error { return work(&Session{native: session}) })
		if receipt == nil {
			reject(err)
			return
		}
		record, claimErr := inbox.Next(context.Background())
		if claimErr != nil {
			reject(claimErr)
			return
		}
		expected, _ := receipt.Result()
		actual, _ := record.Receipt().Result()
		if expected.Context.Correlation != actual.Context.Correlation {
			reject(fail(ErrState, "evidence-origin"))
			return
		}
		value, waitErr := receipt.WaitReleased(context.Background())
		if waitErr != nil {
			reject(waitErr)
			return
		}
		projected := project(value.Outcome.Value, snapshot.Info(), state.identity)
		_ = call.Resolve(adapters.Outcome[Result]{Present: true, Value: projected, Primary: translate(errors.Join(err, value.Outcome.Primary), "use"), Cleanup: translate(value.Outcome.Cleanup, "cleanup")})
		_ = record.Release()
	}
	req := request("use", c.id, c.budget.WorkBytes, c.budget.EvidenceBytes)
	if c.source != nil {
		return adapters.Using(live, c.endpoint, *c.source, req, run)
	}
	return c.endpoint.Run(live, req, func(call *adapters.Call[Result]) { run(call, c.direct) })
}
