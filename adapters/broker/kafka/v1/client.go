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
	"errors"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/adapters/broker/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Client is a concurrent, non-owning facade. Retained Consumer/Group operations
// reject concurrent use. Copies share source limits and permanent producer fences.
type Client struct {
	private
	endpoint adapters.Endpoint[Result]
	direct   Handle
	source   *resource.Ref[Handle]
	lifetime context.Context
	budget   broker.Budget
	id       string
}

// Using retains the actually borrowed public generation through every native
// promise or session child. budget must cover every possible adopted generation;
// larger replacements are refused before dispatch, not silently undercharged.
func Using(lifetime context.Context, ref resource.Ref[Handle], budget broker.Budget, deps Dependencies) (*Client, error) {
	if lifetime == nil || budget.WorkBytes < 1 || budget.WorkBytes > 1<<40 || budget.EvidenceBytes < 1 || budget.EvidenceBytes > 1<<40 {
		return nil, fail(ErrInput, "using")
	}
	if _, err := ref.Inspect(); err != nil {
		return nil, err
	}
	endpoint, err := bind(deps)
	if err != nil {
		return nil, err
	}
	return &Client{endpoint: endpoint, source: &ref, lifetime: lifetime, budget: budget}, nil
}

// WithID freezes optional opaque public correlation; native IDs are code-owned.
func (client *Client) WithID(id string) (*Client, error) {
	if client == nil || client.lifetime == nil || len(id) > 256 || !utf8.ValidString(id) {
		return nil, fail(ErrInput, "id")
	}
	for _, char := range id {
		if char < 0x20 || char == 0x7f {
			return nil, fail(ErrInput, "id")
		}
	}
	copy := *client
	copy.id = strings.Clone(id)
	return &copy, nil
}
func request(operation, id string, work, evidence int64) adapters.Request {
	return adapters.Request{Operation: "broker.kafka." + operation, ID: id, WorkBytes: work, EvidenceBytes: evidence}
}

type operation struct {
	family   *family
	state    *sourceState
	endpoint adapters.Endpoint[Result]
	call     *adapters.Call[Result]
	guard    adapters.Guard
	lifetime context.Context
	stop     func()
	kept     bool
	id       string
}

func (client *Client) dispatch(ctx context.Context, name string, work func(*operation, *native.Client)) (*adapters.Receipt[Result], error) {
	if client == nil || client.lifetime == nil || ctx == nil {
		return nil, fail(ErrInput, name)
	}
	lifetime, stopLifetime := joinContext(ctx, client.lifetime)
	kept := false
	defer func() {
		if !kept {
			stopLifetime()
		}
	}()
	run := func(call *adapters.Call[Result], handle Handle) {
		state := handle.state
		if state == nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: fail(ErrState, name)})
			return
		}
		if state.policy.Budget.WorkBytes > client.budget.WorkBytes || state.policy.Budget.EvidenceBytes > client.budget.EvidenceBytes {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: fail(ErrLimit, name)})
			return
		}
		release, err := state.use()
		if err != nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		live, end := joinContext(call.Context(), state.call.Context())
		stop := func() { end(); release(); stopLifetime() }
		serial, ok := state.next()
		if !ok {
			stop()
			_ = call.Resolve(adapters.Outcome[Result]{Primary: fail(ErrLimit, name)})
			return
		}
		inbox, err := invocation.NewInbox[native.Result](2, 2*state.policy.Budget.EvidenceBytes)
		if err != nil {
			stop()
			_ = call.Resolve(adapters.Outcome[Result]{Primary: translate(err, name)})
			return
		}
		bound, err := native.Bind(state.assembly, state.selection, inbox, nil)
		if err != nil {
			stop()
			_ = call.Resolve(adapters.Outcome[Result]{Primary: translate(err, name)})
			return
		}
		guard, err := call.Hold()
		if err != nil {
			stop()
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		op := &operation{family: &family{inbox: inbox, id: serial}, state: state, endpoint: client.endpoint, call: call, guard: guard, lifetime: live, stop: stop, id: client.id}
		defer func() {
			if op.kept {
				kept = true
			} else {
				stop()
				_ = guard.Release()
			}
		}()
		work(op, bound)
	}
	req := request(name, client.id, client.budget.WorkBytes, client.budget.EvidenceBytes)
	if client.source != nil {
		return adapters.UsingWithLifetime(ctx, lifetime, client.endpoint, *client.source, req, run)
	}
	return client.endpoint.RunWithLifetime(ctx, lifetime, req, func(call *adapters.Call[Result]) { run(call, client.direct) })
}

func (op *operation) child(ctx context.Context, name string, work func(context.Context, fault.Correlation) (*invocation.Receipt[native.Result], error)) (Result, error) {
	if op == nil || ctx == nil {
		return Result{}, fail(ErrInput, name)
	}
	if !op.family.gate.TryLock() {
		return Result{}, fail(ErrState, name)
	}
	defer op.family.gate.Unlock()
	receipt, err := op.endpoint.Child(ctx, op.call.Scope(), request(name, op.id, 0, op.state.policy.Budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		live, stop := joinContext(call.Context(), op.lifetime)
		defer stop()
		child := &operation{family: op.family, call: call}
		nativeReceipt, err := work(live, child.correlation())
		child.finish(nativeReceipt, err)
	})
	return result(receipt, err)
}

func joinContext(ctx, owner context.Context) (context.Context, func()) {
	work, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	stop := context.AfterFunc(owner, func() { defer close(done); cancel(errors.Join(owner.Err(), context.Cause(owner))) })
	if owner.Err() != nil {
		cancel(errors.Join(owner.Err(), context.Cause(owner)))
	}
	return work, sync.OnceFunc(func() {
		if !stop() {
			<-done
		}
		cancel(nil)
	})
}
