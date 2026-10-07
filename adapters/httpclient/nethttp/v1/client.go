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
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/adapters/httpclient/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	native "github.com/frost-leo/fathomry/internal/httpclient/nethttp/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Client is concurrent-safe, non-owning and never exposes a native client.
// Each root captures correlation and borrows exactly one resource generation.
type Client struct {
	private
	endpoint adapters.Endpoint[Result]
	direct   Handle
	source   *resource.Ref[Handle]
	lifetime context.Context
	budget   httpclient.Budget
	id       string
}

// Using composes a Fixed/Follow reference with caller-owned operation mechanisms.
// budget must cover every adopted generation; larger replacements are refused
// before dispatch. Active sessions never move to a new generation.
func Using(lifetime context.Context, ref resource.Ref[Handle], budget httpclient.Budget, dependencies Dependencies) (*Client, error) {
	if lifetime == nil || budget.WorkBytes < 1 || budget.WorkBytes > 1<<40 || budget.EvidenceBytes < 1 || budget.EvidenceBytes > 1<<40 {
		return nil, fail(ErrInput, "using")
	}
	if _, err := ref.Inspect(); err != nil {
		return nil, err
	}
	limits, err := dependencies.Runtime.Options()
	if err != nil {
		return nil, err
	}
	evidence, err := dependencies.Evidence.Options()
	if err != nil {
		return nil, err
	}
	if limits.MaxWorkBytes < budget.WorkBytes || limits.MaxTasks < 2 || limits.MaxDepth < 2 || limits.MaxHolds < 2 ||
		evidence.Capacity < 2 || evidence.MaxBytes < 2*budget.EvidenceBytes {
		return nil, fail(ErrLimit, "using")
	}
	endpoint, err := bind(dependencies)
	if err != nil {
		return nil, err
	}
	return &Client{endpoint: endpoint, source: &ref, lifetime: lifetime, budget: budget}, nil
}

// WithID returns a facade copy with opaque public correlation, never native IDs
// or diagnostic text. Descendants freeze the ID of their admitted root.
func (client *Client) WithID(id string) (*Client, error) {
	if client == nil || client.lifetime == nil || len(id) > 256 || !utf8.ValidString(id) {
		return nil, fail(ErrInput, "id")
	}
	for _, char := range id {
		if char < 32 || char == 127 {
			return nil, fail(ErrInput, "id")
		}
	}
	copy := *client
	copy.id = strings.Clone(id)
	return &copy, nil
}
func request(name, id string, work, evidence int64) adapters.Request {
	return adapters.Request{Operation: "httpclient.nethttp." + name, ID: id, WorkBytes: work, EvidenceBytes: evidence}
}

type family struct {
	gate     sync.Mutex
	endpoint adapters.Endpoint[Result]
	call     *adapters.Call[Result]
	guard    adapters.Guard
	inbox    *invocation.Inbox[native.Result]
	native   *native.Client
	lifetime context.Context
	stop     func()
	rootID   string
	id       string
	budget   httpclient.Budget
}

// dispatch runs native setup synchronously, taking a guard before native work.
// attach transfers actual asynchronous work to one bounded join worker.
func (client *Client) dispatch(ctx, lifetime context.Context, name string, work func(*family)) (*adapters.Receipt[Result], error) {
	if client == nil || client.lifetime == nil || ctx == nil || lifetime == nil {
		return nil, fail(ErrInput, name)
	}
	live, stopLifetime := joinContexts(lifetime, client.lifetime)
	retained := false
	defer func() {
		if !retained {
			stopLifetime()
		}
	}()
	run := func(call *adapters.Call[Result], handle Handle) {
		state := handle.state
		reject := func(err error) { _ = call.Resolve(adapters.Outcome[Result]{Primary: translate(err, name)}) }
		if state == nil {
			reject(fail(ErrState, name))
			return
		}
		if state.policy.Budget.WorkBytes > client.budget.WorkBytes || state.policy.Budget.EvidenceBytes > client.budget.EvidenceBytes {
			reject(fail(ErrLimit, name))
			return
		}
		release, err := state.use()
		if err != nil {
			reject(err)
			return
		}
		owned, stopOwner := joinContexts(call.Context(), state.call.Context())
		owned, cancelTimeout := context.WithTimeout(owned, state.timeout)
		stop := sync.OnceFunc(func() { cancelTimeout(); stopOwner(); stopLifetime(); release() })
		guard, err := call.Hold()
		if err != nil {
			stop()
			reject(err)
			return
		}
		inbox, err := invocation.NewInbox[native.Result](2, 2*state.policy.Budget.EvidenceBytes)
		var connection *native.Client
		if err == nil {
			connection, err = native.Bind(state.assembly, state.selection, inbox, nil)
		}
		serial, ok := state.next()
		if !ok {
			err = fail(ErrLimit, name)
		}
		if err != nil {
			stop()
			reject(err)
			_ = guard.Release()
			return
		}
		group := &family{endpoint: client.endpoint, call: call, guard: guard, inbox: inbox, native: connection, lifetime: owned, stop: stop,
			rootID: "nethttp-" + strconv.FormatUint(serial, 10), id: client.id, budget: client.budget}
		group.gate.Lock()
		retained = true
		work(group)
	}
	req := request(name, client.id, client.budget.WorkBytes, client.budget.EvidenceBytes)
	if client.source != nil {
		return adapters.UsingWithLifetime(ctx, live, client.endpoint, *client.source, req, run)
	}
	return client.endpoint.RunWithLifetime(ctx, live, req, func(call *adapters.Call[Result]) { run(call, client.direct) })
}
func (group *family) rootCorrelation() fault.Correlation {
	return fault.Correlation{Call: group.rootID}
}
func (group *family) childCorrelation(call *adapters.Call[Result]) fault.Correlation {
	metadata, _ := call.Receipt().Snapshot()
	return fault.Correlation{Call: group.rootID + "-" + strconv.FormatUint(metadata.Info().Sequence, 10), Parent: group.rootID}
}

// attach is called while gate is held. Claiming the live root immediately avoids
// FIFO head-of-line blocking of independently released incremental children.
func (group *family) attach(call *adapters.Call[Result], guard adapters.Guard, receipt *invocation.Receipt[native.Result], setup error, root bool, stop func(), lifetime context.Context, cleanup func(context.Context) error) {
	if receipt == nil {
		if setup == nil {
			setup = fail(ErrState, "missing-receipt")
		}
		_ = call.Resolve(adapters.Outcome[Result]{Primary: translate(setup, "dispatch")})
		group.gate.Unlock()
		if stop != nil {
			stop()
		}
		if root {
			group.stop()
		}
		_ = guard.Release()
		return
	}
	record, err := group.inbox.Next(context.Background())
	if err != nil {
		_ = call.Resolve(adapters.Outcome[Result]{Primary: translate(err, "evidence")})
		// A violated custody contract cannot authorize releasing actual work.
		group.gate.Unlock()
		return
	}
	actual, _ := record.Receipt().Result()
	expected, _ := receipt.Result()
	if actual.Context.Correlation != expected.Context.Correlation {
		_ = call.Resolve(adapters.Outcome[Result]{Primary: fail(ErrState, "evidence-identity")})
		group.gate.Unlock()
		return
	}
	go func() {
		value, err := receipt.WaitReleased(lifetime)
		if err != nil {
			if cleanup != nil {
				_ = cleanup(context.Background())
			}
			value, err = receipt.WaitReleased(context.Background())
		}
		group.gate.Lock()
		if err != nil {
			group.gate.Unlock()
			return
		}
		if stop != nil {
			stop()
		}
		metadata, _ := call.Receipt().Snapshot()
		projected := project(value, metadata.Info())
		outcome := adapters.Outcome[Result]{Present: true, Value: projected, Primary: translate(value.Outcome.Primary, "operation"), Cleanup: translate(value.Outcome.Cleanup, "cleanup")}
		if err := call.Resolve(outcome); err != nil {
			group.gate.Unlock()
			return
		}
		if err := record.Release(); err != nil {
			group.gate.Unlock()
			return
		}
		if root {
			group.stop()
		}
		group.gate.Unlock()
		_ = guard.Release()
	}()
	group.gate.Unlock()
}

func (group *family) child(ctx context.Context, name string, work func(context.Context, fault.Correlation) (*invocation.Receipt[native.Result], error, func(context.Context) error)) (*adapters.Receipt[Result], error) {
	if group == nil || ctx == nil {
		return nil, fail(ErrInput, name)
	}
	if !group.gate.TryLock() {
		return nil, fail(ErrState, "session-busy")
	}
	dispatched := false
	receipt, err := group.endpoint.Child(ctx, group.call.Scope(), request(name, group.id, 0, group.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		dispatched = true
		guard, err := call.Hold()
		if err != nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			group.gate.Unlock()
			return
		}
		live, stop := joinContexts(call.Context(), group.lifetime)
		nativeReceipt, nativeErr, cleanup := work(live, group.childCorrelation(call))
		group.attach(call, guard, nativeReceipt, nativeErr, false, stop, live, cleanup)
	})
	if !dispatched {
		group.gate.Unlock()
	}
	return receipt, err
}
func joinContexts(ctx, owner context.Context) (context.Context, func()) {
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
