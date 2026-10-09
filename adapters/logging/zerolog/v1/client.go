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

package zerolog

import (
	"context"
	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

// Client is a concurrent non-owning capability. Each root captures its actual
// Fixed/Follow generation. Retained With families never retarget.
type Client struct {
	private
	endpoint adapters.Endpoint[Result]
	runtime  adapters.Runtime
	direct   Handle
	source   *resource.Ref[Handle]
	lifetime context.Context
	budget   Budget
	id       string
	family   *viewFamily
}

// UsesRuntime allows explicit compositions to refuse nested roots on the same
// public Runtime. It compares actual binding identity, not a configured name,
// and grants no Runtime ownership. Fixed/Follow changes do not alter this binding.
func (client *Client) UsesRuntime(runtime *adapters.Runtime) bool {
	return client != nil && client.endpoint.UsesRuntime(runtime)
}

// Using consumes a Fixed/Follow resource and a frozen envelope covering every
// adopted generation. A larger candidate is refused before native dispatch.
func Using(lifetime context.Context, ref resource.Ref[Handle], budget Budget, dependencies Dependencies) (*Client, error) {
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
	if limits.MaxWorkBytes < budget.WorkBytes || evidence.MaxBytes < budget.EvidenceBytes {
		return nil, fail(ErrLimit, "using")
	}
	endpoint, err := bind(dependencies)
	if err != nil {
		return nil, err
	}
	return &Client{endpoint: endpoint, runtime: *dependencies.Runtime, source: &ref, lifetime: lifetime, budget: budget}, nil
}

// WithID freezes public correlation without creating another owner or allowance.
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
	return adapters.Request{Operation: "logging.zerolog." + name, ID: id, WorkBytes: work, EvidenceBytes: evidence}
}

type operation struct {
	call           *adapters.Call[Result]
	guard          adapters.Guard
	inbox          *invocation.Inbox[native.Result]
	native         *native.Logger
	context        context.Context
	stop           func()
	id             fault.Correlation
	policyRevision string
	outputs        []logging.Output
	limit          int
}

// dispatch retains one bounded family before native admission, including failures.
// Cancellation never substitutes for native release or independent evidence transfer.
func (client *Client) dispatch(ctx context.Context, name string, work func(*operation)) (*adapters.Receipt[Result], error) {
	if client == nil || client.lifetime == nil || ctx == nil {
		return nil, fail(ErrInput, name)
	}
	live, stopLifetime := joinContexts(ctx, client.lifetime)
	retained := false
	defer func() {
		if !retained {
			stopLifetime()
		}
	}()
	run := func(call *adapters.Call[Result], handle Handle) {
		reject := func(err error) { _ = call.Resolve(adapters.Outcome[Result]{Primary: translate(err, name)}) }
		state := handle.state
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
		if err := client.checkComposition(state); err != nil {
			release()
			reject(err)
			return
		}
		owned, stopOwner := joinContexts(call.Context(), state.call.Context())
		stop := sync.OnceFunc(func() { stopOwner(); stopLifetime(); release() })
		guard, err := call.Hold()
		if err != nil {
			stop()
			reject(err)
			return
		}
		inbox, err := invocation.NewInbox[native.Result](1, state.prepared.metadata.EvidenceBytes)
		var connection *native.Logger
		if err == nil {
			connection, err = native.Bind(state.physical.assembly, state.physical.selection, inbox, nil)
			if err == nil {
				connection, err = connection.WithPolicy(state.prepared.native)
			}
		}
		serial, ok := state.physical.next()
		if !ok {
			err = fail(ErrLimit, name)
		}
		if err != nil {
			stop()
			reject(err)
			_ = guard.Release()
			return
		}
		retained = true
		operation := &operation{call: call, guard: guard, inbox: inbox, native: connection, context: owned, stop: stop, policyRevision: state.prepared.native.Description().Revision, outputs: state.outputs, limit: state.prepared.metadata.MaxRecordBytes, id: fault.Correlation{Call: "zerolog-" + strconv.FormatUint(serial, 10)}}
		work(operation)
	}
	req := request(name, client.id, client.budget.WorkBytes, client.budget.EvidenceBytes)
	if client.family != nil {
		req.WorkBytes = 0
		return client.endpoint.ChildWithLifetime(ctx, live, client.family.call.Scope(), req, func(call *adapters.Call[Result]) { run(call, client.direct) })
	}
	if client.source != nil {
		return adapters.UsingWithLifetime(ctx, live, client.endpoint, *client.source, req, run)
	}
	return client.endpoint.RunWithLifetime(ctx, live, req, func(call *adapters.Call[Result]) { run(call, client.direct) })
}

func (client *Client) checkComposition(state *sourceState) error {
	for _, check := range state.physical.checks {
		runtime := client.runtime
		if err := check.CheckRuntime(&runtime); err != nil {
			return err
		}
	}
	return nil
}

func joinContexts(parent, other context.Context) (context.Context, func()) {
	joined, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(other, func() { cancel(context.Cause(other)) })
	if other.Err() != nil {
		cancel(context.Cause(other))
	}
	return joined, func() { stop(); cancel(nil) }
}

func (operation *operation) finish(receipt *invocation.Receipt[native.Result], setup error) {
	if receipt == nil && operation.inbox.Usage().Outstanding == 0 {
		if setup == nil {
			setup = fail(ErrState, "missing-receipt")
		}
		_ = operation.call.Resolve(adapters.Outcome[Result]{Primary: translate(setup, "dispatch")})
		operation.stop()
		_ = operation.guard.Release()
		return
	}
	record, err := operation.inbox.Next(context.Background())
	if err != nil {
		_ = operation.call.Resolve(adapters.Outcome[Result]{Primary: translate(err, "evidence")})
		return
	}
	if receipt == nil {
		receipt = record.Receipt()
	}
	actual, _ := record.Receipt().Result()
	expected, _ := receipt.Result()
	if actual.Context.Correlation != expected.Context.Correlation {
		_ = operation.call.Resolve(adapters.Outcome[Result]{Primary: fail(ErrState, "evidence-identity")})
		return
	}
	complete := func() {
		value, err := receipt.WaitReleased(context.Background())
		if err != nil {
			return
		}
		metadata, _ := operation.call.Receipt().Snapshot()
		outcome := adapters.Outcome[Result]{Present: true, Value: project(value, metadata.Info(), operation.policyRevision, operation.outputs),
			Primary: translate(value.Outcome.Primary, "operation"), Cleanup: translate(value.Outcome.Cleanup, "cleanup")}
		if err := operation.call.Resolve(outcome); err != nil {
			return
		}
		if err := record.Release(); err != nil {
			return
		}
		operation.stop()
		_ = operation.guard.Release()
	}
	snapshot, _ := receipt.Result()
	if snapshot.Released {
		complete()
	} else {
		go complete()
	}
}

func (client *Client) finite(ctx context.Context, name string, work func(*operation) (*invocation.Receipt[native.Result], error)) (*adapters.Receipt[Result], error) {
	var setup error
	receipt, err := client.dispatch(ctx, name, func(operation *operation) {
		nativeReceipt, nativeErr := work(operation)
		setup = translate(nativeErr, name)
		operation.finish(nativeReceipt, setup)
	})
	return receipt, joinErrors(name, err, setup)
}
