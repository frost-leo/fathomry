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

package redis

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/adapters/cache/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/cache/redis/v9"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Client is a concurrent non-owning facade. Its views share source capacity,
// lifetime and identity; they cannot refresh credentials or close native pools.
type Client struct {
	private
	endpoint adapters.Endpoint[Result]
	direct   Handle
	source   *resource.Ref[Handle]
	lifetime context.Context
	budget   cache.Budget
	id       string
}

// View selects the root semantic owner. A mixed Pipeline retains each command's
// declared owner while setup/dispatch/lifecycle failures belong to this root.
type View struct {
	private
	client     *Client
	capability Capability
}

func (client *Client) Cache() View     { return View{client: client, capability: Cache} }
func (client *Client) Messaging() View { return View{client: client, capability: Messaging} }
func (view View) Command(args ...string) (Command, error) {
	return NewCommand(view.capability, args...)
}

// Using retains each actual Fixed/Follow generation through native completion
// and receipt transfer. budget must cover every adoptable generation; a larger
// source is refused before native dispatch, never silently undercharged.
func Using(lifetime context.Context, ref resource.Ref[Handle], budget cache.Budget, deps Dependencies) (*Client, error) {
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
func request(capability Capability, name, id string, work, evidence int64) adapters.Request {
	return adapters.Request{Operation: string(capability) + ".redis." + name, ID: id, WorkBytes: work, EvidenceBytes: evidence}
}

type operation struct {
	family     *family
	state      *sourceState
	endpoint   adapters.Endpoint[Result]
	call       *adapters.Call[Result]
	guard      adapters.Guard
	lifetime   context.Context
	stop       func()
	kept       atomic.Bool
	held       bool
	id         string
	capability Capability
	kind       ResultKind
	commands   []Command
	parent     string
	record     *invocation.DeliveryRecord[native.Result]
}

func (view View) dispatch(ctx context.Context, name string, kind ResultKind, commands []Command, work func(*operation, *native.Client)) (*adapters.Receipt[Result], error) {
	client := view.client
	if client == nil || client.lifetime == nil || ctx == nil || !view.capability.valid() {
		return nil, problem(view.capability, ErrInput, name)
	}
	if len(commands) > 256 {
		return nil, problem(view.capability, ErrLimit, name)
	}
	for _, command := range commands {
		if !command.valid {
			return nil, problem(view.capability, ErrInput, name)
		}
	}
	commands = append([]Command(nil), commands...)
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
			_ = call.Resolve(adapters.Outcome[Result]{Primary: problem(view.capability, ErrState, name)})
			return
		}
		if state.policy.Budget.WorkBytes > client.budget.WorkBytes || state.policy.Budget.EvidenceBytes > client.budget.EvidenceBytes {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: problem(view.capability, ErrLimit, name)})
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
			_ = call.Resolve(adapters.Outcome[Result]{Primary: problem(view.capability, ErrLimit, name)})
			return
		}
		inbox, err := invocation.NewInbox[native.Result](2, 2*state.metadata.EvidenceBytes)
		if err != nil {
			stop()
			_ = call.Resolve(adapters.Outcome[Result]{Primary: translate(err, name, view.capability)})
			return
		}
		bound, err := native.Bind(state.assembly, state.selection, inbox, nil)
		if err != nil {
			stop()
			_ = call.Resolve(adapters.Outcome[Result]{Primary: translate(err, name, view.capability)})
			return
		}
		guard, err := call.Hold()
		if err != nil {
			stop()
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		op := &operation{family: &family{inbox: inbox, id: serial}, state: state, endpoint: client.endpoint, call: call, guard: guard,
			lifetime: live, stop: stop, id: client.id, capability: view.capability, kind: kind, commands: commands, held: true}
		defer func() {
			if op.kept.Load() {
				kept = true
			} else {
				stop()
				_ = guard.Release()
			}
		}()
		work(op, bound)
	}
	req := request(view.capability, name, client.id, client.budget.WorkBytes, client.budget.EvidenceBytes)
	if client.source != nil {
		return adapters.UsingWithLifetime(ctx, lifetime, client.endpoint, *client.source, req, run)
	}
	return client.endpoint.RunWithLifetime(ctx, lifetime, req, func(call *adapters.Call[Result]) { run(call, client.direct) })
}

// Execute sends one finite command. A blocking Redis operation uses this same
// finite source timeout; stopping a wait never proves no dequeue or mutation.
func (view View) Execute(ctx context.Context, command Command) (Result, error) {
	if command.capability != view.capability {
		return Result{}, problem(view.capability, ErrInput, "classification")
	}
	return result(view.dispatch(ctx, "execute", Commands, []Command{command}, func(op *operation, bound *native.Client) {
		receipt, err := bound.Execute(op.lifetime, op.correlation(), command.native)
		op.finish(receipt, err)
	}))
}

// Pipeline preserves mixed semantic owners and positional partial effects. It is
// not atomic; independent Cluster commands can span slots. No split transaction
// or business retry is inferred from this operation.
func (view View) Pipeline(ctx context.Context, commands ...Command) (Result, error) {
	return result(view.dispatch(ctx, "pipeline", Commands, commands, func(op *operation, bound *native.Client) {
		receipt, err := bound.Pipeline(op.lifetime, op.correlation(), nativeCommands(op.commands)...)
		op.finish(receipt, err)
	}))
}

// Submit uses the opt-in native auto-batcher and returns only a public immutable
// receipt. Native work, source generation and both evidence holds outlive a
// canceled receipt wait. The native batcher ignores per-command cancellation and
// deadlines AFTER enqueue, including owner/runtime cancellation. Accepted queued
// mutations may still execute; guards remain held until actual native completion.
// Timeout bounds native phases, not total queue-to-completion time. Use Execute
// when a per-command execution context is required.
func (view View) Submit(ctx context.Context, command Command) (*adapters.Receipt[Result], error) {
	if command.capability != view.capability {
		return nil, problem(view.capability, ErrInput, "classification")
	}
	return view.dispatch(ctx, "submit", Commands, []Command{command}, func(op *operation, bound *native.Client) {
		receipt, err := bound.Submit(op.lifetime, op.correlation(), command.native)
		if receipt == nil {
			op.finish(nil, err)
			return
		}
		op.kept.Store(true)
		go func() {
			if op.finish(receipt, err) {
				op.stop()
				_ = op.guard.Release()
			}
		}()
	})
}

// Automatic waits for Submit; stopping its wait does not revoke native enqueue.
func (view View) Automatic(ctx context.Context, command Command) (Result, error) {
	receipt, err := view.Submit(ctx, command)
	if err != nil {
		return Result{}, err
	}
	snapshot, err := receipt.WaitReleased(ctx)
	value, _ := snapshot.ValueCopy()
	return value, combine(err, snapshot.Primary(), snapshot.Cleanup())
}
func nativeCommands(commands []Command) []native.Command {
	result := make([]native.Command, len(commands))
	for index, command := range commands {
		result[index] = command.native
	}
	return result
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
