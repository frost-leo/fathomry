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

package httpcloak

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
	native "github.com/frost-leo/fathomry/internal/httpclient/httpcloak/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Client is concurrent-safe and non-owning. Roots borrow exactly the generation
// used; response streams and late native work never migrate to a replacement.
type Client struct {
	private
	endpoint adapters.Endpoint[Result]
	direct   Handle
	source   *resource.Ref[Handle]
	lifetime context.Context
	budget   httpclient.Budget
	id       string
}

// Using composes a Fixed/Follow reference with borrowed public mechanisms.
// budget must cover every adopted generation; oversized candidates are refused
// before native dispatch. This function never enlarges the caller's Runtime.
func Using(lifetime context.Context, ref resource.Ref[Handle], budget httpclient.Budget, dependencies Dependencies) (*Client, error) {
	if lifetime == nil || budget.WorkBytes < 1 || budget.WorkBytes > 1<<40 || budget.EvidenceBytes < 1 || budget.EvidenceBytes > 1<<40 || nativePresent(dependencies.Native) {
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
	if limits.MaxWorkBytes < budget.WorkBytes || limits.MaxTasks < 1 || limits.MaxDepth < 1 || limits.MaxHolds < 1 || evidence.Capacity < 1 || evidence.MaxBytes < budget.EvidenceBytes {
		return nil, fail(ErrLimit, "using")
	}
	endpoint, err := bind(dependencies)
	if err != nil {
		return nil, err
	}
	return &Client{endpoint: endpoint, source: &ref, lifetime: lifetime, budget: budget}, nil
}

// WithID returns a facade copy with opaque correlation; it never changes sources.
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
	return adapters.Request{Operation: "httpclient.httpcloak." + name, ID: id, WorkBytes: work, EvidenceBytes: evidence}
}

type family struct {
	call     *adapters.Call[Result]
	guard    adapters.Guard
	inbox    *invocation.Inbox[native.Result]
	native   *native.Client
	lifetime context.Context
	stop     func()
	rootID   string
}

func (group *family) rootCorrelation() fault.Correlation {
	return fault.Correlation{Call: group.rootID}
}

// dispatch holds actual-work authority before entering native setup. One bounded
// worker transfers independent Internal evidence without releasing work early.
func (client *Client) dispatch(ctx, requestContext context.Context, name string, work func(*family)) (*adapters.Receipt[Result], error) {
	if client == nil || client.lifetime == nil || ctx == nil || requestContext == nil {
		return nil, fail(ErrInput, name)
	}
	live, stopLifetime := joinContexts(ctx, client.lifetime)
	live, stopRequest := joinContexts(live, requestContext)
	stopContexts := sync.OnceFunc(func() { stopRequest(); stopLifetime() })
	retained := false
	defer func() {
		if !retained {
			stopContexts()
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
		owned, stopOwner := joinContexts(call.Context(), state.call.Context())
		owned, cancelTimeout := context.WithTimeout(owned, state.timeout)
		stop := sync.OnceFunc(func() { cancelTimeout(); stopOwner(); stopContexts(); release() })
		guard, err := call.Hold()
		if err != nil {
			stop()
			reject(err)
			return
		}
		inbox, err := invocation.NewInbox[native.Result](1, state.policy.Budget.EvidenceBytes)
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
		group := &family{call: call, guard: guard, inbox: inbox, native: connection, lifetime: owned, stop: stop, rootID: "httpcloak-" + strconv.FormatUint(serial, 10)}
		retained = true
		work(group)
	}
	req := request(name, client.id, client.budget.WorkBytes, client.budget.EvidenceBytes)
	if client.source != nil {
		return adapters.UsingWithLifetime(live, live, client.endpoint, *client.source, req, run)
	}
	return client.endpoint.RunWithLifetime(live, live, req, func(call *adapters.Call[Result]) { run(call, client.direct) })
}
func (group *family) attach(receipt *invocation.Receipt[native.Result], setup error, cleanup func(context.Context) error) {
	if receipt == nil {
		if setup == nil {
			setup = fail(ErrState, "missing-receipt")
		}
		_ = group.call.Resolve(adapters.Outcome[Result]{Primary: translate(setup, "dispatch")})
		group.stop()
		_ = group.guard.Release()
		return
	}
	record, err := group.inbox.Next(context.Background())
	if err != nil {
		_ = group.call.Resolve(adapters.Outcome[Result]{Primary: translate(err, "evidence")})
		return
	}
	actual, _ := record.Receipt().Result()
	expected, _ := receipt.Result()
	if actual.Context.Correlation != expected.Context.Correlation {
		_ = group.call.Resolve(adapters.Outcome[Result]{Primary: fail(ErrState, "evidence-identity")})
		return
	}
	go func() {
		value, err := receipt.WaitReleased(group.lifetime)
		if err != nil {
			if cleanup != nil {
				_ = cleanup(context.Background())
			}
			value, err = receipt.WaitReleased(context.Background())
		}
		if err != nil {
			return
		}
		metadata, _ := group.call.Receipt().Snapshot()
		outcome := adapters.Outcome[Result]{Present: true, Value: project(value, metadata.Info()), Primary: translate(value.Outcome.Primary, "operation"), Cleanup: translate(value.Outcome.Cleanup, "cleanup")}
		if err := group.call.Resolve(outcome); err != nil {
			return
		}
		if err := record.Release(); err != nil {
			return
		}
		group.stop()
		_ = group.guard.Release()
	}()
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
