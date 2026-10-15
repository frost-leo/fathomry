/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package lark

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/adapters/notification/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/notification/lark/v3"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Client is a concurrent non-owning facade. Each operation retains one original
// source generation through actual native work and evidence transfer.
type Client struct {
	private
	endpoint         adapters.Endpoint[Result]
	direct           Handle
	source           *resource.Ref[Handle]
	lifetime         context.Context
	budget           notification.Budget
	id               string
	runtime          *adapters.Runtime
	observer         *adapters.Observer
	receiverLifetime time.Duration
}

func Using(lifetime context.Context, ref resource.Ref[Handle], policy Policy, dependencies Dependencies) (*Client, error) {
	budget := policy.Budget
	if lifetime == nil || budget.WorkBytes < 1 || budget.WorkBytes > 1<<40 || budget.EvidenceBytes < 1 || budget.EvidenceBytes > 1<<40 || budget.AdmissionTimeout < time.Millisecond || budget.AdmissionTimeout > 5*time.Minute || nativePresent(dependencies.Native) {
		return nil, fail(ErrInput, "using")
	}
	if policy.ReceiverLifetime < 0 || policy.ReceiverLifetime > 24*time.Hour || policy.Runtime.MaxActive < 1 || policy.Runtime.MaxTasks < 1 || policy.Evidence.Capacity < 1 || policy.SourceWorkBytes < 1 {
		return nil, fail(ErrInput, "using-policy")
	}
	if err := checkPolicy(policy, dependencies); err != nil {
		return nil, err
	}
	if _, err := ref.Inspect(); err != nil {
		return nil, err
	}
	endpoint, err := bind(dependencies)
	if err != nil {
		return nil, err
	}
	return &Client{endpoint: endpoint, source: &ref, lifetime: lifetime, budget: budget, runtime: dependencies.Runtime, observer: dependencies.Observer, receiverLifetime: policy.ReceiverLifetime}, nil
}

// WithID returns an immutable view; it creates no quota or new source.
func (client *Client) WithID(id string) (*Client, error) {
	if client == nil || client.lifetime == nil || len(id) > 256 || !utf8.ValidString(id) {
		return nil, fail(ErrInput, "id")
	}
	for _, char := range id {
		if char < 32 || char == 127 {
			return nil, fail(ErrInput, "id")
		}
	}
	cloned := *client
	cloned.id = strings.Clone(id)
	return &cloned, nil
}
func request(name, id string, work, evidence int64) adapters.Request {
	return adapters.Request{Operation: "notification.lark." + strings.ToLower(name), ID: id, WorkBytes: work, EvidenceBytes: evidence}
}

func (client *Client) dispatch(ctx context.Context, name string, work func(context.Context, *native.Client, fault.Correlation) (*invocation.Receipt[native.Result], error)) (Result, error) {
	return client.dispatchWith(ctx, name, work, nil)
}

func (client *Client) dispatchWith(ctx context.Context, name string, work func(context.Context, *native.Client, fault.Correlation) (*invocation.Receipt[native.Result], error), after func(context.Context, Result) (Result, error)) (Result, error) {
	if client == nil || client.lifetime == nil || ctx == nil {
		return Result{}, fail(ErrInput, name)
	}
	live, stop := joinContexts(ctx, client.lifetime)
	defer stop()
	admission, stopAdmission := context.WithTimeout(live, client.budget.AdmissionTimeout)
	defer stopAdmission()
	run := func(call *adapters.Call[Result], handle Handle) {
		reject := func(err error) { _ = call.Resolve(adapters.Outcome[Result]{Primary: translate(err, name)}) }
		state := handle.state
		if state == nil {
			reject(fail(ErrState, name))
			return
		}
		if state.policy.Budget.WorkBytes > client.budget.WorkBytes || state.policy.Budget.EvidenceBytes > client.budget.EvidenceBytes || client.budget.AdmissionTimeout > state.timeout {
			reject(fail(ErrLimit, name))
			return
		}
		release, err := state.use()
		if err != nil {
			reject(err)
			return
		}
		defer release()
		owned, stopOwner := joinContexts(call.Context(), state.call.Context())
		defer stopOwner()
		owned, stopExecution := context.WithTimeout(owned, state.timeout)
		defer stopExecution()
		inbox, err := invocation.NewInbox[native.Result](1, state.policy.Budget.EvidenceBytes)
		if err != nil {
			reject(err)
			return
		}
		connection, err := native.Bind(state.assembly, state.selection, inbox, nil)
		if err != nil {
			reject(err)
			return
		}
		serial, ok := state.next()
		if !ok {
			reject(fail(ErrLimit, name))
			return
		}
		id := fault.Correlation{Call: "lark-" + strconv.FormatUint(serial, 10)}
		receipt, setup := invoke(owned, connection, id, work)
		if receipt == nil {
			if setup == nil {
				setup = fail(ErrState, "missing-receipt")
			}
			reject(setup)
			return
		}
		record, err := inbox.Next(context.Background())
		if err != nil {
			reject(err)
			return
		}
		observed, err := receipt.WaitReleased(context.Background())
		if err != nil {
			reject(err)
			return
		}
		actual, _ := record.Receipt().Result()
		if actual.Context.Correlation != observed.Context.Correlation {
			reject(fail(ErrState, "evidence-identity"))
			return
		}
		metadata, _ := call.Receipt().Snapshot()
		outcome := adapters.Outcome[Result]{Present: true, Value: project(observed, metadata.Info()), Primary: translate(errors.Join(setup, observed.Outcome.Primary), name), Cleanup: translate(observed.Outcome.Cleanup, "cleanup")}
		if after != nil && outcome.Primary == nil {
			outcome.Value, err = after(owned, outcome.Value)
			outcome.Primary = translate(err, name)
		}
		outcome.Value.primary, outcome.Value.cleanup = outcome.Primary, outcome.Cleanup
		if err = call.Resolve(outcome); err != nil {
			return
		}
		_ = record.Release()
	}
	req := request(name, client.id, client.budget.WorkBytes, client.budget.EvidenceBytes)
	var receipt *adapters.Receipt[Result]
	var err error
	if client.source != nil {
		receipt, err = adapters.UsingWithLifetime(admission, live, client.endpoint, *client.source, req, run)
	} else {
		receipt, err = client.endpoint.RunWithLifetime(admission, live, req, func(call *adapters.Call[Result]) { run(call, client.direct) })
	}
	if receipt == nil {
		return Result{}, translate(err, name)
	}
	snapshot, waitErr := receipt.WaitReleased(context.Background())
	value, _ := snapshot.ValueCopy()
	return value, errors.Join(err, waitErr, snapshot.Err())
}

func invoke(ctx context.Context, connection *native.Client, id fault.Correlation, work func(context.Context, *native.Client, fault.Correlation) (*invocation.Receipt[native.Result], error)) (receipt *invocation.Receipt[native.Result], err error) {
	returned := false
	defer func() {
		recovered := recover()
		if !returned {
			cause, _ := recovered.(error)
			err = fail(ErrOperation, "native-panic", cause, ctx.Err(), context.Cause(ctx))
		}
	}()
	receipt, err = work(ctx, connection, id)
	returned = true
	return
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
