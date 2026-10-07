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

package duckdb

import (
	"context"
	"errors"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Client is concurrent-safe and has no shutdown authority. It creates no pool.
// Retained handles are not concurrent-safe; overlapping use is refused.
type Client struct {
	private
	endpoint adapters.Endpoint[Result]
	direct   Handle
	source   *resource.Ref[Handle]
	lifetime context.Context
	budget   sqlengine.Budget
	id       string
}

// Using borrows one generation per root operation and retains it for the whole
// transaction/preparation family. lifetime fences all calls and owns retained work
// after short setup contexts end. budget must cover every adopted generation; a larger
// replacement is refused before native dispatch, never undercharged.
func Using(lifetime context.Context, ref resource.Ref[Handle], budget sqlengine.Budget, dependencies Dependencies) (*Client, error) {
	if lifetime == nil || budget.WorkBytes < 1 || budget.WorkBytes > 1<<40 || budget.EvidenceBytes < 1 || budget.EvidenceBytes > 1<<40 {
		return nil, fail(ErrInput, "using")
	}
	if _, err := ref.Inspect(); err != nil {
		return nil, err
	}
	endpoint, err := bind(dependencies)
	if err != nil {
		return nil, err
	}
	return &Client{endpoint: endpoint, source: &ref, lifetime: lifetime, budget: budget}, nil
}

// WithID returns an immutable facade copy with optional opaque correlation for
// future roots and descendants. Native ASCII IDs are independently code-owned.
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
	return adapters.Request{Operation: "database.duckdb." + operation, ID: id, WorkBytes: work, EvidenceBytes: evidence}
}

type session struct {
	family   *evidenceFamily
	endpoint adapters.Endpoint[Result]
	state    *sourceState
	call     *adapters.Call[Result]
	retained *retainedResult
	lifetime context.Context
	stop     func()
	id       string
}

func (client *Client) dispatch(ctx, lifetime context.Context, operation string, work func(*session, *native.Database)) (*adapters.Receipt[Result], error) {
	if client == nil || client.lifetime == nil || ctx == nil || lifetime == nil {
		return nil, fail(ErrInput, operation)
	}
	lifetime, stopLifetime := joinContext(lifetime, client.lifetime)
	retained := false
	defer func() {
		if !retained {
			stopLifetime()
		}
	}()
	run := func(call *adapters.Call[Result], handle Handle) {
		state := handle.state
		if state == nil || state.inspection == nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: fail(ErrState, operation)})
			return
		}
		if state.owner.context().Err() != nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: fail(ErrState, operation, state.owner.context().Err(), context.Cause(state.owner.context()))})
			return
		}
		if state.policy.Budget.WorkBytes > client.budget.WorkBytes || state.policy.Budget.EvidenceBytes > client.budget.EvidenceBytes {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: fail(ErrLimit, operation)})
			return
		}
		id, ok := state.owner.next()
		if !ok {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: fail(ErrLimit, operation)})
			return
		}
		family, err := newFamily(id, 2, 2*state.policy.Budget.EvidenceBytes)
		if err != nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: translate(err, operation)})
			return
		}
		database, err := native.Bind(state.owner.assembly, state.selection, family.inbox, nil)
		if err != nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: translate(err, operation)})
			return
		}
		family.gate.Lock()
		defer family.gate.Unlock()
		live, stop := joinContext(call.Context(), state.owner.context())
		release, err := state.owner.use()
		if err != nil {
			stop()
			_ = call.Resolve(adapters.Outcome[Result]{Primary: fail(ErrState, operation, err)})
			return
		}
		end := stop
		stop = func() { end(); release(); stopLifetime() }
		operationSession := &session{family: family, endpoint: client.endpoint, state: state, call: call, lifetime: live, stop: stop, id: client.id}
		defer func() {
			if operationSession.retained == nil {
				stop()
			} else {
				retained = true
			}
		}()
		work(operationSession, database)
	}
	req := request(operation, client.id, client.budget.WorkBytes, client.budget.EvidenceBytes)
	if client.source != nil {
		return adapters.UsingWithLifetime(ctx, lifetime, client.endpoint, *client.source, req, run)
	}
	return client.endpoint.RunWithLifetime(ctx, lifetime, req, func(call *adapters.Call[Result]) { run(call, client.direct) })
}

func (parent *session) child(ctx, lifetime context.Context, operation string, work func(*session)) (*adapters.Receipt[Result], error) {
	if parent == nil || ctx == nil || lifetime == nil {
		return nil, fail(ErrInput, operation)
	}
	if !parent.family.gate.TryLock() {
		return nil, fail(ErrState, operation)
	}
	defer parent.family.gate.Unlock()
	return parent.endpoint.ChildWithLifetime(ctx, lifetime, parent.call.Scope(), request(operation, parent.id, 0, parent.state.policy.Budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		live, stop := joinContext(call.Context(), parent.state.owner.context())
		child := &session{family: parent.family, endpoint: parent.endpoint, state: parent.state, call: call, lifetime: live, stop: stop, id: parent.id}
		defer func() {
			if child.retained == nil {
				stop()
			}
		}()
		work(child)
	})
}

func (value *session) setup(ctx context.Context) (context.Context, func()) {
	return joinContext(ctx, value.lifetime)
}

func (value *session) finish(receipt *invocation.Receipt[native.Result], err error) {
	if transfer := value.family.finish(value.call, receipt, err); transfer != nil {
		_ = value.call.Resolve(adapters.Outcome[Result]{Primary: translate(transfer, "evidence")})
	}
}

func (value *session) keep(guard adapters.Guard, receipt *invocation.Receipt[native.Result], cleanup func(context.Context)) error {
	retained, err := value.family.retain(value.call, guard, receipt, value.lifetime, value.stop, cleanup)
	if err == nil {
		value.retained = retained
	}
	return translate(err, "evidence")
}

// Run performs one finite request. Inputs are borrowed until return. cleanupCtx
// independently authorizes rollback; its cancellation never proves absent effects.
func (client *Client) Run(ctx, cleanupCtx context.Context, input Request) (Result, error) {
	return client.run(ctx, cleanupCtx, []Request{input}, false)
}

// Transaction executes the ordered list on one generation and connection.
// It never splits an oversized atomic request or retries an accepted statement.
func (client *Client) Transaction(ctx, cleanupCtx context.Context, inputs []Request) (Result, error) {
	return client.run(ctx, cleanupCtx, inputs, true)
}

func (client *Client) run(ctx, cleanupCtx context.Context, inputs []Request, transaction bool) (Result, error) {
	if cleanupCtx == nil {
		return Result{}, fail(ErrInput, "run")
	}
	operation := "run"
	if transaction {
		operation = "transaction"
	}
	return result(client.dispatch(ctx, ctx, operation, func(value *session, database *native.Database) {
		requests, err := inward(inputs, value.state.prepared.Options())
		if err != nil {
			value.finish(nil, err)
			return
		}
		var receipt *invocation.Receipt[native.Result]
		if transaction {
			receipt, err = database.Transaction(value.lifetime, cleanupCtx, value.family.correlation(value.call), requests)
		} else {
			receipt, err = database.Run(value.lifetime, cleanupCtx, value.family.correlation(value.call), requests[0])
		}
		value.finish(receipt, err)
	}))
}

// Execute runs one finite native statement, preserving changed-row knownness.
func (client *Client) Execute(ctx, cleanupCtx context.Context, sql string, args ...any) (Result, error) {
	return client.Run(ctx, cleanupCtx, Request{Mode: Execute, SQL: sql, Args: args})
}

// Query consumes one bounded snapshot, including DML RETURNING when admitted.
func (client *Client) Query(ctx, cleanupCtx context.Context, sql string, args ...any) (Result, error) {
	return client.Run(ctx, cleanupCtx, Request{Mode: Query, SQL: sql, Args: args})
}

// ExecuteMany prepares once and executes all parameter rows in one transaction.
func (client *Client) ExecuteMany(ctx, cleanupCtx context.Context, sql string, rows [][]any) (Result, error) {
	return client.Run(ctx, cleanupCtx, Request{Mode: ExecuteMany, SQL: sql, Rows: rows})
}

// Append performs one bounded transactional native Appender, optionally selecting
// columns. Accepted/flushed/closed stages are not commit or per-item proof.
func (client *Client) Append(ctx, cleanupCtx context.Context, schema, table string, columns []string, rows [][]any) (Result, error) {
	return client.Run(ctx, cleanupCtx, Request{Mode: Append, Schema: schema, Table: table, Columns: columns, Rows: rows})
}

// joinContext adds another explicit owner's cancellation without detaching caller work.
// The idempotent release joins its cancellation callback; values/deadline come from ctx.
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
