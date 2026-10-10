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

package temporal

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/google/uuid"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
)

type endpoints struct {
	operations adapters.Endpoint[Result]
	workers    adapters.Endpoint[WorkerResult]
	tasks      adapters.Endpoint[TaskResult]
}

func bindEndpoints(dependencies Dependencies) (endpoints, error) {
	var result endpoints
	var err error
	result.operations, err = adapters.Bind(dependencies.Runtime, adapters.Declaration[Result]{Evidence: dependencies.Evidence, Observer: dependencies.Observer, Copy: func(value Result) Result { return value }})
	if err != nil {
		return result, err
	}
	result.workers, err = adapters.Bind(dependencies.Runtime, adapters.Declaration[WorkerResult]{Evidence: dependencies.Workers, Observer: dependencies.Observer, Copy: func(value WorkerResult) WorkerResult { return value }})
	if err != nil {
		return result, err
	}
	result.tasks, err = adapters.Bind(dependencies.Runtime, adapters.Declaration[TaskResult]{Evidence: dependencies.Tasks, Observer: dependencies.Observer, Copy: func(value TaskResult) TaskResult { return value }})
	return result, err
}

// Client is one retained use identity. WithID copies share that identity; Borrow
// creates a distinct one. Close revokes future calls/decodes and stops this use's
// Workers, without closing peers or the physical source.
type Client struct {
	private
	use    *use
	native *native.Executions
	raw    *native.Client
	id     string
}

type use struct {
	owner       *sourceState
	assembly    *source.Assembly
	context     context.Context
	cancel      context.CancelFunc
	stopSource  func() bool
	endpoints   endpoints
	attribution Attribution
	release     func() error
	generation  *generationCustody
	children    map[*use]struct{}
	parent      *use
	mu          sync.Mutex
	closing     bool
	active      int
	idle        chan struct{}
	idleOnce    sync.Once
	cleanup     cleanupAttempt
	complete    atomic.Bool
}

func (state *sourceState) borrow(lifetime context.Context, bound endpoints, generation uint64, custody *generationCustody, release func() error, parent *use) (*Client, error) {
	if lifetime == nil {
		return nil, fail(ErrInput, "borrow")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closing || state.call.Context().Err() != nil || lifetime.Err() != nil {
		return nil, fail(ErrState, "borrow")
	}
	if len(state.uses) >= state.prepared.settings.MaxUses || state.serial == math.MaxUint64 {
		return nil, fail(ErrLimit, "borrow")
	}
	state.serial++
	selected := source.Borrow("use-"+strconv.FormatUint(state.serial, 10), state.assembly, state.selection)
	assembly, err := source.Assemble(lifetime, context.Background(), "temporal-use-"+strconv.FormatUint(state.serial, 10), selected)
	if err != nil {
		if assembly != nil {
			_ = assembly.Close(context.Background())
		}
		return nil, translate(err, "borrow")
	}
	executions, err := native.BindExecutions(assembly, selected, state.executions, nil)
	if err != nil {
		_ = assembly.Close(context.Background())
		return nil, translate(err, "borrow")
	}
	raw, err := native.Bind(assembly, selected, state.rpcs, nil)
	if err != nil {
		_ = assembly.Close(context.Background())
		return nil, translate(err, "borrow")
	}
	executions, raw, err = native.WithWorkEnvelope(executions, raw, state.policy.NativeWorkBytes)
	if err != nil {
		_ = assembly.Close(context.Background())
		return nil, translate(err, "borrow")
	}
	owned, cancel := context.WithCancel(lifetime)
	use := &use{owner: state, assembly: assembly, context: owned, cancel: cancel, endpoints: bound,
		attribution: state.attribution(generation), release: release, generation: custody, children: make(map[*use]struct{}), parent: parent, idle: make(chan struct{})}
	use.attribution.UseID = state.serial
	use.stopSource = context.AfterFunc(state.call.Context(), cancel)
	client := bindClient(use, executions, raw, "")
	state.uses[use] = struct{}{}
	if parent != nil {
		parent.mu.Lock()
		parent.children[use] = struct{}{}
		if parent.closing {
			cancel()
		}
		parent.mu.Unlock()
	}
	context.AfterFunc(owned, func() { _ = use.close(context.Background()) })
	return client, nil
}

func bindClient(use *use, executions *native.Executions, raw *native.Client, id string) *Client {
	policy, bound := use.owner.policy, use.endpoints
	executions = native.WithAdmissions(executions,
		admission(use, bound.operations, policy.Budget.WorkBytes, executionResult, id),
		admission(use, bound.workers, policy.Budget.WorkerBytes, workerResult, id),
		admission(use, bound.tasks, policy.Budget.WorkerBytes, taskResult, id))
	raw = native.WithRPCAdmission(raw, admission(use, bound.operations, policy.Budget.WorkBytes, rpcResult, id))
	raw = native.WithFreshRPCIDs(raw)
	raw = native.WithRPCErrorMapper(raw, func(err error) error { return translate(err, "rpc") })
	return &Client{use: use, native: executions, raw: raw, id: id}
}

func (use *use) enter() error {
	use.mu.Lock()
	defer use.mu.Unlock()
	if use.closing || use.context.Err() != nil {
		return fail(ErrState, "entry")
	}
	use.active++
	return nil
}
func (use *use) leave() {
	use.mu.Lock()
	defer use.mu.Unlock()
	use.active--
	if use.closing && use.active == 0 {
		use.idleOnce.Do(func() { close(use.idle) })
	}
}
func (use *use) seal() {
	use.mu.Lock()
	use.closing = true
	use.cancel()
	if use.active == 0 {
		use.idleOnce.Do(func() { close(use.idle) })
	}
	use.mu.Unlock()
	for _, child := range childUses(use) {
		child.seal()
	}
}

func childUses(parent *use) []*use {
	parent.mu.Lock()
	defer parent.mu.Unlock()
	children := make([]*use, 0, len(parent.children))
	for child := range parent.children {
		children = append(children, child)
	}
	return children
}
func (use *use) close(ctx context.Context) error {
	if use == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	use.seal()
	return use.cleanup.run(ctx, use.complete.Load, func() error {
		// Closing a Borrow seals its Access even while admitted descendants remain.
		_ = use.assembly.Close(context.Background())
		<-use.idle
		for _, child := range childUses(use) {
			if err := child.close(context.Background()); err != nil {
				return err
			}
		}
		err := use.assembly.Close(context.Background())
		for _, entry := range use.assembly.Snapshot().Sources {
			if !entry.Returned || entry.Pending {
				return translate(errors.Join(err, source.ErrIncomplete), "close")
			}
		}
		if err != nil {
			return translate(err, "close")
		}
		if use.release != nil {
			if err := use.release(); err != nil {
				return translate(err, "close")
			}
			use.release = nil
		}
		use.stopSource()
		if err := use.generation.drop(); err != nil {
			return translate(err, "close")
		}
		use.owner.mu.Lock()
		delete(use.owner.uses, use)
		use.owner.mu.Unlock()
		if use.parent != nil {
			use.parent.mu.Lock()
			delete(use.parent.children, use)
			use.parent.mu.Unlock()
		}
		use.complete.Store(true)
		return nil
	})
}

// Close is repeatable; a canceled wait retains the same cleanup and identity.
func (client *Client) Close(ctx context.Context) error {
	if client == nil || client.use == nil {
		return fail(ErrInput, "close")
	}
	return client.use.close(ctx)
}
func (client *Client) Closed() bool {
	return client != nil && client.use != nil && client.use.complete.Load()
}
func (client *Client) Namespace() string {
	if client == nil || client.native == nil {
		return ""
	}
	return client.native.Namespace()
}

// CheckHealth preserves the native health request/result within admission. It is
// not qualification of Workflows, Workers or namespace-gated capabilities.
func (client *Client) CheckHealth(ctx context.Context, request *sdk.CheckHealthRequest) (*sdk.CheckHealthResponse, error) {
	value, err := client.executions().CheckHealth(ctx, client.correlation(), request)
	return value, translate(err, "health")
}
func (client *Client) Attribution() Attribution {
	if client == nil || client.use == nil {
		return Attribution{}
	}
	return client.use.attribution
}

// Borrow creates a new independently revocable use of this physical source.
func (client *Client) Borrow(lifetime context.Context) (*Client, error) {
	if client == nil || client.use == nil {
		return nil, fail(ErrInput, "borrow")
	}
	if err := client.use.enter(); err != nil {
		return nil, err
	}
	defer client.use.leave()
	if err := client.use.generation.hold(); err != nil {
		return nil, err
	}
	borrowed, err := client.use.owner.borrow(lifetime, client.use.endpoints, client.use.attribution.Generation, client.use.generation, nil, nil)
	if err != nil {
		_ = client.use.generation.drop()
	}
	return borrowed, err
}

// WithID creates only a correlation view. It does not grant another lifetime,
// source, native quota, or right to transfer WithStart intentions between uses.
func (client *Client) WithID(id string) (*Client, error) {
	if client == nil || client.use == nil || len(id) > 256 || !utf8.ValidString(id) {
		return nil, fail(ErrInput, "id")
	}
	for _, char := range id {
		if char < 32 || char == 127 {
			return nil, fail(ErrInput, "id")
		}
	}
	return bindClient(client.use, client.native, client.raw, strings.Clone(id)), nil
}
func (client *Client) correlation() fault.Correlation {
	return fault.Correlation{Call: uuid.NewString()}
}

// WorkflowService exposes generated methods under exact grants, not a transport
// or owning SDK Client. Each explicit call gets independent bounded evidence.
func (client *Client) WorkflowService() workflowservice.WorkflowServiceClient {
	var raw *native.Client
	if client != nil {
		raw = client.raw
	}
	if raw == nil {
		raw = native.WithRPCErrorMapper(&native.Client{}, func(err error) error { return translate(err, "rpc") })
	}
	return raw.WorkflowService(client.correlation())
}
func (client *Client) OperatorService() operatorservice.OperatorServiceClient {
	var raw *native.Client
	if client != nil {
		raw = client.raw
	}
	if raw == nil {
		raw = native.WithRPCErrorMapper(&native.Client{}, func(err error) error { return translate(err, "rpc") })
	}
	return raw.OperatorService(client.correlation())
}

// Binding selects a generation only on Retain. Existing Clients, lazy decoders,
// WithStart intentions and Workers never follow a subsequent replacement.
type Binding struct {
	private
	source    resource.Ref[Handle]
	endpoints endpoints
	budget    Budget
	lifetime  context.Context
}

// Using binds a Fixed/Follow source without acquiring it. Retain returns an
// explicitly owned Client use; callers must close it or cancel its lifetime.
func Using(lifetime context.Context, ref resource.Ref[Handle], budget Budget, dependencies Dependencies) (*Binding, error) {
	if lifetime == nil || budget.WorkBytes < 1 || budget.WorkerBytes < 1 || budget.EvidenceBytes < publicRecordBytes {
		return nil, fail(ErrInput, "using")
	}
	if _, err := ref.Inspect(); err != nil {
		return nil, err
	}
	bound, err := bindEndpoints(dependencies)
	if err != nil {
		return nil, err
	}
	limits, err := dependencies.Runtime.Options()
	if err != nil {
		return nil, err
	}
	if limits.MaxWorkBytes < max(budget.WorkBytes, budget.WorkerBytes) {
		return nil, fail(ErrLimit, "using")
	}
	return &Binding{source: ref, endpoints: bound, budget: budget, lifetime: lifetime}, nil
}
func (binding *Binding) Retain(ctx context.Context) (*Client, error) {
	if binding == nil || ctx == nil || binding.lifetime == nil {
		return nil, fail(ErrInput, "retain")
	}
	lease, err := binding.source.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	handle, err := lease.Value()
	if err != nil {
		_ = lease.Release()
		return nil, err
	}
	if handle.state == nil || handle.state.policy.Budget.WorkBytes > binding.budget.WorkBytes ||
		handle.state.policy.Budget.WorkerBytes > binding.budget.WorkerBytes {
		_ = lease.Release()
		return nil, fail(ErrLimit, "retain")
	}
	custody := &generationCustody{refs: 1, release: lease.Release}
	client, err := handle.state.borrow(binding.lifetime, binding.endpoints, lease.Generation(), custody, nil, nil)
	if err != nil {
		_ = custody.drop()
	}
	return client, err
}

type publicScopeKey struct{}

// Derived uses retain the exact acquired generation, never a new Ref selection.
type generationCustody struct {
	mu      sync.Mutex
	refs    int
	release func() error
}

func (custody *generationCustody) hold() error {
	if custody == nil {
		return nil
	}
	custody.mu.Lock()
	defer custody.mu.Unlock()
	if custody.refs == 0 {
		return fail(ErrState, "generation")
	}
	custody.refs++
	return nil
}
func (custody *generationCustody) drop() error {
	if custody == nil {
		return nil
	}
	custody.mu.Lock()
	if custody.refs == 0 {
		custody.mu.Unlock()
		return fail(ErrState, "generation")
	}
	custody.refs--
	last := custody.refs == 0
	custody.mu.Unlock()
	if last {
		return custody.release()
	}
	return nil
}

type publicScope struct {
	scope  adapters.Scope
	active atomic.Bool
}

func admission[N, P any](use *use, endpoint adapters.Endpoint[P], bytes int64, convert func(N, Attribution) P, id string) native.Admission[N] {
	return func(ctx, lifetime, parent context.Context, request invocation.Request) (context.Context, func(*invocation.DeliveryRecord[N], error), error) {
		if err := use.enter(); err != nil {
			return nil, nil, err
		}
		work, cancel := context.WithCancelCause(lifetime)
		stop := context.AfterFunc(use.context, func() { cancel(context.Cause(use.context)) })
		if use.context.Err() != nil {
			cancel(context.Cause(use.context))
		}
		end := sync.OnceFunc(func() { stop(); cancel(nil); use.leave() })
		req := adapters.Request{Operation: "temporal." + request.Name, ID: request.Correlation.Call, WorkBytes: bytes, EvidenceBytes: publicRecordBytes}
		if id != "" {
			req.ID = id
		}
		var call *adapters.Call[P]
		var guard adapters.Guard
		var beginError error
		token := &publicScope{}
		producer := func(accepted *adapters.Call[P]) {
			guard, beginError = accepted.Hold()
			if beginError != nil {
				_ = accepted.Resolve(adapters.Outcome[P]{Primary: beginError})
				return
			}
			call = accepted
			token.scope = call.Scope()
			token.active.Store(true)
		}
		var receipt *adapters.Receipt[P]
		var err error
		if parent != nil {
			outer, ok := parent.Value(publicScopeKey{}).(*publicScope)
			if !ok || !outer.active.Load() {
				end()
				return nil, nil, fail(ErrState, "parent")
			}
			req.WorkBytes = 0
			receipt, err = endpoint.ChildWithLifetime(ctx, work, outer.scope, req, producer)
		} else {
			if inherited, ok := ctx.Value(publicScopeKey{}).(*publicScope); ok && inherited.active.Load() {
				end()
				return nil, nil, fail(ErrState, "recursive-root")
			}
			receipt, err = endpoint.RunWithLifetime(ctx, work, req, producer)
		}
		if err != nil {
			end()
			return nil, nil, err
		}
		if call == nil {
			end()
			if beginError != nil {
				return nil, nil, beginError
			}
			snapshot, _ := receipt.Snapshot()
			return nil, nil, snapshot.Err()
		}
		finished := sync.OnceFunc(func() { token.active.Store(false); _ = guard.Release(); end() })
		finish := func(record *invocation.DeliveryRecord[N], setup error) {
			defer finished()
			if record == nil {
				_ = call.Resolve(adapters.Outcome[P]{Primary: translate(setup, request.Name)})
				return
			}
			result, err := record.Receipt().WaitReleased(context.Background())
			if err != nil {
				_ = call.Resolve(adapters.Outcome[P]{Primary: translate(err, request.Name)})
				return
			}
			value := convert(result.Outcome.Value, use.attribution)
			primary := translate(result.Outcome.Primary, request.Name)
			if exact, ok := any(value).(interface{ NativeError() (error, bool) }); ok {
				if semantic, present := exact.NativeError(); present && primary != nil {
					primary = &Error{safe: primary, semantic: semantic}
				}
			}
			cleanup := translate(errors.Join(result.Outcome.Cleanup, record.Release()), "cleanup")
			_ = call.Resolve(adapters.Outcome[P]{Value: value, Present: result.Outcome.Present, Primary: primary, Cleanup: cleanup})
		}
		return context.WithValue(call.Context(), publicScopeKey{}, token), finish, nil
	}
}
