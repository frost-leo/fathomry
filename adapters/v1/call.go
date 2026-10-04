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

package adapters

import (
	"context"
	"errors"
	"sync"
)

// Source identifies a public resource generation actually borrowed. Zero means
// no borrow has been captured in this observation, including pending/failed
// acquisition; it does not prove no binding was configured or that one is ready.
type Source struct {
	Name       string
	Generation uint64
}

// Info is detached operation metadata. IDs/facts are deliberately inspectable,
// potentially sensitive data. Sequence is local to its runtime, not durable.
type Info struct {
	Runtime   string
	Operation string
	ID        string
	Sequence  uint64
	Parent    uint64
	Depth     int
	Source    Source
	Resolved  bool
	Released  bool
}

// Endpoint binds one component fact type to common admission and evidence.
type Endpoint[T any] struct {
	state *endpoint[T]
	_     typeIdentity[T]
}
type endpoint[T any] struct {
	runtime     *runtimeState
	declaration Declaration[T]
}

// Bind installs no global registration or worker. Evidence and Copy are required;
// separate endpoints may explicitly share a runtime and its allowance.
func Bind[T any](runtime *Runtime, declaration Declaration[T]) (Endpoint[T], error) {
	if runtime == nil || runtime.state == nil {
		return Endpoint[T]{}, failureOf(ErrHandle, "bind", "", Details{})
	}
	if declaration.Copy == nil || declaration.Evidence == nil || declaration.Evidence.state == nil || declaration.Observer != nil && declaration.Observer.state == nil {
		return Endpoint[T]{}, failureOf(ErrOptions, "bind", "", Details{})
	}
	if runtime.state.stopped() {
		return Endpoint[T]{}, failureOf(ErrClosed, "bind", runtime.state.options.Name, Details{})
	}
	inbox := *declaration.Evidence
	declaration.Evidence = &inbox
	if declaration.Observer != nil {
		observer := *declaration.Observer
		declaration.Observer = &observer
	}
	return Endpoint[T]{state: &endpoint[T]{runtime.state, declaration}}, nil
}

// Call is producer authority. Concrete Adapter facades must not return it to
// ordinary consumers. Producers and copy functions must not panic or wait on
// lifecycle progress requiring their own return.
type Call[T any] struct {
	state *resultState[T]
	_     typeIdentity[T]
}
type resultState[T any] struct {
	node    *node
	copy    func(T) T
	outcome Outcome[T]
}

// Scope is a non-owning parent-call authority for child operations. It grants no
// resource/native client or runtime shutdown authority.
type Scope struct{ node *node }

// Guard retains actual work. Copies share one release authority. Release only
// after that work ends; context cancellation cannot replace it.
type Guard struct{ state *guardState }
type guardState struct {
	mu      sync.Mutex
	release func()
}

// Receipt is read-only outcome and release observation; it cannot stop a runtime.
// Copies observe the same private state. Overwriting a caller's handle does not
// modify another handle or the independent evidence receiver.
type Receipt[T any] struct {
	state *resultState[T]
	_     typeIdentity[T]
}

// Snapshot is an immutable observation. ValueCopy calls the component's copy
// contract; native causes remain borrowed for explicit potentially sensitive access.
type Snapshot[T any] struct {
	info    Info
	outcome Outcome[T]
	copy    func(T) T
	valid   bool
	_       typeIdentity[T]
}

func (value *node) infoLocked() Info {
	parent := uint64(0)
	if value.parent != nil {
		parent = value.parent.sequence
	}
	return Info{value.owner.options.Name, value.request.Operation, value.request.ID, value.sequence, parent, value.depth, value.source, value.resolved, value.released}
}
func (endpoint Endpoint[T]) begin(ctx context.Context, request Request, parent Scope) (*Call[T], error) {
	return endpoint.beginContexts(ctx, ctx, request, parent)
}

func (endpoint Endpoint[T]) beginContexts(ctx, lifetime context.Context, request Request, parent Scope) (*Call[T], error) {
	if endpoint.state == nil {
		return nil, failureOf(ErrHandle, "begin", "", Details{})
	}
	state := endpoint.state.runtime
	if ctx == nil || lifetime == nil || !validRequest(request, parent.node != nil) {
		return nil, failureOf(ErrRequest, "begin", state.options.Name, Details{})
	}
	if parent.node != nil && parent.node.owner != state {
		return nil, failureOf(ErrHandle, "child", state.options.Name, Details{})
	}
	if ctx.Err() != nil {
		return nil, failureOf(ErrWait, "begin", state.options.Name, Details{}, ctx.Err(), context.Cause(ctx))
	}
	reserved, err := endpoint.state.declaration.Evidence.reserve(request.EvidenceBytes)
	if err != nil {
		return nil, err
	}
	root := parent.node == nil
	if root {
		if err = state.acquire(ctx, request.WorkBytes); err != nil {
			reserved.abort()
			return nil, err
		}
	}
	state.mu.Lock()
	var control *node
	if ctx.Err() != nil {
		err = failureOf(ErrWait, "begin", state.options.Name, Details{}, ctx.Err(), context.Cause(ctx))
	} else {
		control, err = state.newNodeLocked(lifetime, request, parent.node, endpoint.state.declaration.Observer)
	}
	if err != nil {
		if root {
			state.releasePermitLocked(request.WorkBytes)
		}
		state.mu.Unlock()
		reserved.abort()
		return nil, err
	}
	result := &resultState[T]{node: control, copy: endpoint.state.declaration.Copy}
	control.unreported = func(err error) { result.outcome = Outcome[T]{Primary: err} }
	control.addCleanup = func(err error) { result.outcome.Cleanup = errors.Join(result.outcome.Cleanup, err) }
	control.evidenceReady = func() {
		state := reserved.inbox
		state.mu.Lock()
		state.notifyLocked()
		state.mu.Unlock()
	}
	call := &Call[T]{state: result}
	reserved.commit(result)
	control.observer.publish(Event{Operation: request.Operation, Phase: Admitted})
	state.mu.Unlock()
	return call, nil
}
func (call *Call[T]) dispatch(producer func(*Call[T])) {
	control := call.state.node
	defer control.returned(false)
	cause := control.cancellation()
	if control.owner.stopped() || cause != nil {
		_ = call.Resolve(Outcome[T]{Primary: failureOf(ErrClosed, control.request.Operation, control.owner.options.Name, Details{Sequence: control.sequence},
			cause, control.owner.base.Err(), context.Cause(control.owner.base))})
		return
	}
	producer(call)
}

// Run admits then invokes producer synchronously, retaining its full stack.
// A returned receipt denotes accepted work, not successful completion.
func (endpoint Endpoint[T]) Run(ctx context.Context, request Request, producer func(*Call[T])) (*Receipt[T], error) {
	if producer == nil {
		return nil, failureOf(ErrOptions, "run", "", Details{})
	}
	call, err := endpoint.begin(ctx, request, Scope{})
	if err != nil {
		return nil, err
	}
	call.dispatch(producer)
	return call.Receipt(), nil
}

// Start differs from Run only by invoking producer in one owned goroutine.
// Capacity remains charged through all guards/children, even after producer returns.
func (endpoint Endpoint[T]) Start(ctx context.Context, request Request, producer func(*Call[T])) (*Receipt[T], error) {
	if producer == nil {
		return nil, failureOf(ErrOptions, "start", "", Details{})
	}
	call, err := endpoint.begin(ctx, request, Scope{})
	if err != nil {
		return nil, err
	}
	go call.dispatch(producer)
	return call.Receipt(), nil
}

// Child synchronously invokes a child under the same runtime/root work envelope.
// It has a separate evidence reservation; a full Inbox never reaches native work.
// The parent remains owned until this child and its descendants actually release.
func (endpoint Endpoint[T]) Child(ctx context.Context, parent Scope, request Request, producer func(*Call[T])) (*Receipt[T], error) {
	if parent.node == nil || producer == nil {
		return nil, failureOf(ErrHandle, "child", "", Details{})
	}
	call, err := endpoint.begin(ctx, request, parent)
	if err != nil {
		return nil, err
	}
	call.dispatch(producer)
	return call.Receipt(), nil
}

// StartChild owns an asynchronous child producer without acquiring another root
// admission slot. Family-size/depth and evidence limits still apply.
func (endpoint Endpoint[T]) StartChild(ctx context.Context, parent Scope, request Request, producer func(*Call[T])) (*Receipt[T], error) {
	if parent.node == nil || producer == nil {
		return nil, failureOf(ErrHandle, "child", "", Details{})
	}
	call, err := endpoint.begin(ctx, request, parent)
	if err != nil {
		return nil, err
	}
	go call.dispatch(producer)
	return call.Receipt(), nil
}

// Context is the accepted operation context, canceled by its caller, parent call
// or runtime shutdown. A zero Call returns nil. Waiting uses a separate context.
func (call *Call[T]) Context() context.Context {
	if call == nil || call.state == nil {
		return nil
	}
	return call.state.node.ctx
}

// Cancel requests cancellation, never completion or rollback.
func (call *Call[T]) Cancel(cause error) error {
	if call == nil || call.state == nil {
		return failureOf(ErrHandle, "cancel", "", Details{})
	}
	call.state.node.cancel(cause)
	return nil
}

// Scope returns child-operation authority, or a zero Scope for a nil/zero Call.
func (call *Call[T]) Scope() Scope {
	if call == nil || call.state == nil {
		return Scope{}
	}
	return Scope{node: call.state.node}
}

// Receipt returns a fresh read-only wrapper over the same facts, or nil for an
// uninitialized Call. The wrapper does not grant cancellation or custody authority.
func (call *Call[T]) Receipt() *Receipt[T] {
	if call == nil || call.state == nil {
		return nil
	}
	return &Receipt[T]{state: call.state}
}

// Hold must precede dispatch of additional native/callback work. Guards bound
// actual references, not native goroutines or memory allocated by arbitrary code.
func (call *Call[T]) Hold() (Guard, error) {
	if call == nil || call.state == nil {
		return Guard{}, failureOf(ErrHandle, "hold", "", Details{})
	}
	control := call.state.node
	state := control.owner
	state.mu.Lock()
	defer state.mu.Unlock()
	if control.finishing || control.released {
		return Guard{}, failureOf(ErrReleased, "hold", state.options.Name, control.details())
	}
	if control.guards >= state.options.MaxHolds {
		return Guard{}, failureOf(ErrLimit, "hold", state.options.Name, control.details())
	}
	control.guards++
	control.refs++
	return Guard{state: &guardState{release: func() { control.returned(true) }}}, nil
}

// Release relinquishes exactly one work reference. Copies cannot release twice.
func (guard Guard) Release() error {
	if guard.state == nil {
		return failureOf(ErrHandle, "release", "", Details{})
	}
	guard.state.mu.Lock()
	release := guard.state.release
	if release == nil {
		guard.state.mu.Unlock()
		return failureOf(ErrReleased, "release", "", Details{})
	}
	guard.state.release = nil
	guard.state.mu.Unlock()
	release()
	return nil
}

// Resolve publishes one component-final outcome. It copies present data outside
// state locks. It does not release the submitter, guards, children or source lease.
// Late updates refuse rather than rewriting an observed result.
func (call *Call[T]) Resolve(outcome Outcome[T]) error {
	if call == nil || call.state == nil {
		return failureOf(ErrHandle, "resolve", "", Details{})
	}
	control := call.state.node
	state := control.owner
	state.mu.Lock()
	if control.resolved || control.resolving || control.finishing {
		err := failureOf(ErrSettled, "resolve", state.options.Name, control.details())
		state.mu.Unlock()
		return err
	}
	control.resolving = true
	control.refs++
	state.mu.Unlock()
	if outcome.Present {
		outcome.Value = call.state.copy(outcome.Value)
	} else {
		var zero T
		outcome.Value = zero
	}
	state.mu.Lock()
	call.state.outcome = outcome
	control.resolving = false
	control.resolved = true
	control.failed = outcome.Primary != nil || outcome.Cleanup != nil
	control.refs--
	control.observer.publish(Event{Operation: control.request.Operation, Phase: Resolved, Failed: control.failed})
	close(control.ready)
	finishNow := control.canFinishLocked()
	state.mu.Unlock()
	if finishNow {
		finish(control)
	}
	return nil
}

// Snapshot returns a detached status/outcome observation. Its bool reports
// whether an outcome exists; unresolved work is not a successful empty result.
func (receipt *Receipt[T]) Snapshot() (Snapshot[T], bool) {
	if receipt == nil || receipt.state == nil {
		return Snapshot[T]{}, false
	}
	control := receipt.state.node
	control.owner.mu.Lock()
	value := Snapshot[T]{info: control.infoLocked(), outcome: receipt.state.outcome, copy: receipt.state.copy, valid: true}
	control.owner.mu.Unlock()
	return value, value.info.Resolved
}

// Wait observes outcome publication. Its error concerns only the wait; inspect
// Snapshot.Err for the accepted operation's technical outcome.
func (receipt *Receipt[T]) Wait(ctx context.Context) (Snapshot[T], error) {
	return receipt.wait(ctx, false)
}

// WaitReleased joins actual work and instance borrowing, not remote effects,
// source-wide shutdown or durable acknowledgement.
func (receipt *Receipt[T]) WaitReleased(ctx context.Context) (Snapshot[T], error) {
	return receipt.wait(ctx, true)
}
func (receipt *Receipt[T]) wait(ctx context.Context, released bool) (Snapshot[T], error) {
	if receipt == nil || receipt.state == nil {
		return Snapshot[T]{}, failureOf(ErrHandle, "wait", "", Details{})
	}
	if ctx == nil {
		return Snapshot[T]{}, failureOf(ErrOptions, "wait", "", Details{})
	}
	control := receipt.state.node
	done := control.ready
	if released {
		done = control.done
	}
	select {
	case <-done:
		value, _ := receipt.Snapshot()
		return value, nil
	default:
	}
	select {
	case <-done:
		value, _ := receipt.Snapshot()
		return value, nil
	case <-ctx.Done():
		value, _ := receipt.Snapshot()
		return value, failureOf(ErrWait, "wait", control.owner.options.Name, Details{Sequence: control.sequence, Pending: !value.info.Released}, ctx.Err(), context.Cause(ctx))
	}
}

// Info deliberately exposes captured identifiers without component/native values.
func (value Snapshot[T]) Info() Info { return value.info }

// ValueCopy returns a component-owned copy and a presence flag; absence and zero
// are different. Copy must obey the declaration's concurrency/lifetime contract.
func (value Snapshot[T]) ValueCopy() (T, bool) {
	if !value.valid || !value.info.Resolved || !value.outcome.Present {
		var zero T
		return zero, false
	}
	return value.copy(value.outcome.Value), true
}

// Primary and Cleanup deliberately expose original cause objects, not safe text.
func (value Snapshot[T]) Primary() error { return value.outcome.Primary }
func (value Snapshot[T]) Cleanup() error { return value.outcome.Cleanup }

// Err safely wraps component/native causes without formatting them. It describes
// technical failure only; even nil does not certify business success.
func (value Snapshot[T]) Err() error {
	if !value.valid {
		return failureOf(ErrHandle, "result", "", Details{})
	}
	if !value.info.Resolved {
		return failureOf(ErrPending, "result", value.info.Runtime, Details{Sequence: value.info.Sequence, Pending: true})
	}
	if value.outcome.Primary == nil && value.outcome.Cleanup == nil {
		return nil
	}
	return failureOf(ErrOperation, value.info.Operation, value.info.Runtime, Details{Sequence: value.info.Sequence, Parent: value.info.Parent, Pending: !value.info.Released}, value.outcome.Primary, value.outcome.Cleanup)
}
