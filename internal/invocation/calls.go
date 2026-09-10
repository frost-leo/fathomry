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

// Package invocation implements process-local controlled calls and technical
// evidence handoff for framework integrations. It reuses resource's authoritative
// borrowing records and fault's technical context. It imports no SDK, exporter,
// orchestration or business-terminal policy. It is not Temporal Workflow code.
package invocation

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Phase identifies a budget's purpose, not a mandatory lifecycle state machine.
// SDKs that retain the initiating context need their native header/enqueue timeout
// facilities or a longer owning phase; ending a phase must not cancel a live body.
type Phase uint8

const (
	Admission Phase = iota + 1
	Execute
	Establish
	Consume
	Submit
	Delivery
	Lifetime
	Message
	Cleanup
	Waiting
)

// Budget derives a cooperative phase deadline: the earlier of now+Limit and the
// parent's deadline minus Reserve. Durations are nanoseconds, never milliseconds.
// Zero Limit inherits a parent's deadline; only Lifetime may instead inherit an
// explicitly cancellable parent without a deadline. Negative durations are invalid.
// Reserve leaves time inside a parent's deadline for cleanup/reporting; it cannot
// protect against independent parent cancellation or force an SDK to terminate.
type Budget struct {
	Limit   time.Duration
	Reserve time.Duration
}

// Context starts the phase budget. Its caller owns cancel and calls it only when
// the SDK no longer needs this context. Nothing is detached or run in a goroutine.
// Cleanup must receive its separately authorized caller-owned context and budget.
func (budget Budget) Context(parent context.Context, phase Phase) (context.Context, context.CancelFunc, error) {
	if parent == nil || phase < Admission || phase > Waiting || budget.Limit < 0 || budget.Reserve < 0 {
		return nil, nil, ErrBudget.New(fault.Context{})
	}
	if err := parent.Err(); err != nil {
		return nil, nil, ErrBudget.New(fault.Context{}, err, context.Cause(parent))
	}
	deadline, hasDeadline := parent.Deadline()
	if hasDeadline {
		deadline = deadline.Add(-budget.Reserve)
	} else if budget.Reserve != 0 {
		return nil, nil, ErrBudget.New(fault.Context{})
	}
	if budget.Limit > 0 {
		limit := time.Now().Add(budget.Limit)
		if !hasDeadline || limit.Before(deadline) {
			deadline, hasDeadline = limit, true
		}
	}
	if !hasDeadline {
		if phase != Lifetime || parent.Done() == nil {
			return nil, nil, ErrBudget.New(fault.Context{})
		}
		ctx, cancel := context.WithCancel(parent)
		return ctx, cancel, nil
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	if err := ctx.Err(); err != nil {
		cancel()
		return nil, nil, ErrBudget.New(fault.Context{}, err, context.Cause(ctx))
	}
	return ctx, cancel, nil
}

// Request is supplied by a trusted capability/adapter, not mutable instance state.
// Bytes reserves the entire root borrowing tree's working envelope. EvidenceBytes
// separately reserves retained outcome data, even after resource use ends. Both
// are declared upper bounds that the Provider must enforce before allocation.
// Correlation.Call is required. Higher-level execution attribution belongs to the
// caller's boundary, which must retain its association through evidence reception.
// AttemptsKnown may be true only when every SDK attempt is intercepted; MaxAttempts
// is then a positive local ceiling. Opaque SDK retries require false and zero.
type Request struct {
	Name          string
	Correlation   fault.Correlation
	Shape         Shape
	Bytes         int64
	EvidenceBytes int64
	Admission     Budget
	AttemptsKnown bool
	MaxAttempts   uint64
}

func (request Request) valid() bool {
	return request.Name != "" && (fault.Context{Operation: request.Name, Correlation: request.Correlation}).Valid() &&
		request.Correlation.Call != "" && request.Shape >= Finite && request.Shape <= Session &&
		request.Bytes >= 0 && request.EvidenceBytes > 0 &&
		(request.AttemptsKnown && request.MaxAttempts > 0 || !request.AttemptsKnown && request.MaxAttempts == 0)
}

// Scope grants nested use of an already held reservation, not another resource or
// permission to expand its byte/SDK capacity. It has no release or client method.
type Scope struct {
	runtimeValue
	lease       *resource.Lease
	access      *resource.Access
	correlation fault.Correlation
	suppressed  bool
}

// Guard owns an explicit retained obligation, e.g. an enqueue call still on its
// stack when its delivery callback fires. End confirms actual use has stopped,
// not merely that a caller stopped waiting. Copies share idempotent ownership.
type Guard struct {
	runtimeValue
	scope Scope
}

func (guard *Guard) Scope() Scope {
	if guard == nil {
		return Scope{}
	}
	return guard.scope
}

func (guard *Guard) End() {
	if guard != nil {
		guard.scope.lease.Release()
	}
}

// Hold must precede handing work/handles to another owner. It never waits on an
// admission quota. Cleanup and synchronous nested SDK helpers can simply use the
// existing Scope and Budget.Context, even with no spare borrowing-node capacity.
func (scope Scope) Hold() (*Guard, error) {
	lease, err := scope.lease.Retain()
	if err != nil {
		return nil, err
	}
	scope.lease = lease
	return &Guard{scope: scope}, nil
}

// Call is the Provider's one-shot completion authority; consumers receive Receipt.
// Complete ends only this authority's own use, not retained descendant obligations.
type Call[T any] struct {
	runtimeValue
	state *callState[T]
}

// Receipt is internal read-only access to the same call state. Copies retain the
// original observation; it never owns producer completion or inbox capacity.
type Receipt[T any] struct {
	runtimeValue
	state *callState[T]
}

type callState[T any] struct {
	mu          sync.Mutex
	scope       Scope
	location    fault.Context
	shape       Shape
	nested      bool
	attempts    Attempts
	maxAttempts uint64
	outcome     Outcome[T]
	resolved    bool
	final       bool
	released    bool
	executing   bool
	ready       chan struct{}
	finalized   chan struct{}
	started     time.Time
	observer    *Observer
	observation Observation
}

// Begin reserves necessary evidence BEFORE waiting for source admission. Full
// evidence capacity rejects without executing SDK work. It creates no goroutine,
// retry or exporter callback. ctx governs admission only; each subsequent phase
// receives an explicit context. Rejected admission has no accepted SDK operation
// or receipt; its attributed error is returned to the caller.
func Begin[T any](ctx context.Context, access *resource.Access, request Request, inbox *Inbox[T], observer *Observer) (*Call[T], error) {
	return begin(ctx, access, Scope{}, request, inbox, observer)
}

// BeginNested creates an independently attributed result within an existing
// reservation, e.g. a statement in a transaction or a message in a session.
// Bytes must be zero (the root already reserved the entire envelope); evidence
// has a separate reservation. Parent must match the parent's logical Call ID.
// Neither evidence saturation nor a full borrowing tree waits behind itself.
// Finish/drain bounded child results incrementally, not at session shutdown.
func BeginNested[T any](ctx context.Context, parent Scope, request Request, inbox *Inbox[T], observer *Observer) (*Call[T], error) {
	if parent.lease == nil || request.Bytes != 0 || request.Correlation.Parent != parent.correlation.Call {
		return nil, ErrInvalid.New(fault.Context{})
	}
	return begin(ctx, parent.access, parent, request, inbox, observer)
}

func begin[T any](ctx context.Context, access *resource.Access, parent Scope, request Request, inbox *Inbox[T], observer *Observer) (*Call[T], error) {
	if access == nil || !request.valid() || ctx == nil {
		return nil, ErrInvalid.New(fault.Context{})
	}
	info := access.Info()
	location := fault.Context{Operation: request.Name, Correlation: request.Correlation,
		Provider: info.Configuration.Identity.Provider, Source: info.Configuration.Identity.Name, Scope: info.Scope}
	phaseCtx, cancel, err := request.Admission.Context(ctx, Admission)
	if err != nil {
		return nil, ErrBudget.New(location, err)
	}
	defer cancel()
	if err := inbox.reserve(request.EvidenceBytes); err != nil {
		return nil, ErrEvidence.New(location, err)
	}
	var lease *resource.Lease
	if parent.lease != nil {
		lease, err = parent.lease.Retain()
	} else {
		lease, err = access.Acquire(phaseCtx, request.Bytes)
	}
	if err == nil && phaseCtx.Err() != nil {
		lease.Release()
		err = errors.Join(phaseCtx.Err(), context.Cause(phaseCtx))
	}
	if err != nil {
		inbox.unreserve(request.EvidenceBytes)
		return nil, ErrFailed.New(location, err)
	}
	state := &callState[T]{scope: Scope{lease: lease, access: access, correlation: request.Correlation,
		suppressed: parent.suppressed || observationSuppressed(ctx)},
		location: location, shape: request.Shape, nested: parent.lease != nil,
		ready: make(chan struct{}), finalized: make(chan struct{}), started: time.Now(),
		attempts: Attempts{Exact: request.AttemptsKnown}, maxAttempts: request.MaxAttempts, observer: observer}
	call := &Call[T]{state: state}
	inbox.publish(&Receipt[T]{state: state}, request.EvidenceBytes)
	return call, nil
}

func (call *Call[T]) Receipt() *Receipt[T] {
	if call == nil || call.state == nil {
		return nil
	}
	return &Receipt[T]{state: call.state}
}

func (call *Call[T]) Scope() Scope {
	if call == nil || call.state == nil {
		return Scope{}
	}
	return call.state.scope
}

// Attempt is invoked immediately BEFORE a genuinely intercepted SDK attempt.
// It adds no retry and acquires no resource quota. Opaque-mode observations are
// lower bounds. SDK integrations must apply upper shared-quota controls at their
// actual account/application/worker scope, including retries and refresh traffic.
func (call *Call[T]) Attempt() (uint64, error) {
	if call == nil || call.state == nil {
		return 0, ErrState.New(fault.Context{})
	}
	state := call.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.final {
		return 0, ErrState.New(state.location)
	}
	if state.attempts.Observed == math.MaxUint64 || state.maxAttempts > 0 && state.attempts.Observed >= state.maxAttempts {
		return 0, ErrAttempts.New(state.location)
	}
	state.attempts.Observed++
	return state.attempts.Observed, nil
}

// Resolve publishes an early technical outcome but retains the original lease
// and its reserved cleanup evidence slot. Finish must follow when all technical
// facts, including error-producing cleanup, are known. No new quota, inbox slot or
// borrowing node is needed for that cleanup, even when every limit is saturated.
// Resolve is one-shot; false means the supplied value remains the caller's.
func (call *Call[T]) Resolve(outcome Outcome[T]) bool {
	if call == nil || call.state == nil {
		return false
	}
	state := call.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.resolved || state.executing {
		return false
	}
	state.resolve(outcome)
	return true
}

func (state *callState[T]) resolve(outcome Outcome[T]) {
	if outcome.Primary != nil {
		outcome.Primary = attributeError(ErrFailed, state.location, outcome.Primary)
	}
	if outcome.Cleanup != nil {
		outcome.Cleanup = attributeError(ErrCleanup, state.location, outcome.Cleanup)
	}
	state.outcome = outcome
	state.resolved = true
	close(state.ready)
}

func attributeError(identity fault.Kind, location fault.Context, cause error) error {
	if occurrence, ok := cause.(*fault.Error); ok && occurrence != nil && occurrence.Diagnostic().Context == location {
		return occurrence
	}
	return identity.New(location, cause)
}

// Finish records final technical/cleanup facts after Resolve WITHOUT releasing
// resource use. An observed cleanup failure can thus be handed off even when local
// termination remains unconfirmed, without another slot or lease. Earlier errors
// remain inspectable. It is one-shot, not an automatic cleanup/reconciliation retry.
// Further error-producing operations need their own reserved results; this is not
// an unlimited history. Release separately confirms that this producer is quiescent.
func (call *Call[T]) Finish(cleanup error) bool {
	if call == nil || call.state == nil {
		return false
	}
	state := call.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.resolved || state.final || state.executing {
		return false
	}
	if cleanup != nil {
		state.outcome.Cleanup = ErrCleanup.New(state.location, state.outcome.Cleanup, cleanup)
	}
	state.finish()
	return true
}

// Release positively confirms that this producer no longer needs the resource.
// It requires Finish first, is idempotent, and never interprets an error, timeout
// or SDK Close return as that evidence. Descendants remain protected independently.
// A false return means invalid, premature or duplicate confirmation.
func (call *Call[T]) Release() bool {
	if call == nil || call.state == nil {
		return false
	}
	state := call.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.final || state.released || state.executing {
		return false
	}
	state.release()
	return true
}

// Complete combines Resolve, Finish and Release when ALL technical facts and this
// producer's local-use completion are known. It never invokes caller code, waits for an
// exporter or drops its inbox reservation. Retained local use is still protected.
// Use Resolve/Finish, not Complete, when late cleanup can produce further facts.
func (call *Call[T]) Complete(outcome Outcome[T]) bool {
	if call == nil || call.state == nil {
		return false
	}
	state := call.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.resolved || state.executing {
		return false
	}
	state.resolve(outcome)
	state.finish()
	state.release()
	return true
}

func (state *callState[T]) finish() {
	state.final = true
	state.observation = state.observer.offer(Event{Shape: state.shape, Nested: state.nested, Duration: time.Since(state.started),
		Failed: state.outcome.Primary != nil, CleanupFailed: state.outcome.Cleanup != nil, Attempts: state.attempts}, state.scope.suppressed)
	close(state.finalized)
}

func (state *callState[T]) release() {
	state.released = true
	if !state.executing {
		state.scope.lease.Release()
	}
}

// Execute runs one finite callback synchronously, then completes it. It does not
// infer cancellation, effects or SDK attempt counts from the callback's return.
// Arbitrary callbacks cannot be forcibly timed out; their lease remains held.
// Use this helper only when callback return confirms its own local use is over;
// remaining users must already be retained. Otherwise use manual Finish/Release.
// Other shapes use Begin, phase budgets, explicit guards and native completion.
// Its returned error covers execution setup only. SDK/cleanup errors are retained
// in the receipt's Result.Err; a nil Execute error is not technical success.
// Execute exclusively owns completion while running: other Resolve, Finish,
// Release and Complete return false, so reentrant/concurrent producers cannot discard its
// returned facts. A callback panic leaves explicit unresolved responsibility.
func (call *Call[T]) Execute(ctx context.Context, budget Budget, run func(context.Context, Scope) Outcome[T]) error {
	if call == nil || call.state == nil {
		return ErrState.New(fault.Context{})
	}
	state := call.state
	state.mu.Lock()
	if state.resolved || state.executing || state.shape != Finite || run == nil {
		state.mu.Unlock()
		return ErrState.New(state.location)
	}
	state.executing = true
	state.mu.Unlock()
	complete := func(outcome Outcome[T]) {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.resolve(outcome)
		state.finish()
		state.release()
	}
	defer func() {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.executing = false
		if state.released {
			state.scope.lease.Release()
		}
	}()
	phaseCtx, cancel, err := budget.Context(ctx, Execute)
	if err != nil {
		complete(Outcome[T]{Primary: err})
		return ErrBudget.New(state.location, err)
	}
	defer cancel()
	if err := phaseCtx.Err(); err != nil {
		cause := errors.Join(err, context.Cause(phaseCtx))
		complete(Outcome[T]{Primary: cause})
		return ErrBudget.New(state.location, cause)
	}
	complete(run(phaseCtx, call.Scope()))
	return nil
}

// Result returns whether a technical outcome has been published. Released can
// remain false after publication. A false return means no outcome yet; nil Err
// alone never proves data completeness, effects, or cleanup success.
func (receipt *Receipt[T]) Result() (Result[T], bool) {
	if receipt == nil || receipt.state == nil {
		return Result[T]{}, false
	}
	state := receipt.state
	state.mu.Lock()
	defer state.mu.Unlock()
	result := Result[T]{Outcome: state.outcome, Context: state.location, Source: state.scope.access.Info(),
		Limits: state.scope.access.Limits(),
		Shape:  state.shape, Nested: state.nested, Attempts: state.attempts, Observation: state.observation, Final: state.final}
	select {
	case <-state.scope.lease.Done():
		result.Released = true
	default:
	}
	return result, state.resolved
}

// Wait waits for the technical outcome only. Its error describes waiting,
// independently of Result.Err. It never requests cancellation or releases work.
func (receipt *Receipt[T]) Wait(ctx context.Context) (Result[T], error) {
	return receipt.wait(ctx, waitResolved)
}

// WaitFinal waits for the final technical/cleanup report, independently of local
// use ending. This permits evidence reception while failed cleanup remains owned.
func (receipt *Receipt[T]) WaitFinal(ctx context.Context) (Result[T], error) {
	return receipt.wait(ctx, waitFinal)
}

// WaitReleased additionally waits for explicit release and all local descendants.
// None of these waits establishes external effects or durable acknowledgement.
func (receipt *Receipt[T]) WaitReleased(ctx context.Context) (Result[T], error) {
	return receipt.wait(ctx, waitReleased)
}

type completionStage uint8

const (
	waitResolved completionStage = iota
	waitFinal
	waitReleased
)

func (receipt *Receipt[T]) wait(ctx context.Context, stage completionStage) (Result[T], error) {
	if receipt == nil || receipt.state == nil || ctx == nil {
		return Result[T]{}, ErrWait.New(fault.Context{})
	}
	var done <-chan struct{} = receipt.state.ready
	switch stage {
	case waitFinal:
		done = receipt.state.finalized
	case waitReleased:
		done = receipt.state.scope.lease.Done()
	}
	select {
	case <-done:
		result, _ := receipt.Result()
		return result, nil
	default:
	}
	select {
	case <-done:
		result, _ := receipt.Result()
		return result, nil
	case <-ctx.Done():
		result, _ := receipt.Result()
		return result, ErrWait.New(receipt.state.location, ctx.Err(), context.Cause(ctx))
	}
}
