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
	"github.com/google/uuid"
	"sync"
	"sync/atomic"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Owner owns physical source cleanup and seals all of its retained Client/Worker
// uses on Close. It never cancels remote Workflows. Do not copy an Owner.
type Owner struct {
	private
	state  *sourceState
	client *Client
}

// Handle grants source use, not source ownership.
type Handle struct {
	private
	state *sourceState
}

type sourceState struct {
	id         string
	prepared   Prepared
	policy     Policy
	assembly   *source.Assembly
	selection  source.Selection[native.Source]
	endpoints  endpoints
	call       *adapters.Call[Result]
	guard      adapters.Guard
	primary    error
	executions *invocation.Inbox[native.Execution]
	rpcs       *invocation.Inbox[native.RPCResult]
	workers    *invocation.Inbox[native.WorkerResult]
	tasks      *invocation.Inbox[native.TaskResult]
	mu         sync.Mutex
	uses       map[*use]struct{}
	closing    bool
	serial     uint64
	cleanup    cleanupAttempt
	complete   atomic.Bool
}

type cleanupAttempt struct {
	mu   sync.Mutex
	done chan struct{}
	err  error
}

// run owns cleanup independently from its observer's wait. A completed incomplete
// attempt can be explicitly retried; an in-progress attempt is never duplicated.
func (attempt *cleanupAttempt) run(ctx context.Context, complete func() bool, work func() error) error {
	if ctx == nil {
		return fail(ErrInput, "close")
	}
	attempt.mu.Lock()
	start := attempt.done == nil
	if !start {
		select {
		case <-attempt.done:
			if complete() {
				err := attempt.err
				attempt.mu.Unlock()
				return err
			}
			start = true
		default:
		}
	}
	if start {
		attempt.done = make(chan struct{})
		done := attempt.done
		go func() {
			err := work()
			attempt.mu.Lock()
			attempt.err = err
			close(done)
			attempt.mu.Unlock()
		}()
	}
	done := attempt.done
	attempt.mu.Unlock()
	select {
	case <-done:
		attempt.mu.Lock()
		err := attempt.err
		attempt.mu.Unlock()
		return err
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close")
	}
}

// Open reserves source evidence/work before constructing native clients/plugins.
// ctx owns source lifetime, not just setup observation. A non-nil Owner always
// requires cleanup, including when returned with an error.
func Open(ctx context.Context, settings Settings, dependencies Dependencies) (*Owner, error) {
	prepared, err := Prepare(settings, dependencies.Native)
	if err != nil {
		return nil, err
	}
	return prepared.Open(ctx, dependencies)
}

func (prepared Prepared) Open(ctx context.Context, dependencies Dependencies) (*Owner, error) {
	return prepared.open(ctx, dependencies, prepared.native.Selection())
}

// OpenFrom derives an independently owned namespace client sharing the parent's
// transport. Settings must explicitly select the same endpoint/security profile;
// authentication is inherited, not reconfigured. Closing the parent cannot
// release the physical connection before this derived owner closes.
func (prepared Prepared) OpenFrom(ctx context.Context, parent *Client, dependencies Dependencies) (*Owner, error) {
	if parent == nil || parent.use == nil {
		return nil, fail(ErrInput, "shared-client")
	}
	_, err := prepared.Policy()
	if err != nil {
		return nil, err
	}
	selected, err := prepared.native.SelectionFromExisting(parent.raw, prepared.nativeLimits().Bytes)
	if err != nil {
		return nil, translate(err, "shared-client")
	}
	prepared.parent = parent
	return prepared.open(ctx, dependencies, selected)
}

func (prepared Prepared) open(ctx context.Context, dependencies Dependencies, selected source.Selection[native.Source]) (*Owner, error) {
	if ctx == nil {
		return nil, fail(ErrInput, "open")
	}
	policy, err := prepared.Policy()
	if err != nil {
		return nil, err
	}
	if err = checkPolicy(policy, dependencies); err != nil {
		return nil, err
	}
	if prepared.parent != nil {
		if err := prepared.parent.use.enter(); err != nil {
			return nil, err
		}
		defer prepared.parent.use.leave()
	}
	selected = source.WithLimits(selected, prepared.nativeLimits())
	admitting, cancelAdmission := context.WithCancelCause(ctx)
	defer cancelAdmission(nil)
	if prepared.parent != nil {
		stop := context.AfterFunc(prepared.parent.use.context, func() { cancelAdmission(context.Cause(prepared.parent.use.context)) })
		defer stop()
		if prepared.parent.use.context.Err() != nil {
			cancelAdmission(context.Cause(prepared.parent.use.context))
		}
	}
	admissionContext, stopAdmission, err := (invocation.Budget{Limit: prepared.metadata.AdmissionTimeout}).Context(admitting, invocation.Admission)
	if err != nil {
		return nil, translate(err, "open")
	}
	defer stopAdmission()
	bound, err := bindEndpoints(dependencies)
	if err != nil {
		return nil, err
	}
	var owner *Owner
	var primary error
	receipt, err := bound.operations.RunWithLifetime(admissionContext, ctx, adapters.Request{Operation: "temporal.open", WorkBytes: policy.SourceWorkBytes, EvidenceBytes: policy.SourceEvidenceBytes}, func(call *adapters.Call[Result]) {
		guard, holdErr := call.Hold()
		if holdErr != nil {
			primary = holdErr
			_ = call.Resolve(adapters.Outcome[Result]{Primary: holdErr})
			return
		}
		assembly, assemblyErr := source.Assemble(call.Context(), context.Background(), "temporal", selected)
		primary = translate(assemblyErr, "open")
		if assembly == nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		state := &sourceState{id: uuid.NewString(), prepared: prepared, policy: policy, assembly: assembly, selection: selected, endpoints: bound,
			call: call, guard: guard, primary: primary, uses: make(map[*use]struct{})}
		owner = &Owner{state: state}
		capacity := prepared.settings.InnerEvidenceCapacity
		state.executions, _ = invocation.NewInbox[native.Execution](capacity, int64(capacity)*prepared.metadata.EvidenceBytes)
		state.rpcs, _ = invocation.NewInbox[native.RPCResult](capacity, int64(capacity)*prepared.metadata.RPCEvidenceBytes)
		state.workers, _ = invocation.NewInbox[native.WorkerResult](capacity, int64(capacity)*prepared.metadata.EvidenceBytes)
		state.tasks, _ = invocation.NewInbox[native.TaskResult](capacity, int64(capacity)*prepared.metadata.EvidenceBytes)
		if primary == nil {
			owner.client, primary = state.borrow(call.Context(), bound, 0, nil, nil, nil)
			state.primary = primary
		}
		go func() { <-call.Context().Done(); _ = owner.Close(context.Background()) }()
	})
	if err != nil {
		return nil, err
	}
	if owner == nil && primary == nil {
		snapshot, _ := receipt.Snapshot()
		primary = snapshot.Err()
	}
	return owner, primary
}

func (owner *Owner) Client() *Client {
	if owner == nil {
		return nil
	}
	return owner.client
}
func (owner *Owner) Handle() Handle {
	if owner == nil || owner.client == nil {
		return Handle{}
	}
	return Handle{state: owner.state}
}
func (owner *Owner) ShutdownComplete() bool {
	return owner != nil && owner.state != nil && owner.state.complete.Load()
}
func (owner *Owner) Release(ctx context.Context) resource.ReleaseResult {
	err := owner.Close(ctx)
	return resource.ReleaseResult{Complete: owner.ShutdownComplete(), Err: err}
}

// Close seals every alias and stops local Workers, then joins actual local work.
// Canceling this wait neither releases dependencies nor cancels remote executions.
// Derived owners may retain the transport; incomplete cleanup stays retryable.
func (owner *Owner) Close(ctx context.Context) error {
	if owner == nil || owner.state == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	state := owner.state
	state.mu.Lock()
	state.closing = true
	uses := make([]*use, 0, len(state.uses))
	for current := range state.uses {
		current.seal()
		uses = append(uses, current)
	}
	state.mu.Unlock()
	_ = state.call.Cancel(nil)
	return state.cleanup.run(ctx, state.complete.Load, func() error {
		var errs []error
		for _, current := range uses {
			if err := current.close(context.Background()); err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) > 0 {
			return translate(errors.Join(errs...), "close")
		}
		err := state.assembly.Close(context.Background())
		for _, entry := range state.assembly.Snapshot().Sources {
			if entry.Pending || !entry.Quiescent || !entry.Released {
				return translate(errors.Join(err, source.ErrIncomplete), "close")
			}
		}
		final := translate(err, "close")
		_ = state.call.Resolve(adapters.Outcome[Result]{Value: Result{Source: state.attribution(0), SourceOpened: state.primary == nil, SourceReleased: true}, Present: true, Primary: state.primary, Cleanup: final})
		_ = state.guard.Release()
		state.complete.Store(true)
		return final
	})
}

func (state *sourceState) attribution(generation uint64) Attribution {
	return Attribution{SourceID: state.id, Name: state.prepared.settings.Name, Namespace: state.prepared.settings.Namespace, Generation: generation}
}

func checkPolicy(policy Policy, dependencies Dependencies) error {
	limits, err := dependencies.Runtime.Options()
	if err != nil {
		return err
	}
	evidence, err := dependencies.Evidence.Options()
	if err != nil {
		return err
	}
	workers, err := dependencies.Workers.Options()
	if err != nil {
		return err
	}
	tasks, err := dependencies.Tasks.Options()
	if err != nil {
		return err
	}
	want := policy.Runtime
	if limits.MaxActive < want.MaxActive || limits.MaxQueued < want.MaxQueued || limits.MaxWorkBytes < want.MaxWorkBytes ||
		limits.MaxQueuedBytes < want.MaxQueuedBytes || limits.MaxTasks < want.MaxTasks || limits.MaxDepth < want.MaxDepth || limits.MaxHolds < want.MaxHolds ||
		evidence.Capacity < policy.Evidence.Capacity || evidence.MaxBytes < policy.Evidence.MaxBytes ||
		workers.Capacity < policy.Workers.Capacity || workers.MaxBytes < policy.Workers.MaxBytes ||
		tasks.Capacity < policy.Tasks.Capacity || tasks.MaxBytes < policy.Tasks.MaxBytes {
		return fail(ErrLimit, "policy")
	}
	return nil
}
