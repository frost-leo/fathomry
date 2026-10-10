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
	"sync/atomic"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Admission reserves an outer operation and its evidence before native entry.
// ctx governs admission; lifetime governs accepted work. A nil parent denotes a
// root; otherwise parent identifies an existing outer family, never a new root.
// Successful hooks return a non-nil work context and non-panicking finish function.
// finish receives direct custody of the exact inner record, or a nil record and
// setup error when inner admission failed. It must retain actual descendants,
// transfer the outcome, and release that custody without acquiring new capacity.
// Hooks are trusted composition, immutable after binding and concurrent-safe.
type Admission[T any] func(ctx, lifetime, parent context.Context, request invocation.Request) (work context.Context, finish func(*invocation.DeliveryRecord[T], error), err error)

type admissions struct {
	execution Admission[Execution]
	worker    Admission[WorkerResult]
	task      Admission[TaskResult]
}

type admissionFrameKey struct{}

type admissionFrame struct {
	access  *resource.Access
	scope   invocation.Scope
	callID  string
	context context.Context
	closed  atomic.Bool
}

func newAdmissionFrame[T any](ctx context.Context, access *resource.Access, call *invocation.Call[T]) *admissionFrame {
	result, _ := call.Receipt().Result()
	return &admissionFrame{access: access, scope: call.Scope(), callID: result.Context.Correlation.Call, context: ctx}
}

func inheritedAdmission(ctx context.Context, access *resource.Access, request *invocation.Request) (context.Context, *invocation.Scope, error) {
	frame, _ := ctx.Value(admissionFrameKey{}).(*admissionFrame)
	if frame == nil {
		return nil, nil, nil
	}
	if frame.closed.Load() || !frame.access.SameScope(access) {
		return nil, nil, failure(ErrAuthority, "admission-origin")
	}
	request.Bytes = 0
	request.Correlation.Parent = frame.callID
	return frame.context, &frame.scope, nil
}

// WithAdmissions returns another facade over the same retained access, installing
// exact outer reservations for process operations, Workers and user callbacks.
// Copies retain the original use identity and never acquire source ownership.
func WithAdmissions(client *Executions, execution Admission[Execution], worker Admission[WorkerResult], task Admission[TaskResult]) *Executions {
	if client == nil {
		return nil
	}
	copy := *client
	copy.admissions = admissions{execution: execution, worker: worker, task: task}
	return &copy
}

// WithRPCAdmission installs exact outer admission for the generated service view.
// It preserves the bound source, retained-use identity and shared inner limits.
func WithRPCAdmission(client *Client, admission Admission[RPCResult]) *Client {
	if client == nil {
		return nil
	}
	copy := *client
	copy.admission = admission
	return &copy
}

// WithWorkEnvelope selects the whole native operation-family envelope for two
// facades of the same retained scope. Nested calls share it without another root
// reservation. Composition must cover every permitted native descendant in this
// envelope and bound their count using the source's immutable MaxLeases policy.
// The envelope cannot exceed the authoritative source limit or shrink one RPC.
func WithWorkEnvelope(executions *Executions, rpc *Client, bytes int64) (*Executions, *Client, error) {
	if executions == nil || rpc == nil || executions.owner == nil || executions.owner != rpc.owner ||
		!executions.access.SameScope(rpc.access) || bytes < executions.owner.settings.reservation() ||
		bytes > executions.access.Limits().Bytes || bytes > rpc.access.Limits().Bytes {
		return nil, nil, failure(ErrInput, "work-envelope")
	}
	copyExecutions, copyRPC := *executions, *rpc
	copyExecutions.workBytes, copyRPC.workBytes = bytes, bytes
	return &copyExecutions, &copyRPC, nil
}

func operationWorkBytes(configured int64, owner *connection) int64 {
	if configured > 0 {
		return configured
	}
	return owner.settings.reservation()
}

func beginAdmitted[T any](ctx, lifetime, parent context.Context, request invocation.Request, access *resource.Access, scope *invocation.Scope, inbox *invocation.Inbox[T], observer *invocation.Observer, admission Admission[T]) (*invocation.Call[T], context.Context, func(), error) {
	if admission == nil {
		var call *invocation.Call[T]
		var err error
		if scope == nil {
			call, err = invocation.Begin(ctx, access, request, inbox, observer)
		} else {
			call, err = invocation.BeginNested(ctx, *scope, request, inbox, observer)
		}
		return call, lifetime, func() {}, err
	}
	admissionContext, cancelAdmission, err := request.Admission.Context(ctx, invocation.Admission)
	if err != nil {
		return nil, nil, nil, err
	}
	defer cancelAdmission()
	work, finish, err := admission(admissionContext, lifetime, parent, request)
	if err != nil {
		return nil, nil, nil, err
	}
	if work == nil || finish == nil {
		err := failure(ErrInput, "admission-contract")
		if finish != nil {
			finish(nil, err)
		}
		return nil, nil, nil, err
	}
	admitting, stop := joinAdmissionContext(admissionContext, work)
	defer stop()
	var call *invocation.Call[T]
	var delivery *invocation.DeliveryRecord[T]
	if scope == nil {
		call, delivery, err = invocation.BeginClaimed(admitting, access, request, inbox, observer)
	} else {
		call, delivery, err = invocation.BeginNestedClaimed(admitting, *scope, request, inbox, observer)
	}
	if err != nil {
		finish(nil, err)
		return nil, nil, nil, err
	}
	return call, work, func() { finish(delivery, nil) }, nil
}

func joinAdmissionContext(ctx, lifetime context.Context) (context.Context, func()) {
	work, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	stop := context.AfterFunc(lifetime, func() {
		defer close(done)
		cancel(errors.Join(lifetime.Err(), context.Cause(lifetime)))
	})
	if lifetime.Err() != nil {
		cancel(errors.Join(lifetime.Err(), context.Cause(lifetime)))
	}
	return work, func() {
		if !stop() {
			<-done
		}
		cancel(nil)
	}
}
