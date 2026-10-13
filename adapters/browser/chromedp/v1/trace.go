/*
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

package chromedp

import (
	"context"
	"errors"
	"sync"

	"github.com/chromedp/cdproto/tracing"
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
	resource "github.com/frost-leo/fathomry/internal/resource"
)

// Trace owns an incremental browser-global trace. A connection to an external
// browser never grants this owner-only capability. Stop and Close use the
// reservation made before native startup; Read retains independent chunk evidence.
type Trace struct {
	private
	state *traceState
}

type traceState struct {
	native            *native.Trace
	owner             *Owner
	call              *adapters.Call[Result]
	guard             adapters.Guard
	setupDone         chan struct{}
	gate              chan struct{}
	mu                sync.Mutex
	active            int
	idle              chan struct{}
	closing, complete bool
	final             error
}

// TraceObservation separates native producer completion, byte consumption and
// positive stream cleanup. Completion does not imply that no data was lost.
type TraceObservation struct {
	Completed, StreamClosed, EOF, DataLossOccurred bool
	Bytes                                          int64
	Format                                         tracing.StreamFormat
	Compression                                    tracing.StreamCompression
}

// Trace starts one native browser-global producer. Parameters are copied after
// admission. TransferMode must be absent or ReturnAsStream; JSON/proto and
// compression options keep their native meanings. No video encoder is provided.
func (owner *Owner) Trace(ctx context.Context, params tracing.StartParams) (trace *Trace, receipt *adapters.Receipt[Result], err error) {
	if owner == nil || owner.state == nil || ctx == nil {
		return nil, nil, fail(ErrInput, "trace")
	}
	source := owner.state
	var primary error
	receipt, err = owner.client.endpoint.Child(ctx, source.call.Scope(), request("trace", "", 0, source.prepared.policy.Budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := source.use()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		defer finish()
		guard, err := call.Hold()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		capability, _, err := resource.Bind(source.assembly, source.selected)
		var value *native.Trace
		if err == nil {
			value, err = native.StartTrace(call.Context(), capability, params)
		}
		primary = translate(err, "trace")
		if value == nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		state := &traceState{native: value, owner: owner, call: call, guard: guard, setupDone: make(chan struct{}), gate: make(chan struct{}, 1)}
		defer close(state.setupDone)
		trace = &Trace{state: state}
		source.mu.Lock()
		if source.traces == nil {
			source.traces = make(map[*traceState]struct{})
		}
		source.traces[state] = struct{}{}
		source.mu.Unlock()
		go func() { <-value.Stopping(); _ = trace.Close(context.Background()) }()
	})
	return trace, receipt, errors.Join(err, receiptError(receipt, primary))
}

// Stop waits for native completion without reading the stream. It can progress
// with a full Inbox or exhausted ordinary command allowance. Repeated calls
// observe the same completion rather than starting or stopping a new producer.
func (trace *Trace) Stop(ctx context.Context) (tracing.EventTracingComplete, error) {
	if trace == nil || trace.state == nil || ctx == nil {
		return tracing.EventTracingComplete{}, fail(ErrInput, "trace-stop")
	}
	value, err := trace.state.native.Stop(ctx)
	return value, translate(err, "trace-stop")
}

func (trace *Trace) Read(ctx context.Context, size int) (chunk Chunk, receipt *adapters.Receipt[Result], err error) {
	if trace == nil || trace.state == nil || ctx == nil {
		return chunk, nil, fail(ErrInput, "trace-read")
	}
	state, source := trace.state, trace.state.owner.state
	var primary error
	receipt, err = state.owner.client.endpoint.Child(ctx, state.call.Scope(), request("trace_read", "", 0, source.prepared.policy.Budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		state.mu.Lock()
		if state.closing || state.complete {
			state.mu.Unlock()
			primary = fail(ErrState, "trace-read")
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			return
		}
		if state.active == 0 {
			state.idle = make(chan struct{})
		}
		state.active++
		state.mu.Unlock()
		defer func() {
			state.mu.Lock()
			state.active--
			if state.active == 0 {
				close(state.idle)
			}
			state.mu.Unlock()
		}()
		value, err := state.native.Read(call.Context(), size)
		chunk, primary = Chunk{native: value}, translate(err, "trace-read")
		info, _ := call.Receipt().Snapshot()
		_ = call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{payload: value.DataCopy(), attribution: attribution(info.Info()), observation: browser.Observation{Returned: err == nil, EOF: value.EOF(), Effect: browser.Unknown}}, Primary: primary})
	})
	return chunk, receipt, errors.Join(err, receiptError(receipt, primary))
}

func (trace *Trace) Closed() bool {
	if trace == nil || trace.state == nil {
		return false
	}
	trace.state.mu.Lock()
	defer trace.state.mu.Unlock()
	return trace.state.complete
}

func (trace *Trace) Close(ctx context.Context) error {
	if trace == nil || trace.state == nil || ctx == nil {
		return fail(ErrInput, "trace-close")
	}
	state := trace.state
	state.mu.Lock()
	state.closing = true
	active, idle := state.active, state.idle
	state.mu.Unlock()
	_ = state.call.Cancel(nil)
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "trace-join")
	}
	select {
	case <-state.setupDone:
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "trace-setup")
	}
	state.mu.Lock()
	complete, final := state.complete, state.final
	state.mu.Unlock()
	if complete {
		return final
	}
	cleanup := translate(state.native.Close(ctx), "trace-close")
	if !state.native.Closed() {
		return cleanup
	}
	if active != 0 {
		select {
		case <-idle:
		case <-ctx.Done():
			return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "trace-read-join")
		}
	}
	completion, consumed, completed, eof := state.native.Completion()
	primary := translate(state.native.Err(), "trace")
	info, _ := state.call.Receipt().Snapshot()
	value := Result{attribution: attribution(info.Info()), trace: TraceObservation{Completed: completed, StreamClosed: true, EOF: eof, DataLossOccurred: completion.DataLossOccurred, Bytes: consumed, Format: completion.TraceFormat, Compression: completion.StreamCompression}, observation: browser.Observation{Returned: primary == nil, StreamClosed: true, EOF: eof}}
	if err := state.call.Resolve(adapters.Outcome[Result]{Present: true, Value: value, Primary: primary, Cleanup: cleanup}); err != nil {
		return err
	}
	state.mu.Lock()
	state.complete, state.final = true, errors.Join(primary, cleanup)
	state.mu.Unlock()
	source := state.owner.state
	source.mu.Lock()
	delete(source.traces, state)
	source.mu.Unlock()
	_ = state.guard.Release()
	return state.final
}
