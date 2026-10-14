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

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/tracing"
	sdk "github.com/chromedp/chromedp"
)

// Trace owns one browser-global producer and its browser-origin incremental IO.
// Only a positively owned local browser can start it; owning a connection is
// insufficient. Copies share stop, offset and cleanup state.
type Trace struct {
	private
	state *traceState
}

type traceState struct {
	owner                         *owner
	life                          context.Context
	cancel                        context.CancelCauseFunc
	stopCaller                    func() bool
	unsubscribe                   func()
	done                          chan struct{}
	gate                          chan struct{}
	mu                            sync.Mutex
	completion                    tracing.EventTracingComplete
	stream                        *Stream
	active                        int
	idle                          chan struct{}
	endAttempted, closing, closed bool
	primary, cleanup              error
}

// StartTrace forces incremental native output. ReportEvents is a different
// producer contract and cannot silently become an unbounded event accumulator.
func StartTrace(ctx context.Context, source Source, params tracing.StartParams) (trace *Trace, err error) {
	defer func() {
		if err != nil {
			err = failure(ErrNative, "trace-start", err)
		}
	}()
	own := source.owner
	if ctx == nil || own == nil {
		return nil, failure(ErrInput, "trace")
	}
	if own.settings.RemoteURL != "" {
		return nil, failure(ErrState, "browser-owner-required")
	}
	if params.TransferMode != "" && params.TransferMode != tracing.StartTransferModeReturnAsStream {
		return nil, failure(ErrInput, "trace-transfer-mode")
	}
	if err := own.start(ctx); err != nil {
		return nil, err
	}
	if err := own.lock(ctx); err != nil {
		return nil, err
	}
	defer own.unlock()
	if own.closed || own.life.Err() != nil || own.process == nil {
		return nil, failure(ErrState, "trace-owner")
	}
	own.mu.Lock()
	busy := own.trace != nil
	own.mu.Unlock()
	if busy {
		return nil, failure(ErrState, "trace-producer")
	}
	if own.commands >= uint64(own.settings.MaxCommands) {
		return nil, failure(ErrLimit, "owner-commands")
	}
	own.commands++
	buffer := boundedBuffer{limit: own.settings.MaxCommandBytes}
	if err := marshalBounded(&buffer, params); err != nil {
		return nil, err
	}
	var copied tracing.StartParams
	if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
		return nil, err
	}
	if copied.TransferMode != "" && copied.TransferMode != tracing.StartTransferModeReturnAsStream {
		return nil, failure(ErrInput, "trace-transfer-mode")
	}
	copied.TransferMode = tracing.StartTransferModeReturnAsStream
	life, cancel := context.WithCancelCause(own.life)
	state := &traceState{owner: own, life: life, cancel: cancel, done: make(chan struct{}), gate: make(chan struct{}, 1)}
	state.stopCaller = context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	native := sdk.FromContext(own.root).Browser
	raw, unsubscribe := native.Subscribe(tracing.TracingComplete.Method)
	state.unsubscribe = unsubscribe
	own.mu.Lock()
	own.trace = state
	own.mu.Unlock()
	go state.receive(raw)
	work, stop := context.WithCancelCause(life)
	stopContext := context.AfterFunc(ctx, func() { stop(context.Cause(ctx)) })
	_, err = cdp.Call(state.work(work), native, tracing.Start, copied)
	stopContext()
	stop(nil)
	if err != nil {
		err = failure(ErrNative, "trace-start", err)
		state.fail(err)
	}
	return &Trace{state: state}, err
}

func (state *traceState) work(ctx context.Context) context.Context {
	return sdk.WithCallLimits(ctx, sdk.CallLimits{MaxInputBytes: state.owner.settings.MaxCommandBytes, MaxOutputBytes: 65536})
}
func (state *traceState) fail(err error) {
	state.mu.Lock()
	if state.primary == nil {
		state.primary = err
	}
	state.mu.Unlock()
}
func (state *traceState) receive(raw <-chan jsonv2.Value) {
	defer close(state.done)
	defer state.unsubscribe()
	data, ok := <-raw
	if !ok {
		state.fail(failure(ErrDisconnected, "trace-completion"))
		return
	}
	if len(data) > 65536 {
		state.fail(failure(ErrLimit, "trace-completion"))
		return
	}
	var message struct {
		tracing.EventTracingComplete
		Lost *bool `json:"dataLossOccurred"`
	}
	if err := jsonv2.Unmarshal(data, &message, sdk.DefaultUnmarshalOptions); err != nil {
		state.fail(failure(ErrNative, "trace-completion", err))
		return
	}
	if message.Stream == "" || message.Lost == nil {
		state.fail(failure(ErrNative, "trace-stream-identity"))
		return
	}
	message.DataLossOccurred = *message.Lost
	life, cancel := context.WithCancel(state.life)
	stream := &Stream{state: &streamState{owner: state.owner, executor: sdk.FromContext(state.owner.root).Browser, id: message.Stream, life: life, cancel: cancel, gate: make(chan struct{}, 1)}}
	state.mu.Lock()
	state.completion, state.stream = message.EventTracingComplete, stream
	state.mu.Unlock()
}

// Stop sends End once using its existing cleanup reservation, then waits for
// positive native completion. It does not read, buffer or close the trace stream.
func (trace *Trace) Stop(ctx context.Context) (tracing.EventTracingComplete, error) {
	if trace == nil || trace.state == nil || ctx == nil {
		return tracing.EventTracingComplete{}, failure(ErrInput, "trace-stop")
	}
	state := trace.state
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return tracing.EventTracingComplete{}, failure(ErrState, "trace-stop", ctx.Err(), context.Cause(ctx))
	}
	value, err := state.stop(ctx)
	if err != nil && !trace.Closed() {
		state.fail(err)
	}
	return value, err
}

func (state *traceState) stop(ctx context.Context) (tracing.EventTracingComplete, error) {
	if err := ctx.Err(); err != nil {
		return tracing.EventTracingComplete{}, failure(ErrState, "trace-stop", err, context.Cause(ctx))
	}
	state.mu.Lock()
	attempted, closed := state.endAttempted || state.stream != nil, state.closed
	state.endAttempted = true
	state.mu.Unlock()
	if closed {
		return tracing.EventTracingComplete{}, failure(ErrState, "trace-closed")
	}
	var endError error
	if !attempted {
		_, endError = cdp.Call(state.work(ctx), sdk.FromContext(state.owner.root).Browser, tracing.End, cdp.Empty{})
		if endError != nil {
			endError = failure(ErrNative, "trace-end", endError)
			state.fail(endError)
		}
	}
	select {
	case <-state.done:
		state.mu.Lock()
		defer state.mu.Unlock()
		if state.stream == nil {
			return tracing.EventTracingComplete{}, failure(ErrNative, "trace-completion", state.primary)
		}
		return state.completion, errors.Join(endError, ctx.Err(), context.Cause(ctx))
	case <-ctx.Done():
		return tracing.EventTracingComplete{}, failure(ErrState, "trace-completion", ctx.Err(), context.Cause(ctx))
	}
}

func (trace *Trace) Read(ctx context.Context, size int) (Chunk, error) {
	if trace == nil || trace.state == nil || ctx == nil {
		return Chunk{}, failure(ErrInput, "trace-read")
	}
	state := trace.state
	state.mu.Lock()
	stream, closing := state.stream, state.closing || state.closed
	if closing || stream == nil {
		state.mu.Unlock()
		return Chunk{}, failure(ErrState, "trace-not-readable")
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
	chunk, err := stream.Read(ctx, size)
	if err != nil {
		state.fail(err)
	}
	return chunk, err
}
func (trace *Trace) Stopping() <-chan struct{} {
	if trace == nil || trace.state == nil {
		return nil
	}
	return trace.state.life.Done()
}
func (trace *Trace) Err() error {
	if trace == nil || trace.state == nil {
		return failure(ErrInput, "trace")
	}
	trace.state.mu.Lock()
	defer trace.state.mu.Unlock()
	return trace.state.primary
}
func (trace *Trace) Closed() bool {
	if trace == nil || trace.state == nil {
		return false
	}
	trace.state.mu.Lock()
	defer trace.state.mu.Unlock()
	return trace.state.closed
}

// Close needs no new command/evidence admission. No observed completion means
// unknown custody until a later completion or actual owned-process exit.
func (trace *Trace) Close(ctx context.Context) error {
	if trace == nil || trace.state == nil || ctx == nil {
		return failure(ErrInput, "trace-close")
	}
	state := trace.state
	state.mu.Lock()
	state.closing = true
	state.mu.Unlock()
	state.cancel(context.Canceled)
	work, cancel := context.WithTimeout(ctx, state.owner.settings.CleanupTimeout)
	defer cancel()
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-work.Done():
		return failure(ErrCleanup, "trace-join", work.Err(), context.Cause(work))
	}
	state.mu.Lock()
	closed, history := state.closed, state.cleanup
	state.mu.Unlock()
	if closed {
		return history
	}
	gone := (&streamState{owner: state.owner}).resourcesGone()
	if gone {
		state.unsubscribe()
		select {
		case <-state.done:
		case <-work.Done():
			return failure(ErrCleanup, "trace-producer", work.Err(), context.Cause(work))
		}
	} else {
		if _, err := state.stop(work); err != nil {
			return state.rememberCleanup(err)
		}
	}
	state.mu.Lock()
	stream := state.stream
	state.mu.Unlock()
	if stream != nil {
		cleanup := stream.Close(work)
		if !stream.Closed() {
			return state.rememberCleanup(cleanup)
		}
		history = errors.Join(history, cleanup)
	}
	state.mu.Lock()
	active, idle := state.active, state.idle
	state.mu.Unlock()
	if active != 0 {
		select {
		case <-idle:
		case <-work.Done():
			return failure(ErrCleanup, "trace-reader-join", work.Err(), context.Cause(work))
		}
	}
	state.stopCaller()
	state.mu.Lock()
	state.closed, state.cleanup = true, history
	state.mu.Unlock()
	state.owner.mu.Lock()
	if state.owner.trace == state {
		state.owner.trace = nil
	}
	state.owner.mu.Unlock()
	return history
}

func (state *traceState) rememberCleanup(err error) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	err = failure(ErrCleanup, "trace", err)
	if state.cleanup == nil {
		state.cleanup = err
	}
	return errors.Join(state.cleanup, err)
}

// Completion returns immutable completion metadata only, not trace file bytes.
func (trace *Trace) Completion() (tracing.EventTracingComplete, int64, bool, bool) {
	if trace == nil || trace.state == nil {
		return tracing.EventTracingComplete{}, 0, false, false
	}
	state := trace.state
	state.mu.Lock()
	defer state.mu.Unlock()
	var bytes int64
	var eof bool
	if state.stream != nil {
		state.stream.state.mu.Lock()
		bytes, eof = state.stream.state.bytes, state.stream.state.eof
		state.stream.state.mu.Unlock()
	}
	return state.completion, bytes, state.stream != nil, eof
}
