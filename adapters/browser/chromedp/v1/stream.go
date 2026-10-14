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

	"github.com/chromedp/cdproto/page"
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// Stream owns its native target-origin IO handle, not an arbitrary string ID.
// Close needs no fresh admission and preserves failed cleanup for retry.
type Stream struct {
	private
	state *streamState
}
type streamState struct {
	native            *native.Stream
	context           *Context
	page              *Page
	call              *adapters.Call[Result]
	guard             adapters.Guard
	mu                sync.Mutex
	closing, complete bool
	active            int
	idle              chan struct{}
	gate              chan struct{}
	primary, final    error
	initial           []byte
}
type Chunk struct {
	private
	native native.Chunk
}

func (c Chunk) DataCopy() []byte { return c.native.DataCopy() }
func (c Chunk) EOF() bool        { return c.native.EOF() }

// PDFStream uses incremental IO, never the SDK's whole-buffered convenience.
// Parameters are copied after admission. TransferMode must be absent or stream.
func (p *Page) PDFStream(ctx context.Context, params page.PrintToPDFParams) (*Stream, *adapters.Receipt[Result], error) {
	return p.openStream(ctx, "pdf_stream", func(work context.Context) (*native.Stream, error) {
		return p.native.PDFStream(work, params)
	})
}

func (p *Page) openStream(ctx context.Context, name string, create func(context.Context) (*native.Stream, error)) (*Stream, *adapters.Receipt[Result], error) {
	return p.openStreamResult(ctx, name, func(work context.Context) (*native.Stream, []byte, error) {
		value, err := create(work)
		return value, nil, err
	})
}

func (p *Page) openStreamResult(ctx context.Context, name string, create func(context.Context) (*native.Stream, []byte, error)) (*Stream, *adapters.Receipt[Result], error) {
	if p == nil || p.context == nil || ctx == nil {
		return nil, nil, fail(ErrInput, name)
	}
	c := p.context
	var stream *Stream
	var primary error
	receipt, err := c.endpoint.Child(ctx, c.call.Scope(), request(name, c.id, 0, c.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := p.beginOperation()
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
		value, initial, err := create(call.Context())
		primary = translate(err, name)
		if value == nil {
			info, _ := call.Receipt().Snapshot()
			_ = call.Resolve(adapters.Outcome[Result]{Present: len(initial) != 0, Value: Result{payload: initial, attribution: attribution(info.Info()), observation: browser.Observation{Returned: primary == nil}}, Primary: primary})
			_ = guard.Release()
			return
		}
		state := &streamState{native: value, context: c, page: p, call: call, guard: guard, gate: make(chan struct{}, 1), primary: primary, initial: initial}
		stream = &Stream{state: state}
		c.mu.Lock()
		if c.streams == nil {
			c.streams = make(map[*streamState]struct{})
		}
		c.streams[state] = struct{}{}
		c.mu.Unlock()
	})
	return stream, receipt, errors.Join(err, receiptError(receipt, primary))
}
func (s *Stream) Read(ctx context.Context, size int) (chunk Chunk, receipt *adapters.Receipt[Result], err error) {
	return s.read(ctx, size, nil)
}

// ReadAt seeks to an explicit native byte offset, including zero. It updates
// the position for later Read calls and uses the same cumulative byte allowance.
// Sequential-only producers reject it. Native seek failures remain visible.
func (s *Stream) ReadAt(ctx context.Context, size int, offset int64) (Chunk, *adapters.Receipt[Result], error) {
	return s.read(ctx, size, &offset)
}

func (s *Stream) read(ctx context.Context, size int, offset *int64) (chunk Chunk, receipt *adapters.Receipt[Result], err error) {
	if s == nil || s.state == nil || ctx == nil {
		return chunk, nil, fail(ErrInput, "stream-read")
	}
	state, c := s.state, s.state.context
	var primary error
	receipt, err = c.endpoint.Child(ctx, state.call.Scope(), request("stream_read", c.id, 0, c.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := state.page.beginOperation()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		defer finish()
		state.mu.Lock()
		if state.closing {
			state.mu.Unlock()
			primary = fail(ErrState, "stream")
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
		var value native.Chunk
		if offset == nil {
			value, err = state.native.Read(call.Context(), size)
		} else {
			value, err = state.native.ReadAt(call.Context(), size, *offset)
		}
		chunk = Chunk{native: value}
		primary = translate(err, "stream-read")
		state.mu.Lock()
		if primary != nil && state.primary == nil {
			state.primary = primary
		}
		state.mu.Unlock()
		info, _ := call.Receipt().Snapshot()
		_ = call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{payload: value.DataCopy(), attribution: attribution(info.Info()), observation: browser.Observation{Returned: err == nil, Effect: browser.Unknown, EOF: value.EOF()}}, Primary: primary})
	})
	return chunk, receipt, errors.Join(err, receiptError(receipt, primary))
}
func (s *Stream) Close(ctx context.Context) error {
	if s == nil || s.state == nil || ctx == nil {
		return fail(ErrInput, "stream-close")
	}
	state := s.state
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "stream-close")
	}
	state.mu.Lock()
	if state.complete {
		err := state.final
		state.mu.Unlock()
		return err
	}
	state.closing = true
	active, idle := state.active, state.idle
	state.mu.Unlock()
	_ = state.call.Cancel(nil)
	cleanup := translate(state.native.Close(ctx), "stream-close")
	if !state.native.Closed() {
		return cleanup
	}
	if active != 0 {
		select {
		case <-idle:
		case <-ctx.Done():
			return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "stream-join")
		}
	}
	state.mu.Lock()
	primary := state.primary
	state.mu.Unlock()
	info, _ := state.call.Receipt().Snapshot()
	if err := state.call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{payload: state.initial, attribution: attribution(info.Info()), observation: browser.Observation{Returned: true, StreamClosed: true}}, Primary: primary, Cleanup: cleanup}); err != nil {
		return err
	}
	state.mu.Lock()
	state.complete = true
	state.final = errors.Join(primary, cleanup)
	state.mu.Unlock()
	state.context.mu.Lock()
	delete(state.context.streams, state)
	state.context.mu.Unlock()
	_ = state.guard.Release()
	return state.final
}
func (s *Stream) Closed() bool {
	if s == nil || s.state == nil {
		return false
	}
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	return s.state.complete
}
