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
	cdpio "github.com/chromedp/cdproto/io"
	"github.com/chromedp/cdproto/page"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Stream owns a target-origin IO handle. Copies share offsets and cleanup.
type Stream struct {
	private
	state *streamState
}
type streamState struct {
	session             *Session
	owner               *owner
	executor            cdp.Session
	id                  cdpio.StreamHandle
	mu                  sync.Mutex
	gate                chan struct{}
	life                context.Context
	cancel              context.CancelFunc
	sealed, closed, eof bool
	bytes               int64
	primary             error
	cleanup             error
	sequential          bool
}

// Chunk keeps positive EOF distinct from a short or empty read.
type Chunk struct {
	private
	data []byte
	eof  bool
}

func (c Chunk) DataCopy() []byte { return append([]byte(nil), c.data...) }
func (c Chunk) EOF() bool        { return c.eof }

// PDFStream copies native parameters after admission and returns a real IO
// stream. It does not use the whole-buffered native PDFStream convenience.
func (session *Session) PDFStream(ctx context.Context, params page.PrintToPDFParams) (*Stream, error) {
	work, leave, err := session.enter(ctx, false)
	if err != nil {
		session.remember(err)
		return nil, err
	}
	defer leave()
	stream, err := session.reserveStream()
	if err != nil {
		session.remember(err)
		return nil, err
	}
	state, own := stream.state, session.owner
	abandon := func(err error) (*Stream, error) {
		state.cancel()
		state.finish()
		session.remember(err)
		return nil, err
	}
	if err := session.reserveCommand(); err != nil {
		return abandon(err)
	}
	buffer := boundedBuffer{limit: own.settings.MaxCommandBytes}
	if err := marshalBounded(&buffer, params); err != nil {
		return abandon(err)
	}
	var copied page.PrintToPDFParams
	if err := jsonv2.Unmarshal(buffer.data, &copied); err != nil {
		return abandon(err)
	}
	if copied.TransferMode != "" && copied.TransferMode != page.PrintToPDFTransferModeReturnAsStream {
		return abandon(failure(ErrInput, "pdf-stream-mode"))
	}
	copied.TransferMode = page.PrintToPDFTransferModeReturnAsStream
	result, err := cdp.Call(session.commandWork(work, false), sdk.FromContext(session.native).Target, page.PrintToPDF, copied)
	state.id = result.Stream
	if err == nil && state.id == "" {
		err = failure(ErrNative, "missing-stream")
	}
	if err != nil {
		err = failure(ErrNative, "pdf-stream", err)
	}
	state.primary = err
	session.remember(err)
	return stream, err
}

func (session *Session) reserveStream() (*Stream, error) {
	own, record := session.owner, session.browserContext
	own.mu.Lock()
	defer own.mu.Unlock()
	if len(record.streams) >= own.settings.MaxStreams {
		return nil, failure(ErrLimit, "streams")
	}
	life, cancel := context.WithCancel(session.work)
	state := &streamState{session: session, gate: make(chan struct{}, 1), life: life, cancel: cancel}
	if record.streams == nil {
		record.streams = make(map[*streamState]struct{})
	}
	record.streams[state] = struct{}{}
	return &Stream{state: state}, nil
}

// Read returns at most size bytes. size must be 1..65536. Offsets are owned;
// no caller-supplied stream ID or implicit seek can change this stream's origin.
func (stream *Stream) Read(ctx context.Context, size int) (Chunk, error) {
	return stream.read(ctx, size, nil)
}

// ReadAt seeks to an explicit nonnegative native byte offset before reading.
// It changes the position used by later Read calls. Sequential-only producers
// reject seeking. All bytes count against the same cumulative output allowance.
func (stream *Stream) ReadAt(ctx context.Context, size int, offset int64) (Chunk, error) {
	if offset < 0 {
		return Chunk{}, failure(ErrInput, "stream-offset")
	}
	return stream.read(ctx, size, &offset)
}

func (stream *Stream) read(ctx context.Context, size int, offset *int64) (Chunk, error) {
	if stream == nil || stream.state == nil || ctx == nil || size < 1 || size > 65536 {
		return Chunk{}, failure(ErrInput, "stream-read")
	}
	state := stream.state
	if offset != nil && state.sequential {
		return Chunk{}, failure(ErrUnsupported, "stream-sequential")
	}
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return Chunk{}, failure(ErrState, "stream-wait", ctx.Err(), context.Cause(ctx))
	}
	state.mu.Lock()
	closed, eof, total := state.sealed || state.closed, state.eof, state.bytes
	state.mu.Unlock()
	if closed || state.life.Err() != nil {
		return Chunk{}, failure(ErrState, "stream", state.life.Err(), context.Cause(state.life))
	}
	if eof && offset == nil {
		return Chunk{data: []byte{}, eof: true}, nil
	}
	remaining := state.owning().settings.MaxStreamBytes - total
	probe := remaining <= 0
	if probe {
		size = 1
	} else {
		size = min(size, int(remaining))
	}
	work, cancel := context.WithCancelCause(state.life)
	stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	defer stop()
	defer cancel(nil)
	work = sdk.WithCallLimits(work, sdk.CallLimits{MaxInputBytes: 4096, MaxOutputBytes: int64(size)*6 + 1024})
	if offset != nil {
		state.mu.Lock()
		state.eof = false
		state.mu.Unlock()
	}
	result, err := cdp.Call(work, state.origin(), cdpio.Read, cdpio.ReadParams{Handle: state.id, Offset: offset, Size: int64(size)})
	err = errors.Join(err, ctx.Err(), context.Cause(ctx), state.life.Err(), context.Cause(state.life))
	if len(result.Data) > size {
		err = errors.Join(err, failure(ErrLimit, "stream-chunk"))
		result.Data = nil
		result.EOF = false
	}
	state.mu.Lock()
	if probe && len(result.Data) != 0 {
		err = errors.Join(err, failure(ErrLimit, "stream-bytes"))
		state.sealed = true
		result.Data, result.EOF = nil, false
	}
	state.bytes += int64(len(result.Data))
	if err == nil {
		state.eof = result.EOF
	}
	if err != nil && state.primary == nil {
		state.primary = err
	}
	state.mu.Unlock()
	if state.session != nil {
		state.session.remember(err)
	}
	if err != nil {
		err = failure(ErrNative, "stream-read", err)
	}
	return Chunk{data: result.Data, eof: err == nil && result.EOF}, err
}

// Close needs no new root admission. A failed native close retains its slot and
// exact target-origin handle for another cleanup attempt.
func (stream *Stream) Close(ctx context.Context) error {
	if stream == nil || stream.state == nil || ctx == nil {
		return failure(ErrInput, "stream-close")
	}
	state := stream.state
	state.mu.Lock()
	state.sealed = true
	state.mu.Unlock()
	state.cancel()
	work, done, err := (invocation.Budget{Limit: state.owning().settings.CleanupTimeout}).Context(ctx, invocation.Cleanup)
	if err != nil {
		return failure(ErrCleanup, "stream-wait", err)
	}
	defer done()
	ctx = work
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return failure(ErrCleanup, "stream-join", ctx.Err(), context.Cause(ctx))
	}
	state.mu.Lock()
	closed, history := state.closed, state.cleanup
	state.mu.Unlock()
	if closed {
		return history
	}
	if state.resourcesGone() {
		state.finish()
		return history
	}
	if state.id == "" {
		return failure(ErrCleanup, "unknown-stream")
	}
	if err := ctx.Err(); err != nil {
		return failure(ErrCleanup, "stream-wait", err, context.Cause(ctx))
	}
	work = sdk.WithCallLimits(ctx, sdk.CallLimits{MaxInputBytes: 4096, MaxOutputBytes: 4096})
	_, err = cdp.Call(work, state.origin(), cdpio.Close, cdpio.CloseParams{Handle: state.id})
	if err != nil {
		err = failure(ErrCleanup, "stream-close", err)
		state.mu.Lock()
		if state.cleanup == nil {
			state.cleanup = err
		}
		history = state.cleanup
		state.mu.Unlock()
		if state.session != nil {
			state.session.rememberCleanup(history)
		}
		return errors.Join(history, err)
	}
	state.finish()
	return history
}

func (state *streamState) finish() {
	state.mu.Lock()
	state.closed = true
	state.mu.Unlock()
	if state.session != nil {
		own := state.session.owner
		own.mu.Lock()
		delete(state.session.browserContext.streams, state)
		own.mu.Unlock()
	}
}

func (state *streamState) owning() *owner {
	if state.session != nil {
		return state.session.owner
	}
	return state.owner
}

func (state *streamState) origin() cdp.Session {
	if state.executor != nil {
		return state.executor
	}
	return sdk.FromContext(state.session.native).Target
}

func (state *streamState) resourcesGone() bool {
	if state.session != nil {
		return state.session.nativeResourcesGone()
	}
	if state.owner != nil && state.owner.processDone != nil {
		select {
		case <-state.owner.processDone:
			return true
		default:
		}
	}
	return false
}
func (stream *Stream) Closed() bool {
	if stream == nil || stream.state == nil {
		return false
	}
	stream.state.mu.Lock()
	defer stream.state.mu.Unlock()
	return stream.state.closed
}
func (stream *Stream) Bytes() int64 {
	if stream == nil || stream.state == nil {
		return 0
	}
	stream.state.mu.Lock()
	defer stream.state.mu.Unlock()
	return stream.state.bytes
}
