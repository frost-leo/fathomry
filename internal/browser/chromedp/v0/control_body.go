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

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	sdk "github.com/chromedp/chromedp"
)

type responseBodyState struct {
	streaming bool
	settled   bool
	stream    *Stream
	evidence  responseBodyEvidence
}

const responseStreamResultBytes = 2048

type responseBodyEvidence struct {
	Method        string `json:"method"`
	Attempted     bool   `json:"attempted"`
	Acknowledged  bool   `json:"acknowledged"`
	Body          []byte `json:"body,omitempty"`
	Base64Encoded bool   `json:"base64Encoded"`
}

// ResponseBody reads a response-stage Fetch pause once, without ordinary action
// admission. maxBytes bounds the native encoded response before decoding. The
// control ledger reserves twice that limit plus metadata before dispatch. Body
// contains decoded bytes, independently copied into the control evidence.
// Cancellation without a native reply keeps cleanup incomplete until teardown.
func (pending Pending) ResponseBody(ctx context.Context, maxBytes int64) (fetch.GetResponseBodyResult, error) {
	var value fetch.GetResponseBodyResult
	work, finish, err := pending.beginResponseBody(ctx, false, maxBytes)
	if err != nil {
		return value, err
	}
	defer finish()
	p, state := pending.state, pending.state.control
	responded := false
	work = sdk.WithCallLimits(work, sdk.CallLimits{MaxInputBytes: 4096, Result: func(_ string, size int64) error {
		responded = true
		if size > maxBytes {
			return sdk.ErrCommandLimit
		}
		return nil
	}})
	value, err = cdp.Call(work, sdk.FromContext(state.session.native).Target, fetch.GetResponseBody, fetch.GetResponseBodyParams{RequestID: p.request})
	nativeErr := err
	err = errors.Join(err, ctx.Err(), context.Cause(ctx), state.life.Err(), context.Cause(state.life))
	state.mu.Lock()
	p.body.settled = responded
	p.body.evidence.Acknowledged = nativeErr == nil
	if nativeErr == nil {
		p.body.evidence.Body = append([]byte(nil), value.Body...)
		p.body.evidence.Base64Encoded = value.Base64encoded
	}
	state.mu.Unlock()
	if err != nil {
		if errors.Is(err, sdk.ErrCommandLimit) {
			err = failure(ErrLimit, "response-body", err)
		} else {
			err = failure(ErrNative, "response-body", err)
		}
		state.fail(err)
	}
	return value, err
}

// ResponseBodyStream takes a response-stage Fetch body as a sequential IO
// stream. The control owns ignored streams. ContinueRequest/ContinueResponse are
// no longer valid after this attempt; read through EOF, then fulfill or fail.
// A non-nil stream requires Close even when creation returns an error.
func (pending Pending) ResponseBodyStream(ctx context.Context) (*Stream, error) {
	work, finish, err := pending.beginResponseBody(ctx, true, 0)
	if err != nil {
		return nil, err
	}
	defer finish()
	p, state := pending.state, pending.state.control
	stream := p.body.stream
	work = sdk.WithCallLimits(work, sdk.CallLimits{MaxInputBytes: state.session.owner.settings.MaxCommandBytes, MaxOutputBytes: responseStreamResultBytes})
	result, err := cdp.Call(work, sdk.FromContext(state.session.native).Target, fetch.TakeResponseBodyAsStream, fetch.TakeResponseBodyAsStreamParams{RequestID: p.request})
	if err == nil && result.Stream == "" {
		err = failure(ErrNative, "missing-response-stream")
	}
	stream.state.mu.Lock()
	stream.state.id = result.Stream
	stream.state.primary = err
	stream.state.mu.Unlock()
	state.mu.Lock()
	p.body.settled = err == nil
	p.body.evidence.Acknowledged = err == nil
	state.mu.Unlock()
	err = errors.Join(err, ctx.Err(), context.Cause(ctx), state.life.Err(), context.Cause(state.life))
	if err != nil {
		err = failure(ErrNative, "response-stream", err)
		state.fail(err)
	}
	return stream, err
}

func (pending Pending) beginResponseBody(ctx context.Context, streaming bool, maxBytes int64) (context.Context, func(), error) {
	if ctx == nil || pending.state == nil || pending.state.control == nil || !streaming && maxBytes <= 0 {
		return nil, nil, failure(ErrInput, "response-body")
	}
	p, state := pending.state, pending.state.control
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closing || state.closed || state.active != 0 || state.life.Err() != nil || ctx.Err() != nil || p.attempted || p.body != nil {
		return nil, nil, failure(ErrState, "response-body", ctx.Err(), context.Cause(ctx))
	}
	if p.method != fetch.RequestPaused.Method || !p.response || p.request == "" {
		return nil, nil, failure(ErrInput, "response-body-origin")
	}
	if streaming {
		maxBytes = responseStreamResultBytes
	}
	remaining := state.session.owner.settings.MaxResultBytes - state.bytes
	if remaining < 512 || maxBytes > (remaining-512)/2 {
		return nil, nil, failure(ErrLimit, "response-body-evidence")
	}
	body := &responseBodyState{streaming: streaming, evidence: responseBodyEvidence{Method: fetch.GetResponseBody.Method, Attempted: true}}
	own, family := state.session.owner, state.session.browserContext
	own.mu.Lock()
	defer own.mu.Unlock()
	if own.closed || own.transferred || own.life.Err() != nil {
		return nil, nil, failure(ErrState, "response-body-source")
	}
	if streaming {
		if len(family.streams) >= own.settings.MaxStreams {
			return nil, nil, failure(ErrLimit, "streams")
		}
		life, cancel := context.WithCancel(state.life)
		stream := &streamState{session: state.session, gate: make(chan struct{}, 1), life: life, cancel: cancel, sequential: true}
		if family.streams == nil {
			family.streams = make(map[*streamState]struct{})
		}
		family.streams[stream] = struct{}{}
		body.stream = &Stream{state: stream}
		body.evidence.Method = fetch.TakeResponseBodyAsStream.Method
	}
	if own.cleanupControls == nil {
		own.cleanupControls = make(map[*controlState]struct{})
	}
	own.cleanupControls[state] = struct{}{}
	p.body = body
	state.bytes += 2*maxBytes + 512
	state.active++
	state.idle = make(chan struct{})
	work, cancel := context.WithCancelCause(state.life)
	stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	return work, func() {
		stop()
		cancel(nil)
		state.mu.Lock()
		state.active--
		close(state.idle)
		state.mu.Unlock()
	}, nil
}

func (pending *pendingState) responseReplyReady() error {
	if pending.body == nil {
		return nil
	}
	if !pending.body.settled {
		return failure(ErrState, "response-body-unsettled")
	}
	if pending.body.stream != nil {
		state := pending.body.stream.state
		state.mu.Lock()
		eof := state.eof
		state.mu.Unlock()
		if !eof {
			return failure(ErrState, "response-body-not-consumed")
		}
	}
	return nil
}

func (state *controlState) closeResponseBodies(ctx context.Context) (bool, error) {
	var joined error
	for _, pending := range state.records {
		body := pending.body
		if body == nil {
			continue
		}
		if body.stream != nil {
			err := body.stream.Close(ctx)
			joined = errors.Join(joined, err)
			if !body.stream.Closed() {
				return false, errors.Join(joined, failure(ErrCleanup, "response-stream"))
			}
		} else if !body.settled && !state.session.nativeResourcesGone() {
			return false, errors.Join(joined, failure(ErrCleanup, "response-body-unsettled"))
		}
		if body.streaming && !pending.attempted && !state.session.nativeResourcesGone() {
			_, err := cdp.Call(state.work(ctx), sdk.FromContext(state.session.native).Target, fetch.FailRequest, fetch.FailRequestParams{RequestID: pending.request, ErrorReason: network.ErrorReasonAborted})
			joined = errors.Join(joined, err)
			if err != nil {
				return false, failure(ErrCleanup, "response-body-abort", joined)
			}
			if err == nil {
				state.mu.Lock()
				pending.attempted, pending.acknowledged, pending.reply = true, true, fetch.FailRequest.Method
				state.mu.Unlock()
			}
		}
	}
	if joined != nil {
		return true, failure(ErrCleanup, "response-body-close", joined)
	}
	return true, nil
}
