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

package minio

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/frost-leo/fathomry/internal/invocation"
)

type exchangeKey struct{}
type cleanupKey struct{}
type exchange struct {
	mu              sync.Mutex
	call            *invocation.Call[Result]
	maximum         int
	requests        int
	remaining       int64
	controlExceeded bool
	payload         bool
	payloadLimit    int64
	bodies          []*boundedBody
	failures        []error
	closes          []error
	lastHeader      http.Header
	dials           sync.WaitGroup
	finishing       bool
}

func newExchange(value settings, call *invocation.Call[Result], payload bool) *exchange {
	return &exchange{call: call, maximum: value.MaxRequests, remaining: value.MaxResponseBytes, payload: payload, payloadLimit: value.MaxTransferBytes}
}
func (state *exchange) attempt() error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.requests >= state.maximum {
		return failure(ErrLimit, "http-exchanges")
	}
	if state.call != nil {
		if _, err := state.call.Attempt(); err != nil {
			return err
		}
	}
	state.requests++
	return nil
}
func (state *exchange) note(err error, cleanup bool) {
	if err == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if cleanup {
		state.closes = append(state.closes, err)
	} else {
		state.failures = append(state.failures, err)
	}
}
func (state *exchange) finish() (error, error) {
	state.mu.Lock()
	state.finishing = true
	state.mu.Unlock()
	state.dials.Wait()
	// All selected native methods are synchronous with respect to response use.
	// Core.GetObject's body is explicitly consumed/closed by our transfer, not a
	// lazy Object receiver. Still close every captured body, even on SDK error paths.
	for _, body := range state.bodies {
		_ = body.Close()
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return errors.Join(state.failures...), errors.Join(state.closes...)
}
func (state *exchange) beginDial() bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.finishing {
		return false
	}
	state.dials.Add(1)
	return true
}
func (state *exchange) header() http.Header {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.lastHeader.Clone()
}

type transport struct {
	base     http.RoundTripper
	endpoint string
	settings settings
}

func (wire *transport) RoundTrip(request *http.Request) (*http.Response, error) {
	state, _ := request.Context().Value(exchangeKey{}).(*exchange)
	refuse := func(err error) (*http.Response, error) {
		if request.Body != nil {
			if closeErr := request.Body.Close(); closeErr != nil {
				if state != nil {
					state.note(closeErr, true)
				}
				err = errors.Join(err, failure(ErrCleanup, "refused-request-body", closeErr))
			}
		}
		return nil, err
	}
	if state == nil {
		return refuse(failure(ErrAuthority, "unowned-request"))
	}
	if err := request.Context().Err(); err != nil {
		return refuse(err)
	}
	if request.URL.Scheme+"://"+request.URL.Host != wire.endpoint ||
		request.URL.User != nil || request.URL.Path != "/"+wire.settings.Bucket && !strings.HasPrefix(request.URL.Path, "/"+wire.settings.Bucket+"/") {
		return refuse(failure(ErrAuthority, "request-target"))
	}
	if err := state.attempt(); err != nil {
		return refuse(err)
	}
	var input *requestBody
	if request.Body != nil {
		request = request.Clone(request.Context())
		input = &requestBody{ReadCloser: request.Body, state: state, done: make(chan struct{})}
		request.Body = input
		defer func() { <-input.done }()
	}
	response, err := wire.base.RoundTrip(request)
	cleanup, _ := request.Context().Value(cleanupKey{}).(bool)
	if err != nil {
		state.note(err, cleanup)
		return response, err
	}
	response.Header.Del("Set-Cookie")
	isPayload := state.payload && request.Method == http.MethodGet && (response.StatusCode == 200 || response.StatusCode == 206)
	body := &boundedBody{body: response.Body, state: state, payload: isPayload, remaining: state.payloadLimit, cleanup: cleanup}
	response.Body = body
	state.mu.Lock()
	state.bodies = append(state.bodies, body)
	state.lastHeader = response.Header.Clone()
	state.mu.Unlock()
	if capture, _ := request.Context().Value(controlResponseKey{}).(*controlResponseCapture); capture != nil {
		response.Body = capture.observe(response.Body, response.StatusCode, response.Header.Get("Server"))
	}
	if input != nil {
		// Drain the bounded reply before joining request-body closure: an early
		// server reply can otherwise leave net/http's writer waiting on the socket.
		content, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		response.Body = &replayedBody{Reader: bytes.NewReader(content), terminal: readErr}
	}
	return response, nil
}

type requestBody struct {
	io.ReadCloser
	state  *exchange
	done   chan struct{}
	mu     sync.Mutex
	reads  sync.WaitGroup
	closed bool
	once   sync.Once
	err    error
}

func (body *requestBody) Read(buffer []byte) (int, error) {
	body.mu.Lock()
	if body.closed {
		body.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	body.reads.Add(1)
	body.mu.Unlock()
	defer body.reads.Done()
	return body.ReadCloser.Read(buffer)
}
func (body *requestBody) Close() error {
	body.once.Do(func() {
		body.mu.Lock()
		body.closed = true
		body.mu.Unlock()
		body.err = body.ReadCloser.Close()
		body.reads.Wait()
		body.state.note(body.err, true)
		close(body.done)
	})
	return body.err
}

type replayedBody struct {
	io.Reader
	terminal error
}

func (body *replayedBody) Read(buffer []byte) (int, error) {
	count, err := body.Reader.Read(buffer)
	if err == io.EOF && body.terminal != nil {
		err = body.terminal
	}
	return count, err
}
func (*replayedBody) Close() error { return nil }

type controlResponseKey struct{}
type controlResponseCapture struct{ pages []*controlResponse }
type controlResponse struct {
	body       bytes.Buffer
	statusCode int
	server     string
	readErr    error
}
type controlResponseBody struct {
	io.ReadCloser
	response *controlResponse
}

func (capture *controlResponseCapture) observe(body io.ReadCloser, statusCode int, server string) io.ReadCloser {
	response := &controlResponse{statusCode: statusCode, server: server}
	capture.pages = append(capture.pages, response)
	return &controlResponseBody{ReadCloser: body, response: response}
}
func (body *controlResponseBody) Read(buffer []byte) (int, error) {
	count, err := body.ReadCloser.Read(buffer)
	_, _ = body.response.body.Write(buffer[:count])
	if err != nil {
		body.response.readErr = err
	}
	return count, err
}

type boundedBody struct {
	mu        sync.Mutex
	body      io.ReadCloser
	state     *exchange
	payload   bool
	cleanup   bool
	remaining int64
	once      sync.Once
	closeErr  error
	readErr   error
}

func (body *boundedBody) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if body.readErr != nil {
		return 0, body.readErr
	}
	body.mu.Lock()
	raw := body.body
	body.mu.Unlock()
	if raw == nil {
		return 0, io.ErrClosedPipe
	}
	body.state.mu.Lock()
	if !body.payload && body.state.controlExceeded {
		body.state.mu.Unlock()
		body.readErr = failure(ErrLimit, "response-bytes")
		return 0, body.readErr
	}
	remaining := body.state.remaining
	if body.payload {
		remaining = body.remaining
	}
	body.state.mu.Unlock()
	probe := remaining == 0
	var scratch [1]byte
	target := buffer
	if probe {
		target = scratch[:]
	} else if int64(len(target)) > remaining {
		target = target[:remaining]
	}
	count, err := raw.Read(target)
	body.state.mu.Lock()
	if body.payload && !probe {
		body.remaining -= int64(count)
	} else if !body.payload && !probe {
		body.state.remaining -= int64(count)
	}
	if !body.payload && probe && count > 0 {
		body.state.controlExceeded = true
	}
	body.state.mu.Unlock()
	if probe && count > 0 {
		count = 0
		if err == io.EOF {
			err = nil
		}
		err = failure(ErrLimit, "response-bytes", err)
	}
	if err != nil {
		body.readErr = err
		if err != io.EOF {
			body.state.note(err, body.cleanup)
		}
	}
	return count, err
}
func (body *boundedBody) Close() error {
	body.once.Do(func() {
		body.closeErr = body.body.Close()
		body.state.note(body.closeErr, true)
		body.mu.Lock()
		body.body = nil
		body.mu.Unlock()
	})
	return body.closeErr
}
