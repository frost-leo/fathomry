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

package tlsclient

import (
	"context"
	"errors"
	"io"
	"sync"

	http "github.com/bogdanfinn/fhttp"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

type operation struct {
	client                *Client
	call                  *invocation.Call[Result]
	ctx                   context.Context
	cancel                context.CancelFunc
	callbacks             *activity
	mu                    sync.Mutex
	closing               bool
	primary               error
	data                  *resultData
	route                 routeChoice
	inputs                []*requestBody
	outputs               []*responseBody
	finalResponse         *http.Response
	reads                 *activity
	sent, received        int64
	exchanges, replays    int
	inputGate, outputGate chan struct{}
	once                  sync.Once
	done                  chan struct{}
	finalErr              error
}

// Open borrows input readers only after admission and returns a controlled native
// response stream. Metadata and access choices are frozen before queueing.
func (client *Client) Open(ctx context.Context, id fault.Correlation, request *http.Request, options ...RequestOptionsV1) (*Stream, *invocation.Receipt[Result], error) {
	return client.open(ctx, id, request, invocation.Stream, options...)
}

// Do retains a bounded response and closes it using an independent cleanup wait.
// HTTP statuses remain data; a non-nil receipt is evidence even alongside error.
func (client *Client) Do(ctx, cleanupCtx context.Context, id fault.Correlation, request *http.Request, options ...RequestOptionsV1) (*invocation.Receipt[Result], error) {
	if cleanupCtx == nil {
		return nil, failure(ErrInput, "cleanup-context")
	}
	stream, receipt, err := client.open(ctx, id, request, invocation.Finite, options...)
	if stream == nil {
		return receipt, err
	}
	buffer := make([]byte, 32<<10)
	var data []byte
	var primary error
	for {
		count, readErr := stream.Read(buffer)
		if needed := len(data) + count; needed > cap(data) {
			capacity := min(int(client.owner.settings.MaxResponseBytes), max(needed, 2*cap(data)))
			grown := make([]byte, len(data), capacity)
			copy(grown, data)
			data = grown
		}
		data = append(data, buffer[:count]...)
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				primary = readErr
			}
			break
		}
	}
	stream.op.mu.Lock()
	stream.op.data.body, stream.op.data.retained = data, true
	stream.op.mu.Unlock()
	cleanup := stream.Close(cleanupCtx)
	if result, ok := receipt.Result(); ok && result.Final {
		return receipt, errors.Join(result.Err(), cleanup)
	}
	return receipt, errors.Join(primary, cleanup)
}
func (client *Client) open(ctx context.Context, id fault.Correlation, request *http.Request, shape invocation.Shape, options ...RequestOptionsV1) (*Stream, *invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, nil, err
	}
	if err := validateRequest(ctx, request, client.owner.settings); err != nil {
		return nil, nil, err
	}
	if err := client.replayable(request); err != nil {
		return nil, nil, err
	}
	route, err := client.route(ctx, options)
	if err != nil {
		return nil, nil, err
	}
	if err := client.validateRoute(request, route); err != nil {
		return nil, nil, err
	}
	snapshot := copyRequest(request, ctx)
	op, err := client.begin(ctx, id, shape, route)
	if err != nil {
		if op == nil {
			return nil, nil, err
		}
		return nil, op.call.Receipt(), err
	}
	op.inputGate, op.outputGate = make(chan struct{}, 1), make(chan struct{}, 1)
	ready := make(chan struct{})
	var mu sync.Mutex
	var stream *Stream
	var nativeErr error
	available, abandoned := false, false
	op.callbacks.enter()
	go func() {
		defer op.callbacks.leave()
		value, err := op.nativeOpen(snapshot)
		mu.Lock()
		if abandoned {
			mu.Unlock()
			if value != nil {
				value.reads.stop()
			}
			op.finish()
			return
		}
		stream, nativeErr, available = value, err, true
		close(ready)
		mu.Unlock()
		if err != nil {
			op.finish()
		}
	}()
	select {
	case <-ready:
	case <-op.ctx.Done():
		mu.Lock()
		if !available {
			abandoned = true
			mu.Unlock()
			op.finish()
			return nil, op.call.Receipt(), invocation.ErrWait.New(fault.Context{Provider: ProviderID, Operation: "headers"}, op.ctx.Err(), context.Cause(op.ctx))
		}
		mu.Unlock()
	}
	mu.Lock()
	defer mu.Unlock()
	return stream, op.call.Receipt(), nativeErr
}
func (client *Client) replayable(request *http.Request) error {
	if client.owner.settings.Mode == HTTP3Racing && request.URL.Scheme == "https" && request.Body != nil && request.Body != http.NoBody && request.GetBody == nil {
		return failure(ErrUnsupported, "racing-replay")
	}
	return nil
}
func (op *operation) nativeOpen(original *http.Request) (*Stream, error) {
	request := copyRequest(original, op.ctx)
	if len(request.Header) == 0 {
		request.Header = op.client.owner.native.DefaultHeaders.Clone()
		if request.Header == nil {
			request.Header = make(http.Header)
		}
	}
	if original.Body != nil && original.Body != http.NoBody {
		body, err := op.input(original.Body)
		if err != nil {
			op.fail(err)
			return nil, err
		}
		request.Body = body
	}
	if original.GetBody != nil {
		get := original.GetBody
		request.GetBody = func() (io.ReadCloser, error) {
			if !op.callbacks.enter() {
				return nil, failure(ErrState, "replay")
			}
			defer op.callbacks.leave()
			op.mu.Lock()
			available := !op.closing && op.replays < op.client.owner.settings.MaxReplays
			if available {
				op.replays++
			}
			op.mu.Unlock()
			if !available {
				return nil, failure(ErrLimit, "replays")
			}
			if err := op.ctx.Err(); err != nil {
				return nil, err
			}
			raw, err := get()
			if nilLike(raw) || raw == nil && err == nil {
				return nil, failure(ErrInput, "replay-reader", err)
			}
			if raw == nil {
				return nil, err
			}
			tracked, trackErr := op.input(raw)
			if trackErr != nil {
				var cleanup error
				if tracked == nil {
					cleanup = raw.Close()
				}
				return nil, errors.Join(trackErr, err, cleanup)
			}
			return tracked, err
		}
	}
	native := &http.Client{Transport: exchange{op}, CheckRedirect: op.redirect}
	if op.client.owner.native.Jar != nil {
		native.Jar = operationJar{op}
	}
	response, err := native.Do(request)
	op.mu.Lock()
	if err == nil {
		err = op.primary
	}
	if response != nil {
		op.finalResponse = response
	}
	op.mu.Unlock()
	if err != nil {
		err = failure(ErrTransport, "request", err, op.ctx.Err(), context.Cause(op.ctx))
		op.fail(err)
		return nil, err
	}
	reads := newActivity()
	op.reads = reads
	return &Stream{op: op, response: response, reads: reads}, nil
}

type exchange struct{ op *operation }

func (current exchange) RoundTrip(request *http.Request) (*http.Response, error) {
	op := current.op
	if err := op.ctx.Err(); err != nil {
		return nil, err
	}
	if err := op.before(request); err != nil {
		return nil, err
	}
	if err := validateRequest(op.ctx, request, op.client.owner.settings); err != nil {
		return nil, err
	}
	if err := op.client.replayable(request); err != nil {
		return nil, err
	}
	if err := op.client.validateRoute(request, op.route); err != nil {
		return nil, err
	}
	op.mu.Lock()
	prior := op.primary
	if op.exchanges >= op.client.owner.settings.MaxExchanges {
		prior = failure(ErrLimit, "exchanges")
	}
	op.mu.Unlock()
	if prior != nil {
		return nil, prior
	}
	binding, err := op.client.owner.getBinding(op.ctx, request.URL, op.route)
	if err != nil {
		return nil, err
	}
	var once sync.Once
	release := func() { once.Do(func() { op.client.owner.returnBinding(binding) }) }
	submitted := copyRequest(request, context.WithValue(request.Context(), sdk.ContextKeyHeader{}, nil))
	if submitted.Host == "" {
		submitted.Host = request.URL.Host
	}
	submitted.URL.Host = authority(submitted.URL)
	if _, err := op.call.Attempt(); err != nil {
		release()
		return nil, err
	}
	op.mu.Lock()
	op.exchanges++
	op.mu.Unlock()
	response, err := binding.native.Do(submitted)
	if response != nil {
		if response.Body == nil {
			response.Body = http.NoBody
		}
		expected := response.ContentLength
		if request.Method == "HEAD" || response.Uncompressed || response.StatusCode == 204 || response.StatusCode == 304 {
			expected = -1
		}
		tracked := &responseBody{raw: response.Body, op: op, reads: newActivity(), expected: expected, release: release, done: make(chan struct{})}
		response.Body = tracked
		op.mu.Lock()
		op.outputs = append(op.outputs, tracked)
		op.mu.Unlock()
		if !headerFits(response.Header, op.client.owner.settings.MaxHeaderBytes, false) {
			_ = tracked.Close()
			return nil, failure(ErrLimit, "response-headers")
		}
		op.mu.Lock()
		op.data.metadata = Metadata{present: true, status: response.StatusCode, protocol: response.Proto, url: request.URL.String(), headers: response.Header.Clone(), length: response.ContentLength, uncompressed: response.Uncompressed}
		op.mu.Unlock()
		if response.StatusCode == http.StatusSwitchingProtocols {
			_ = tracked.Close()
			return nil, failure(ErrUnsupported, "upgrade")
		}
	} else {
		release()
	}
	op.after(request, response, err)
	return response, err
}
func (op *operation) fail(err error) {
	if err == nil {
		return
	}
	op.mu.Lock()
	if op.primary == nil {
		op.primary = err
	}
	op.mu.Unlock()
}
func (op *operation) finish() {
	op.once.Do(func() {
		op.mu.Lock()
		op.closing = true
		op.mu.Unlock()
		op.cancel()
		op.callbacks.stop()
		go func() {
			op.closeBodies()
			<-op.callbacks.done
			op.closeBodies()
			if op.reads != nil {
				<-op.reads.stop()
			}
			op.mu.Lock()
			data := *op.data
			data.sent, data.received, data.exchanges = op.sent, op.received, op.exchanges
			data.warnings = append([]error(nil), data.warnings...)
			var cleanup []error
			for _, body := range op.inputs {
				if body.readErr != nil {
					data.inputErrors = append(data.inputErrors, body.readErr)
				}
				if body.err != nil {
					cleanup = append(cleanup, body.err)
				}
			}
			for _, body := range op.outputs {
				if body.err != nil {
					cleanup = append(cleanup, body.err)
				}
			}
			primary := op.primary
			if primary != nil {
				data.complete = false
			}
			response := op.finalResponse
			op.mu.Unlock()
			if data.complete && response != nil {
				if headerFits(response.Trailer, op.client.owner.settings.MaxHeaderBytes, false) {
					data.trailers = response.Trailer.Clone()
				} else {
					primary = failure(ErrLimit, "trailers")
					data.complete = false
				}
			}
			var cleanupErr error
			if len(cleanup) > 0 {
				cleanupErr = failure(ErrCleanup, "body", cleanup...)
			}
			op.call.Complete(invocation.Outcome[Result]{Present: data.metadata.present, Value: Result{data: &data}, Primary: primary, Cleanup: cleanupErr})
			op.finalErr = errors.Join(primary, cleanupErr)
			close(op.done)
		}()
	})
}
func (op *operation) closeBodies() {
	op.mu.Lock()
	inputs := append([]*requestBody(nil), op.inputs...)
	outputs := append([]*responseBody(nil), op.outputs...)
	op.mu.Unlock()
	for _, body := range inputs {
		_ = body.Close()
	}
	for _, body := range outputs {
		_ = body.Close()
	}
}
func (op *operation) wait(ctx context.Context) error {
	select {
	case <-op.done:
		return op.finalErr
	case <-ctx.Done():
		return invocation.ErrWait.New(fault.Context{Provider: ProviderID, Operation: "cleanup"}, ctx.Err(), context.Cause(ctx))
	}
}
