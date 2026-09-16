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

package surf

import (
	"context"
	"errors"
	"io"
	"sync"

	http "github.com/enetx/http"
	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/pkg/connectproxy"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

type operation struct {
	client   *Client
	call     *invocation.Call[Result]
	ctx      context.Context
	cancel   context.CancelFunc
	work     *activity
	mu       sync.Mutex
	closing  bool
	original *http.Request
	route    routeChoice
	data     resultData
	primary  error
	cleanup  error
	inputs   []*inputBody
	response *sdk.Response
	replays  int
	once     sync.Once
	done     chan struct{}
	finalErr error
}

// Open admits a request and returns its controlled response stream. Request
// metadata/routing are copied before waiting; readers transfer only on admission.
// A non-nil receipt remains independently observable when the caller stops waiting.
func (client *Client) Open(ctx context.Context, id fault.Correlation, request *http.Request, options ...RequestOptionsV1) (*Stream, *invocation.Receipt[Result], error) {
	return client.open(ctx, id, request, invocation.Stream, options)
}

// Do retains a bounded body and waits for cleanup using the separate caller
// context. HTTP status is data, not a business failure classification.
func (client *Client) Do(ctx, cleanupCtx context.Context, id fault.Correlation, request *http.Request, options ...RequestOptionsV1) (*invocation.Receipt[Result], error) {
	if cleanupCtx == nil {
		return nil, failure(ErrInput, "cleanup-context")
	}
	stream, receipt, err := client.open(ctx, id, request, invocation.Finite, options)
	if stream == nil {
		return receipt, err
	}
	buffer := make([]byte, 32<<10)
	var data []byte
	for {
		count, readErr := stream.Read(buffer)
		data = append(data, buffer[:count]...)
		if readErr != nil {
			if readErr != io.EOF {
				stream.op.fail(readErr)
			}
			break
		}
	}
	stream.op.mu.Lock()
	stream.op.data.body = data
	stream.op.data.retained = true
	stream.op.mu.Unlock()
	err = stream.Close(cleanupCtx)
	if result, ok := receipt.Result(); ok && result.Final {
		return receipt, errors.Join(result.Err(), err)
	}
	return receipt, err
}
func (client *Client) open(ctx context.Context, id fault.Correlation, request *http.Request, shape invocation.Shape, options []RequestOptionsV1) (*Stream, *invocation.Receipt[Result], error) {
	if client == nil || client.owner == nil || client.access == nil || ctx == nil {
		return nil, nil, failure(ErrInput, "call")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, failure(ErrState, "call", err, context.Cause(ctx))
	}
	if err := validateRequest(ctx, request, client.owner.settings); err != nil {
		return nil, nil, err
	}
	route, err := client.route(options)
	if err != nil {
		return nil, nil, err
	}
	snapshot := copyRequest(request, ctx)
	value := client.owner.settings
	call, err := invocation.Begin(ctx, client.access, invocation.Request{Name: "request", Shape: shape, Correlation: id,
		Bytes: value.reservation(), EvidenceBytes: value.evidenceBytes(), Admission: invocation.Budget{Limit: value.AdmissionTimeout},
		AttemptsKnown: false}, client.inbox, client.observer)
	if err != nil {
		return nil, nil, err
	}
	work, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(ctx, invocation.Lifetime)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return nil, call.Receipt(), err
	}
	op := &operation{client: client, call: call, work: newActivity(), cancel: cancel, original: snapshot, route: route, done: make(chan struct{}),
		data: resultData{proxyMode: route.mode()}}
	op.ctx = context.WithValue(work, operationKey{}, op)
	if snapshot.Body != nil && snapshot.Body != http.NoBody {
		snapshot.Body = op.input(snapshot.Body)
	}
	if snapshot.GetBody != nil {
		factory := snapshot.GetBody
		snapshot.GetBody = func() (io.ReadCloser, error) { return op.replay(factory) }
	}
	ready := make(chan struct{})
	var handedMu sync.Mutex
	var response *sdk.Response
	var nativeErr error
	available, abandoned := false, false
	op.work.enter()
	go func() {
		got, err := op.nativeOpen()
		op.work.leave()
		handedMu.Lock()
		response, nativeErr, available = got, err, true
		if abandoned {
			handedMu.Unlock()
			op.finish()
			return
		}
		close(ready)
		handedMu.Unlock()
		if err != nil {
			op.finish()
		}
	}()
	select {
	case <-ready:
	case <-op.ctx.Done():
		handedMu.Lock()
		if !available {
			abandoned = true
			handedMu.Unlock()
			op.fail(errors.Join(op.ctx.Err(), context.Cause(op.ctx)))
			op.finish()
			return nil, call.Receipt(), failure(ErrState, "headers-wait", op.ctx.Err(), context.Cause(op.ctx))
		}
		handedMu.Unlock()
	}
	handedMu.Lock()
	defer handedMu.Unlock()
	if nativeErr != nil {
		return nil, call.Receipt(), nativeErr
	}
	stream := &Stream{op: op, gate: make(chan struct{}, 1)}
	if response.Body != nil {
		stream.body = response.Body.Reader
	} else {
		stream.body = http.NoBody
	}
	op.mu.Lock()
	op.response = response
	op.mu.Unlock()
	return stream, call.Receipt(), nil
}
func (op *operation) nativeOpen() (*sdk.Response, error) {
	binding, err := op.client.owner.binding(op.ctx, op.route)
	if err != nil {
		op.nativeCleanup(err)
		op.fail(err)
		return nil, err
	}
	request := copyRequest(op.original, op.ctx)
	result := binding.client.FathomryRequest(request).Do()
	if result.IsErr() {
		op.nativeCleanup(result.Err())
		err = failure(ErrTransport, "request", result.Err(), op.ctx.Err(), context.Cause(op.ctx))
		if errors.Is(result.Err(), sdk.ErrFathomryCallback) {
			err = failure(ErrCallback, "native", err)
		}
		if split, ok := result.Err().(*sdk.FathomryFailure); !ok || split.Primary != nil {
			op.fail(err)
		}
		return nil, err
	}
	response := result.Ok()
	op.mu.Lock()
	op.response = response
	err = op.primary
	op.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return response, nil
}
func (op *operation) nativeCleanup(err error) {
	var split *sdk.FathomryFailure
	if errors.As(err, &split) {
		op.recordCleanup(split.Cleanup)
	}
}
func (op *operation) fail(err error) {
	if err == nil {
		return
	}
	op.mu.Lock()
	defer op.mu.Unlock()
	op.primary = errors.Join(op.primary, err)
}
func (op *operation) recordCleanup(err error) {
	if err == nil {
		return
	}
	op.mu.Lock()
	op.cleanup = errors.Join(op.cleanup, err)
	op.mu.Unlock()
}
func (op *operation) closeRejected(body io.Closer) error {
	err := invoke("rejected-body-close", body.Close)
	op.recordCleanup(err)
	return err
}
func (op *operation) roundTrip(transport http.RoundTripper, request *http.Request, headers http.Header) (*http.Response, error) {
	if err := op.ctx.Err(); err != nil {
		return nil, errors.Join(err, context.Cause(op.ctx))
	}
	if !headerFits(request.Header, op.client.owner.settings.MaxHeaderBytes, true) {
		return nil, failure(ErrLimit, "request-headers")
	}
	op.mu.Lock()
	if op.closing || op.data.roundTrips >= op.client.owner.settings.MaxRoundTrips || op.primary != nil {
		err := op.primary
		op.mu.Unlock()
		return nil, errors.Join(failure(ErrLimit, "round-trips"), err)
	}
	op.data.roundTrips++
	op.mu.Unlock()
	if _, err := op.call.Attempt(); err != nil {
		return nil, err
	}
	if len(headers) > 0 {
		request = request.WithContext(context.WithValue(request.Context(), connectproxy.ContextKeyHeader{}, headers.Clone()))
	}
	response, err := transport.RoundTrip(request)
	if response != nil {
		if !headerFits(response.Header, op.client.owner.settings.MaxHeaderBytes, false) {
			if response.Body != nil {
				err = errors.Join(err, op.closeRejected(response.Body))
			}
			return nil, errors.Join(failure(ErrLimit, "response-headers"), err)
		}
		op.mu.Lock()
		op.data.metadata = metadata(response, request.URL.String())
		op.mu.Unlock()
		if response.StatusCode == 101 {
			var cleanup error
			if response.Body != nil {
				cleanup = op.closeRejected(response.Body)
			}
			return nil, errors.Join(failure(ErrUnsupported, "upgrade"), cleanup)
		}
	}
	return response, err
}
func metadata(response *http.Response, url string) Metadata {
	return Metadata{present: true, status: response.StatusCode, protocol: response.Proto, url: url, headers: response.Header.Clone(), length: response.ContentLength}
}
func (op *operation) finish() {
	op.once.Do(func() {
		op.mu.Lock()
		op.closing = true
		op.mu.Unlock()
		op.cancel()
		op.work.stop()
		go func() {
			op.closeBodies()
			<-op.work.done
			op.closeBodies()
			op.mu.Lock()
			data := op.data
			var cleanup []error
			if op.cleanup != nil {
				cleanup = append(cleanup, op.cleanup)
			}
			for _, input := range op.inputs {
				if input.closeErr != nil {
					cleanup = append(cleanup, input.closeErr)
				}
			}
			response := op.response
			primary := op.primary
			op.mu.Unlock()
			if response != nil && response.Body != nil {
				if err := response.Body.Close(); err != nil {
					cleanup = append(cleanup, err)
				}
			}
			if data.complete && response != nil {
				trailers := response.GetResponse().Trailer
				if !headerFits(trailers, op.client.owner.settings.MaxHeaderBytes, false) {
					primary = errors.Join(primary, failure(ErrLimit, "trailers"))
				} else {
					data.trailers = trailers.Clone()
				}
			}
			if primary != nil {
				data.complete = false
			}
			var cleanupErr error
			if len(cleanup) > 0 {
				cleanupErr = failure(ErrCleanup, "body", cleanup...)
			}
			op.finalErr = errors.Join(primary, cleanupErr)
			op.call.Complete(invocation.Outcome[Result]{Value: Result{data: &data}, Present: data.metadata.present, Primary: primary, Cleanup: cleanupErr})
			close(op.done)
		}()
	})
}
func (op *operation) closeBodies() {
	op.mu.Lock()
	inputs := append([]*inputBody(nil), op.inputs...)
	response := op.response
	op.mu.Unlock()
	for _, input := range inputs {
		_ = input.Close()
	}
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
}
func (op *operation) wait(ctx context.Context) error {
	select {
	case <-op.done:
		return op.finalErr
	case <-ctx.Done():
		return failure(ErrCleanup, "cleanup-wait", ctx.Err(), context.Cause(ctx))
	}
}
