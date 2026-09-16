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

package nuki

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	http "github.com/nukilabs/http"
	sdk "github.com/nukilabs/tlsclient"
)

type operationKey struct{}

// Do admits one finite call and returns its receipt before executing native work.
// Metadata is copied before admission. Body/GetBody are borrowed only after
// admission, through receipt release. The worker and cancellation cleanup are
// bounded by the original source allowance; caller Wait cancellation is separate.
func (client *Client) Do(ctx context.Context, id fault.Correlation, request *http.Request, options ...RequestOptionsV1) (*invocation.Receipt[Result], error) {
	return client.start(ctx, id, request, nil, options)
}

// Consume admits a streaming call. The callback receives an expiring read-only
// response capability, not an owning native Body. It must obey ctx and cannot
// retain the capability or start unaccounted work. Returning before EOF yields
// explicit incomplete response evidence, without inventing a read failure.
func (client *Client) Consume(ctx context.Context, id fault.Correlation, request *http.Request, consume func(context.Context, *Response) error, options ...RequestOptionsV1) (*invocation.Receipt[Result], error) {
	if consume == nil {
		return nil, failure(ErrInput, "consumer")
	}
	return client.start(ctx, id, request, consume, options)
}

func (client *Client) start(ctx context.Context, id fault.Correlation, request *http.Request, consume func(context.Context, *Response) error, options []RequestOptionsV1) (*invocation.Receipt[Result], error) {
	if len(options) > 1 {
		return nil, failure(ErrInput, "request-options")
	}
	var option RequestOptionsV1
	if len(options) == 1 {
		option = options[0]
	}
	copied, choice, err := client.prepare(ctx, request, option)
	if err != nil {
		return nil, err
	}
	value := client.owner.settings
	shape := invocation.Finite
	if consume != nil {
		shape = invocation.Stream
	}
	call, err := invocation.Begin(ctx, client.access, invocation.Request{Name: "request", Correlation: id, Shape: shape,
		Bytes: value.reservation(), EvidenceBytes: value.evidenceBytes(), Admission: invocation.Budget{Limit: value.AdmissionTimeout}},
		client.inbox, client.observer)
	if err != nil {
		return nil, err
	}
	work, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(ctx, invocation.Lifetime)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return call.Receipt(), err
	}
	op := &operation{client: client, call: call, cancel: cancel, request: copied, route: choice,
		data: &resultData{proxyMode: choice.mode}, input: byteBudget{limit: value.MaxRequestBytes},
		encoded: byteBudget{limit: value.MaxEncodedBytes}, response: byteBudget{limit: value.MaxResponseBytes}}
	op.ctx = context.WithValue(work, operationKey{}, op)
	if choice.forceHTTP3 {
		op.ctx = sdk.WithForceHTTP3(op.ctx)
	}
	op.request = op.request.WithContext(op.ctx)
	go op.run(consume)
	return call.Receipt(), nil
}

type operation struct {
	client         *Client
	call           *invocation.Call[Result]
	ctx            context.Context
	cancel         context.CancelFunc
	request        *http.Request
	route          routeChoice
	mu             sync.Mutex
	primary        error
	cleanup        error
	bodies         []*body
	data           *resultData
	input          byteBudget
	encoded        byteBudget
	response       byteBudget
	finalBody      *body
	wireBody       *body
	nativeResponse *http.Response
	view           *Response
	factories      activity
	finishing      bool
	responseFailed bool
}

func (op *operation) primaryError(err error) {
	if err == nil {
		return
	}
	op.mu.Lock()
	op.primary = errors.Join(op.primary, err)
	op.mu.Unlock()
}
func (op *operation) cleanupError(err error) {
	if err == nil {
		return
	}
	op.mu.Lock()
	op.cleanup = errors.Join(op.cleanup, failure(ErrCleanup, "body-close", err))
	op.mu.Unlock()
}
func (op *operation) currentError() error { op.mu.Lock(); defer op.mu.Unlock(); return op.primary }

func (op *operation) responseError(err error) {
	op.mu.Lock()
	defer op.mu.Unlock()
	op.responseFailed = true
	op.primary = errors.Join(op.primary, failure(ErrRead, "response", err, op.ctx.Err(), context.Cause(op.ctx)))
}

func (op *operation) run(consume func(context.Context, *Response) error) {
	aborted := make(chan struct{})
	stop := context.AfterFunc(op.ctx, func() { op.closeBodies(); close(aborted) })
	defer func() {
		if recovered := recover(); recovered != nil {
			op.primaryError(callbackFailure(recovered))
		}
		if op.view != nil {
			op.view.revoke()
		}
		op.mu.Lock()
		op.finishing = true
		op.mu.Unlock()
		op.closeBodies()
		op.factories.stop()
		op.closeBodies()
		if !stop() {
			<-aborted
		}
		if op.ctx.Err() != nil && !op.data.complete {
			op.primaryError(failure(ErrState, "lifetime", op.ctx.Err(), context.Cause(op.ctx)))
		}
		op.cancel()
		op.mu.Lock()
		readers := append([]*body(nil), op.bodies...)
		op.mu.Unlock()
		for _, reader := range readers {
			reader.reading.Wait()
			// Only native wire responses supply upload diagnostics. A caller's
			// input reader may have an unrelated method with the same name.
			if reader.budget == &op.encoded {
				if upload, ok := reader.raw.(interface{ UploadError() error }); ok {
					op.recordInputError(upload.UploadError())
				}
			}
		}
		op.data.inputBytes, op.data.encodedBytes, op.data.decodedBytes = op.input.count(), op.encoded.count(), op.response.count()
		if op.nativeResponse != nil {
			if err := validateHeaders(op.nativeResponse.Trailer, op.client.owner.settings.MaxHeaderBytes); err != nil {
				op.primaryError(err)
			} else {
				op.data.trailers = op.nativeResponse.Trailer.Clone()
			}
		}
		op.mu.Lock()
		primary, cleanup := op.primary, op.cleanup
		op.mu.Unlock()
		op.call.Complete(invocation.Outcome[Result]{Value: Result{data: op.data}, Present: op.data.metadata.present, Primary: primary, Cleanup: cleanup})
	}()
	if op.request.Body != nil && op.request.Body != http.NoBody {
		expected := op.request.ContentLength
		if expected == 0 {
			expected = -1
		}
		op.request.Body = op.ownBody(op.request.Body, &op.input, expected, true)
	}
	if getBody := op.request.GetBody; getBody != nil {
		op.request.GetBody = func() (io.ReadCloser, error) {
			if !op.factories.enter() {
				return nil, failure(ErrState, "replay-ended")
			}
			defer op.factories.leave()
			op.mu.Lock()
			if op.data.replays >= op.client.owner.settings.MaxReplays {
				op.mu.Unlock()
				return nil, failure(ErrLimit, "replays")
			}
			op.data.replays++
			op.mu.Unlock()
			if err := op.ctx.Err(); err != nil {
				return nil, errors.Join(err, context.Cause(op.ctx))
			}
			reader, err := getBody()
			if err != nil {
				if reader != nil {
					op.cleanupError(reader.Close())
				}
				op.recordInputError(err)
				return nil, err
			}
			if nilObject(reader) {
				return nil, failure(ErrInput, "nil-replay")
			}
			if op.aliasesBody(reader) {
				return nil, failure(ErrInput, "aliased-replay")
			}
			expected := op.request.ContentLength
			if expected == 0 {
				expected = -1
			}
			return op.ownBody(reader, &op.input, expected, true), nil
		}
	}
	if err := op.ctx.Err(); err != nil {
		op.primaryError(failure(ErrState, "execute", err, context.Cause(op.ctx)))
		return
	}
	route, err := op.client.owner.routeFor(op.route)
	if err != nil {
		op.primaryError(err)
		return
	}
	logical := route.client.Client
	if op.client.owner.native.Jar != nil {
		logical.Jar = &guardedJar{owner: op.client.owner, jar: op.client.owner.native.Jar, op: op}
	}
	res, err := logical.Do(op.request)
	if err != nil {
		op.primaryError(failure(ErrTransport, "request", err, op.ctx.Err(), context.Cause(op.ctx)))
		return
	}
	if res == nil || res.Body == nil {
		op.primaryError(failure(ErrTransport, "missing-response"))
		return
	}
	op.nativeResponse = res
	encodedLength := res.ContentLength
	if !op.client.owner.settings.DisableDecompression {
		sdk.DecompressBody(res)
	}
	op.data.metadata = responseMetadata(res, encodedLength)
	op.finalBody = op.ownBody(res.Body, &op.response, -1, false)
	if consume == nil {
		output, readErr := io.ReadAll(op.finalBody)
		if int64(len(output)) > op.client.owner.settings.MaxResponseBytes {
			output = output[:op.client.owner.settings.MaxResponseBytes]
		}
		op.data.body, op.data.retained = output, true
		if readErr != nil {
			op.primaryError(failure(ErrRead, "response", readErr, op.ctx.Err(), context.Cause(op.ctx)))
			return
		}
	} else {
		op.view = &Response{reader: op.finalBody, metadata: op.data.metadata}
		if err := consume(op.ctx, op.view); err != nil {
			op.primaryError(failure(ErrCallback, "consume", err))
			return
		}
		op.view.revoke()
	}
	if op.finalBody.reachedEOF() {
		if op.wireBody != nil {
			var witness [1]byte
			_, err := op.wireBody.Read(witness[:])
			if err != io.EOF {
				if err == nil {
					err = failure(ErrIntegrity, "encoded-remainder")
				}
				op.primaryError(failure(ErrIntegrity, "encoded-framing", err))
				return
			}
		}
		op.mu.Lock()
		op.data.complete = !op.responseFailed
		op.mu.Unlock()
	}
}

func (op *operation) closeBodies() {
	op.mu.Lock()
	readers := append([]*body(nil), op.bodies...)
	op.mu.Unlock()
	for index := len(readers) - 1; index >= 0; index-- {
		_ = readers[index].Close()
	}
}
func (op *operation) recordInputError(err error) {
	if err == nil || err == io.EOF {
		return
	}
	op.mu.Lock()
	op.data.readErrors = append(op.data.readErrors, err)
	op.mu.Unlock()
}

type exchangeTransport struct {
	owner  *owner
	native *sdk.RoundTripper
}

func (transport exchangeTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	op, ok := request.Context().Value(operationKey{}).(*operation)
	if !ok {
		return nil, failure(ErrState, "native-entry")
	}
	if err := op.currentError(); err != nil {
		return nil, err
	}
	if err := request.Context().Err(); err != nil {
		return nil, errors.Join(err, context.Cause(request.Context()))
	}
	op.mu.Lock()
	limit := op.data.exchanges >= transport.owner.settings.MaxExchanges
	op.mu.Unlock()
	if limit {
		return nil, failure(ErrLimit, "exchanges")
	}
	copied := request.Clone(request.Context())
	copied.Response = nil
	for _, hook := range transport.owner.native.Before {
		view := metadataRequest(copied)
		if err := hook(op.ctx, view); err != nil {
			return nil, failure(ErrCallback, "before", err)
		}
		if view.Body != nil || view.GetBody != nil || view.Response != nil || view.TLS != nil || view.Cancel != nil {
			return nil, failure(ErrUnsupported, "hook-owning-input")
		}
		if err := validateRequestMetadata(view, transport.owner.settings); err != nil {
			return nil, err
		}
		copied.Method, copied.URL, copied.Host, copied.Header, copied.Trailer = view.Method, view.URL, view.Host, view.Header, view.Trailer
	}
	if err := canonicalizeURL(copied.URL); err != nil {
		return nil, err
	}
	if err := validateRequest(copied, transport.owner.settings); err != nil {
		return nil, err
	}
	if err := transport.owner.validateRouteRequest(copied, op.route); err != nil {
		return nil, err
	}
	if err := transport.owner.admitOrigin(op.route, copied); err != nil {
		return nil, err
	}
	if err := op.ctx.Err(); err != nil {
		return nil, errors.Join(err, context.Cause(op.ctx))
	}
	if _, err := op.call.Attempt(); err != nil {
		return nil, err
	}
	op.mu.Lock()
	op.data.exchanges++
	op.mu.Unlock()
	res, err := transport.native.RoundTrip(copied)
	if res != nil && res.Body != nil {
		expected := res.ContentLength
		if copied.Method == "HEAD" || res.StatusCode == 204 || res.StatusCode == 304 || res.StatusCode >= 100 && res.StatusCode < 200 {
			expected = -1
		}
		res.Body = op.ownBody(res.Body, &op.encoded, expected, false)
		op.wireBody = res.Body.(*body)
	}
	if err != nil {
		return res, err
	}
	if res == nil {
		return nil, failure(ErrTransport, "missing-response")
	}
	if res.StatusCode == 101 {
		if res.Body != nil {
			_ = res.Body.Close()
		}
		return nil, failure(ErrUnsupported, "upgrade")
	}
	if err := validateHeaders(res.Header, transport.owner.settings.MaxHeaderBytes); err != nil {
		if res.Body != nil {
			_ = res.Body.Close()
		}
		return nil, err
	}
	for _, hook := range transport.owner.native.After {
		if err := hook(op.ctx, responseMetadata(res, res.ContentLength)); err != nil {
			if res.Body != nil {
				_ = res.Body.Close()
			}
			return nil, failure(ErrCallback, "after", err)
		}
	}
	return res, nil
}
func (transport exchangeTransport) Close() error          { return transport.native.Close() }
func (transport exchangeTransport) CloseIdleConnections() { transport.native.CloseIdleConnections() }

func responseMetadata(res *http.Response, encodedLength int64) Metadata {
	value := Metadata{present: true, status: res.StatusCode, protocol: res.Proto, headers: res.Header.Clone(),
		length: res.ContentLength, encodedLength: encodedLength, uncompressed: res.Uncompressed}
	if res.Request != nil && res.Request.URL != nil {
		value.url = res.Request.URL.String()
	}
	return value
}
func validateRequestMetadata(request *http.Request, value settings) error {
	copied := *request
	if copied.ContentLength > 0 {
		copied.Body = http.NoBody
	}
	return validateRequest(&copied, value)
}

func (owner *owner) redirect(request *http.Request, via []*http.Request) error {
	op, ok := request.Context().Value(operationKey{}).(*operation)
	if !ok {
		return failure(ErrState, "redirect")
	}
	if !owner.settings.FollowRedirects {
		return http.ErrUseLastResponse
	}
	if len(via) >= owner.settings.MaxExchanges {
		return failure(ErrLimit, "redirect")
	}
	if callback := owner.native.CheckRedirect; callback != nil {
		copied := metadataRequest(request)
		history := make([]*http.Request, len(via))
		for index := range via {
			history[index] = metadataRequest(via[index])
		}
		if err := callback(copied, history); err != nil {
			return err
		}
		if copied.Body != nil || copied.GetBody != nil || copied.Response != nil || copied.TLS != nil || copied.Cancel != nil {
			return failure(ErrUnsupported, "redirect-owning-input")
		}
		request.Method, request.URL, request.Host, request.Header, request.Trailer = copied.Method, copied.URL, copied.Host, copied.Header, copied.Trailer
	}
	validation := *request
	validation.Response = nil
	if err := validateRequest(&validation, owner.settings); err != nil {
		return err
	}
	return owner.validateRouteRequest(request, op.route)
}
