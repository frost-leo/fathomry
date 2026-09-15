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

package httpcloak

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	http "github.com/sardanioss/http"
	"github.com/sardanioss/http/httptrace"
	"github.com/sardanioss/httpcloak/transport"
)

type operation struct {
	client      *Client
	call        *invocation.Call[Result]
	ctx         context.Context
	cancel      context.CancelFunc
	parentStop  func()
	stopCause   error
	option      RequestOptionsV1
	mu          sync.Mutex
	inputMu     sync.Mutex
	primary     error
	cleanup     []error
	data        resultData
	inputs      []*inputBody
	outputs     []*wireBody
	nativeDone  chan struct{}
	collectDone chan struct{}
	collectOnce sync.Once
	done        chan struct{}
	stopOnce    sync.Once
	finalErr    error
	stream      *Stream
}

// Open returns a controlled, bounded decoded stream. A non-nil receipt retains
// responsibility even when the caller stops waiting before headers arrive.
// Request readers are borrowed only after admission, through WaitReleased.
func (client *Client) Open(ctx context.Context, id fault.Correlation, request *http.Request, options ...RequestOptionsV1) (*Stream, *invocation.Receipt[Result], error) {
	return client.open(ctx, id, request, invocation.Stream, options...)
}

// Do retains decoded response bytes. cleanupCtx bounds only the cleanup wait;
// it never proves native work has ended when the wait expires.
func (client *Client) Do(ctx, cleanupCtx context.Context, id fault.Correlation, request *http.Request, options ...RequestOptionsV1) (*invocation.Receipt[Result], error) {
	if cleanupCtx == nil {
		return nil, failure(ErrInput, "cleanup-context")
	}
	stream, receipt, err := client.open(ctx, id, request, invocation.Finite, options...)
	if stream == nil {
		return receipt, err
	}
	data, readErr := io.ReadAll(stream)
	if int64(len(data)) > client.owner.settings.MaxResponseBytes {
		data = data[:client.owner.settings.MaxResponseBytes]
	}
	stream.op.mu.Lock()
	stream.op.data.body = data
	stream.op.data.retained = true
	stream.op.mu.Unlock()
	stream.op.finishCollect()
	closeErr := stream.Close(cleanupCtx)
	if result, ok := receipt.Result(); ok && result.Final {
		return receipt, result.Err()
	}
	return receipt, errors.Join(readErr, closeErr)
}
func (client *Client) open(ctx context.Context, id fault.Correlation, request *http.Request, shape invocation.Shape, options ...RequestOptionsV1) (*Stream, *invocation.Receipt[Result], error) {
	if client == nil || client.owner == nil || client.access == nil || ctx == nil {
		return nil, nil, failure(ErrInput, "client")
	}
	if err := validateRequest(ctx, request, client.owner.settings); err != nil {
		return nil, nil, err
	}
	option, err := freezeOptions(client, options)
	if err != nil {
		return nil, nil, err
	}
	parent, parentStop := requestParent(ctx, request.Context())
	snapshot := request.Clone(parent)
	if snapshot.Header == nil {
		snapshot.Header = make(http.Header)
	}
	tlsOnly := client.owner.native.Transport.TLSOnly
	if option.TLSOnly != nil {
		tlsOnly = *option.TLSOnly
	}
	if len(option.ExactHeaders) == 0 && !tlsOnly {
		seedPresetCredentials(snapshot, client.owner.native.Preset)
	}
	if len(option.ExactHeaders) > 0 {
		snapshot.Header = make(http.Header)
		for _, entry := range option.ExactHeaders {
			snapshot.Header.Add(entry.Key, entry.Value)
		}
		if err := validateRequest(parent, snapshot, client.owner.settings); err != nil {
			parentStop()
			return nil, nil, err
		}
	}
	value := client.owner.settings
	call, err := invocation.Begin(parent, client.access, invocation.Request{Name: "request", Correlation: id, Shape: shape, Bytes: value.reservation(), EvidenceBytes: value.evidenceBytes(), Admission: invocation.Budget{Limit: value.AdmissionTimeout}}, client.inbox, client.observer)
	if err != nil {
		parentStop()
		return nil, nil, err
	}
	work, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(parent, invocation.Lifetime)
	if err != nil {
		parentStop()
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return nil, call.Receipt(), err
	}
	op := &operation{client: client, call: call, ctx: work, cancel: cancel, parentStop: parentStop, option: option, nativeDone: make(chan struct{}), done: make(chan struct{})}
	op.collectDone = make(chan struct{})
	if shape != invocation.Finite {
		op.finishCollect()
	}
	op.data.proxyMode = []string{"provider", "direct", "proxy"}[option.Proxy]
	ready := make(chan struct{})
	var stream *Stream
	var nativeErr error
	go func() {
		defer close(op.nativeDone)
		stream, nativeErr = op.execute(snapshot)
		op.mu.Lock()
		op.stream = stream
		op.mu.Unlock()
		close(ready)
		if nativeErr != nil {
			op.fail(nativeErr)
			op.stop()
		}
	}()
	context.AfterFunc(work, op.stop)
	select {
	case <-ready:
		if nativeErr != nil {
			op.finishCollect()
			return nil, call.Receipt(), nativeErr
		}
		if err := work.Err(); err != nil {
			op.finishCollect()
			op.stop()
			return nil, call.Receipt(), failure(ErrState, "headers", err, context.Cause(work))
		}
		return stream, call.Receipt(), nil
	case <-work.Done():
		op.finishCollect()
		op.stop()
		select {
		case <-ready:
			if nativeErr != nil {
				return nil, call.Receipt(), nativeErr
			}
		default:
		}
		return nil, call.Receipt(), invocation.ErrWait.New(fault.Context{Provider: ProviderID, Operation: "headers"}, work.Err(), context.Cause(work))
	}
}
func (op *operation) execute(request *http.Request) (*Stream, error) {
	request = request.Clone(op.ctx)
	request = request.WithContext(httptrace.WithClientTrace(request.Context(), &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		op.mu.Lock()
		op.data.writes++
		op.mu.Unlock()
		if info.Err != nil {
			op.fail(failure(ErrTransport, "request-write", info.Err))
		}
	}}))
	expectedInput := request.ContentLength
	if expectedInput == 0 && request.Body != nil && request.Body != http.NoBody {
		expectedInput = -1
	}
	if request.Body != nil && request.Body != http.NoBody {
		request.Body = op.input(request.Body, expectedInput)
	}
	if request.GetBody != nil {
		open := request.GetBody
		request.GetBody = func() (io.ReadCloser, error) {
			op.mu.Lock()
			op.data.replays++
			count := op.data.replays
			op.mu.Unlock()
			if count > op.client.owner.settings.MaxReplays {
				return nil, failure(ErrLimit, "replays")
			}
			if err := op.ctx.Err(); err != nil {
				return nil, err
			}
			reader, err := open()
			if reader == nil || nilLike(reader) {
				return nil, errors.Join(failure(ErrInput, "replay-reader"), err)
			}
			return op.input(reader, expectedInput), err
		}
	}
	native := &http.Client{Transport: exchange{op}, Jar: op.client.owner.native.Jar, CheckRedirect: func(request *http.Request, via []*http.Request) error {
		if !op.option.FollowRedirects {
			return http.ErrUseLastResponse
		}
		if len(via) >= op.client.owner.settings.MaxExchanges {
			return failure(ErrLimit, "redirects")
		}
		if len(via) > 0 && request.Method != via[len(via)-1].Method && (request.Body == nil || request.Body == http.NoBody) {
			request.Header.Del("Content-Length")
		}
		if callback := op.client.owner.native.CheckRedirect; callback != nil {
			history := make([]*http.Request, len(via))
			for index, item := range via {
				history[index] = preview(item)
			}
			return callback(preview(request), history)
		}
		return nil
	}}
	if len(op.option.ExactHeaders) > 0 {
		native.Jar = nil
	}
	response, err := native.Do(request)
	if err != nil {
		return nil, failure(ErrTransport, "request", err, op.ctx.Err(), context.Cause(op.ctx))
	}
	if response == nil || response.Body == nil {
		return nil, failure(ErrTransport, "missing-response")
	}
	wire, ok := response.Body.(*wireBody)
	if !ok {
		return nil, failure(ErrState, "native-body")
	}
	var reader io.Reader = wire
	var closer io.Closer
	if request.Method == "HEAD" || response.StatusCode == 204 || response.StatusCode == 304 {
		err = nil
	} else {
		reader, closer, err = decode(wire, response.Header.Get("Content-Encoding"), op.client.owner.settings.MaxResponseBytes)
	}
	if err != nil {
		return nil, failure(ErrRead, "decode", err)
	}
	return &Stream{op: op, wire: wire, reader: reader, decoder: closer}, nil
}

type exchange struct{ op *operation }

func (exchange exchange) RoundTrip(request *http.Request) (*http.Response, error) {
	op := exchange.op
	op.mu.Lock()
	prior := op.primary
	op.mu.Unlock()
	if prior != nil {
		return nil, prior
	}
	if err := op.ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateRequest(op.ctx, request, op.client.owner.settings); err != nil {
		return nil, err
	}
	op.mu.Lock()
	op.data.exchanges++
	count := op.data.exchanges
	op.mu.Unlock()
	if count > op.client.owner.settings.MaxExchanges {
		return nil, failure(ErrLimit, "exchanges")
	}
	current, err := op.client.owner.checkout(op.ctx, request.URL, op.option)
	if err != nil {
		return nil, err
	}
	if _, err := op.call.Attempt(); err != nil {
		op.clean(op.client.owner.returnBinding(current, true))
		op.recordNotifications(current)
		return nil, err
	}
	exact := op.option.ExactHeaders
	tlsOnly := op.option.TLSOnly
	if len(exact) > 0 {
		exact = append(exact[:0:0], exact...)
		kept := exact[:0]
		indexes := make(map[string]int)
		for _, entry := range exact {
			key := http.CanonicalHeaderKey(entry.Key)
			values := request.Header.Values(key)
			index := indexes[key]
			indexes[key]++
			if index < len(values) {
				entry.Value = values[index]
				kept = append(kept, entry)
			}
		}
		exact = kept
		if len(exact) == 0 {
			value := true
			tlsOnly = &value
			request = request.Clone(request.Context())
			request.Header = make(http.Header)
		}
	}
	response, err := current.native.RoundTrip(request, &transport.Request{HeaderOrder: op.option.HeaderOrder, ExactHeaders: exact, TLSOnly: tlsOnly, DisableClientHints: op.option.DisableClientHints})
	if errors.Is(err, transport.FathomryErrHeaderLimit) {
		err = failure(ErrLimit, "native-headers", err)
	}
	if errors.Is(err, transport.FathomryErrRequestLength) {
		err = failure(ErrIntegrity, "request-framing", err)
	}
	if response == nil {
		cleanup := op.client.owner.returnBinding(current, true)
		op.recordNotifications(current)
		op.clean(cleanup)
		return nil, err
	}
	if response.Body == nil {
		response.Body = http.NoBody
	}
	headers, order, casing := transport.FathomryHeaderMetadata(response.Header)
	response.Header = headers
	expected := response.ContentLength
	if request.Method == "HEAD" || response.StatusCode == 204 || response.StatusCode == 304 {
		expected = 0
	}
	body := &wireBody{raw: response.Body, op: op, binding: current, response: response, expected: expected}
	response.Body = body
	op.mu.Lock()
	op.outputs = append(op.outputs, body)
	op.data.metadata = Metadata{present: true, status: response.StatusCode, protocol: response.Proto, url: request.URL.String(), headers: response.Header.Clone(), length: response.ContentLength, encoding: response.Header.Get("Content-Encoding"), order: order, casing: casing}
	op.mu.Unlock()
	if lengthErr := responseLength(response); lengthErr != nil {
		return nil, lengthErr
	}
	if !headerFits(response.Header, op.client.owner.settings.MaxHeaderBytes) {
		return nil, failure(ErrLimit, "response-headers")
	}
	if expected > op.client.owner.settings.MaxWireBytes {
		return nil, failure(ErrLimit, "response-body")
	}
	if response.StatusCode == 101 {
		return nil, failure(ErrUnsupported, "upgrade")
	}
	if err != nil {
		return nil, err
	}
	return response, err
}
func (op *operation) input(raw io.ReadCloser, expected int64) *inputBody {
	body := &inputBody{raw: raw, op: op, expected: expected}
	op.mu.Lock()
	op.inputs = append(op.inputs, body)
	op.mu.Unlock()
	return body
}
func (op *operation) fail(err error) {
	if err == nil {
		return
	}
	op.mu.Lock()
	op.primary = errors.Join(op.primary, err)
	op.mu.Unlock()
	op.cancel()
}
func (op *operation) clean(err error) {
	if err == nil {
		return
	}
	op.mu.Lock()
	op.cleanup = append(op.cleanup, err)
	op.mu.Unlock()
}
func (op *operation) stop() {
	op.stopOnce.Do(func() {
		op.stopCause = errors.Join(op.ctx.Err(), context.Cause(op.ctx))
		op.cancel()
		go func() {
			op.closeInputs()
			<-op.nativeDone
			<-op.collectDone
			op.closeInputs()
			op.mu.Lock()
			stream := op.stream
			outputs := append([]*wireBody(nil), op.outputs...)
			op.mu.Unlock()
			for _, body := range outputs {
				op.clean(body.Close())
			}
			if stream != nil {
				stream.readMu.Lock()
				if stream.decoder != nil {
					op.clean(stream.decoder.Close())
				}
				stream.readMu.Unlock()
			}
			op.mu.Lock()
			data := op.data
			data.inputComplete = true
			for _, body := range op.inputs {
				if body.readErr != nil || body.expected >= 0 && body.read != body.expected || body.expected < 0 && !body.eof {
					data.inputComplete = false
				}
			}
			primary := op.primary
			if data.complete && !data.inputComplete && primary == nil {
				primary = failure(ErrIntegrity, "request-input", io.ErrUnexpectedEOF)
			}
			if !data.complete && primary == nil && op.stopCause != nil {
				primary = failure(ErrState, "lifetime", op.stopCause)
			}
			if primary != nil {
				data.complete = false
			}
			cleanup := errors.Join(op.cleanup...)
			if cleanup != nil {
				cleanup = failure(ErrCleanup, "close", cleanup)
			}
			op.mu.Unlock()
			op.parentStop()
			op.call.Complete(invocation.Outcome[Result]{Value: Result{data: &data}, Present: data.metadata.present, Primary: primary, Cleanup: cleanup})
			op.finalErr = errors.Join(primary, cleanup)
			close(op.done)
		}()
	})
}
func (op *operation) closeInputs() {
	op.mu.Lock()
	inputs := append([]*inputBody(nil), op.inputs...)
	op.mu.Unlock()
	for _, body := range inputs {
		op.clean(body.Close())
	}
}

func (op *operation) finishCollect() { op.collectOnce.Do(func() { close(op.collectDone) }) }

type inputBody struct {
	emptyReads     int
	expected, read int64
	eof            bool
	raw            io.ReadCloser
	op             *operation
	once           sync.Once
	err            error
	readErr        error
}

func (body *inputBody) Read(data []byte) (int, error) {
	body.op.inputMu.Lock()
	defer body.op.inputMu.Unlock()
	if len(data) == 0 {
		return 0, nil
	}
	if body.readErr != nil {
		return 0, body.readErr
	}
	if err := body.op.ctx.Err(); err != nil {
		return 0, errors.Join(err, context.Cause(body.op.ctx))
	}
	body.op.mu.Lock()
	remaining := body.op.client.owner.settings.MaxRequestBytes - body.op.data.input
	body.op.mu.Unlock()
	if remaining < 0 {
		return 0, failure(ErrLimit, "request-body")
	}
	if int64(len(data)) > remaining+1 {
		data = data[:remaining+1]
	}
	n, err := body.raw.Read(data)
	if n < 0 || n > len(data) {
		n = 0
		err = errors.Join(failure(ErrInput, "reader-count"), err)
	}
	if n == 0 && err == nil {
		body.emptyReads++
		if body.emptyReads >= 100 {
			err = io.ErrNoProgress
		}
	} else {
		body.emptyReads = 0
	}
	body.read += int64(n)
	if err == io.EOF {
		body.eof = true
	}
	body.op.mu.Lock()
	body.op.data.input += int64(n)
	body.op.mu.Unlock()
	if int64(n) > remaining {
		err = errors.Join(failure(ErrLimit, "request-body"), err)
	}
	if err != nil && err != io.EOF {
		body.readErr = err
		body.op.fail(failure(ErrRead, "request-body", err))
	}
	return n, err
}
func (body *inputBody) Close() error {
	body.once.Do(func() { body.err = body.raw.Close() })
	return body.err
}

func requestParent(parent, request context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	done := make(chan struct{})
	stop := context.AfterFunc(request, func() {
		defer close(done)
		cancel(context.Cause(request))
	})
	return ctx, func() {
		if !stop() {
			<-done
		}
		cancel(context.Canceled)
	}
}
