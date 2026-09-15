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

package nethttp

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

type operationKey struct{}
type operation struct {
	proxy            proxyChoice
	client           *Client
	call             *invocation.Call[Result]
	ctx              context.Context
	cancel           context.CancelFunc
	callbacks        *activity
	mu               sync.Mutex
	primary          error
	extraCleanup     error
	data             *resultData
	requests         []*requestBody
	responses        []*responseBody
	sockets          []*socket
	bindings         []*transportBinding
	finalResponse    *http.Response
	sent             int64
	received         int64
	requestReserved  int64
	responseReserved int64
	reads            *activity
	exchanges        int
	closing          bool
	once             sync.Once
	done             chan struct{}
	onDone           func()
}

// Open returns a controlled response stream and its terminal receipt. The caller
// MUST Close the stream, including after EOF; header arrival is not completion.
// ctx replaces Request.Context for native control and values. Request metadata
// is copied; Body/GetBody are borrowed until receipt release and closed as native
// request bodies. They must support concurrent Close interrupting Read.
// A nil receipt means rejection before admission. After admission an error may
// accompany a non-nil receipt, which is also retained independently by Inbox.
// Optional RequestOptionsV1 binds this logical call's route without mutating the
// Provider; omission uses its configured routing behavior.
func (client *Client) Open(ctx context.Context, id fault.Correlation, request *http.Request, options ...RequestOptionsV1) (*Stream, *invocation.Receipt[Result], error) {
	return client.open(ctx, id, request, invocation.Stream, nil, nil, options...)
}

func (client *Client) open(ctx context.Context, id fault.Correlation, request *http.Request, shape invocation.Shape, parent *operation, transport http.RoundTripper, options ...RequestOptionsV1) (*Stream, *invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, nil, err
	}
	if err := validateRequest(ctx, request, client.owner.settings); err != nil {
		return nil, nil, err
	}
	proxy, err := requestOptions(options, client.owner.settings.RoutingLocked)
	if err != nil {
		return nil, nil, err
	}
	op, err := client.begin(ctx, id, shape, parent, proxy)
	if err != nil {
		if op == nil {
			return nil, nil, err
		}
		return nil, op.call.Receipt(), err
	}
	if transport == nil {
		transport = client.owner.transport
	}
	return op.open(request, transport)
}

func (op *operation) open(original *http.Request, transport http.RoundTripper) (*Stream, *invocation.Receipt[Result], error) {
	snapshot := copyRequest(original, op.ctx)
	ready := make(chan struct{})
	var deliveryMu sync.Mutex
	var stream *Stream
	var nativeErr error
	available, abandoned := false, false
	if !op.callbacks.enter() {
		return nil, op.call.Receipt(), failure(ErrState, "native-entry")
	}
	go func() {
		defer op.callbacks.leave()
		result, _, err := op.nativeOpen(snapshot, transport)
		deliveryMu.Lock()
		if abandoned {
			deliveryMu.Unlock()
			if result != nil {
				result.reads.stop()
			}
			op.finish()
			return
		}
		stream, nativeErr, available = result, err, true
		close(ready)
		deliveryMu.Unlock()
		if err != nil {
			op.finish()
		}
	}()
	select {
	case <-ready:
	case <-op.ctx.Done():
		deliveryMu.Lock()
		if !available {
			abandoned = true
			deliveryMu.Unlock()
			op.finish()
			return nil, op.call.Receipt(), invocation.ErrWait.New(fault.Context{Provider: ProviderID, Operation: "headers"}, op.ctx.Err(), context.Cause(op.ctx))
		}
		deliveryMu.Unlock()
	}
	deliveryMu.Lock()
	defer deliveryMu.Unlock()
	return stream, op.call.Receipt(), nativeErr
}

func (op *operation) nativeOpen(original *http.Request, transport http.RoundTripper) (*Stream, *invocation.Receipt[Result], error) {
	work := httptrace.WithClientTrace(op.ctx, &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { claimConnection(info.Conn) }})
	request := copyRequest(original, work)
	if original.Body != nil && original.Body != http.NoBody {
		request.Body = op.wrapRequestBody(original.Body)
	}
	if original.GetBody != nil {
		getBody := original.GetBody
		request.GetBody = func() (io.ReadCloser, error) {
			if !op.callbacks.enter() {
				return nil, failure(ErrState, "replay")
			}
			defer op.callbacks.leave()
			if err := op.ctx.Err(); err != nil {
				return nil, err
			}
			op.mu.Lock()
			capacity := len(op.requests) < op.client.owner.settings.MaxExchanges+1
			op.mu.Unlock()
			if !capacity {
				return nil, failure(ErrLimit, "replay")
			}
			body, err := getBody()
			if typedNil(body) || body == nil && err == nil {
				return nil, failure(ErrInput, "replay-body", err)
			}
			if body != nil {
				body = op.wrapRequestBody(body)
			}
			return body, err
		}
	}
	native := &http.Client{Transport: exchange{op: op, native: transport}, CheckRedirect: op.redirect}
	if op.client.owner.native.Jar != nil {
		native.Jar = operationJar{op: op}
	}
	response, err := native.Do(request)
	if err == nil {
		op.mu.Lock()
		err = op.primary
		op.mu.Unlock()
	}
	if err != nil {
		err = failure(ErrTransport, "request", err, op.ctx.Err(), context.Cause(op.ctx))
		op.fail(err)
		return nil, op.call.Receipt(), err
	}
	op.mu.Lock()
	op.finalResponse = response
	op.mu.Unlock()
	reads := newActivity()
	op.reads = reads
	return &Stream{op: op, response: response, reads: reads}, op.call.Receipt(), nil
}

// Do retains a bounded body and closes its native lifetime using cleanupCtx,
// independently of request cancellation. HTTP status codes remain result data.
// A returned receipt is always required evidence, including alongside an error.
func (client *Client) Do(ctx, cleanupCtx context.Context, id fault.Correlation, request *http.Request, options ...RequestOptionsV1) (*invocation.Receipt[Result], error) {
	if cleanupCtx == nil {
		return nil, failure(ErrInput, "cleanup-context")
	}
	stream, receipt, err := client.open(ctx, id, request, invocation.Finite, nil, nil, options...)
	if stream == nil {
		return receipt, err
	}
	return consume(stream, cleanupCtx, receipt)
}

func consume(stream *Stream, cleanupCtx context.Context, receipt *invocation.Receipt[Result]) (*invocation.Receipt[Result], error) {
	buffer := make([]byte, 32<<10)
	var body []byte
	var primary error
	for {
		count, err := stream.Read(buffer)
		if required := len(body) + count; required > cap(body) {
			capacity := min(int(stream.op.client.owner.settings.MaxResponseBytes), max(required, 2*cap(body)))
			grown := make([]byte, len(body), capacity)
			copy(grown, body)
			body = grown
		}
		body = append(body, buffer[:count]...)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				primary = err
			}
			break
		}
	}
	stream.op.mu.Lock()
	stream.op.data.body, stream.op.data.retained = body, true
	stream.op.mu.Unlock()
	cleanup := stream.Close(cleanupCtx)
	if result, ok := receipt.Result(); ok && result.Final {
		return receipt, errors.Join(result.Err(), cleanup)
	}
	return receipt, errors.Join(primary, cleanup)
}

func validateRequest(ctx context.Context, request *http.Request, value settings) error {
	if ctx == nil || request == nil || request.URL == nil || request.RequestURI != "" || typedNil(request.Body) ||
		request.ContentLength < -1 || request.ContentLength > value.MaxRequestBytes ||
		request.Body == nil && request.ContentLength > 0 ||
		len(request.TransferEncoding) > 2 ||
		len(request.Host) > 8192 || strings.ContainsAny(request.Host, "\r\n\x00") {
		return failure(ErrInput, "request")
	}
	metadataBytes := int64(len(request.Method)) + int64(len(request.Host))
	for _, encoding := range request.TransferEncoding {
		metadataBytes += int64(len(encoding))
	}
	if metadataBytes > value.MaxHeaderBytes {
		return failure(ErrLimit, "request-metadata")
	}
	if request.Method != "" && !token(request.Method) {
		return failure(ErrInput, "method")
	}
	if httptrace.ContextClientTrace(ctx) != nil {
		return failure(ErrUnsupported, "native-trace")
	}
	if request.URL.Scheme != "http" && request.URL.Scheme != "https" || request.URL.Hostname() == "" ||
		request.URL.Scheme == "http" && !value.HTTP1 && !value.UnencryptedHTTP2 ||
		request.URL.Scheme == "https" && !value.HTTP1 && !value.HTTP2 {
		return failure(ErrUnsupported, "scheme")
	}
	if !urlFits(request.URL, value.MaxHeaderBytes) {
		return failure(ErrLimit, "url")
	}
	if !headerFits(request.Header, value.MaxHeaderBytes) || !headerFits(request.Trailer, value.MaxHeaderBytes) {
		return failure(ErrLimit, "headers")
	}
	if request.Cancel != nil {
		select {
		case <-request.Cancel:
			return failure(ErrState, "request", context.Canceled)
		default:
		}
	}
	return nil
}

func urlFits(address *url.URL, maximum int64) bool {
	if address == nil {
		return false
	}
	total := int64(0)
	for _, part := range []string{address.Scheme, address.Opaque, address.Host, address.Path, address.RawPath, address.RawQuery, address.Fragment, address.RawFragment} {
		if int64(len(part)) > maximum-total {
			return false
		}
		total += int64(len(part))
	}
	if address.User != nil {
		password, _ := address.User.Password()
		for _, part := range []string{address.User.Username(), password} {
			if int64(len(part)) > maximum-total {
				return false
			}
			total += int64(len(part))
		}
	}
	return int64(len(address.String())) <= maximum
}
func token(value string) bool {
	if value == "" {
		return false
	}
	for index := range len(value) {
		char := value[index]
		if char <= 32 || char >= 127 || strings.ContainsRune("()<>@,;:\\\"/[]?={}", rune(char)) {
			return false
		}
	}
	return true
}
func headerFits(header http.Header, maximum int64) bool {
	total := int64(0)
	for key, values := range header {
		if !token(key) {
			return false
		}
		total += int64(len(key)) + 64
		for _, value := range values {
			total += int64(len(value)) + 16
			if total > maximum {
				return false
			}
			for index := range len(value) {
				if value[index] < 32 && value[index] != '\t' || value[index] == 127 {
					return false
				}
			}
		}
		if total > maximum {
			return false
		}
	}
	return true
}
func copyRequest(original *http.Request, ctx context.Context) *http.Request {
	request := original.WithContext(ctx)
	address := *original.URL
	request.URL = &address
	request.Header = original.Header.Clone()
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	request.Trailer = original.Trailer.Clone()
	request.TransferEncoding = slices.Clone(original.TransferEncoding)
	request.Form, request.PostForm, request.MultipartForm = nil, nil, nil
	request.Response, request.TLS = nil, nil
	return request
}
func previewRequest(original *http.Request) *http.Request {
	request := copyRequest(original, original.Context())
	request.Body, request.GetBody, request.Cancel = nil, nil, nil
	return request
}

type exchange struct {
	op     *operation
	native http.RoundTripper
}

func (value exchange) RoundTrip(request *http.Request) (*http.Response, error) {
	op := value.op
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	op.mu.Lock()
	prior := op.primary
	if op.exchanges >= op.client.owner.settings.MaxExchanges {
		prior = failure(ErrLimit, "exchanges")
	}
	if prior == nil {
		op.exchanges++
	}
	op.mu.Unlock()
	if prior != nil {
		return nil, prior
	}
	// The context now carries our private trace, not a consumer's native callback.
	if !headerFits(request.Header, op.client.owner.settings.MaxHeaderBytes) || !headerFits(request.Trailer, op.client.owner.settings.MaxHeaderBytes) {
		return nil, failure(ErrLimit, "headers")
	}
	if _, err := op.call.Attempt(); err != nil {
		return nil, err
	}
	native := value.native
	if transport, ok := native.(*http.Transport); ok && transport == op.client.owner.transport {
		binding, err := op.client.owner.bindTransport(request)
		if err != nil {
			return nil, err
		}
		op.mu.Lock()
		op.bindings = append(op.bindings, binding)
		op.mu.Unlock()
		request = request.WithContext(context.WithValue(request.Context(), bindingKey{}, binding))
		native = binding.transport
	}
	response, err := native.RoundTrip(request)
	if response != nil {
		if response.Body == nil {
			response.Body = http.NoBody
		}
		tracked := &responseBody{raw: response.Body, op: op, reads: newActivity(), done: make(chan struct{})}
		response.Body = tracked
		op.mu.Lock()
		op.responses = append(op.responses, tracked)
		op.mu.Unlock()
		if !headerFits(response.Header, op.client.owner.settings.MaxHeaderBytes) {
			_ = tracked.Close()
			return nil, failure(ErrLimit, "response-headers")
		}
		op.mu.Lock()
		op.data.metadata = Metadata{present: true, status: response.StatusCode, protocol: response.Proto, url: request.URL.String(),
			headers: response.Header.Clone(), length: response.ContentLength, uncompressed: response.Uncompressed}
		op.mu.Unlock()
		if response.StatusCode == http.StatusSwitchingProtocols {
			_ = tracked.Close()
			return nil, failure(ErrUnsupported, "upgrade")
		}
	}
	return response, err
}

func (op *operation) redirect(request *http.Request, previous []*http.Request) error {
	if len(previous) >= op.client.owner.settings.MaxExchanges {
		return failure(ErrLimit, "redirect")
	}
	callback := op.client.owner.native.CheckRedirect
	if callback == nil {
		return nil
	}
	if !op.callbacks.enter() {
		return failure(ErrState, "redirect")
	}
	defer op.callbacks.leave()
	preview := previewRequest(request)
	history := make([]*http.Request, len(previous))
	for index, prior := range previous {
		history[index] = previewRequest(prior)
	}
	err := callback(preview, history)
	if err != nil {
		return err
	}
	if preview.Body != nil || preview.GetBody != nil || preview.URL == nil {
		return failure(ErrUnsupported, "redirect-body")
	}
	check := *preview
	check.Body = request.Body
	if err := validateRequest(op.ctx, &check, op.client.owner.settings); err != nil {
		return err
	}
	address := *preview.URL
	request.URL = &address
	request.Header = preview.Header.Clone()
	request.Method, request.Host = preview.Method, preview.Host
	return nil
}

type operationJar struct{ op *operation }

func (jar operationJar) Cookies(address *url.URL) []*http.Cookie {
	op := jar.op
	if !op.callbacks.enter() {
		return nil
	}
	defer op.callbacks.leave()
	copyURL := *address
	cookies := op.client.owner.native.Jar.Cookies(&copyURL)
	result, ok := copyCookies(cookies, op.client.owner.settings.MaxHeaderBytes)
	if !ok {
		op.fail(failure(ErrLimit, "cookies"))
		return nil
	}
	return result
}
func (jar operationJar) SetCookies(address *url.URL, cookies []*http.Cookie) {
	op := jar.op
	if !op.callbacks.enter() {
		return
	}
	defer op.callbacks.leave()
	copied, ok := copyCookies(cookies, op.client.owner.settings.MaxHeaderBytes)
	if !ok {
		op.fail(failure(ErrLimit, "cookies"))
		return
	}
	copyURL := *address
	op.client.owner.native.Jar.SetCookies(&copyURL, copied)
}
func copyCookies(cookies []*http.Cookie, maximum int64) ([]*http.Cookie, bool) {
	if int64(len(cookies))*128 > maximum {
		return nil, false
	}
	total := int64(0)
	for _, cookie := range cookies {
		if cookie == nil || cookie.Valid() != nil || len(cookie.Unparsed) > 64 {
			return nil, false
		}
		total += int64(len(cookie.Name)+len(cookie.Value)+len(cookie.Domain)+len(cookie.Path)+len(cookie.Raw)+len(cookie.RawExpires)) + 128
		for _, field := range cookie.Unparsed {
			total += int64(len(field)) + 16
		}
		if total > maximum {
			return nil, false
		}
	}
	result := make([]*http.Cookie, len(cookies))
	for index, cookie := range cookies {
		value := *cookie
		value.Unparsed = slices.Clone(cookie.Unparsed)
		result[index] = &value
	}
	return result, true
}

func (op *operation) fail(err error) {
	if err == nil {
		return
	}
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.primary == nil {
		op.primary = err
	}
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
			var cleanup []error
			if op.extraCleanup != nil {
				cleanup = append(cleanup, op.extraCleanup)
			}
			for _, body := range op.requests {
				if body.err != nil {
					cleanup = append(cleanup, body.err)
				}
			}
			for _, body := range op.responses {
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
			if response != nil && data.complete {
				if headerFits(response.Trailer, op.client.owner.settings.MaxHeaderBytes) {
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
			for _, binding := range op.bindings {
				op.client.owner.returnTransport(binding)
			}
			op.call.Complete(invocation.Outcome[Result]{Present: data.metadata.present || data.connected,
				Value: Result{data: &data}, Primary: primary, Cleanup: cleanupErr})
			if op.onDone != nil {
				op.onDone()
			}
			close(op.done)
		}()
	})
}
func (op *operation) closeBodies() {
	op.mu.Lock()
	requests := slices.Clone(op.requests)
	responses := slices.Clone(op.responses)
	op.mu.Unlock()
	for _, body := range requests {
		_ = body.Close()
	}
	for _, body := range responses {
		_ = body.Close()
	}
}
func (op *operation) wait(ctx context.Context) error {
	select {
	case <-op.done:
		result, _ := op.call.Receipt().Result()
		return result.Outcome.Cleanup
	default:
	}
	select {
	case <-op.done:
		result, _ := op.call.Receipt().Result()
		return result.Outcome.Cleanup
	case <-ctx.Done():
		return invocation.ErrWait.New(fault.Context{Provider: ProviderID, Operation: "close"}, ctx.Err(), context.Cause(ctx))
	}
}
