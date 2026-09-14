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

package trino

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql/driver"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/google/uuid"
	native "github.com/trinodb/trino-go-client/trino"
)

type exchange struct {
	settings    settings
	collect     bool
	read        bool
	call        *invocation.Call[Result]
	transport   *http.Transport
	mu          sync.Mutex
	data        resultData
	next        string
	primary     error
	cleanup     error
	resultBytes int
	seenRows    int
	signatures  []signature
	stopped     bool
	dials       sync.WaitGroup
	sockets     map[*ownedSocket]struct{}
}
type requestContextKey struct{}
type ownedSocket struct {
	net.Conn
	owner *exchange
	once  sync.Once
	err   error
}

func (s *ownedSocket) Close() error {
	s.once.Do(func() {
		s.err = s.Conn.Close()
		s.owner.mu.Lock()
		if s.owner.cleanup == nil {
			s.owner.cleanup = s.err
		}
		delete(s.owner.sockets, s)
		s.owner.mu.Unlock()
	})
	return s.err
}
func newExchange(s settings, collect, read bool, call *invocation.Call[Result]) *exchange {
	e := &exchange{settings: s, collect: collect, read: read, call: call, data: resultData{json: []byte("[")},
		resultBytes: 2, sockets: make(map[*ownedSocket]struct{})}
	trust, _ := roots(s.RootCAPEM)
	e.transport = &http.Transport{Proxy: nil, DisableKeepAlives: true, DisableCompression: true,
		MaxConnsPerHost: 1, MaxResponseHeaderBytes: 32 << 10, ResponseHeaderTimeout: s.Timeout,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: trust},
		DialContext:     e.dialPlain, DialTLSContext: e.dialTLS}
	return e
}
func (e *exchange) dialPlain(ctx context.Context, network, address string) (net.Conn, error) {
	return e.dial(ctx, network, address, false)
}
func (e *exchange) dialTLS(ctx context.Context, network, address string) (net.Conn, error) {
	return e.dial(ctx, network, address, true)
}
func (e *exchange) dial(ctx context.Context, network, address string, secure bool) (net.Conn, error) {
	original, _ := ctx.Value(requestContextKey{}).(context.Context)
	if original == nil {
		return nil, failure(ErrAuthority, "unowned-dial")
	}
	e.mu.Lock()
	if e.stopped {
		e.mu.Unlock()
		return nil, failure(ErrAuthority, "closed")
	}
	e.dials.Add(1)
	e.mu.Unlock()
	defer e.dials.Done()
	raw, err := (&net.Dialer{Timeout: e.settings.Timeout}).DialContext(original, network, address)
	if err != nil {
		return nil, err
	}
	socket := &ownedSocket{Conn: raw, owner: e}
	e.mu.Lock()
	e.sockets[socket] = struct{}{}
	stopped := e.stopped
	e.mu.Unlock()
	if stopped || original.Err() != nil {
		return nil, errors.Join(failure(ErrAuthority, "closed"), socket.Close())
	}
	if secure {
		config := e.transport.TLSClientConfig.Clone()
		config.ServerName, _, err = net.SplitHostPort(address)
		if err != nil {
			return nil, errors.Join(err, socket.Close())
		}
		secured := tls.Client(socket, config)
		if err = secured.HandshakeContext(original); err != nil {
			e.noteCleanup(secured.Close())
			if header, ok := err.(tls.RecordHeaderError); ok {
				header.Conn = nil
				err = header
			}
			return nil, err
		}
		return secured, nil
	}
	return socket, nil
}
func (e *exchange) open() (driver.Conn, error) {
	key := "fathomry-trino-" + uuid.NewString()
	if err := native.RegisterCustomClient(key, &http.Client{Transport: e,
		CheckRedirect: func(*http.Request, []*http.Request) error { return failure(ErrAuthority, "redirect") }}); err != nil {
		return nil, err
	}
	defer native.DeregisterCustomClient(key)
	u, _ := url.Parse(e.settings.Endpoint)
	u.User = url.User(e.settings.User)
	config := native.Config{ServerURI: u.String(), Source: "fathomry", Catalog: e.settings.Catalog, Schema: e.settings.Schema,
		CustomClientName: key, DisableExplicitPrepare: true, SessionProperties: map[string]string{"retry_policy": "NONE"}}
	dsn, err := config.FormatDSN()
	if err != nil {
		return nil, err
	}
	return (&native.Driver{}).Open(dsn)
}
func (e *exchange) noteCleanup(err error) {
	if err == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cleanup == nil {
		e.cleanup = err
	}
}
func (e *exchange) allowedURI(text string) bool {
	if len(text) > 2048 {
		return false
	}
	u, err := url.Parse(text)
	base, _ := url.Parse(e.settings.Endpoint)
	valid := err == nil && u != nil && u.Scheme == base.Scheme && u.Host == base.Host && u.User == nil &&
		u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && u.RawPath == "" && u.Opaque == "" &&
		u.String() == text && strings.HasPrefix(u.Path, "/v1/statement/") &&
		!strings.Contains(u.Path, "..") && !strings.Contains(u.Path, "//") && !strings.ContainsAny(u.Path, "%\\")
	if !valid {
		return false
	}
	parts := strings.Split(u.Path, "/")
	if len(parts) != 7 || parts[3] != "queued" && parts[3] != "executing" ||
		parts[4] != e.data.queryID || !queryID(parts[4]) || !queryID(parts[5]) {
		return false
	}
	token, err := strconv.ParseUint(parts[6], 10, 63)
	return err == nil && strconv.FormatUint(token, 10) == parts[6]
}
func (e *exchange) RoundTrip(request *http.Request) (*http.Response, error) {
	// Native cancellation uses an unrelated global timeout; only finish owns
	// authorized remote cancellation with the caller's cleanup context.
	if request.Method == http.MethodDelete {
		return nil, failure(ErrAuthority, "implicit-cancel")
	}
	e.mu.Lock()
	if e.stopped || request.Context().Err() != nil {
		e.mu.Unlock()
		return nil, failure(ErrOperation, "request-context", request.Context().Err())
	}
	address := request.URL.String()
	valid := request.Method == http.MethodPost && address == e.settings.Endpoint+"/v1/statement" && e.data.posts == 0 ||
		request.Method == http.MethodGet && e.next != "" && address == e.next && e.allowedURI(address)
	if !valid {
		e.mu.Unlock()
		return nil, failure(ErrAuthority, "routing")
	}
	if e.data.pages >= uint64(e.settings.MaxPages) || e.data.wireBytes >= e.settings.MaxWireBytes {
		e.mu.Unlock()
		return nil, failure(ErrLimit, "pages")
	}
	e.mu.Unlock()
	copy := request.Clone(request.Context())
	copy.Header.Del("X-Trino-Query-Data-Encoding")
	copy.Header.Del("X-Trino-Prepared-Statement")
	copy.GetBody = nil
	if copy.Method == http.MethodPost && (copy.ContentLength < 0 || copy.ContentLength > int64(e.settings.MaxSQLBytes)) {
		return nil, failure(ErrLimit, "sql")
	}
	if e.call != nil {
		if _, err := e.call.Attempt(); err != nil {
			return nil, err
		}
	}
	e.mu.Lock()
	if copy.Method == http.MethodPost {
		e.data.posts++
	}
	e.mu.Unlock()
	response, body, err := e.perform(copy, false)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data.pages++
	if response.StatusCode != http.StatusOK {
		e.primary = failure(ErrOperation, "http-status", &native.ErrQueryFailed{StatusCode: response.StatusCode})
		return nil, e.primary
	}
	kind, _, parseErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if parseErr != nil || kind != "application/json" || response.Header.Get("Content-Encoding") != "" {
		return nil, failure(ErrProtocol, "content-type")
	}
	if err = e.page(body); err != nil {
		e.primary = err
		return nil, err
	}
	for key := range response.Header {
		name := strings.ToLower(key)
		if strings.HasPrefix(name, "x-trino-set-") || strings.HasPrefix(name, "x-trino-clear-") ||
			strings.HasPrefix(name, "x-trino-added-") || strings.HasPrefix(name, "x-trino-deallocated-") ||
			name == "x-trino-started-transaction-id" {
			e.primary = failure(ErrUnsupported, "session-response")
			return nil, e.primary
		}
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response, nil
}
func (e *exchange) perform(request *http.Request, cleanup bool) (*http.Response, []byte, error) {
	request = request.Clone(context.WithValue(request.Context(), requestContextKey{}, request.Context()))
	request.Header.Set("X-Trino-User", e.settings.User)
	if e.settings.Password != "" {
		request.SetBasicAuth(e.settings.User, e.settings.Password)
	}
	if e.settings.BearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+e.settings.BearerToken)
	}
	request.Header.Del("Accept-Encoding")
	response, err := e.transport.RoundTrip(request)
	if err != nil {
		return nil, nil, err
	}
	limit := int64(e.settings.MaxPageBytes)
	e.mu.Lock()
	if !cleanup && e.settings.MaxWireBytes-e.data.wireBytes < limit {
		limit = e.settings.MaxWireBytes - e.data.wireBytes
	}
	e.mu.Unlock()
	if response.ContentLength > limit {
		e.noteCleanup(response.Body.Close())
		return nil, nil, failure(ErrLimit, "response")
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, limit+1))
	e.noteCleanup(response.Body.Close())
	e.mu.Lock()
	e.data.wireBytes += int64(len(body))
	e.mu.Unlock()
	if readErr != nil {
		return nil, nil, readErr
	}
	if int64(len(body)) > limit {
		return nil, nil, failure(ErrLimit, "response")
	}
	return response, body, nil
}
func (e *exchange) cancellationTarget() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.data.terminal {
		return ""
	}
	target := e.next
	if target == "" && e.data.queryID != "" {
		target = e.settings.Endpoint + "/v1/query/" + e.data.queryID
	}
	return target
}

func (e *exchange) finish(cleanup context.Context) {
	if target := e.cancellationTarget(); target != "" {
		ctx, cancel, err := (invocation.Budget{Limit: e.settings.CleanupTimeout}).Context(cleanup, invocation.Cleanup)
		if err == nil {
			request, _ := http.NewRequestWithContext(ctx, http.MethodDelete, target, nil)
			e.mu.Lock()
			e.data.cancelAttempted = true
			e.mu.Unlock()
			if e.call != nil {
				_, err = e.call.Attempt()
			}
			if err == nil {
				var response *http.Response
				response, _, err = e.perform(request, true)
				if err == nil {
					if response.StatusCode == http.StatusNoContent {
						e.mu.Lock()
						e.data.cancelAcknowledged = true
						e.mu.Unlock()
					} else {
						err = failure(ErrCleanup, "cancel-status", &native.ErrQueryFailed{StatusCode: response.StatusCode})
					}
				}
			}
			cancel()
		}
		e.noteCleanup(err)
	}
	e.closeLocal()
}

func (e *exchange) closeLocal() {
	e.mu.Lock()
	e.stopped = true
	sockets := make([]*ownedSocket, 0, len(e.sockets))
	for socket := range e.sockets {
		sockets = append(sockets, socket)
	}
	e.mu.Unlock()
	e.transport.CloseIdleConnections()
	for _, socket := range sockets {
		e.noteCleanup(socket.Close())
	}
	e.dials.Wait()
}
func (e *exchange) result() *resultData {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data.json = append(e.data.json, ']')
	switch {
	case e.data.posts == 0:
		e.data.effect = NotSubmitted
	case e.read:
		e.data.effect = ReadOnly
	case e.data.success:
		e.data.effect = Acknowledged
	default:
		e.data.effect = Unknown
	}
	return &e.data
}
