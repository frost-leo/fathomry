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

package doris

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"

	"github.com/frost-leo/fathomry/internal/invocation"
)

func isClosed(err error) bool { return errors.Is(err, net.ErrClosed) }

type httpSession struct {
	transport   *http.Transport
	cancel      context.CancelFunc
	mu          sync.Mutex
	sealed      bool
	dials       sync.WaitGroup
	connections []*httpConn
	cleanup     []error
}

type httpConn struct {
	net.Conn
	once sync.Once
	err  error
}

func (conn *httpConn) Close() error {
	conn.once.Do(func() { conn.err = conn.Conn.Close() })
	return conn.err
}

func newHTTPSession(ctx context.Context, s settings) (*httpSession, error) {
	transport, err := s.transport()
	if err != nil {
		return nil, err
	}
	work, cancel := context.WithCancel(ctx)
	session := &httpSession{transport: transport, cancel: cancel}
	transport.DialContext = func(_ context.Context, network, address string) (net.Conn, error) {
		session.mu.Lock()
		if session.sealed {
			session.mu.Unlock()
			return nil, context.Canceled
		}
		session.dials.Add(1)
		session.mu.Unlock()
		defer session.dials.Done()
		raw, err := s.dialer().DialContext(nativeContext{work}, network, address)
		if err != nil {
			return nil, err
		}
		conn := &httpConn{Conn: raw}
		session.mu.Lock()
		defer session.mu.Unlock()
		if session.sealed {
			session.cleanup = append(session.cleanup, conn.Close())
			return nil, context.Canceled
		}
		session.connections = append(session.connections, conn)
		return conn, nil
	}
	return session, nil
}
func (session *httpSession) close() error {
	session.mu.Lock()
	session.sealed = true
	session.cancel()
	session.mu.Unlock()
	session.transport.CloseIdleConnections()
	session.dials.Wait()
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, conn := range session.connections {
		// Transport can already have closed this socket; net.ErrClosed is not a
		// second cleanup failure. This closes any still-active transport I/O.
		err := conn.Close()
		if err != nil && !isClosed(err) {
			session.cleanup = append(session.cleanup, err)
		}
	}
	return joined(ErrCleanup, "http", session.cleanup...)
}

func (c *Client) exchange(ctx context.Context, call *invocation.Call[Result], method, path, query string, headers http.Header, payload []byte, data *resultData) (body []byte, primary, cleanup error) {
	s := c.owner.settings
	session, err := newHTTPSession(ctx, s)
	if err != nil {
		return nil, err, nil
	}
	defer func() { cleanup = joined(ErrCleanup, "http", cleanup, session.close()) }()
	target, err := url.Parse(s.HTTPOrigins[0] + path)
	if err != nil {
		return nil, failure(ErrInput, "url"), nil
	}
	target.RawQuery = query
	for hop := 0; hop < 3; hop++ {
		if err := ctx.Err(); err != nil {
			return nil, failure(ErrTransport, "http", err, context.Cause(ctx)), nil
		}
		request, err := http.NewRequestWithContext(nativeContext{ctx}, method, target.String(), bytes.NewReader(payload))
		if err != nil {
			return nil, failure(ErrInput, "request"), nil
		}
		request.GetBody = nil
		request.Header = headers.Clone()
		request.SetBasicAuth(s.User, s.Password)
		_, _ = call.Attempt()
		data.dispatched = true
		if data.loadPresent {
			data.load.State = LoadUnknown
		}
		response, err := session.transport.RoundTrip(request)
		if err != nil {
			return nil, failure(ErrTransport, "http", err, ctx.Err(), context.Cause(ctx)), nil
		}
		if data.loadPresent {
			data.load.HTTPStatus = response.StatusCode
		}
		if response.StatusCode == http.StatusTemporaryRedirect || response.StatusCode == http.StatusPermanentRedirect {
			location, locationErr := response.Location()
			cleanup = joined(ErrCleanup, "body", cleanup, response.Body.Close())
			if locationErr != nil || hop == 2 || !s.redirectAllowed(target, location, path, query) {
				return nil, failure(ErrUnsupported, "redirect"), cleanup
			}
			target = location
			// Doris echoes the original Basic credentials in redirect userinfo.
			// Validate them but never carry credentials into request URLs/errors.
			target.User = nil
			continue
		}
		body, err = io.ReadAll(io.LimitReader(response.Body, int64(s.MaxHTTPResponseBytes)+1))
		cleanup = joined(ErrCleanup, "body", cleanup, response.Body.Close())
		if len(body) > s.MaxHTTPResponseBytes {
			return nil, failure(ErrLimit, "http-response"), cleanup
		}
		if err != nil {
			return nil, failure(ErrTransport, "http-response", err, ctx.Err(), context.Cause(ctx)), cleanup
		}
		if response.StatusCode != http.StatusOK {
			return nil, failure(ErrTransport, "http-status"), cleanup
		}
		return body, nil, cleanup
	}
	return nil, failure(ErrUnsupported, "redirect"), cleanup
}
func (s settings) redirectAllowed(previous, next *url.URL, path, query string) bool {
	if next == nil || next.Opaque != "" || next.Fragment != "" ||
		next.Path != path || next.RawPath != "" || next.RawQuery != query || next.ForceQuery ||
		previous.Scheme == "https" && next.Scheme != "https" {
		return false
	}
	if next.User != nil {
		password, present := next.User.Password()
		if !present || next.User.Username() != s.User || password != s.Password {
			return false
		}
	}
	value := next.Scheme + "://" + next.Host
	if _, err := origin(value, s.Plaintext); err != nil {
		return false
	}
	for _, allowed := range s.HTTPOrigins {
		if value == allowed {
			return true
		}
	}
	return false
}
