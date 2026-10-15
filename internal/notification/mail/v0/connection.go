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

package mail

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	smtp "github.com/wneessen/go-mail/smtp"
)

// Source is an opaque composition capability, not an operation or close handle.
type Source struct {
	private
	owner *owner
}
type owner struct {
	settings settings
	hooks    NativeOptions
	mu       sync.Mutex
	idle     []*connection
	closed   bool
	closeErr error
}
type connection struct {
	raw              *limitedConn
	native           *smtp.Client
	info             SessionInfo
	responses        []error
	connectionErrors []error
}

// Select freezes configuration and strict overlays without I/O. Connections are
// lazily opened only under the call's admission/evidence/timeout authority.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	prepared, err := PrepareV1(options, layers...)
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return prepared.Select(), nil
}

// Client is a concurrent non-owning facade. Copies and resource.Borrow aliases
// share the authoritative source, connection capacity and shutdown.
type Client struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Bind attaches the required independently owned inbox and optional diagnostics.
// The inbox must be drained/released by composition even if callers handle errors.
func Bind(assembly *resource.Assembly, selected resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Client, error) {
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	value, limits := source.owner.settings, access.Limits()
	if limits.Active > value.MaxActive || limits.Bytes < value.reservation() || limits.MaxLeases < 1 ||
		limits.Queued > 32 || limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}

func (owned *owner) acquire(ctx context.Context) (*connection, func(), error) {
	return owned.acquireBounded(ctx, owned.settings.MaxReplyBytes)
}
func (owned *owner) acquireBounded(ctx context.Context, remaining int) (*connection, func(), error) {
	owned.mu.Lock()
	var conn *connection
	if len(owned.idle) > 0 {
		conn = owned.idle[len(owned.idle)-1]
		owned.idle = owned.idle[:len(owned.idle)-1]
	}
	closed := owned.closed
	owned.mu.Unlock()
	if closed {
		return nil, func() {}, failure(ErrInput, "closed")
	}
	if conn != nil {
		conn.connectionErrors = nil
		conn.info.ConnectionAttempts = 0
		stop, err := conn.arm(ctx, remaining)
		owned.configureHandlers(ctx, conn)
		return conn, stop, err
	}
	value := owned.settings
	config, _ := value.tls()
	if owned.hooks.TLS != nil {
		config = owned.hooks.TLS.Clone()
		if config.ServerName == "" {
			config.ServerName = value.Host
		}
	}
	port, attempts := value.Port, 1
	conn, wire, stop, err := owned.dialTransport(ctx, port, remaining, config)
	if err != nil && value.FallbackPort != 0 && ctx.Err() == nil {
		previous := err
		stop()
		if conn != nil {
			remaining = conn.raw.remaining
			previous = errors.Join(previous, conn.raw.Close())
		}
		port, attempts = value.FallbackPort, 2
		conn, wire, stop, err = owned.dialTransport(ctx, port, remaining, config)
		if conn != nil {
			conn.connectionErrors = []error{previous}
		} else {
			err = errors.Join(previous, err)
		}
	}
	if err != nil {
		return conn, stop, err
	}
	conn.native, err = smtp.NewClient(wire, value.Host)
	if err != nil {
		return conn, stop, err
	}
	owned.configureHandlers(ctx, conn)
	hello := value.Hello
	if ip := net.ParseIP(hello); ip != nil {
		if ip.To4() == nil {
			hello = "IPv6:" + hello
		}
		hello = "[" + hello + "]"
	}
	if err = conn.native.Hello(hello); err != nil {
		return conn, stop, err
	}
	if value.TLSMode == "starttls" || value.TLSMode == "opportunistic" {
		supported, _ := conn.native.Extension("STARTTLS")
		if !supported && value.TLSMode == "starttls" {
			return conn, stop, failure(ErrUnsupported, "starttls")
		}
		if supported {
			if err = conn.native.StartTLS(config); err != nil {
				return conn, stop, err
			}
		}
	}
	state, secure := conn.native.TLSConnectionState()
	if (value.TLSMode == "implicit" || value.TLSMode == "starttls" || secure || value.Auth != "none") && (!secure || !state.HandshakeComplete || len(state.VerifiedChains) == 0) {
		return conn, stop, failure(ErrUnsupported, "tls")
	}
	conn.info = SessionInfo{Network: value.Network, TLS: secure, TLSVersion: state.Version, CipherSuite: state.CipherSuite, Authentication: "none", Port: port, ConnectionAttempts: attempts}
	conn.native.SkipSMTPUTF8(value.DisableSMTPUTF8)
	conn.native.SetDSNMailReturnOption(value.DSNReturn)
	conn.native.SetDSNRcptNotifyOption(strings.Join(value.DSNNotify, ","))
	if value.Auth != "none" {
		available, mechanisms := conn.native.Extension("AUTH")
		if !available {
			return conn, stop, failure(ErrUnsupported, "auth")
		}
		auth, selected, authErr := owned.authentication(ctx, state, mechanisms)
		if authErr != nil {
			return conn, stop, authErr
		}
		if err = conn.native.Auth(auth); err != nil {
			return conn, stop, err
		}
		conn.info.Authentication = selected
	}
	if !value.SkipNoop {
		if err := conn.native.Noop(); err != nil {
			return conn, stop, err
		}
	}
	return conn, stop, nil
}

func (owned *owner) dialTransport(ctx context.Context, port, remaining int, config *tls.Config) (*connection, net.Conn, func(), error) {
	stop := func() {}
	if remaining <= 0 {
		return nil, nil, stop, failure(ErrLimit, "reply")
	}
	value := owned.settings
	address := net.JoinHostPort(value.Host, strconv.Itoa(port))
	if value.Network == "unix" {
		address = value.SocketPath
	}
	dial := owned.hooks.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	raw, err := dial(ctx, value.Network, address)
	if nilInterface(raw) {
		if err == nil {
			err = failure(ErrInput, "dial")
		}
		return nil, nil, stop, err
	}
	conn := &connection{raw: &limitedConn{Conn: raw, remaining: remaining}}
	if err != nil {
		return conn, nil, stop, err
	}
	stop, err = conn.arm(ctx, remaining)
	if err != nil {
		return conn, nil, stop, err
	}
	var wire net.Conn = conn.raw
	if value.TLSMode == "implicit" {
		encrypted := tls.Client(wire, config)
		if err = encrypted.HandshakeContext(ctx); err != nil {
			return conn, nil, stop, err
		}
		wire = encrypted
	}
	return conn, wire, stop, nil
}

func (owned *owner) configureHandlers(ctx context.Context, conn *connection) {
	registry := smtp.NewErrorHandlerRegistry()
	conn.responses = nil
	record := func(err error) bool {
		if len(conn.responses) >= 16 {
			return false
		}
		conn.responses = append(conn.responses, err)
		return true
	}
	registry.SetDefaultHandler(responseHandler{ctx: ctx, hook: owned.hooks.DefaultResponseHandler, record: record})
	for command, hook := range owned.hooks.ResponseHandlers {
		registry.RegisterHandler(owned.settings.Host, command, responseHandler{ctx: ctx, hook: hook, record: record})
	}
	conn.native.ErrorHandlerRegistry = registry
}

// arm bounds incoming wire bytes including TLS overhead. The cancellation hook
// is joined before reuse: a late callback must never close a subsequent call.
func (conn *connection) arm(ctx context.Context, bytes int) (func(), error) {
	conn.raw.remaining = bytes
	deadline, ok := ctx.Deadline()
	if !ok {
		return func() {}, failure(ErrInput, "deadline")
	}
	if err := conn.raw.SetDeadline(deadline); err != nil {
		return func() {}, err
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = conn.raw.Close(); close(done) })
	return func() {
		if !stop() {
			<-done
		}
	}, nil
}
func (owned *owner) put(conn *connection) {
	owned.mu.Lock()
	owned.idle = append(owned.idle, conn)
	owned.mu.Unlock()
}
func (owned *owner) close(ctx context.Context) resource.ReleaseResult {
	owned.mu.Lock()
	defer owned.mu.Unlock()
	if owned.closed {
		return resource.ReleaseResult{Quiescent: true, Released: true, Err: owned.closeErr}
	}
	owned.closed = true
	work, cancel, err := (invocation.Budget{Limit: owned.settings.CloseTimeout}).Context(ctx, invocation.Cleanup)
	if err == nil {
		defer cancel()
	}
	var causes []error
	if err != nil {
		causes = append(causes, err)
	}
	for _, conn := range owned.idle {
		if err == nil && work.Err() == nil {
			stop, armErr := conn.arm(work, owned.settings.MaxReplyBytes)
			if armErr == nil {
				owned.configureHandlers(work, conn)
				armErr = conn.native.Quit()
			}
			stop()
			if armErr != nil {
				causes = append(causes, armErr)
			}
		} else if err == nil {
			causes = append(causes, work.Err(), context.Cause(work))
		}
		if closeErr := conn.raw.Close(); closeErr != nil {
			causes = append(causes, closeErr)
		}
	}
	owned.idle = nil
	if joined := errors.Join(causes...); joined != nil {
		if work != nil {
			owned.closeErr = phaseFailure(ErrCleanup, "close", work, joined)
		} else {
			owned.closeErr = failure(ErrCleanup, "close", joined)
		}
	}
	return resource.ReleaseResult{Quiescent: true, Released: true, Err: owned.closeErr}
}

type limitedConn struct {
	net.Conn
	remaining int
	closeOnce sync.Once
	closeErr  error
}

func (conn *limitedConn) Read(data []byte) (int, error) {
	if conn.remaining <= 0 {
		return 0, failure(ErrLimit, "reply")
	}
	if len(data) > conn.remaining {
		data = data[:conn.remaining]
	}
	count, err := conn.Conn.Read(data)
	conn.remaining -= count
	return count, err
}
func (conn *limitedConn) Close() error {
	conn.closeOnce.Do(func() { conn.closeErr = conn.Conn.Close() })
	return conn.closeErr
}
func (conn *limitedConn) SetDeadline(deadline time.Time) error {
	return conn.Conn.SetDeadline(deadline)
}
