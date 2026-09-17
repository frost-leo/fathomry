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
	mu       sync.Mutex
	idle     []*connection
	closed   bool
	closeErr error
}
type connection struct {
	raw    *limitedConn
	native *smtp.Client
}

// Select freezes configuration and strict overlays without I/O. Connections are
// lazily opened only under the call's admission/evidence/timeout authority.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	version := options.Version
	if version == 0 {
		version = 1
	}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		if ctx.Err() != nil {
			return resource.Resource[Source]{}, failure(ErrInput, "construct", ctx.Err(), context.Cause(ctx))
		}
		owned := &owner{settings: value}
		return resource.Resource[Source]{Acquired: true, Capability: Source{owner: owned}, Release: owned.close}, nil
	}), nil
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
		stop, err := conn.arm(ctx, owned.settings.MaxReplyBytes)
		return conn, stop, err
	}
	value := owned.settings
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(value.Host, strconv.Itoa(value.Port)))
	if err != nil {
		return nil, func() {}, err
	}
	conn = &connection{raw: &limitedConn{Conn: raw}}
	stop, err := conn.arm(ctx, value.MaxReplyBytes)
	if err != nil {
		return conn, stop, err
	}
	config, _ := value.tls()
	var wire net.Conn = conn.raw
	if value.TLSMode == "implicit" {
		encrypted := tls.Client(wire, config)
		if err = encrypted.HandshakeContext(ctx); err != nil {
			return conn, stop, err
		}
		wire = encrypted
	}
	conn.native, err = smtp.NewClient(wire, value.Host)
	if err != nil {
		return conn, stop, err
	}
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
	if value.TLSMode == "starttls" {
		if supported, _ := conn.native.Extension("STARTTLS"); !supported {
			return conn, stop, failure(ErrUnsupported, "starttls")
		}
		if err = conn.native.StartTLS(config); err != nil {
			return conn, stop, err
		}
	}
	state, secure := conn.native.TLSConnectionState()
	if !secure || !state.HandshakeComplete || len(state.VerifiedChains) == 0 {
		return conn, stop, failure(ErrUnsupported, "tls")
	}
	if value.Auth != "none" {
		available, mechanisms := conn.native.Extension("AUTH")
		wanted := strings.ToUpper(value.Auth)
		found := false
		for _, mechanism := range strings.Fields(mechanisms) {
			if mechanism == wanted {
				found = true
			}
		}
		if !available || !found {
			return conn, stop, failure(ErrUnsupported, "auth")
		}
		var auth smtp.Auth
		switch value.Auth {
		case "plain":
			auth = smtp.PlainAuth("", value.Username, value.Password, value.Host, false)
		case "login":
			auth = smtp.LoginAuth(value.Username, value.Password, value.Host, false)
		case "xoauth2":
			auth = smtp.XOAuth2Auth(value.Username, value.Password)
		}
		if err = conn.native.Auth(auth); err != nil {
			return conn, stop, err
		}
	}
	return conn, stop, nil
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
