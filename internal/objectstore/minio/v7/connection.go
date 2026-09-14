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
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	native "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Source is an opaque non-owning assembly capability; Bind grants operations.
type Source struct {
	private
	owner *connection
}
type connection struct {
	settings  settings
	native    *native.Core
	transport *http.Transport
	wire      *transport
	dialer    *net.Dialer
	mu        sync.Mutex
	stopped   bool
	sockets   map[*ownedSocket]struct{}
	closeErrs []error
}

// Select freezes and validates configuration without I/O. Assembly readiness
// sends one bucket HEAD; it does not enumerate keys or create resources.
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
		trust, err := tlsConfig(value)
		if err != nil {
			return resource.Resource[Source]{}, err
		}
		endpoint, _ := url.Parse(value.Endpoint)
		dialer := &net.Dialer{Timeout: value.Timeout, KeepAlive: 30 * time.Second}
		httpTransport := &http.Transport{Proxy: nil, TLSClientConfig: trust, ResponseHeaderTimeout: value.Timeout,
			MaxResponseHeaderBytes: 32 << 10, MaxIdleConns: value.MaxActive, MaxIdleConnsPerHost: value.MaxActive,
			MaxConnsPerHost: value.MaxActive, IdleConnTimeout: 30 * time.Second, DisableCompression: true,
			ForceAttemptHTTP2: false}
		owner := &connection{settings: value, transport: httpTransport, dialer: dialer, sockets: make(map[*ownedSocket]struct{})}
		httpTransport.DialContext = owner.dialPlain
		httpTransport.DialTLSContext = owner.dialTLS
		owner.wire = &transport{base: httpTransport, endpoint: value.Endpoint, settings: value}
		result := resource.Resource[Source]{Capability: Source{owner: owner}, Acquired: true, Release: owner.close}
		owner.native, err = native.NewCore(endpoint.Host, &native.Options{Creds: credentials.NewStaticV4(value.AccessKey, value.SecretKey, value.SessionToken),
			Secure: !value.Plaintext, Transport: owner.wire, Region: value.Region, BucketLookup: native.BucketLookupPath,
			MaxRetries: 1})
		if err != nil {
			return result, failure(ErrConnect, "construct", err)
		}
		result.Check = func(ctx context.Context) error {
			work, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(ctx, invocation.Establish)
			if err != nil {
				return err
			}
			defer cancel()
			state := newExchange(value, nil, false)
			exists, err := owner.native.BucketExists(controlledContext(work, state), value.Bucket)
			primary, cleanup := state.finish()
			if err == nil && !exists {
				err = ErrMissing
			}
			if err != nil || primary != nil || cleanup != nil {
				return failure(ErrConnect, "readiness", err, primary, cleanup, work.Err(), context.Cause(work))
			}
			return nil
		}
		return result, nil
	}), nil
}
func (owner *connection) close(context.Context) resource.ReleaseResult {
	owner.mu.Lock()
	owner.stopped = true
	sockets := make([]*ownedSocket, 0, len(owner.sockets))
	for socket := range owner.sockets {
		sockets = append(sockets, socket)
	}
	owner.mu.Unlock()
	owner.transport.CloseIdleConnections()
	for _, socket := range sockets {
		_ = socket.Close()
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return resource.ReleaseResult{Quiescent: true, Released: true, Err: errors.Join(owner.closeErrs...)}
}

type dialContextKey struct{}
type ownedSocket struct {
	net.Conn
	owner *connection
	once  sync.Once
	err   error
}

func (socket *ownedSocket) Close() error {
	socket.once.Do(func() {
		socket.err = socket.Conn.Close()
		socket.owner.mu.Lock()
		defer socket.owner.mu.Unlock()
		delete(socket.owner.sockets, socket)
		if socket.err != nil {
			socket.owner.stopped = true
			socket.owner.closeErrs = append(socket.owner.closeErrs, socket.err)
		}
	})
	return socket.err
}
func (owner *connection) dialPlain(ctx context.Context, network, address string) (net.Conn, error) {
	return owner.dial(ctx, network, address, false)
}
func (owner *connection) dialTLS(ctx context.Context, network, address string) (net.Conn, error) {
	return owner.dial(ctx, network, address, true)
}
func (owner *connection) dial(ctx context.Context, network, address string, secure bool) (net.Conn, error) {
	original, ok := ctx.Value(dialContextKey{}).(context.Context)
	state, _ := ctx.Value(exchangeKey{}).(*exchange)
	if !ok || state == nil || !state.beginDial() {
		return nil, failure(ErrAuthority, "unowned-dial")
	}
	defer state.dials.Done()
	work, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(original, func() { cancel(context.Cause(original)) })
	defer stop()
	defer cancel(nil)
	if original.Err() != nil {
		cancel(context.Cause(original))
	}
	owner.mu.Lock()
	stopped := owner.stopped
	owner.mu.Unlock()
	if stopped {
		return nil, failure(ErrAuthority, "closed-network")
	}
	cleanup, _ := ctx.Value(cleanupKey{}).(bool)
	raw, err := owner.dialer.DialContext(work, network, address)
	if err != nil {
		state.note(err, cleanup)
		return nil, err
	}
	socket := &ownedSocket{Conn: raw, owner: owner}
	owner.mu.Lock()
	owner.sockets[socket] = struct{}{}
	stopped = owner.stopped
	owner.mu.Unlock()
	if stopped || work.Err() != nil {
		state.note(socket.Close(), true)
		err = failure(ErrConnect, "dial-interrupted", work.Err(), context.Cause(work))
		state.note(err, cleanup)
		return nil, err
	}
	if !secure {
		return socket, nil
	}
	config := owner.transport.TLSClientConfig.Clone()
	config.ServerName, _, err = net.SplitHostPort(address)
	if err != nil {
		state.note(socket.Close(), true)
		state.note(err, cleanup)
		return nil, err
	}
	secured := tls.Client(socket, config)
	if err = secured.HandshakeContext(work); err != nil {
		state.note(secured.Close(), true)
		if header, ok := err.(tls.RecordHeaderError); ok {
			header.Conn = nil
			err = header
		}
		state.note(err, cleanup)
		return nil, err
	}
	return secured, nil
}

// Client is concurrent and non-owning. All aliases share admission and transport.
type Client struct {
	private
	owner    *connection
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Bind joins the exact selected source, its effective limits and an independently
// owned evidence inbox. Binding does not transfer source shutdown authority.
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
	if limits.Active > value.MaxActive || limits.Queued > value.QueuedCalls || limits.Bytes < value.reservation() ||
		limits.MaxLeases < 1 || limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}

// EvidenceBytes is a declared retained-result envelope, not an RSS measurement.
func (client *Client) EvidenceBytes() int64 {
	if client == nil || client.owner == nil {
		return 0
	}
	return client.owner.settings.evidenceReservation()
}
func (client *Client) valid(ctx context.Context) error {
	if client == nil || client.owner == nil || ctx == nil {
		return failure(ErrInput, "call")
	}
	return nil
}
func (client *Client) start(ctx context.Context, id fault.Correlation, name string, payload bool,
	run func(context.Context, *exchange, *resultData) (error, error)) (*invocation.Receipt[Result], error) {
	value := client.owner.settings
	call, err := invocation.Begin(ctx, client.access, invocation.Request{Name: name, Correlation: id, Shape: invocation.Async,
		Bytes: value.reservation(), EvidenceBytes: value.evidenceReservation(), Admission: invocation.Budget{Limit: value.Timeout},
		AttemptsKnown: true, MaxAttempts: uint64(value.MaxRequests)}, client.inbox, client.observer)
	if err != nil {
		return nil, err
	}
	work, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(ctx, invocation.Execute)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return call.Receipt(), nil
	}
	state := newExchange(value, call, payload)
	go func() {
		defer cancel()
		data := &resultData{}
		primary, cleanup := run(controlledContext(work, state), state, data)
		wireFailure, closeFailure := state.finish()
		if primary != nil || wireFailure != nil {
			primary = failure(operationKind(name), name, primary, wireFailure, work.Err(), context.Cause(work))
		}
		if cleanup != nil || closeFailure != nil {
			cleanup = failure(ErrCleanup, name, cleanup, closeFailure)
		}
		call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: primary, Cleanup: cleanup})
	}()
	return call.Receipt(), nil
}
func operationKind(name string) fault.Kind {
	switch name {
	case "read", "download", "stat", "get-tags":
		return ErrRead
	case "list", "list-uploads", "list-parts":
		return ErrList
	case "remove":
		return ErrRemove
	default:
		return ErrWrite
	}
}

type nativeContext struct{ context.Context }

func (nativeContext) Value(any) any { return nil }
func controlledContext(ctx context.Context, state *exchange) context.Context {
	return context.WithValue(context.WithValue(nativeContext{ctx}, exchangeKey{}, state), dialContextKey{}, ctx)
}
