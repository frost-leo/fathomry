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
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

type owner struct {
	settings    settings
	native      NativeOptionsV1
	transport   *http.Transport
	direct      *http.Transport
	callbacks   *activity
	mu          sync.Mutex
	stopping    bool
	pending     int
	sockets     map[*socket]struct{}
	bindings    map[string]*transportBinding
	retiring    int
	cleanup     error
	releaseMu   sync.Mutex
	releaseDone chan struct{}
}

func newOwner(value settings, native NativeOptionsV1) (*owner, error) {
	native, err := copyNative(native)
	if err != nil {
		return nil, err
	}
	config := native.TLS
	if config == nil {
		config, err = configuredTLS(value)
		if err != nil {
			return nil, err
		}
	}
	own := &owner{settings: value, native: native, callbacks: newActivity(), sockets: make(map[*socket]struct{}), bindings: make(map[string]*transportBinding)}
	own.callbacks.limit = 4 * value.MaxConnections
	protocols := new(http.Protocols)
	protocols.SetHTTP1(value.HTTP1)
	protocols.SetHTTP2(value.HTTP2)
	protocols.SetUnencryptedHTTP2(value.UnencryptedHTTP2)
	own.transport = &http.Transport{
		DialContext: own.dial, Proxy: own.proxy,
		TLSClientConfig: own.guardTLS(config), HTTP2: own.guardHTTP2(native.HTTP2),
		Protocols: protocols, MaxConnsPerHost: value.MaxConnections,
		MaxIdleConns: value.MaxConnections, MaxIdleConnsPerHost: value.MaxConnections,
		MaxResponseHeaderBytes: value.MaxHeaderBytes,
		DialTLSContext:         nil, TLSHandshakeTimeout: value.TLSHandshakeTimeout,
		ResponseHeaderTimeout: value.ResponseHeaderTimeout, IdleConnTimeout: value.IdleConnTimeout,
		ExpectContinueTimeout: value.ResponseHeaderTimeout,
		DisableKeepAlives:     value.DisableKeepAlives, DisableCompression: value.DisableCompression,
	}
	own.direct = own.transport.Clone()
	own.direct.TLSClientConfig.NextProtos = slices.Clone(own.direct.TLSClientConfig.NextProtos)
	// Direct connections do not participate in native pool accounting. Source-wide
	// socket admission below still applies to both transports.
	own.direct.MaxConnsPerHost = 0
	return own, nil
}

func (own *owner) guardHTTP2(config *http.HTTP2Config) *http.HTTP2Config {
	if config == nil {
		return nil
	}
	cloned := *config
	if callback := cloned.CountError; callback != nil {
		cloned.CountError = func(kind string) {
			if !own.callbacks.enterPrompt() {
				return
			}
			defer own.callbacks.leave()
			callback(kind)
		}
	}
	return &cloned
}

func (own *owner) proxy(request *http.Request) (*url.URL, error) {
	if request.Context().Err() != nil {
		return nil, request.Context().Err()
	}
	choice, bound := request.Context().Value(boundProxyKey{}).(proxyChoice)
	if !bound {
		if op, ok := request.Context().Value(operationKey{}).(*operation); ok {
			choice = op.proxy
		}
	}
	if choice.selection != ProxyFromProvider {
		if choice.selection == ProxyDirect {
			return nil, nil
		}
		address := *choice.address
		return &address, nil
	}
	if own.settings.ProxyURL != "" {
		return url.Parse(own.settings.ProxyURL)
	}
	if own.native.Proxy == nil {
		return nil, nil
	}
	if !own.callbacks.enter() {
		return nil, failure(ErrState, "proxy")
	}
	defer own.callbacks.leave()
	preview := previewRequest(request)
	result, err := own.native.Proxy(preview)
	if err != nil {
		return nil, err
	}
	if !validProxy(result) {
		return nil, failure(ErrInput, "proxy")
	}
	if result == nil {
		return nil, nil
	}
	cloned := *result
	return &cloned, nil
}

func (own *owner) reserveSocket() bool {
	own.mu.Lock()
	defer own.mu.Unlock()
	if own.stopping || len(own.sockets)+own.pending >= own.settings.MaxConnections {
		return false
	}
	own.pending++
	return true
}

type transportBinding struct {
	transport *http.Transport
	users     int
}

type bindingKey struct{}

func (own *owner) bindTransport(request *http.Request) (*transportBinding, error) {
	// Resolve before native H2's authority cache, which may return a connection
	// before Transport.Proxy runs. Each binding owns independent native H2 state.
	proxy, err := own.proxy(request)
	if err != nil {
		return nil, err
	}
	key := ""
	if proxy != nil {
		key = proxy.String()
	}
	for {
		own.mu.Lock()
		if own.stopping {
			own.mu.Unlock()
			return nil, failure(ErrState, "transport")
		}
		if binding := own.bindings[key]; binding != nil {
			binding.users++
			own.mu.Unlock()
			return binding, nil
		}
		if len(own.bindings)+own.retiring < own.settings.MaxConnections {
			transport := own.transport.Clone()
			// H2 setup mutates the ALPN slice retained by tls.Config.Clone.
			transport.TLSClientConfig.NextProtos = slices.Clone(transport.TLSClientConfig.NextProtos)
			transport.Proxy = nil
			if proxy != nil {
				address := *proxy
				transport.Proxy = func(*http.Request) (*url.URL, error) { copy := address; return &copy, nil }
			}
			binding := &transportBinding{transport: transport, users: 1}
			own.bindings[key] = binding
			own.mu.Unlock()
			return binding, nil
		}
		var retired *transportBinding
		for previous, binding := range own.bindings {
			if binding.users == 0 {
				retired = binding
				delete(own.bindings, previous)
				own.retiring++
				break
			}
		}
		own.mu.Unlock()
		if retired == nil {
			return nil, failure(ErrLimit, "route-transports", resource.ErrCapacity)
		}
		retired.transport.CloseIdleConnections()
		own.closeTransportSockets(retired)
		own.mu.Lock()
		own.retiring--
		own.mu.Unlock()
		if err := request.Context().Err(); err != nil {
			return nil, err
		}
	}
}

func (own *owner) returnTransport(binding *transportBinding) {
	own.mu.Lock()
	binding.users--
	own.mu.Unlock()
}

func (own *owner) closeIdleTransports() {
	own.mu.Lock()
	transports := make([]*http.Transport, 0, len(own.bindings))
	for _, binding := range own.bindings {
		transports = append(transports, binding.transport)
	}
	own.mu.Unlock()
	for _, transport := range transports {
		transport.CloseIdleConnections()
	}
}

func (own *owner) dial(ctx context.Context, network, address string) (net.Conn, error) {
	op, ok := ctx.Value(operationKey{}).(*operation)
	if !ok || !op.callbacks.enter() {
		return nil, failure(ErrState, "dial")
	}
	defer op.callbacks.leave()
	if !own.callbacks.enter() {
		return nil, failure(ErrState, "dial")
	}
	defer own.callbacks.leave()
	if !own.reserveSocket() {
		own.closeIdleTransports()
		if !own.reserveSocket() {
			return nil, failure(ErrLimit, "connections", resource.ErrCapacity)
		}
	}
	work, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(op.ctx, cancel)
	defer stop()
	defer cancel()
	work, timeoutCancel := context.WithTimeout(work, own.settings.DialTimeout)
	defer timeoutCancel()
	dial := own.native.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	var connection net.Conn
	var err error
	if op.ctx.Err() != nil {
		err = op.ctx.Err()
	} else {
		connection, err = dial(work, network, address)
	}
	if typedNil(connection) {
		connection = nil
		err = errors.Join(err, failure(ErrInput, "nil-connection"))
	}
	own.mu.Lock()
	own.pending--
	stopping := own.stopping
	var tracked *socket
	if connection != nil {
		binding, _ := ctx.Value(bindingKey{}).(*transportBinding)
		tracked = &socket{native: connection, owner: own, io: newActivity(), binding: binding}
		own.sockets[tracked] = struct{}{}
	}
	own.mu.Unlock()
	if connection == nil && err == nil {
		err = failure(ErrState, "nil-connection")
	}
	if err != nil || stopping || work.Err() != nil {
		if stopping {
			err = errors.Join(err, failure(ErrState, "dial-closed"))
		}
		if tracked != nil {
			err = errors.Join(err, tracked.Close())
		}
		return nil, errors.Join(err, work.Err(), op.ctx.Err())
	}
	tracked.closeMu.Lock()
	tracked.cancel = context.AfterFunc(op.ctx, tracked.closeUnclaimed)
	tracked.closeMu.Unlock()
	op.mu.Lock()
	if len(op.sockets) >= own.settings.MaxExchanges+1 {
		op.mu.Unlock()
		_ = tracked.Close()
		return nil, failure(ErrLimit, "dial-attempts")
	}
	op.sockets = append(op.sockets, tracked)
	op.mu.Unlock()
	return tracked, nil
}

type socket struct {
	native    net.Conn
	binding   *transportBinding
	owner     *owner
	io        *activity
	closeMu   sync.Mutex
	released  bool
	claimed   bool
	lastError error
	cancel    func() bool
}

func (conn *socket) Read(data []byte) (int, error) {
	if !conn.io.enter() {
		return 0, net.ErrClosed
	}
	defer conn.io.leave()
	return conn.native.Read(data)
}
func (conn *socket) Write(data []byte) (int, error) {
	if !conn.io.enter() {
		return 0, net.ErrClosed
	}
	defer conn.io.leave()
	return conn.native.Write(data)
}

type socketAddress struct{ network, address string }

func (address socketAddress) Network() string { return address.network }
func (address socketAddress) String() string  { return address.address }
func copyAddress(address net.Addr) net.Addr {
	if address == nil || typedNil(address) {
		return socketAddress{}
	}
	return socketAddress{network: address.Network(), address: address.String()}
}
func (conn *socket) LocalAddr() net.Addr {
	if !conn.io.enter() {
		return socketAddress{}
	}
	defer conn.io.leave()
	return copyAddress(conn.native.LocalAddr())
}
func (conn *socket) RemoteAddr() net.Addr {
	if !conn.io.enter() {
		return socketAddress{}
	}
	defer conn.io.leave()
	return copyAddress(conn.native.RemoteAddr())
}
func (conn *socket) SetDeadline(until time.Time) error {
	if !conn.io.enter() {
		return net.ErrClosed
	}
	defer conn.io.leave()
	return conn.native.SetDeadline(until)
}
func (conn *socket) SetReadDeadline(until time.Time) error {
	if !conn.io.enter() {
		return net.ErrClosed
	}
	defer conn.io.leave()
	return conn.native.SetReadDeadline(until)
}
func (conn *socket) SetWriteDeadline(until time.Time) error {
	if !conn.io.enter() {
		return net.ErrClosed
	}
	defer conn.io.leave()
	return conn.native.SetWriteDeadline(until)
}
func (conn *socket) Close() error {
	conn.closeMu.Lock()
	defer conn.closeMu.Unlock()
	return conn.closeLocked()
}
func (conn *socket) closeUnclaimed() {
	conn.closeMu.Lock()
	defer conn.closeMu.Unlock()
	if !conn.claimed {
		_ = conn.closeLocked()
	}
}
func (conn *socket) closeLocked() error {
	if conn.released {
		return nil
	}
	done := conn.io.stop()
	err := conn.native.Close()
	<-done
	if err == nil || errors.Is(err, net.ErrClosed) {
		conn.released = true
		conn.owner.mu.Lock()
		delete(conn.owner.sockets, conn)
		conn.owner.mu.Unlock()
	}
	if err != nil && !errors.Is(err, net.ErrClosed) {
		if conn.lastError == nil {
			conn.lastError = err
		}
		conn.owner.mu.Lock()
		if conn.owner.cleanup == nil {
			conn.owner.cleanup = failure(ErrCleanup, "socket", err)
		}
		conn.owner.mu.Unlock()
	}
	return err
}
func (conn *socket) claim() {
	conn.closeMu.Lock()
	// Stopping AfterFunc does not retract a callback already queued to close.
	conn.claimed = true
	if conn.cancel != nil {
		conn.cancel()
		conn.cancel = nil
	}
	conn.closeMu.Unlock()
}
func claimConnection(connection net.Conn) {
	for {
		switch wrapped := connection.(type) {
		case *socket:
			wrapped.claim()
			return
		case *tls.Conn:
			connection = wrapped.NetConn()
		default:
			return
		}
	}
}

func (own *owner) release(ctx context.Context) resource.ReleaseResult {
	own.releaseMu.Lock()
	if own.releaseDone == nil {
		own.releaseDone = make(chan struct{})
		done := own.releaseDone
		own.mu.Lock()
		own.stopping = true
		own.mu.Unlock()
		own.callbacks.stop()
		go func() {
			own.closeIdleTransports()
			own.direct.CloseIdleConnections()
			own.closeSockets()
			<-own.callbacks.done
			own.closeSockets()
			close(done)
		}()
	}
	done := own.releaseDone
	own.releaseMu.Unlock()
	select {
	case <-done:
		own.mu.Lock()
		complete := own.pending == 0 && len(own.sockets) == 0 && own.retiring == 0
		if complete {
			clear(own.bindings)
		}
		err := own.cleanup
		own.mu.Unlock()
		if complete {
			return resource.ReleaseResult{Quiescent: true, Released: true, Err: err}
		}
		own.releaseMu.Lock()
		own.releaseDone = nil
		own.releaseMu.Unlock()
		return resource.ReleaseResult{Err: err, Continue: own.release}
	case <-ctx.Done():
		return resource.ReleaseResult{Err: errors.Join(ctx.Err(), context.Cause(ctx)), Continue: own.release}
	}
}
func (own *owner) closeSockets() {
	own.mu.Lock()
	sockets := make([]*socket, 0, len(own.sockets))
	for connection := range own.sockets {
		sockets = append(sockets, connection)
	}
	own.mu.Unlock()
	for _, connection := range sockets {
		_ = connection.Close()
	}
}

func (own *owner) closeTransportSockets(binding *transportBinding) {
	own.mu.Lock()
	var sockets []*socket
	for connection := range own.sockets {
		if connection.binding == binding {
			sockets = append(sockets, connection)
		}
	}
	own.mu.Unlock()
	for _, connection := range sockets {
		_ = connection.Close()
	}
}
