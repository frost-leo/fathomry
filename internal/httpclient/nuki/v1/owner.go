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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/frost-leo/fathomry/internal/resource"
	http "github.com/nukilabs/http"
	sdk "github.com/nukilabs/tlsclient"
	"github.com/nukilabs/tlsclient/proxy"
)

type owner struct {
	settings     settings
	native       NativeOptionsV1
	callbacks    activity
	routeMu      sync.Mutex
	routes       map[routeKey]*route
	origins      map[originKey]struct{}
	mu           sync.Mutex
	closing      bool
	pending      int
	nativeDials  int
	sockets      map[*socket]struct{}
	cleanup      error
	closeDone    chan struct{}
	closeStarted bool
	retryDone    chan struct{}
	socketWork   sync.WaitGroup
	recorders    map[*guardedRecorder]struct{}
}

type routeKey struct {
	proxy   string
	headers [32]byte
}
type originKey struct {
	route     routeKey
	authority string
}
type route struct {
	client    *sdk.Client
	transport *sdk.RoundTripper
}

func newOwner(value settings, native NativeOptionsV1) (*owner, error) {
	copied, err := copyNative(native)
	if err != nil {
		return nil, err
	}
	return &owner{settings: value, native: copied, routes: make(map[routeKey]*route), origins: make(map[originKey]struct{}),
		sockets: make(map[*socket]struct{}), recorders: make(map[*guardedRecorder]struct{})}, nil
}

func (owner *owner) admitOrigin(choice routeChoice, request *http.Request) error {
	key := originKey{route: routeKey{proxy: choice.address, headers: headerDigest(choice.headers)}, authority: request.URL.Scheme + "://" + request.URL.Host}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if _, exists := owner.origins[key]; exists {
		return nil
	}
	if len(owner.origins) >= owner.settings.MaxOrigins {
		return failure(ErrCapacity, "origins")
	}
	owner.origins[key] = struct{}{}
	return nil
}

func (owner *owner) recordCleanup(err error) {
	if err == nil {
		return
	}
	owner.mu.Lock()
	owner.cleanup = errors.Join(owner.cleanup, err)
	owner.mu.Unlock()
}

func (owner *owner) available() error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.closing {
		return failure(ErrState, "source-closing")
	}
	if owner.cleanup != nil {
		return failure(ErrCleanup, "source-cleanup", owner.cleanup)
	}
	return nil
}

func (owner *owner) routeFor(choice routeChoice) (result *route, err error) {
	owner.routeMu.Lock()
	defer owner.routeMu.Unlock()
	if err := owner.available(); err != nil {
		return nil, err
	}
	key := routeKey{proxy: choice.address, headers: headerDigest(choice.headers)}
	if existing := owner.routes[key]; existing != nil {
		return existing, nil
	}
	if len(owner.routes) >= owner.settings.MaxRoutes {
		return nil, failure(ErrCapacity, "routes")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = callbackFailure(recovered)
		}
	}()
	native, err := copyNative(owner.native)
	if err != nil {
		return nil, err
	}
	native = owner.protectNative(native)
	options := *native.Transport
	if options.IdleConnTimeout == 0 {
		options.IdleConnTimeout = owner.settings.IdleConnTimeout
	}
	if options.MaxResponseHeaderBytes == 0 {
		options.MaxResponseHeaderBytes = owner.settings.MaxNativeHeaderBytes
	}
	if options.MaxCachedOrigins == 0 {
		options.MaxCachedOrigins = owner.settings.MaxOrigins
	}
	if owner.settings.Mode == HTTP1Only || owner.settings.Mode == HTTP2Negotiated {
		options.DisableHTTP3 = true
	}
	if owner.settings.Mode == HTTP3Only {
		options.ForceHTTP3 = true
	}
	address, err := parseProxy(choice.address)
	if err != nil {
		return nil, err
	}
	dialer, err := proxy.NewWithDialer(address, owner.settings.Timeout, native.TLS, &baseDialer{owner}, choice.headers, options.MaxResponseHeaderBytes)
	if err != nil {
		return nil, failure(ErrInput, "route", err)
	}
	if options.ForceHTTP3 && !dialer.SupportHTTP3() {
		return nil, failure(ErrUnsupported, "http3-route")
	}
	dialer = &routeDialer{owner: owner, native: dialer}
	clientOptions := []sdk.Option{sdk.WithTLSConfig(native.TLS), sdk.WithQUICConfig(native.QUIC),
		sdk.WithTransportOptions(options), sdk.WithDialer(dialer), sdk.WithNoAutoDecompress(), sdk.WithNoCookieJar(),
		sdk.WithTimeout(owner.settings.Timeout)}
	if native.Pinner != nil {
		clientOptions = append(clientOptions, sdk.WithPinner(native.Pinner))
	}
	if native.Tracker != nil {
		clientOptions = append(clientOptions, sdk.WithTracker(native.Tracker))
	}
	client := sdk.New(*native.Profile, clientOptions...)
	raw := client.Transport.(*sdk.RoundTripper)
	if err := raw.ValidationError(); err != nil {
		return nil, failure(ErrInput, "native-profile", err, raw.Close())
	}
	client.Jar = native.Jar
	client.CheckRedirect = owner.redirect
	client.Transport = exchangeTransport{owner: owner, native: raw}
	result = &route{client: client, transport: raw}
	owner.routes[key] = result
	return result, nil
}

func (owner *owner) release(ctx context.Context) resource.ReleaseResult {
	if ctx == nil {
		return resource.ReleaseResult{Err: failure(ErrInput, "close"), Continue: owner.release}
	}
	owner.mu.Lock()
	if !owner.closeStarted {
		owner.closeStarted, owner.closing = true, true
		owner.closeDone = make(chan struct{})
		go owner.closeResources()
	} else {
		select {
		case <-owner.closeDone:
		default:
			owner.startRetryLocked()
		}
	}
	done := owner.closeDone
	owner.mu.Unlock()
	select {
	case <-ctx.Done():
		return resource.ReleaseResult{Err: failure(ErrCleanup, "close-wait", ctx.Err(), context.Cause(ctx)), Continue: owner.release}
	case <-done:
		owner.routeMu.Lock()
		nativePending := 0
		for _, route := range owner.routes {
			if !route.transport.ReleaseConfirmed() {
				nativePending++
			}
		}
		owner.routeMu.Unlock()
		owner.mu.Lock()
		pending, cleanup := len(owner.sockets)+len(owner.recorders)+owner.pending+owner.nativeDials+nativePending, owner.cleanup
		owner.mu.Unlock()
		if pending != 0 {
			return resource.ReleaseResult{Err: failure(ErrCleanup, "sockets-retained", cleanup), Continue: owner.retryRelease}
		}
		return resource.ReleaseResult{Quiescent: true, Released: true, Err: cleanup}
	}
}

func (owner *owner) closeResources() {
	defer close(owner.closeDone)
	owner.routeMu.Lock()
	for _, route := range owner.routes {
		owner.recordCleanup(route.transport.Close())
	}
	owner.routeMu.Unlock()
	owner.socketWork.Wait()
	owner.mu.Lock()
	sockets := make([]*socket, 0, len(owner.sockets))
	for socket := range owner.sockets {
		sockets = append(sockets, socket)
	}
	recorders := make([]*guardedRecorder, 0, len(owner.recorders))
	for recorder := range owner.recorders {
		recorders = append(recorders, recorder)
	}
	owner.mu.Unlock()
	for _, socket := range sockets {
		_ = socket.Close()
	}
	for _, recorder := range recorders {
		_ = recorder.Close()
	}
	owner.callbacks.stop()
}

func (owner *owner) retryRelease(ctx context.Context) resource.ReleaseResult {
	owner.mu.Lock()
	owner.startRetryLocked()
	done := owner.retryDone
	owner.mu.Unlock()
	select {
	case <-ctx.Done():
		return resource.ReleaseResult{Err: failure(ErrCleanup, "retry-close-wait", ctx.Err(), context.Cause(ctx)), Continue: owner.retryRelease}
	case <-done:
		return owner.release(ctx)
	}
}

func (owner *owner) startRetryLocked() {
	if owner.retryDone != nil {
		select {
		case <-owner.retryDone:
		default:
			return
		}
	}
	done := make(chan struct{})
	owner.retryDone = done
	sockets := make([]*socket, 0, len(owner.sockets))
	for socket := range owner.sockets {
		sockets = append(sockets, socket)
	}
	recorders := make([]*guardedRecorder, 0, len(owner.recorders))
	for recorder := range owner.recorders {
		recorders = append(recorders, recorder)
	}
	go func() {
		defer close(done)
		for _, socket := range sockets {
			_ = socket.Close()
		}
		for _, recorder := range recorders {
			_ = recorder.Close()
		}
		owner.routeMu.Lock()
		for _, route := range owner.routes {
			owner.recordCleanup(route.transport.Close())
		}
		owner.routeMu.Unlock()
	}()
}

type socket struct {
	owner    *owner
	closer   io.Closer
	mu       sync.Mutex
	released bool
}

func (socket *socket) Close() error {
	socket.mu.Lock()
	defer socket.mu.Unlock()
	if socket.released {
		return nil
	}
	err := socket.closer.Close()
	if closedOnly(err) {
		err = nil
	}
	if err == nil {
		socket.released = true
		socket.owner.mu.Lock()
		delete(socket.owner.sockets, socket)
		socket.owner.mu.Unlock()
	} else {
		socket.owner.recordCleanup(err)
	}
	return err
}

type baseDialer struct{ owner *owner }

func (base *baseDialer) SupportHTTP3() bool { return true }
func (base *baseDialer) reserve() error {
	base.owner.mu.Lock()
	defer base.owner.mu.Unlock()
	if base.owner.closing {
		return failure(ErrState, "dial")
	}
	if base.owner.cleanup != nil {
		return failure(ErrCleanup, "dial", base.owner.cleanup)
	}
	if len(base.owner.sockets)+base.owner.pending >= base.owner.settings.MaxConnections {
		return failure(ErrCapacity, "sockets")
	}
	base.owner.pending++
	base.owner.socketWork.Add(1)
	return nil
}
func (base *baseDialer) publish(closer io.Closer) *socket {
	base.owner.mu.Lock()
	base.owner.pending--
	var owned *socket
	if closer != nil {
		owned = &socket{owner: base.owner, closer: closer}
		base.owner.sockets[owned] = struct{}{}
	}
	closing := base.owner.closing
	base.owner.mu.Unlock()
	if closing && owned != nil {
		_ = owned.Close()
	}
	return owned
}
func (base *baseDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, context.Cause(ctx))
	}
	if err := base.reserve(); err != nil {
		return nil, err
	}
	defer base.owner.socketWork.Done()
	if !base.owner.callbacks.enter() {
		base.publish(nil)
		return nil, failure(ErrState, "dial")
	}
	defer base.owner.callbacks.leave()
	var conn net.Conn
	var err error
	if callback := base.owner.native.DialContext; callback != nil {
		conn, err = callback(ctx, network, address)
	} else {
		conn, err = (&net.Dialer{Timeout: base.owner.settings.Timeout}).DialContext(ctx, network, address)
	}
	if nilObject(conn) {
		conn = nil
	}
	owned := base.publish(conn)
	if err != nil {
		if owned != nil {
			err = errors.Join(err, owned.Close())
		}
		return nil, err
	}
	if conn == nil {
		return nil, failure(ErrInput, "nil-connection")
	}
	return &tcpSocket{Conn: conn, socket: owned}, nil
}
func (base *baseDialer) ListenPacket(ctx context.Context, network, address string) (net.PacketConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, context.Cause(ctx))
	}
	if err := base.reserve(); err != nil {
		return nil, err
	}
	defer base.owner.socketWork.Done()
	if !base.owner.callbacks.enter() {
		base.publish(nil)
		return nil, failure(ErrState, "listen")
	}
	defer base.owner.callbacks.leave()
	var conn net.PacketConn
	var err error
	if callback := base.owner.native.ListenPacket; callback != nil {
		conn, err = callback(ctx, network, address)
	} else {
		conn, err = (&net.ListenConfig{}).ListenPacket(ctx, network, ":0")
	}
	if nilObject(conn) {
		conn = nil
	}
	owned := base.publish(conn)
	if err != nil {
		if owned != nil {
			err = errors.Join(err, owned.Close())
		}
		return nil, err
	}
	if conn == nil {
		return nil, failure(ErrInput, "nil-packet")
	}
	return &packetSocket{PacketConn: conn, socket: owned}, nil
}

type tcpSocket struct {
	net.Conn
	socket *socket
}

func (conn *tcpSocket) Close() error { return conn.socket.Close() }

type packetSocket struct {
	net.PacketConn
	socket *socket
}

func (conn *packetSocket) Close() error { return conn.socket.Close() }
func (conn *packetSocket) SetReadBuffer(size int) error {
	if socket, ok := conn.PacketConn.(interface{ SetReadBuffer(int) error }); ok {
		return socket.SetReadBuffer(size)
	}
	return nil
}
func (conn *packetSocket) SetWriteBuffer(size int) error {
	if socket, ok := conn.PacketConn.(interface{ SetWriteBuffer(int) error }); ok {
		return socket.SetWriteBuffer(size)
	}
	return nil
}

func parseProxy(address string) (*url.URL, error) {
	if address == "" {
		return nil, nil
	}
	value, err := url.Parse(address)
	if err != nil || len(address) > 8192 || !fieldValue(address) || value == nil || value.Hostname() == "" ||
		value.Fragment != "" || value.RawQuery != "" || value.Port() == "" ||
		value.Scheme != "http" && value.Scheme != "https" && value.Scheme != "socks5" && value.Scheme != "socks5h" {
		return nil, failure(ErrInput, "proxy", err)
	}
	if strings.Contains(value.Path, "{") || strings.Contains(value.Path, "%7B") {
		return nil, failure(ErrUnsupported, "masque-write-deadline")
	}
	if value.Path != "" && value.Path != "/" {
		return nil, failure(ErrInput, "proxy-path")
	}
	return value, nil
}

func headerDigest(header http.Header) [32]byte {
	digest := sha256.New()
	keys := make([]string, 0, len(header))
	for key := range header {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var length [8]byte
	for _, key := range keys {
		binary.BigEndian.PutUint64(length[:], uint64(len(key)))
		_, _ = digest.Write(length[:])
		_, _ = io.WriteString(digest, key)
		binary.BigEndian.PutUint64(length[:], uint64(len(header[key])))
		_, _ = digest.Write(length[:])
		for _, value := range header[key] {
			binary.BigEndian.PutUint64(length[:], uint64(len(value)))
			_, _ = digest.Write(length[:])
			_, _ = io.WriteString(digest, value)
		}
	}
	var sum [32]byte
	copy(sum[:], digest.Sum(nil))
	return sum
}
