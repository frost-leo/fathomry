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

package surf

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"reflect"
	"sync"

	"github.com/enetx/http"
	utls "github.com/refraction-networking/utls"
)

// FathomryControlV1 installs technical ownership hooks before native construction.
// Acquisitions return a release function for one physical TCP or packet handle.
// Enter retains caller-owned callback work until the returned function is called.
// Hooks and dialers must not reenter this client. Returned connections transfer
// ownership and their Close must terminate their own I/O, including on error.
type FathomryControlV1 struct {
	BindContext         func(context.Context) (context.Context, func())
	ConstructionContext context.Context
	RequestClient       func(context.Context, http.Client) (http.Client, error)
	HelloSpecFactory    func(context.Context) (utls.ClientHelloSpec, error)
	AcquireTCP          func() (func(), error)
	AcquireUDP          func() (func(), error)
	Enter               func(context.Context) (func(), error)
	DialContext         func(context.Context, string, string) (net.Conn, error)
	ListenPacket        func(context.Context, string, string) (net.PacketConn, error)
	ProxyTLSConfig      *tls.Config
	JAConfig            *utls.Config
	MaxRequestBytes     int64
	MaxResponseBytes    int64
	MaxHeaderBytes      int64
	MaxProxyHeaderBytes int64
}

var ErrFathomryClosed = errors.New("surf: client closed")

type fathomryState struct {
	mu         sync.Mutex
	closing    bool
	configured bool
	active     int
	changed    chan struct{}
	conns      map[*fathomryConn]struct{}
	packets    map[*fathomryPacket]struct{}
	extra      map[io.Closer]struct{}
	control    FathomryControlV1
	cleanup    error
	failed     error
	closeOnce  sync.Once
	done       chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
}

func newFathomryState() *fathomryState {
	ctx, cancel := context.WithCancel(context.Background())
	return &fathomryState{changed: make(chan struct{}), done: make(chan struct{}), ctx: ctx, cancel: cancel,
		conns: make(map[*fathomryConn]struct{}), packets: make(map[*fathomryPacket]struct{}), extra: make(map[io.Closer]struct{})}
}

// ConfigureFathomry installs control exactly once on a client that has not been
// built or used. Native values are borrowed by the SDK; its Provider freezes them.
func (client *Client) ConfigureFathomry(control FathomryControlV1) error {
	state := client.fathomry
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closing || state.configured || state.active != 0 || len(state.conns) != 0 || len(state.packets) != 0 || client.builder != nil {
		return errors.New("surf: control requires an unused client")
	}
	if control.MaxRequestBytes < 0 || control.MaxResponseBytes < 0 || control.MaxHeaderBytes < 0 {
		return errors.New("surf: invalid native limits")
	}
	state.control = control
	state.control.ProxyTLSConfig = state.tlsConfig(control.ProxyTLSConfig)
	if control.JAConfig != nil {
		config := control.JAConfig.Clone()
		state.guardJA(config)
		if config.ClientSessionCache != nil {
			config.ClientSessionCache = fathomryUTLSCache{state: state, cache: config.ClientSessionCache}
		}
		state.control.JAConfig = config
	}
	state.configured = true
	return nil
}

// FathomryQuiescent reports completed owned shutdown; errors remain in Close.
func (client *Client) FathomryQuiescent() bool {
	if client == nil || client.fathomry == nil {
		return false
	}
	select {
	case <-client.fathomry.done:
		return true
	default:
		return false
	}
}

func (state *fathomryState) signal() {
	close(state.changed)
	state.changed = make(chan struct{})
}
func (state *fathomryState) enter(ctx context.Context) (func(), error) {
	state.mu.Lock()
	if state.closing || state.failed != nil || state.cleanup != nil {
		err := fathomryFailure(errors.Join(ErrFathomryClosed, state.failed), state.cleanup)
		state.mu.Unlock()
		return nil, err
	}
	state.active++
	enter := state.control.Enter
	state.mu.Unlock()
	var callback func()
	if enter != nil {
		var err error
		callback, err = enter(ctx)
		if err != nil || callback == nil {
			state.leave()
			if err == nil {
				err = errors.New("surf: missing native work release")
			}
			return nil, err
		}
	}
	return func() {
		if callback != nil {
			callback()
		}
		state.leave()
	}, nil
}
func (state *fathomryState) leave() {
	state.mu.Lock()
	state.active--
	state.signal()
	state.mu.Unlock()
}
func (state *fathomryState) remember(err error) {
	if err == nil || err == net.ErrClosed {
		return
	}
	state.mu.Lock()
	state.cleanup = errors.Join(state.cleanup, err)
	state.mu.Unlock()
	state.cancel()
}
func (state *fathomryState) reserve(packet bool) (func(), error) {
	state.mu.Lock()
	acquire := state.control.AcquireTCP
	if packet {
		acquire = state.control.AcquireUDP
	}
	state.mu.Unlock()
	if acquire == nil {
		return func() {}, nil
	}
	release, err := acquire()
	if err == nil && release == nil {
		err = errors.New("surf: missing native capacity release")
	}
	return release, err
}

func (client *Client) fathomryDial(ctx context.Context, network, address string) (net.Conn, error) {
	ctx, finishContext := client.fathomry.bindContext(ctx)
	defer finishContext()
	state := client.fathomry
	end, err := state.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	release, err := state.reserve(false)
	if err != nil {
		return nil, err
	}
	dial := state.control.DialContext
	if dial == nil {
		dial = client.dialer.DialContext
	}
	work, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(state.ctx, cancel)
	defer func() { stop(); cancel() }()
	raw, err := dial(work, network, address)
	if fathomryNil(raw) {
		release()
		if err == nil {
			err = errors.New("surf: dial returned nil connection")
		}
		return nil, err
	}
	conn := &fathomryConn{Conn: raw, state: state, release: release}
	state.mu.Lock()
	closed := state.closing
	state.conns[conn] = struct{}{}
	state.mu.Unlock()
	if err != nil || closed || work.Err() != nil {
		cleanup := conn.Close()
		return nil, errors.Join(err, cleanup, work.Err())
	}
	return conn, nil
}
func (client *Client) fathomryListen(ctx context.Context, network, address string) (net.PacketConn, error) {
	state := client.fathomry
	end, err := state.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	release, err := state.reserve(true)
	if err != nil {
		return nil, err
	}
	listen := state.control.ListenPacket
	if listen == nil {
		listen = (&net.ListenConfig{}).ListenPacket
	}
	raw, err := listen(ctx, network, address)
	if fathomryNil(raw) {
		release()
		if err == nil {
			err = errors.New("surf: packet listener returned nil")
		}
		return nil, err
	}
	packet := &fathomryPacket{PacketConn: raw, state: state, release: release}
	state.mu.Lock()
	closed := state.closing
	state.packets[packet] = struct{}{}
	state.mu.Unlock()
	if err != nil || closed {
		if closed {
			err = errors.Join(err, ErrFathomryClosed)
		}
		return nil, errors.Join(err, packet.Close())
	}
	return packet, nil
}
func (client *Client) fathomryTLS(ctx context.Context, network, address string, dial func(context.Context, string, string) (net.Conn, error)) (net.Conn, error) {
	ctx, finishContext := client.fathomry.bindContext(ctx)
	defer finishContext()
	end, err := client.fathomry.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	raw, err := dial(ctx, network, address)
	if err != nil {
		return nil, err
	}
	config := client.tlsConfig.Clone()
	if config.ServerName == "" {
		config.ServerName, _, err = net.SplitHostPort(address)
		if err != nil {
			config.ServerName = address
		}
	}
	conn := tls.Client(raw, config)
	if err := conn.HandshakeContext(ctx); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	return conn, nil
}

type fathomryConn struct {
	net.Conn
	ioMu    sync.Mutex
	closed  bool
	state   *fathomryState
	release func()
	once    sync.Once
	err     error
}

func (conn *fathomryConn) Read(buffer []byte) (int, error) {
	conn.ioMu.Lock()
	if conn.closed {
		conn.ioMu.Unlock()
		return 0, net.ErrClosed
	}
	conn.state.beginIO()
	conn.ioMu.Unlock()
	defer conn.state.leave()
	return conn.Conn.Read(buffer)
}
func (conn *fathomryConn) Write(buffer []byte) (int, error) {
	conn.ioMu.Lock()
	if conn.closed {
		conn.ioMu.Unlock()
		return 0, net.ErrClosed
	}
	conn.state.beginIO()
	conn.ioMu.Unlock()
	defer conn.state.leave()
	return conn.Conn.Write(buffer)
}

func (conn *fathomryConn) Close() error {
	conn.once.Do(func() {
		conn.ioMu.Lock()
		conn.closed = true
		conn.ioMu.Unlock()
		conn.err = conn.Conn.Close()
		conn.state.remember(conn.err)
		conn.release()
		conn.state.mu.Lock()
		delete(conn.state.conns, conn)
		conn.state.signal()
		conn.state.mu.Unlock()
	})
	return conn.err
}

type fathomryPacket struct {
	net.PacketConn
	ioMu    sync.Mutex
	closed  bool
	state   *fathomryState
	release func()
	once    sync.Once
	err     error
}

func (packet *fathomryPacket) ReadFrom(buffer []byte) (int, net.Addr, error) {
	packet.ioMu.Lock()
	if packet.closed {
		packet.ioMu.Unlock()
		return 0, nil, net.ErrClosed
	}
	packet.state.beginIO()
	packet.ioMu.Unlock()
	defer packet.state.leave()
	return packet.PacketConn.ReadFrom(buffer)
}
func (packet *fathomryPacket) WriteTo(buffer []byte, address net.Addr) (int, error) {
	packet.ioMu.Lock()
	if packet.closed {
		packet.ioMu.Unlock()
		return 0, net.ErrClosed
	}
	packet.state.beginIO()
	packet.ioMu.Unlock()
	defer packet.state.leave()
	return packet.PacketConn.WriteTo(buffer, address)
}

func (packet *fathomryPacket) Close() error {
	packet.once.Do(func() {
		packet.ioMu.Lock()
		packet.closed = true
		packet.ioMu.Unlock()
		packet.err = packet.PacketConn.Close()
		packet.state.remember(packet.err)
		packet.release()
		packet.state.mu.Lock()
		delete(packet.state.packets, packet)
		packet.state.signal()
		packet.state.mu.Unlock()
	})
	return packet.err
}

func (client *Client) fathomryClose() error {
	state := client.fathomry
	state.closeOnce.Do(func() {
		state.mu.Lock()
		state.closing = true
		state.mu.Unlock()
		state.cancel()
		if client.cli != nil {
			if closer, ok := client.cli.Transport.(io.Closer); ok {
				state.remember(closer.Close())
			} else {
				client.cli.CloseIdleConnections()
			}
		}
		for {
			state.mu.Lock()
			conns := make([]*fathomryConn, 0, len(state.conns))
			for conn := range state.conns {
				conns = append(conns, conn)
			}
			packets := make([]*fathomryPacket, 0, len(state.packets))
			for packet := range state.packets {
				packets = append(packets, packet)
			}
			extra := state.extra
			state.extra = make(map[io.Closer]struct{})
			state.mu.Unlock()
			for closer := range extra {
				state.remember(closer.Close())
			}
			for _, conn := range conns {
				state.remember(conn.Close())
			}
			for _, packet := range packets {
				state.remember(packet.Close())
			}
			state.mu.Lock()
			finished := state.active == 0 && len(state.conns) == 0 && len(state.packets) == 0 && len(state.extra) == 0
			changed := state.changed
			state.mu.Unlock()
			if finished {
				break
			}
			<-changed
		}
		close(state.done)
	})
	state.mu.Lock()
	defer state.mu.Unlock()
	return errors.Join(state.cleanup, state.failed)
}
func (state *fathomryState) own(closer io.Closer) error {
	state.mu.Lock()
	if state.closing {
		state.mu.Unlock()
		return errors.Join(ErrFathomryClosed, closer.Close())
	}
	state.extra[closer] = struct{}{}
	state.mu.Unlock()
	return nil
}

func (state *fathomryState) disown(closer io.Closer) {
	state.mu.Lock()
	delete(state.extra, closer)
	state.mu.Unlock()
}

// FathomryRequest retains native request expressiveness without serializing it.
func (client *Client) FathomryRequest(request *http.Request) *Request {
	return &Request{cli: client, request: request}
}

// FathomrySetTLSConfig replaces the native TLS configuration before Build.
// Ownership is borrowed; callers must freeze it before handing it to the SDK.
func (client *Client) FathomrySetTLSConfig(config *tls.Config) error {
	if config == nil || client.builder != nil {
		return errors.New("surf: TLS configuration requires unused client")
	}
	client.tlsConfig = client.fathomry.tlsConfig(config)
	client.transport.(*http.Transport).TLSClientConfig = client.tlsConfig
	return nil
}

type fathomryAssociation struct {
	closers []io.Closer
	once    sync.Once
	err     error
}

func fathomryNil(value any) bool {
	if value == nil {
		return true
	}
	kind := reflect.ValueOf(value)
	return kind.Kind() == reflect.Pointer && kind.IsNil()
}

func (state *fathomryState) bindContext(ctx context.Context) (context.Context, func()) {
	if bind := state.control.BindContext; bind != nil {
		return bind(ctx)
	}
	return ctx, func() {}
}

func (state *fathomryState) beginIO() {
	state.mu.Lock()
	state.active++
	state.mu.Unlock()
}

func (association *fathomryAssociation) Close() error {
	association.once.Do(func() {
		for _, closer := range association.closers {
			association.err = errors.Join(association.err, closer.Close())
		}
	})
	return association.err
}
