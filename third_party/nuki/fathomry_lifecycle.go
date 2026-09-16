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

package tlsclient

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/nukilabs/quic-go"
	"github.com/nukilabs/quic-go/http3"
	"github.com/nukilabs/tlsclient/proxy"
)

// WithDialer transfers a configuration-frozen dialer's use to this client.
// Any Close method is invoked during terminal transport Close, not per request.
// Callers must not share owning dialers between independent clients.
func WithDialer(dialer proxy.ContextDialer) Option {
	return func(client *Client) { client.dialer = dialer }
}

// Close seals the original transport, cancels and joins native dials and H3
// warming, and closes owned sockets and proxy sessions. It is cooperative with
// custom callbacks; it must not be called reentrantly from a native callback.
func (client *Client) Close() error {
	if closer, ok := client.Transport.(io.Closer); ok {
		return closer.Close()
	}
	return errors.New("tlsclient: transport has no terminal close")
}

type nativeLife struct {
	mu         sync.Mutex
	closed     bool
	stopped    bool
	ctx        context.Context
	cancel     context.CancelFunc
	work       sync.WaitGroup
	packetWork sync.WaitGroup
	tcp        map[*nativeConn]struct{}
	packets    map[*nativePacket]struct{}
	cleanup    error
}

func newNativeLife() *nativeLife {
	ctx, cancel := context.WithCancel(context.Background())
	return &nativeLife{ctx: ctx, cancel: cancel, tcp: make(map[*nativeConn]struct{}), packets: make(map[*nativePacket]struct{})}
}

func (life *nativeLife) enter() error {
	life.mu.Lock()
	defer life.mu.Unlock()
	if life.closed {
		return ErrClientClosed
	}
	if life.cleanup != nil {
		return life.cleanup
	}
	life.work.Add(1)
	return nil
}

func (life *nativeLife) context(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(life.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}

func (life *nativeLife) record(err error) {
	if err == nil {
		return
	}
	life.mu.Lock()
	life.cleanup = errors.Join(life.cleanup, err)
	life.mu.Unlock()
}

func (life *nativeLife) connection(conn net.Conn) net.Conn {
	owned := &nativeConn{Conn: conn, life: life}
	life.mu.Lock()
	life.tcp[owned] = struct{}{}
	closed := life.closed
	life.mu.Unlock()
	if closed {
		_ = owned.Close()
	}
	return owned
}

type nativeConn struct {
	net.Conn
	life     *nativeLife
	mu       sync.Mutex
	released bool
	reported bool
}

func (conn *nativeConn) Close() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.released {
		return nil
	}
	err := conn.Conn.Close()
	if entirelyClosed(err) {
		err = nil
	}
	if err == nil {
		conn.released = true
		conn.life.mu.Lock()
		delete(conn.life.tcp, conn)
		conn.life.mu.Unlock()
	} else if !conn.reported {
		conn.reported = true
		conn.life.record(err)
	}
	return err
}

type nativePacket struct {
	socket    net.PacketConn
	transport *quic.Transport
	conn      *quic.Conn
	life      *nativeLife
	mu        sync.Mutex
	released  bool
	reported  bool
}

func (packet *nativePacket) close() error {
	packet.mu.Lock()
	defer packet.mu.Unlock()
	if packet.released {
		return nil
	}
	var result error
	if packet.conn != nil {
		result = packet.conn.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeNoError), "")
	}
	transportErr, socketErr := packet.transport.Close(), packet.socket.Close()
	confirmed := entirelyClosed(result) && entirelyClosed(transportErr) && entirelyClosed(socketErr)
	if released, ok := packet.socket.(interface{ ReleaseConfirmed() bool }); ok {
		confirmed = entirelyClosed(result) && entirelyClosed(transportErr) && released.ReleaseConfirmed()
	}
	result = errors.Join(result, transportErr, socketErr)
	if entirelyClosed(result) {
		result = nil
	}
	if confirmed {
		packet.released = true
		packet.life.mu.Lock()
		delete(packet.life.packets, packet)
		packet.life.mu.Unlock()
	}
	if result != nil && !packet.reported {
		packet.reported = true
		packet.life.record(result)
	}
	return result
}

func entirelyClosed(err error) bool {
	if err == nil || err == net.ErrClosed || err == io.ErrClosedPipe {
		return true
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		causes := wrapped.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !entirelyClosed(cause) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		cause := wrapped.Unwrap()
		return cause != nil && entirelyClosed(cause)
	}
	return false
}

// Close is terminal; CloseIdleConnections retains the native reusable-client API.
func (rt *RoundTripper) Close() error {
	rt.closeMu.Lock()
	defer rt.closeMu.Unlock()
	life := rt.life
	life.mu.Lock()
	life.closed = true
	life.cancel()
	life.mu.Unlock()
	if rt.racer != nil {
		rt.racer.shutdown()
	}
	rt.CloseIdleConnections()
	life.mu.Lock()
	sockets := make([]*nativeConn, 0, len(life.tcp))
	for conn := range life.tcp {
		sockets = append(sockets, conn)
	}
	life.mu.Unlock()
	for _, conn := range sockets {
		_ = conn.Close()
	}
	life.work.Wait()
	life.mu.Lock()
	packets := make([]*nativePacket, 0, len(life.packets))
	for packet := range life.packets {
		packets = append(packets, packet)
	}
	life.mu.Unlock()
	for _, packet := range packets {
		_ = packet.close()
	}
	life.packetWork.Wait()
	if closer, ok := rt.dialer.(io.Closer); ok {
		life.record(closer.Close())
	}
	life.mu.Lock()
	defer life.mu.Unlock()
	life.stopped = true
	return life.cleanup
}

// ReleaseConfirmed reports positive terminal resource accounting separately
// from historical Close errors. It never equates a later nil error with release.
func (rt *RoundTripper) ReleaseConfirmed() bool {
	rt.life.mu.Lock()
	defer rt.life.mu.Unlock()
	return rt.life.closed && rt.life.stopped && len(rt.life.tcp) == 0 && len(rt.life.packets) == 0
}
