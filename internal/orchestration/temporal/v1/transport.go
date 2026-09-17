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

package temporal

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// gRPC may return from TLS cancellation while its native handshake goroutine is
// still in a caller hook (including opaque x509 CertPool constraints). This gate
// owns the whole native handshake and later TLS I/O, not a guessed list of hooks.
type transportLifetime struct {
	mu                sync.Mutex
	changed           *sync.Cond
	closing, released bool
	active            int
}

func (scope *transportLifetime) enter(cleanup bool) bool {
	scope.mu.Lock()
	defer scope.mu.Unlock()
	if scope.released || scope.closing && !cleanup {
		return false
	}
	scope.active++
	return true
}

func (scope *transportLifetime) leave() {
	scope.mu.Lock()
	scope.active--
	if scope.changed != nil {
		scope.changed.Broadcast()
	}
	scope.mu.Unlock()
}

func (scope *transportLifetime) close(release func()) {
	scope.mu.Lock()
	scope.closing = true
	scope.mu.Unlock()
	defer func() {
		scope.mu.Lock()
		defer scope.mu.Unlock()
		if scope.changed == nil {
			scope.changed = sync.NewCond(&scope.mu)
		}
		for scope.active != 0 {
			scope.changed.Wait()
		}
		scope.released = true
	}()
	release()
}

func (scope *transportLifetime) secureDialOptions(options []grpc.DialOption, config *tls.Config) []grpc.DialOption {
	// The SDK applies mTLS Credentials after plugin configuration. Defer the
	// native config clone until gRPC first uses credentials, after that step.
	native := sync.OnceValue(func() credentials.TransportCredentials { return credentials.NewTLS(config) })
	return append(options, grpc.WithTransportCredentials(&ownedTransportCredentials{native: native, scope: scope}))
}

type ownedTransportCredentials struct {
	native func() credentials.TransportCredentials
	scope  *transportLifetime
}

func (owned *ownedTransportCredentials) Info() credentials.ProtocolInfo { return owned.native().Info() }

func (owned *ownedTransportCredentials) Clone() credentials.TransportCredentials {
	cloned := owned.native().Clone()
	return &ownedTransportCredentials{native: func() credentials.TransportCredentials { return cloned }, scope: owned.scope}
}

func (owned *ownedTransportCredentials) OverrideServerName(name string) error {
	return owned.native().OverrideServerName(name)
}

func (owned *ownedTransportCredentials) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, failure(ErrAuthority, "client-only-tls")
}

func (owned *ownedTransportCredentials) ClientHandshake(ctx context.Context, authority string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	if !owned.scope.enter(false) {
		_ = raw.Close()
		return nil, nil, failure(ErrAuthority, "closed-tls-owner")
	}
	defer owned.scope.leave()
	finished := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(finished); _ = raw.Close() })
	defer func() {
		if !stop() {
			<-finished
		}
	}()
	// Native TLS retains its verification, ALPN, SPIFFE and syscall wrapping.
	// Closing raw interrupts network waits; it does not claim user hooks ended.
	connection, auth, err := owned.native().ClientHandshake(context.WithoutCancel(ctx), authority, raw)
	if err != nil {
		return nil, nil, err
	}
	if ctx.Err() != nil {
		_ = connection.Close()
		return nil, nil, ctx.Err()
	}
	view := &ownedTLSConnection{native: connection, raw: raw, scope: owned.scope}
	if syscalls, ok := connection.(syscall.Conn); ok {
		return &ownedSyscallConnection{ownedTLSConnection: view, syscalls: syscalls}, auth, nil
	}
	return view, auth, nil
}

type ownedTLSConnection struct {
	native, raw net.Conn
	scope       *transportLifetime
}

func (connection *ownedTLSConnection) Read(buffer []byte) (int, error) {
	if !connection.scope.enter(false) {
		return 0, net.ErrClosed
	}
	defer connection.scope.leave()
	return connection.native.Read(buffer)
}

func (connection *ownedTLSConnection) Write(buffer []byte) (int, error) {
	if !connection.scope.enter(false) {
		return 0, net.ErrClosed
	}
	defer connection.scope.leave()
	return connection.native.Write(buffer)
}

func (connection *ownedTLSConnection) Close() error {
	if !connection.scope.enter(true) {
		return connection.raw.Close()
	}
	defer connection.scope.leave()
	return connection.native.Close()
}

func (connection *ownedTLSConnection) LocalAddr() net.Addr { return connection.native.LocalAddr() }

func (connection *ownedTLSConnection) RemoteAddr() net.Addr { return connection.native.RemoteAddr() }

func (connection *ownedTLSConnection) SetDeadline(deadline time.Time) error {
	return connection.native.SetDeadline(deadline)
}

func (connection *ownedTLSConnection) SetReadDeadline(deadline time.Time) error {
	return connection.native.SetReadDeadline(deadline)
}

func (connection *ownedTLSConnection) SetWriteDeadline(deadline time.Time) error {
	return connection.native.SetWriteDeadline(deadline)
}

type ownedSyscallConnection struct {
	*ownedTLSConnection
	syscalls syscall.Conn
}

func (connection *ownedSyscallConnection) SyscallConn() (syscall.RawConn, error) {
	return connection.syscalls.SyscallConn()
}
