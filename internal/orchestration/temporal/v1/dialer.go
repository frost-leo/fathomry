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
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// MaxTransportConnections bounds in-progress explicit dials and transferred,
// not-yet-closed connections within each source or Worker transport scope.
const MaxTransportConnections = 32

type ownedDialConnection struct {
	native net.Conn
	scope  *transportLifetime
	once   sync.Once
	err    error
	closed atomic.Bool
}

func (scope *transportLifetime) dial(native func(context.Context, string) (net.Conn, error), ctx context.Context, address string) (net.Conn, error) {
	scope.mu.Lock()
	if scope.closing || scope.released {
		scope.mu.Unlock()
		return nil, net.ErrClosed
	}
	if scope.dialing+len(scope.connections) >= MaxTransportConnections {
		scope.mu.Unlock()
		return nil, failure(ErrLimit, "dial-connections")
	}
	scope.dialing++
	scope.active++
	scope.mu.Unlock()
	pending := true
	defer func() {
		scope.mu.Lock()
		if pending {
			scope.dialing--
		}
		scope.mu.Unlock()
		scope.leave()
	}()
	connection, err := native(ctx, address)
	if nilRuntime(connection) {
		if err == nil {
			err = failure(ErrConnect, "nil-dial-connection")
		}
		return nil, err
	}
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		_ = connection.Close()
		return nil, err
	}
	owned := &ownedDialConnection{native: connection, scope: scope}
	scope.mu.Lock()
	if scope.closing || scope.released {
		scope.mu.Unlock()
		_ = connection.Close()
		return nil, net.ErrClosed
	}
	if scope.connections == nil {
		scope.connections = make(map[*ownedDialConnection]struct{})
	}
	scope.dialing--
	pending = false
	scope.connections[owned] = struct{}{}
	scope.mu.Unlock()
	return owned, nil
}

func (connection *ownedDialConnection) Read(buffer []byte) (int, error) {
	if connection.closed.Load() || !connection.scope.enter(false) {
		return 0, net.ErrClosed
	}
	defer connection.scope.leave()
	return connection.native.Read(buffer)
}

func (connection *ownedDialConnection) Write(buffer []byte) (int, error) {
	if connection.closed.Load() || !connection.scope.enter(false) {
		return 0, net.ErrClosed
	}
	defer connection.scope.leave()
	return connection.native.Write(buffer)
}

func (connection *ownedDialConnection) Close() error {
	connection.once.Do(func() {
		connection.closed.Store(true)
		if !connection.scope.enter(true) {
			return
		}
		defer connection.scope.leave()
		connection.err = connection.native.Close()
		connection.scope.mu.Lock()
		delete(connection.scope.connections, connection)
		connection.scope.mu.Unlock()
	})
	return connection.err
}

type connectionAddress struct{ network, address string }

func (address connectionAddress) Network() string { return address.network }
func (address connectionAddress) String() string  { return address.address }

func (connection *ownedDialConnection) address(local bool) net.Addr {
	if connection.closed.Load() || !connection.scope.enter(false) {
		return connectionAddress{}
	}
	defer connection.scope.leave()
	var address net.Addr
	if local {
		address = connection.native.LocalAddr()
	} else {
		address = connection.native.RemoteAddr()
	}
	if nilRuntime(address) {
		return connectionAddress{}
	}
	return connectionAddress{network: address.Network(), address: address.String()}
}

func (connection *ownedDialConnection) LocalAddr() net.Addr  { return connection.address(true) }
func (connection *ownedDialConnection) RemoteAddr() net.Addr { return connection.address(false) }

func (connection *ownedDialConnection) SetDeadline(deadline time.Time) error {
	if connection.closed.Load() || !connection.scope.enter(false) {
		return net.ErrClosed
	}
	defer connection.scope.leave()
	return connection.native.SetDeadline(deadline)
}

func (connection *ownedDialConnection) SetReadDeadline(deadline time.Time) error {
	if connection.closed.Load() || !connection.scope.enter(false) {
		return net.ErrClosed
	}
	defer connection.scope.leave()
	return connection.native.SetReadDeadline(deadline)
}

func (connection *ownedDialConnection) SetWriteDeadline(deadline time.Time) error {
	if connection.closed.Load() || !connection.scope.enter(false) {
		return net.ErrClosed
	}
	defer connection.scope.leave()
	return connection.native.SetWriteDeadline(deadline)
}
