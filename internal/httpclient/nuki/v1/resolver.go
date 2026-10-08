// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package nuki

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

func (owner *owner) guardResolver(selected *net.Resolver) *net.Resolver {
	resolver := &net.Resolver{PreferGo: selected.PreferGo, StrictErrors: selected.StrictErrors}
	if selected.Dial == nil {
		return resolver
	}
	resolver.Dial = func(ctx context.Context, network, address string) (result net.Conn, err error) {
		base := &baseDialer{owner}
		if err = base.reserve(); err != nil {
			return nil, err
		}
		defer owner.socketWork.Done()
		if !owner.callbacks.enter() {
			base.publish(nil)
			return nil, failure(ErrState, "resolver")
		}
		defer owner.callbacks.leave()
		var raw net.Conn
		defer func() {
			if recovered := recover(); recovered != nil {
				err = callbackFailure(recovered)
				owner.recordCleanup(err)
			}
			if nilObject(raw) {
				raw = nil
			}
			var packet net.PacketConn
			if raw != nil {
				packet, _ = raw.(net.PacketConn)
				raw = &resolverConnection{Conn: raw}
			}
			owned := base.publish(raw)
			if err != nil || ctx.Err() != nil {
				err = errors.Join(err, ctx.Err(), context.Cause(ctx))
				if owned != nil {
					err = errors.Join(err, owned.Close())
				}
				result = nil
			} else if raw == nil {
				err = failure(ErrInput, "nil-resolver-connection")
			} else {
				result = &tcpSocket{Conn: raw, socket: owned}
				if packet != nil {
					result = &resolverPacket{tcpSocket: result.(*tcpSocket), connection: raw.(*resolverConnection), packet: packet}
				}
			}
		}()
		raw, err = selected.Dial(ctx, network, address)
		return nil, err
	}
	return resolver
}

type resolverPacket struct {
	*tcpSocket
	connection *resolverConnection
	packet     net.PacketConn
}

func (conn *resolverPacket) ReadFrom(data []byte) (int, net.Addr, error) {
	if err := conn.connection.enter(); err != nil {
		return 0, nil, err
	}
	defer conn.connection.work.Done()
	return conn.packet.ReadFrom(data)
}
func (conn *resolverPacket) WriteTo(data []byte, address net.Addr) (int, error) {
	if err := conn.connection.enter(); err != nil {
		return 0, err
	}
	defer conn.connection.work.Done()
	return conn.packet.WriteTo(data, address)
}

type resolverConnection struct {
	net.Conn
	mu     sync.Mutex
	closed bool
	work   sync.WaitGroup
}

func (conn *resolverConnection) enter() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.closed {
		return net.ErrClosed
	}
	conn.work.Add(1)
	return nil
}
func (conn *resolverConnection) Read(data []byte) (int, error) {
	if err := conn.enter(); err != nil {
		return 0, err
	}
	defer conn.work.Done()
	return conn.Conn.Read(data)
}
func (conn *resolverConnection) Write(data []byte) (int, error) {
	if err := conn.enter(); err != nil {
		return 0, err
	}
	defer conn.work.Done()
	return conn.Conn.Write(data)
}
func (conn *resolverConnection) SetDeadline(value time.Time) error {
	if err := conn.enter(); err != nil {
		return err
	}
	defer conn.work.Done()
	return conn.Conn.SetDeadline(value)
}
func (conn *resolverConnection) SetReadDeadline(value time.Time) error {
	if err := conn.enter(); err != nil {
		return err
	}
	defer conn.work.Done()
	return conn.Conn.SetReadDeadline(value)
}
func (conn *resolverConnection) SetWriteDeadline(value time.Time) error {
	if err := conn.enter(); err != nil {
		return err
	}
	defer conn.work.Done()
	return conn.Conn.SetWriteDeadline(value)
}
func (conn *resolverConnection) Close() error {
	conn.mu.Lock()
	conn.closed = true
	conn.mu.Unlock()
	err := conn.Conn.Close()
	conn.work.Wait()
	return err
}
