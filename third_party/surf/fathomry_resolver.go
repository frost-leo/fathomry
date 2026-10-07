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
	"errors"
	"net"
	"strings"
)

// FathomrySetResolver retains DNS callbacks and returned connections independently
// of a stopped dial waiter. Only panic disables the route: net.Resolver otherwise
// converts non-context callback errors to strings and can hide their causes.
func (client *Client) FathomrySetResolver(resolver *net.Resolver) error {
	if resolver == nil || client.builder != nil {
		return errors.New("surf: resolver requires unused client")
	}
	selected := &net.Resolver{PreferGo: resolver.PreferGo, StrictErrors: resolver.StrictErrors}
	if dial := resolver.Dial; dial != nil {
		selected.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			state := client.fathomry
			finish, err := state.enter(ctx)
			if err != nil {
				return nil, err
			}
			release, err := state.reserve(strings.HasPrefix(network, "udp"))
			if err != nil {
				finish()
				return nil, err
			}
			handed := false
			defer func() {
				if !handed {
					release()
					finish()
				}
			}()
			var raw net.Conn
			err = fathomryInvoke(func() error { raw, err = dial(ctx, network, address); return err })
			if errors.Is(err, ErrFathomryCallback) {
				state.callbackFailed(err)
			}
			if fathomryNil(raw) {
				raw = nil
				if err == nil {
					err = errors.New("surf: DNS dial returned nil connection")
				}
			}
			if err != nil {
				var cleanup error
				if raw != nil {
					cleanup = fathomryInvoke(raw.Close)
					state.remember(cleanup)
				}
				return nil, fathomryFailure(err, cleanup)
			}
			ready := make(chan struct{})
			var stop func() bool
			connection := &fathomryConn{Conn: raw, state: state, release: func() {
				<-ready
				stop()
				release()
				finish()
			}}
			state.mu.Lock()
			state.conns[connection] = struct{}{}
			closing := state.closing || state.failed != nil || state.cleanup != nil
			state.mu.Unlock()
			handed = true
			stop = context.AfterFunc(ctx, func() { _ = connection.Close() })
			close(ready)
			if closing || ctx.Err() != nil {
				return nil, fathomryFailure(errors.Join(ErrFathomryClosed, ctx.Err(), context.Cause(ctx)), connection.Close())
			}
			if packet, ok := raw.(net.PacketConn); ok {
				return &fathomryResolverPacket{fathomryConn: connection, packet: packet}, nil
			}
			return connection, nil
		}
	}
	client.dialer.Resolver = selected
	return nil
}

type fathomryResolverPacket struct {
	*fathomryConn
	packet net.PacketConn
}

func (connection *fathomryResolverPacket) ReadFrom(buffer []byte) (int, net.Addr, error) {
	connection.ioMu.Lock()
	if connection.closed {
		connection.ioMu.Unlock()
		return 0, nil, net.ErrClosed
	}
	connection.state.beginIO()
	connection.ioMu.Unlock()
	defer connection.state.leave()
	return connection.packet.ReadFrom(buffer)
}

func (connection *fathomryResolverPacket) WriteTo(buffer []byte, address net.Addr) (int, error) {
	connection.ioMu.Lock()
	if connection.closed {
		connection.ioMu.Unlock()
		return 0, net.ErrClosed
	}
	connection.state.beginIO()
	connection.ioMu.Unlock()
	defer connection.state.leave()
	return connection.packet.WriteTo(buffer, address)
}
