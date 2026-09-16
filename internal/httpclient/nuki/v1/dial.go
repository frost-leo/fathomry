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
	"errors"
	"io"
	"net"

	"github.com/nukilabs/tlsclient/proxy"
)

// Native HTTP pooling may detach setup from request cancellation. Bound the
// entire routing task before proxy serialization, not just its eventual socket.
type routeDialer struct {
	owner  *owner
	native proxy.ContextDialer
}

func (dialer *routeDialer) SupportHTTP3() bool { return dialer.native.SupportHTTP3() }
func (dialer *routeDialer) enter(ctx context.Context) error {
	if ctx.Err() != nil {
		return errors.Join(ctx.Err(), context.Cause(ctx))
	}
	dialer.owner.mu.Lock()
	defer dialer.owner.mu.Unlock()
	if dialer.owner.closing {
		return failure(ErrState, "native-dial")
	}
	if dialer.owner.nativeDials >= dialer.owner.settings.MaxConnections {
		return failure(ErrCapacity, "native-dials")
	}
	dialer.owner.nativeDials++
	return nil
}
func (dialer *routeDialer) leave() {
	dialer.owner.mu.Lock()
	dialer.owner.nativeDials--
	dialer.owner.mu.Unlock()
}
func (dialer *routeDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if err := dialer.enter(ctx); err != nil {
		return nil, err
	}
	defer dialer.leave()
	return dialer.native.DialContext(ctx, network, address)
}
func (dialer *routeDialer) ListenPacket(ctx context.Context, network, address string) (net.PacketConn, error) {
	if err := dialer.enter(ctx); err != nil {
		return nil, err
	}
	defer dialer.leave()
	return dialer.native.ListenPacket(ctx, network, address)
}
func (dialer *routeDialer) Close() error {
	if closer, ok := dialer.native.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
