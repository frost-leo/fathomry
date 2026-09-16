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
	"net"

	utls "github.com/refraction-networking/utls"
)

func guardCallbacks(native *NativeOptionsV1) {
	if dial := native.DialContext; dial != nil {
		native.DialContext = guardedDial(dial)
	}
	if listen := native.ListenPacket; listen != nil {
		native.ListenPacket = func(ctx context.Context, network, address string) (value net.PacketConn, err error) {
			err = invoke("packet-listener", func() error { value, err = listen(ctx, network, address); return err })
			return value, err
		}
	}
	if factory := native.HelloSpecFactory; factory != nil {
		native.HelloSpecFactory = func(ctx context.Context) (spec utls.ClientHelloSpec, err error) {
			err = invoke("hello-factory", func() error { spec, err = factory(ctx); return err })
			return spec, err
		}
	}
	if resolver := native.Resolver; resolver != nil && resolver.Dial != nil {
		native.Resolver = &net.Resolver{
			PreferGo: resolver.PreferGo, StrictErrors: resolver.StrictErrors,
			Dial: guardedDial(resolver.Dial),
		}
	}
}

func guardedDial(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (value net.Conn, err error) {
		err = invoke("dial", func() error { value, err = dial(ctx, network, address); return err })
		return value, err
	}
}
