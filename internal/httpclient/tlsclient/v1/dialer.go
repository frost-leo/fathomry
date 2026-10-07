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
	"net"
	"slices"
	"strings"
	"syscall"
)

func copyLocalAddr(input *net.TCPAddr) (*net.TCPAddr, error) {
	if input == nil {
		return nil, nil
	}
	if len(input.IP) != 0 && len(input.IP) != net.IPv4len && len(input.IP) != net.IPv6len ||
		input.Port < 0 || input.Port > 65535 || len(input.Zone) > 1024 || strings.ContainsAny(input.Zone, "\r\n\x00") {
		return nil, failure(ErrInput, "local-address")
	}
	value := *input
	value.IP = slices.Clone(input.IP)
	return &value, nil
}
func copyDialer(input *net.Dialer) (*net.Dialer, error) {
	if input == nil {
		return nil, nil
	}
	value := *input
	if input.LocalAddr != nil {
		address, ok := input.LocalAddr.(*net.TCPAddr)
		if !ok || address == nil {
			return nil, failure(ErrInput, "dialer-local-address")
		}
		cloned, err := copyLocalAddr(address)
		if err != nil {
			return nil, err
		}
		value.LocalAddr = cloned
	}
	if input.Resolver != nil {
		value.Resolver = &net.Resolver{PreferGo: input.Resolver.PreferGo, StrictErrors: input.Resolver.StrictErrors, Dial: input.Resolver.Dial}
	}
	return &value, nil
}

func (own *owner) dialer() net.Dialer {
	cloned, _ := copyDialer(own.native.Dialer)
	value := *cloned
	value.Timeout = own.settings.Timeout
	control, controlled := value.Control, value.ControlContext
	if control != nil || controlled != nil {
		value.Control = nil
		value.ControlContext = func(ctx context.Context, network, address string, connection syscall.RawConn) error {
			done, err := own.enterCallback(ctx)
			if err != nil {
				return err
			}
			defer done()
			if controlled != nil {
				return controlled(ctx, network, address, connection)
			}
			return control(network, address, connection)
		}
	}
	if resolver := value.Resolver; resolver != nil && resolver.Dial != nil {
		dial := resolver.Dial
		resolver.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			done, err := own.enterCallback(ctx)
			if err != nil {
				return nil, err
			}
			defer done()
			return dial(ctx, network, address)
		}
	}
	return value
}
