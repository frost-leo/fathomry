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

package managed

import (
	"context"
	"net"
	"time"

	"github.com/sardanioss/quic-go"
	tls "github.com/sardanioss/utls"
)

// Controls are immutable source authority used only by the managed entry point.
type Controls struct {
	Resolve          func(context.Context, string) ([]net.IP, error)
	ECH              func(context.Context, string) ([]byte, error)
	InvalidateECH    func(string)
	DialTCP          func(context.Context, string) (net.Conn, error)
	ListenUDP        func(string, *net.UDPAddr) (*net.UDPConn, error)
	DialUDP          func(string, *net.UDPAddr, *net.UDPAddr) (*net.UDPConn, error)
	CloseUDP         func(*net.UDPConn) error
	AcquireQUIC      func() (func(), error)
	AcquireControl   func() (func(), error)
	ProxyURL         string
	ProxyTLS         *tls.Config
	DisableECH       bool
	MaxAddressRaces  int
	MaxHeaderBytes   int
	AddressRaceDelay time.Duration
}

// DialQUIC retains native connection work until its actual terminal context.
func (controls *Controls) DialQUIC(ctx context.Context, dial func(context.Context) (*quic.Conn, error)) (*quic.Conn, error) {
	if controls == nil || controls.AcquireQUIC == nil {
		return dial(ctx)
	}
	release, err := controls.AcquireQUIC()
	if err != nil {
		return nil, err
	}
	conn, err := dial(ctx)
	if conn == nil {
		release()
		return nil, err
	}
	go func() { <-conn.Context().Done(); release() }()
	if err != nil {
		_ = conn.CloseWithError(0, "")
	}
	return conn, err
}
