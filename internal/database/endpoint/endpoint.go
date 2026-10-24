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

package endpoint

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"syscall"
)

var errAddress = errors.New("database endpoint: invalid address")

// ValidHost preserves literal-IP admission unless DNS was explicitly authorized.
// DNS accepts ASCII labels of 1..63 bytes, at most 253 bytes plus one terminal dot.
// Resolver search domains/hosts are deployment inputs, not consulted here.
func ValidHost(host string, resolveDNS bool) bool {
	if address, err := netip.ParseAddr(host); err == nil {
		return address.Zone() == "" && !address.Unmap().IsUnspecified()
	}
	if !resolveDNS || len(host) > 254 {
		return false
	}
	name := strings.TrimSuffix(host, ".")
	if len(name) == 0 || len(name) > 253 {
		return false
	}
	for label := range strings.SplitSeq(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-') {
				return false
			}
		}
	}
	return true
}

// DialContext uses Go's context-aware resolver/dialer without mutating globals.
// DNS may select among addresses before connection; every candidate must be a
// specified IP. It supplies no reconnect or post-connect failover mechanism.
func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if ctx == nil {
		return nil, errAddress
	}
	dialer := newDialer()
	return dialer.DialContext(ctx, network, address)
}
func newDialer() net.Dialer {
	return net.Dialer{Resolver: &net.Resolver{PreferGo: true}, ControlContext: func(_ context.Context, network, address string, _ syscall.RawConn) error {
		if network == "unix" {
			return nil
		}
		host, _, err := net.SplitHostPort(address)
		if err != nil || !ValidHost(host, false) {
			return errAddress
		}
		return nil
	}}
}
