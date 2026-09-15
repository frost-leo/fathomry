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

package tls_client

import (
	"context"
	"net"
	"testing"

	http "github.com/bogdanfinn/fhttp"
)

func TestFathomryStaleRetirementPreservesReplacementKind(t *testing.T) {
	const address = "origin.invalid:443"
	stale, replacement := &http.Transport{}, &http.Transport{}
	rt := &roundTripper{cachedTransports: map[string]http.RoundTripper{address: replacement}, cachedKinds: map[string]transportKind{address: transportHTTP1}}
	rt.dropCachedTransport(address, stale)
	kind, present := rt.cachedKind(address)
	if rt.cachedTransports[address] != replacement || !present || kind != transportHTTP1 {
		t.Fatal("late old failure erased the replacement's protocol state")
	}
	rt.dropCachedTransport(address, replacement)
	_, present = rt.cachedKind(address)
	if rt.cachedTransports[address] != nil || present {
		t.Fatal("current generation retirement left stale protocol state")
	}
}

func TestFathomryCleartextDialAppliesIPFamily(t *testing.T) {
	for _, family := range []string{"tcp", "tcp4", "tcp6"} {
		t.Run(family, func(t *testing.T) {
			local, peer := net.Pipe()
			defer local.Close()
			defer peer.Close()
			observed := ""
			rt := &roundTripper{compat: newCompatibilityState(), disableIPV4: family == "tcp6", disableIPV6: family == "tcp4", dialer: &customContextDialer{dialContext: func(_ context.Context, network, _ string) (net.Conn, error) {
				observed = network
				return local, nil
			}}}
			conn, err := rt.dial(context.Background(), "tcp", "not-contacted.invalid:80")
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if observed != family {
				t.Fatal("cleartext dial ignored IP-family selection", observed)
			}
		})
	}
}
