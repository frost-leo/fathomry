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

package pgx

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestDNSOptInAndPinnedCancelPeer(t *testing.T) {
	for _, secure := range []bool{false, true} {
		peer := newProtocolPeer(t, secure)
		options := peer.options()
		options.Address = "localhost"
		if _, err := Select(options); !errors.Is(err, ErrInput) {
			t.Fatal("DNS enabled by default")
		}
		options.ResolveDNS = true
		if _, err := Select(options); err != nil {
			t.Fatal("DNS opt-in rejected", err)
		}
		config, err := nativeConfig(defaults(options))
		if err != nil {
			t.Fatal(err)
		}
		if config.Host != "localhost" {
			t.Fatal("hostname resolved during preparation")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		connection, err := connect(ctx, config, options.RootCAPEM)
		if err != nil {
			cancel()
			t.Fatal("DNS connect failed", err)
		}
		selected := connection.native.PgConn().Conn().RemoteAddr().String()
		t.Cleanup(func() {
			defer cancel()
			cleanup, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := connection.close(cleanup); err != nil {
				t.Error(err)
			}
		})
		_, port, _ := net.SplitHostPort(selected)
		original := connection.wire.dialer
		calls := 0
		connection.wire.dialer = func(ctx context.Context, network, address string) (net.Conn, error) {
			calls++
			if address != selected {
				t.Error("cancellation was re-resolved or redirected")
			}
			return original(ctx, network, address)
		}
		if err := connection.native.PgConn().CancelRequest(ctx); err != nil {
			t.Fatal("connected peer cancellation refused", err)
		}
		if calls != 1 {
			t.Fatal("unexpected cancellation dialing", calls)
		}
		if socket, err := connection.wire.dial(ctx, "tcp", net.JoinHostPort("localhost", port)); socket != nil || !errors.Is(err, ErrState) {
			if socket != nil {
				socket.Close()
			}
			t.Fatal("DNS name remained authorized after peer selection", err)
		}
		if err := connection.close(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
}
