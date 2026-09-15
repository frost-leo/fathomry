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

package httpcloak

import (
	"errors"
	"net"
	"sync"
	"testing"
)

type failingCloseConn struct {
	net.Conn
	mu    sync.Mutex
	cause error
	calls int
}

func (conn *failingCloseConn) Close() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.calls++
	if conn.calls == 1 {
		return conn.cause
	}
	return conn.Conn.Close()
}
func TestSourceCleanupContinuationRetriesOwnedSocket(t *testing.T) {
	raw, peer := net.Pipe()
	defer peer.Close()
	defer raw.Close()
	cause := errors.New("test close failure")
	native := &failingCloseConn{Conn: raw, cause: cause}
	own := &owner{settings: defaults(OptionsV1{}), done: make(chan struct{}), sockets: make(map[*trackedConn]struct{})}
	release, err := own.acquireConnection()
	if err != nil {
		t.Fatal(err)
	}
	conn := &trackedConn{Conn: native, owner: own, release: release}
	own.sockets[conn] = struct{}{}
	first := own.release(testContext(t))
	if first.Released || first.Continue == nil || !errors.Is(first.Err, cause) {
		t.Fatal("failed cleanup lost its obligation or cause")
	}
	final := first.Continue(testContext(t))
	if !final.Quiescent || !final.Released || !errors.Is(final.Err, cause) || native.calls != 2 {
		t.Fatal("cleanup continuation did not complete the original ownership")
	}
}
