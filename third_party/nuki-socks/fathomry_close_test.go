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

package socks

import (
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
)

type retryPacket struct {
	net.PacketConn
	fail  atomic.Bool
	cause error
}

func (packet *retryPacket) Close() error {
	if packet.fail.Load() {
		return packet.cause
	}
	return packet.PacketConn.Close()
}

func TestPacketCloseRetriesAndJoinsControlMonitor(t *testing.T) {
	raw, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	cause := errors.New("packet-close-failed")
	packet := &retryPacket{PacketConn: raw, cause: cause}
	packet.fail.Store(true)
	control, peer := net.Pipe()
	defer control.Close()
	defer peer.Close()
	owned := &PacketConn{PacketConn: packet, control: control, done: make(chan struct{})}
	go func() {
		defer close(owned.done)
		_, _ = io.Copy(io.Discard, control)
		owned.monitorClose = packet.Close()
	}()
	if err := owned.Close(); !errors.Is(err, cause) || owned.ReleaseConfirmed() {
		t.Fatal("failed packet closure fabricated release", err)
	}
	<-owned.done
	packet.fail.Store(false)
	if err := owned.Close(); !errors.Is(err, cause) || !owned.ReleaseConfirmed() {
		t.Fatal("monitor history was lost or blocked positive release", err)
	}
	if _, err := raw.WriteTo([]byte("closed"), raw.LocalAddr()); !errors.Is(err, net.ErrClosed) {
		t.Fatal("raw packet socket was not closed", err)
	}
	if _, err := peer.Write([]byte("closed")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("control connection was not closed", err)
	}
	if err := owned.Close(); err != nil {
		t.Fatal("idempotent confirmed release", err)
	}
}
