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

package udpbara

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestFathomryCanceledGreetingReleasesAndJoins(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	greeting := make(chan struct{})
	peerDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			peerDone <- err
			return
		}
		defer conn.Close()
		var header [3]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			peerDone <- err
			return
		}
		close(greeting)
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		var one [1]byte
		_, err = conn.Read(one[:])
		peerDone <- err
	}()
	tunnel, err := NewTunnel("socks5://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("test cancel cause")
	done := make(chan error, 1)
	go func() { done <- tunnel.ConnectContext(ctx) }()
	select {
	case <-greeting:
	case <-time.After(time.Second):
		t.Fatal("SOCKS fixture not reached")
	}
	cancel(cause)
	if err := tunnel.FathomryClose(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
			t.Fatal("handshake cancellation cause lost")
		}
	case <-time.After(time.Second):
		t.Fatal("handshake not joined")
	}
	if err := <-peerDone; !errors.Is(err, io.EOF) {
		t.Fatal("control socket survived cancellation", err)
	}
}
func TestFathomryEstablishedRelayClosesAllWorkers(t *testing.T) {
	relay, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		var greeting [3]byte
		if _, err = io.ReadFull(conn, greeting[:]); err != nil {
			done <- err
			return
		}
		_, _ = conn.Write([]byte{5, 0})
		var command [10]byte
		if _, err = io.ReadFull(conn, command[:]); err != nil {
			done <- err
			return
		}
		port := relay.LocalAddr().(*net.UDPAddr).Port
		_, _ = conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, byte(port >> 8), byte(port)})
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		var one [1]byte
		_, err = conn.Read(one[:])
		done <- err
	}()
	tunnel, err := NewTunnel("socks5://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tunnel.ConnectContext(ctx); err != nil {
		t.Fatal(err)
	}
	conn, err := tunnel.DialContext(ctx, net.JoinHostPort("127.0.0.1", strconv.Itoa(relay.LocalAddr().(*net.UDPAddr).Port)))
	if err != nil {
		t.Fatal(err)
	}
	if err := tunnel.FathomryClose(); err != nil {
		t.Fatal(err)
	}
	if err := conn.PacketConn().SetReadDeadline(time.Time{}); !errors.Is(err, net.ErrClosed) {
		t.Fatal("app UDP socket survived shutdown")
	}
	if err := <-done; !errors.Is(err, io.EOF) {
		t.Fatal("SOCKS control socket survived shutdown", err)
	}
}
