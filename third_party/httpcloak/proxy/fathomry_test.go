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

package proxy

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

type observedReadContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *observedReadContext) Err() error {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Err()
}

func TestFathomryMASQUEReadDeadlineUpdateAndCloseJoin(t *testing.T) {
	conn, err := NewMASQUEConn("masque://127.0.0.1:443")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	conn.established = true
	target := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443}
	conn.SetResolvedTarget(target)
	target.IP[0] = 42
	entered := make(chan struct{})
	conn.readContext = &observedReadContext{Context: conn.readContext, entered: entered}
	returned := make(chan error, 1)
	go func() { _, _, err := conn.ReadFrom(make([]byte, 16)); returned <- err }()
	<-entered
	if err := conn.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-returned:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal("read did not observe updated deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("entered read retained obsolete deadline context")
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	conn.datagramCh <- []byte("control")
	packet := make([]byte, 16)
	count, address, err := conn.ReadFrom(packet)
	if err != nil || string(packet[:count]) != "control" || !address.(*net.UDPAddr).IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatal("deadline clear or address copying changed datagrams", count, address, err)
	}
	entered = make(chan struct{})
	conn.readContext = &observedReadContext{Context: conn.readContext, entered: entered}
	go func() { _, _, err := conn.ReadFrom(packet); returned <- err }()
	<-entered
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-returned:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal("closed read lost owner shutdown", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close returned before an entered read terminated")
	}
}

func TestFathomryMASQUEContextZeroIncludesEmptyDatagram(t *testing.T) {
	conn := &MASQUEConn{}
	if value := conn.unwrapDatagram([]byte{0}); value == nil || len(value) != 0 {
		t.Fatal("valid empty UDP datagram was discarded")
	}
	for _, malformed := range [][]byte{nil, {1, 42}, {0x40}, {0x80, 0}} {
		if value := conn.unwrapDatagram(malformed); value != nil {
			t.Fatal("unregistered or malformed datagram context was admitted", malformed)
		}
	}
}
