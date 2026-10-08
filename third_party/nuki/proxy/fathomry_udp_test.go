// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nukilabs/quic-go"
)

type deadlineStream struct {
	incoming chan []byte
	entered  chan struct{}
	closed   chan struct{}
	once     sync.Once
}

func (s *deadlineStream) Read([]byte) (int, error)         { <-s.closed; return 0, io.EOF }
func (s *deadlineStream) Write(data []byte) (int, error)   { return len(data), nil }
func (s *deadlineStream) Close() error                     { s.once.Do(func() { close(s.closed) }); return nil }
func (s *deadlineStream) CancelRead(quic.StreamErrorCode)  { _ = s.Close() }
func (s *deadlineStream) CancelWrite(quic.StreamErrorCode) {}
func (s *deadlineStream) SendDatagramContext(ctx context.Context, _ []byte) error {
	s.entered <- struct{}{}
	<-ctx.Done()
	return context.Cause(ctx)
}
func (s *deadlineStream) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	select {
	case value := <-s.incoming:
		return value, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func TestFathomryTunnelDeadlinesResetClearAndTerminalClose(t *testing.T) {
	stream := &deadlineStream{incoming: make(chan []byte, 1), entered: make(chan struct{}, 2), closed: make(chan struct{})}
	target := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 443}
	tunnel := newH3Conn(stream, target, target)
	defer tunnel.Close()
	if err := tunnel.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tunnel.ReadFrom(make([]byte, 8)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("expired read deadline", err)
	}
	if err := tunnel.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	stream.incoming <- []byte{0, 'o', 'k'}
	data := make([]byte, 8)
	count, address, err := tunnel.ReadFrom(data)
	if err != nil || string(data[:count]) != "ok" || address.String() != target.String() {
		t.Fatal("deadline clear/fixed target", count, address, err)
	}
	if err := tunnel.SetWriteDeadline(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := tunnel.WriteTo([]byte("data"), target); done <- err }()
	<-stream.entered
	if err := tunnel.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("write deadline did not interrupt actual work")
	}
	if err := tunnel.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	go func() { _, err := tunnel.WriteTo([]byte("again"), target); done <- err }()
	<-stream.entered
	if err := tunnel.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not join write")
	}
	if !tunnel.ReleaseConfirmed() {
		t.Fatal("terminal release not confirmed")
	}
	if _, err := tunnel.WriteTo(nil, target); !errors.Is(err, net.ErrClosed) {
		t.Fatal("terminal tunnel write", err)
	}
}
