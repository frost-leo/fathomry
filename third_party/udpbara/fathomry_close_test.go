/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package udpbara

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countedCloseConn struct {
	net.Conn
	calls    atomic.Int32
	started  chan struct{}
	finish   chan struct{}
	firstErr error
}

func (conn *countedCloseConn) Close() error {
	if conn.calls.Add(1) != 1 {
		return net.ErrClosed
	}
	_ = conn.Conn.Close()
	close(conn.started)
	<-conn.finish
	return conn.firstErr
}

func awaitCloseFixture[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("local fixture did not join")
		var zero T
		return zero
	}
}

func TestFathomryCanceledGreetingCloseCustody(t *testing.T) {
	closeFailure := errors.New("synthetic first close failure")
	for _, test := range []struct {
		name     string
		overlap  bool
		firstErr error
	}{
		{"overlap_clean", true, nil},
		{"overlap_real_error", true, closeFailure},
		{"overlap_first_net_closed", true, net.ErrClosed},
		{"close_after_untrack_real_error", false, closeFailure},
		{"overlap_mixed_error", true, errors.Join(closeFailure, net.ErrClosed)},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, peer := net.Pipe()
			defer client.Close()
			defer peer.Close()
			conn := &countedCloseConn{Conn: client, started: make(chan struct{}), finish: make(chan struct{}), firstErr: test.firstErr}
			release := sync.OnceFunc(func() { close(conn.finish) })
			defer release()
			greeting := make(chan error, 1)
			peerDone := make(chan error, 1)
			go func() {
				var message [3]byte
				_, err := io.ReadFull(peer, message[:])
				greeting <- err
				_ = peer.SetReadDeadline(time.Now().Add(2 * time.Second))
				var one [1]byte
				_, err = peer.Read(one[:])
				peerDone <- err
			}()
			config := DefaultConfig()
			config.AutoReconnect = false
			config.Fathomry = &FathomryHooks{
				DialTCP:   func(context.Context, string) (net.Conn, error) { return conn, nil },
				Resolve:   func(context.Context, string) ([]net.IP, error) { return nil, errors.New("unexpected resolution") },
				ListenUDP: func(string, *net.UDPAddr) (*net.UDPConn, error) { return nil, errors.New("unexpected UDP listener") },
				DialUDP: func(string, *net.UDPAddr, *net.UDPAddr) (*net.UDPConn, error) {
					return nil, errors.New("unexpected UDP dial")
				},
				CloseUDP: func(*net.UDPConn) error { return errors.New("unexpected UDP close") },
			}
			tunnel, err := NewTunnel("socks5://127.0.0.1:9", config)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			cause := errors.New("synthetic greeting cancellation")
			connected := make(chan error, 1)
			go func() { connected <- tunnel.ConnectContext(ctx) }()
			if err := awaitCloseFixture(t, greeting); err != nil {
				t.Fatal(err)
			}
			cancel(cause)
			awaitCloseFixture(t, conn.started)
			var closeErr, connectErr error
			if test.overlap {
				closed := make(chan error, 1)
				go func() { closed <- tunnel.FathomryClose() }()
				deadline := time.Now().Add(time.Second)
				for !tunnel.closing.Load() && time.Now().Before(deadline) {
					runtime.Gosched()
				}
				if !tunnel.closing.Load() {
					t.Fatal("tunnel close did not start")
				}
				select {
				case err := <-closed:
					t.Fatal("close returned before its socket close callback joined", err)
				case <-time.After(10 * time.Millisecond):
				}
				release()
				closeErr = awaitCloseFixture(t, closed)
				connectErr = awaitCloseFixture(t, connected)
			} else {
				release()
				connectErr = awaitCloseFixture(t, connected)
				closeErr = tunnel.FathomryClose()
			}
			t.Logf("native close calls=%d; cleanup error=%v", conn.calls.Load(), closeErr)
			if conn.calls.Load() != 1 {
				t.Errorf("one socket was closed %d times", conn.calls.Load())
			}
			if test.firstErr == nil && closeErr != nil {
				t.Errorf("duplicate close manufactured cleanup failure: %v", closeErr)
			}
			if test.firstErr != nil && !errors.Is(closeErr, test.firstErr) {
				t.Errorf("real first close failure was lost: %v", closeErr)
			}
			if !errors.Is(connectErr, context.Canceled) || !errors.Is(connectErr, cause) {
				t.Errorf("connect lost cancellation cause: %v", connectErr)
			}
			if repeated := tunnel.FathomryClose(); !errors.Is(repeated, test.firstErr) || conn.calls.Load() != 1 {
				t.Errorf("repeated close changed its actual cleanup result: %v", repeated)
			}
			if err := awaitCloseFixture(t, peerDone); !errors.Is(err, io.EOF) {
				t.Fatal("pipe peer did not close", err)
			}
		})
	}
}

func TestFathomryControlSocketReconnectAndJoin(t *testing.T) {
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
	type accepted struct {
		conn net.Conn
		err  error
	}
	peers := make(chan accepted, 2)
	go func() {
		for range 2 {
			conn, err := listener.Accept()
			if err != nil {
				peers <- accepted{err: err}
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			var greeting [3]byte
			if _, err = io.ReadFull(conn, greeting[:]); err == nil {
				_, err = conn.Write([]byte{5, 0})
			}
			var request [10]byte
			if err == nil {
				_, err = io.ReadFull(conn, request[:])
			}
			port := relay.LocalAddr().(*net.UDPAddr).Port
			if err == nil {
				_, err = conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, byte(port >> 8), byte(port)})
			}
			peers <- accepted{conn, err}
		}
	}()
	tunnel, err := NewTunnel("socks5://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer tunnel.FathomryClose()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tunnel.ConnectContext(ctx); err != nil {
		t.Fatal(err)
	}
	first := awaitCloseFixture(t, peers)
	if first.err != nil {
		t.Fatal(first.err)
	}
	tunnel.mu.Lock()
	original := tunnel.controlTCP
	tunnel.mu.Unlock()
	_ = first.conn.Close()
	second := awaitCloseFixture(t, peers)
	if second.conn != nil {
		defer second.conn.Close()
	}
	if second.err != nil {
		t.Fatal(second.err)
	}
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		tunnel.mu.Lock()
		replaced := tunnel.controlTCP != nil && tunnel.controlTCP != original
		tunnel.mu.Unlock()
		if replaced {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("control socket was not replaced", ctx.Err())
		case <-poll.C:
		}
	}
	if err := tunnel.FathomryClose(); err != nil {
		t.Fatal("reconnected socket cleanup failed", err)
	}
	var one [1]byte
	if _, err := second.conn.Read(one[:]); !errors.Is(err, io.EOF) {
		t.Fatal("reconnected control socket survived shutdown", err)
	}
}
