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

package temporal

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type reviewBlockingTLSConn struct {
	readEntered, readRelease chan struct{}
	readOnce                 sync.Once
	reads, closes            atomic.Int32
}

func (connection *reviewBlockingTLSConn) Read(buffer []byte) (int, error) {
	connection.reads.Add(1)
	if connection.readEntered != nil {
		connection.readOnce.Do(func() { close(connection.readEntered) })
		<-connection.readRelease
	}
	buffer[0] = 'x'
	return 1, nil
}

func (*reviewBlockingTLSConn) Write(buffer []byte) (int, error) { return len(buffer), nil }

func (connection *reviewBlockingTLSConn) Close() error { connection.closes.Add(1); return nil }

func (*reviewBlockingTLSConn) LocalAddr() net.Addr { return &net.TCPAddr{} }

func (*reviewBlockingTLSConn) RemoteAddr() net.Addr { return &net.TCPAddr{} }

func (*reviewBlockingTLSConn) SetDeadline(time.Time) error { return nil }

func (*reviewBlockingTLSConn) SetReadDeadline(time.Time) error { return nil }

func (*reviewBlockingTLSConn) SetWriteDeadline(time.Time) error { return nil }

func TestReviewTLSPostHandshakeReadAndCloseAreJoined(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var scope transportLifetime
		native := &reviewBlockingTLSConn{}
		raw := &reviewBlockingTLSConn{}
		connection := &ownedTLSConnection{native: native, raw: raw, scope: &scope}
		buffer := make([]byte, 1)
		if count, err := connection.Read(buffer); count != 1 || err != nil || buffer[0] != 'x' {
			t.Fatal("live TLS I/O control failed", count, err)
		}
		native.readEntered, native.readRelease = make(chan struct{}), make(chan struct{})
		var unblock sync.Once
		defer unblock.Do(func() { close(native.readRelease) })
		readDone, closeDone := make(chan struct{}), make(chan struct{})
		go func() { defer close(readDone); _, _ = connection.Read(buffer) }()
		<-native.readEntered
		go func() { defer close(closeDone); scope.close(func() { _ = connection.Close() }) }()
		synctest.Wait()
		select {
		case <-closeDone:
			t.Fatal("transport declared release while entered post-handshake TLS I/O callback remained blocked")
		default:
		}
		if native.closes.Load() != 1 {
			t.Fatal("native connection was not closed before joining callback")
		}
		if _, err := connection.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) || native.reads.Load() != 2 {
			t.Fatal("closing transport admitted new TLS callback use", err)
		}
		if err := connection.Close(); err != nil || native.closes.Load() != 2 {
			t.Fatal("concurrent cleanup was not accounted for while draining", err)
		}
		unblock.Do(func() { close(native.readRelease) })
		<-readDone
		<-closeDone
		if err := connection.Close(); err != nil || native.closes.Load() != 2 || raw.closes.Load() != 1 {
			t.Fatal("late Close reentered released TLS state instead of closing only raw transport", err)
		}
	})
}
