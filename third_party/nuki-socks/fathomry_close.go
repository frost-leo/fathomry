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
	"net"
)

// FathomryCompatibilityRevision identifies local UDP ownership corrections.
const FathomryCompatibilityRevision = "v1"

// Close releases both the relay socket and its associated control connection,
// then joins the control monitor. Borrowed native connections must allow Close
// to interrupt Read; cancellation is not itself cleanup evidence.
func (conn *PacketConn) Close() error {
	conn.closeMu.Lock()
	defer conn.closeMu.Unlock()
	if conn.released {
		return nil
	}
	var err error
	if conn.control != nil {
		err = conn.control.Close()
	}
	err = errors.Join(err, conn.PacketConn.Close())
	if !closedOnly(err) {
		return err
	}
	if conn.done != nil {
		<-conn.done
	}
	conn.released = true
	if !conn.monitorReported && !closedOnly(conn.monitorClose) {
		conn.monitorReported = true
		return conn.monitorClose
	}
	return nil
}

// ReleaseConfirmed distinguishes historical monitor failures from positively
// closed physical resources and a joined monitor.
func (conn *PacketConn) ReleaseConfirmed() bool {
	conn.closeMu.Lock()
	defer conn.closeMu.Unlock()
	return conn.released
}

func (conn *PacketConn) SetReadBuffer(size int) error {
	if socket, ok := conn.PacketConn.(interface{ SetReadBuffer(int) error }); ok {
		return socket.SetReadBuffer(size)
	}
	return nil
}
func (conn *PacketConn) SetWriteBuffer(size int) error {
	if socket, ok := conn.PacketConn.(interface{ SetWriteBuffer(int) error }); ok {
		return socket.SetWriteBuffer(size)
	}
	return nil
}

func closedOnly(err error) bool {
	if err == nil || err == net.ErrClosed {
		return true
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		causes := wrapped.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !closedOnly(cause) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		cause := wrapped.Unwrap()
		return cause != nil && closedOnly(cause)
	}
	return false
}
