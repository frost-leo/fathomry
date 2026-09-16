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

package surf

import (
	"time"

	"github.com/wzshiming/socks5"
)

type fathomrySOCKSConn struct{ *socks5.UDPConn }

func (conn *fathomrySOCKSConn) Read(buffer []byte) (int, error) {
	for {
		count, from, err := conn.UDPConn.ReadFrom(buffer)
		if err != nil {
			return count, err
		}
		if from.String() == conn.RemoteAddr().String() {
			return count, nil
		}
	}
}
func (conn *fathomrySOCKSConn) SetDeadline(deadline time.Time) error {
	return conn.PacketConn.SetDeadline(deadline)
}
func (conn *fathomrySOCKSConn) SetReadDeadline(deadline time.Time) error {
	return conn.PacketConn.SetReadDeadline(deadline)
}
func (conn *fathomrySOCKSConn) SetWriteDeadline(deadline time.Time) error {
	return conn.PacketConn.SetWriteDeadline(deadline)
}
