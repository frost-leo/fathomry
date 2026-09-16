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
	"errors"
	"io"
	"net"
)

// Close is terminal and releases all cached physical proxy sessions. Callers
// first cancel and join outstanding dials; it does not cancel another owner.
func (dialer *Dialer) Close() error {
	dialer.createMu.Lock()
	defer dialer.createMu.Unlock()
	dialer.closed = true
	dialer.h2DialLock.Lock()
	var result error
	for _, session := range dialer.sessions {
		result = errors.Join(result, session.Close())
	}
	dialer.h2DialLock.Unlock()
	dialer.h3DialLock.Lock()
	if dialer.h3ClientConn != nil {
		result = errors.Join(result, dialer.h3ClientConn.CloseWithError(0, ""))
	}
	if dialer.h3Transport != nil {
		result = errors.Join(result, dialer.h3Transport.Close())
	}
	if dialer.h3Socket != nil {
		result = errors.Join(result, dialer.h3Socket.Close())
	}
	dialer.h3DialLock.Unlock()
	if closedOnly(result) {
		return nil
	}
	return result
}

func closedOnly(err error) bool {
	if err == nil || err == net.ErrClosed || err == io.ErrClosedPipe {
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
