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
	"time"

	"github.com/sardanioss/httpcloak/internal/managed"
)

var errDeadlineChanged = errors.New("MASQUE deadline changed")

// NewFathomryMASQUEConn requires source-owned construction authority.
func NewFathomryMASQUEConn(address string, controls *managed.Controls) (*MASQUEConn, error) {
	if controls == nil || controls.ProxyTLS == nil || controls.MaxHeaderBytes < 1024 || controls.MaxHeaderBytes > 1<<20 {
		return nil, errors.New("missing managed MASQUE authority")
	}
	conn, err := NewMASQUEConn(address)
	if err != nil {
		return nil, err
	}
	conn.controls = controls
	return conn, nil
}

func (c *MASQUEConn) closeUDP(conn *net.UDPConn) error {
	if c.controls != nil {
		return c.controls.CloseUDP(conn)
	}
	err := conn.Close()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (c *MASQUEConn) beginIO() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	if !c.established {
		return errors.New("MASQUE connection not established")
	}
	c.workers.Add(1)
	return nil
}

func deadlineContext(parent context.Context, deadline time.Time) (context.Context, func(error)) {
	stopTimer := func() {}
	if !deadline.IsZero() {
		parent, stopTimer = context.WithDeadline(parent, deadline)
	}
	ctx, cancel := context.WithCancelCause(parent)
	return ctx, func(reason error) { cancel(reason); stopTimer() }
}

func (c *MASQUEConn) resetReadContext() {
	if c.readCancel != nil {
		c.readCancel(errDeadlineChanged)
	}
	c.readContext, c.readCancel = deadlineContext(c.ctx, c.readDeadline)
}

func (c *MASQUEConn) resetWriteContext() {
	if c.writeCancel != nil {
		c.writeCancel(errDeadlineChanged)
	}
	c.writeContext, c.writeCancel = deadlineContext(c.ctx, c.writeDeadline)
}

func masqueIOError(operation string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		err = errors.Join(os.ErrDeadlineExceeded, err)
	}
	return &net.OpError{Op: operation, Net: "masque", Err: err}
}
