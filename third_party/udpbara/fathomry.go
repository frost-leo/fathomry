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
	"net"
)

const FathomryCompatibilityRevision = "udpbara-v1"

func (t *Tunnel) fathomryConnecting(ctx context.Context, conn net.Conn) (func(), error) {
	t.mu.Lock()
	if t.closing.Load() {
		t.mu.Unlock()
		return nil, errors.Join(errors.New("tunnel closed"), conn.Close())
	}
	if t.connecting == nil {
		t.connecting = make(map[net.Conn]struct{})
	}
	t.connecting[conn] = struct{}{}
	t.mu.Unlock()
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); _ = conn.Close() })
	return func() {
		if !stop() {
			<-done
		}
		t.mu.Lock()
		delete(t.connecting, conn)
		t.mu.Unlock()
	}, nil
}
func fathomryContextError(ctx context.Context, err error) error {
	if err != nil {
		return errors.Join(err, ctx.Err(), context.Cause(ctx))
	}
	return nil
}

// FathomryClose closes pending handshake sockets and joins connection and
// tunnel workers. It must not be invoked from a tunnel callback.
func (t *Tunnel) FathomryClose() error {
	err := t.Close()
	t.connects.Wait()
	t.workers.Wait()
	return err
}
