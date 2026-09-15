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

package tls_client

import (
	"context"
	"errors"
	"net"
)

// FathomryControlV1 connects the Provider's shared quotas to native acquisition.
// Hooks are installed once before use, return promptly, and never reenter this
// client. A successful acquire returns one release function, called only after
// confirmed native release. TCP counts managed handles/dials, including proxy
// layers; HTTP3 counts native transports, not a claimed exact QUIC attempt count.
type FathomryControlV1 struct {
	AcquireTCP          func() (func(), error)
	AcquireHTTP3        func() (func(), error)
	MaxProxyHeaderBytes int64
}

type compatSocksForward struct {
	ctx   context.Context
	base  *directDialer
	stops []func()
}

func (forward *compatSocksForward) Dial(network, address string) (net.Conn, error) {
	conn, err := forward.base.DialContext(forward.ctx, network, address)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	stop := context.AfterFunc(forward.ctx, func() { _ = conn.Close(); close(done) })
	forward.stops = append(forward.stops, func() {
		if !stop() {
			<-done
		}
	})
	return conn, nil
}

func (forward *compatSocksForward) finish() {
	for _, stop := range forward.stops {
		stop()
	}
}

// ConfigureFathomry binds lifetime control before the client acquires resources.
// The callbacks remain borrowed until FathomryQuiescent is true.
func ConfigureFathomry(client HttpClient, control FathomryControlV1) error {
	if control.MaxProxyHeaderBytes < 0 || control.MaxProxyHeaderBytes > 1<<30 {
		return errors.New("tls-client: invalid proxy header limit")
	}
	native, ok := client.(*httpClient)
	if !ok || native == nil {
		return errors.New("tls-client: unsupported controlled client")
	}
	rt, ok := native.Transport.(*roundTripper)
	if !ok || rt.compat == nil {
		return errors.New("tls-client: unsupported controlled transport")
	}
	state := rt.compat
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed || state.configured || state.active != 0 || len(state.conns) != 0 || len(state.h3) != 0 {
		return errors.New("tls-client: control requires unused client")
	}
	if dialer, ok := rt.dialer.(*connectDialer); ok {
		// Native tunnel deadlines address the physical connection, not an H2 stream.
		dialer.EnableH2ConnReuse = false
	}
	state.control = control
	state.configured = true
	return nil
}

func (state *compatibilityState) proxyHeaderLimit() int64 {
	if state == nil {
		return 0
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.control.MaxProxyHeaderBytes
}

// FathomryQuiescent reports positive managed shutdown evidence, independently of
// earlier errors returned by Close. False means shutdown is not yet confirmed.
func FathomryQuiescent(client HttpClient) bool {
	native, ok := client.(*httpClient)
	if !ok || native == nil {
		return false
	}
	rt, ok := native.Transport.(*roundTripper)
	if !ok || rt.compat == nil {
		return false
	}
	rt.compat.mu.Lock()
	defer rt.compat.mu.Unlock()
	return rt.compat.finished
}

func (state *compatibilityState) reserve(h3 bool) (func(), error) {
	state.mu.Lock()
	acquire := state.control.AcquireTCP
	if h3 {
		acquire = state.control.AcquireHTTP3
	}
	closed := state.closed
	state.mu.Unlock()
	if closed {
		return nil, ErrClientClosed
	}
	if acquire == nil {
		return func() {}, nil
	}
	release, err := acquire()
	if err != nil {
		return nil, err
	}
	if release == nil {
		return nil, errors.New("tls-client: missing resource release")
	}
	return release, nil
}
