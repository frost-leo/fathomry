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

package otel

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
)

// net/http deliberately detaches dial cancellation from the request. These
// private keys restore the admitted lifetime without forwarding caller callbacks.
type networkContextKey struct{}
type scopeKey struct{}
type valueFreeContext struct{ context.Context }

func (valueFreeContext) Value(any) any { return nil }

type networkOwner struct {
	mu          sync.Mutex
	stopped     bool
	changed     chan struct{}
	dials       map[*dialUse]struct{}
	connections map[*ownedConnection]struct{}
	closeErrors []error
	dialContext func(context.Context, string, string) (net.Conn, error)
	tls         *tls.Config
}
type dialUse struct{ cancel context.CancelCauseFunc }
type ownedConnection struct {
	net.Conn
	owner *networkOwner
	once  sync.Once
	err   error
}

func newNetwork(value settings, config *tls.Config) *networkOwner {
	dialer := &net.Dialer{Timeout: value.Timeout, KeepAlive: 30 * time.Second}
	return &networkOwner{changed: make(chan struct{}), dials: make(map[*dialUse]struct{}),
		connections: make(map[*ownedConnection]struct{}), dialContext: dialer.DialContext, tls: config}
}
func (network *networkOwner) signal() { close(network.changed); network.changed = make(chan struct{}) }
func (network *networkOwner) dialPlain(ctx context.Context, kind, address string) (net.Conn, error) {
	return network.dial(ctx, kind, address, false)
}
func (network *networkOwner) dialTLS(ctx context.Context, kind, address string) (net.Conn, error) {
	return network.dial(ctx, kind, address, true)
}
func (network *networkOwner) dial(ctx context.Context, kind, address string, secure bool) (net.Conn, error) {
	original, ok := ctx.Value(networkContextKey{}).(context.Context)
	if !ok {
		return nil, failure(ErrState, "uncontrolled-dial")
	}
	var guard *invocation.Guard
	if scope, ok := original.Value(scopeKey{}).(invocation.Scope); ok {
		var err error
		guard, err = scope.Hold()
		if err != nil {
			return nil, err
		}
		defer guard.End()
	}
	work, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(original, func() { cancel(context.Cause(original)) })
	defer stop()
	defer cancel(nil)
	if original.Err() != nil {
		cancel(context.Cause(original))
	}
	use := &dialUse{cancel: cancel}
	network.mu.Lock()
	if network.stopped {
		network.mu.Unlock()
		return nil, failure(ErrState, "network-stopped")
	}
	network.dials[use] = struct{}{}
	network.mu.Unlock()
	defer func() { network.mu.Lock(); delete(network.dials, use); network.signal(); network.mu.Unlock() }()
	raw, err := network.dialContext(work, kind, address)
	if err != nil {
		if raw != nil {
			closeErr := raw.Close()
			network.recordClose(closeErr)
			err = joined(ErrExport, "dial", err, closeErr)
		}
		return nil, err
	}
	connection := &ownedConnection{Conn: raw, owner: network}
	network.mu.Lock()
	if network.stopped {
		network.mu.Unlock()
		closeErr := raw.Close()
		network.recordClose(closeErr)
		return nil, failure(ErrState, "network-stopped", closeErr)
	}
	network.connections[connection] = struct{}{}
	network.mu.Unlock()
	if !secure {
		return connection, nil
	}
	if network.tls == nil {
		_ = connection.Close()
		return nil, failure(ErrState, "tls-unconfigured")
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	config := network.tls.Clone()
	config.ServerName = host
	secured := tls.Client(connection, config)
	if err := secured.HandshakeContext(work); err != nil {
		closeErr := secured.Close()
		return nil, joined(ErrExport, "tls-handshake", detachedTLSFailure(err), closeErr)
	}
	return secured, nil
}
func (connection *ownedConnection) Close() error {
	connection.once.Do(func() {
		connection.err = connection.Conn.Close()
		connection.owner.recordClose(connection.err)
		connection.owner.mu.Lock()
		delete(connection.owner.connections, connection)
		connection.owner.signal()
		connection.owner.mu.Unlock()
	})
	return connection.err
}

func (network *networkOwner) recordClose(err error) {
	if err == nil {
		return
	}
	network.mu.Lock()
	// Stop further acquisition after a close failure: retained native errors
	// are bounded by the already-owned connections/dials, not lifetime churn.
	network.stopped = true
	network.closeErrors = append(network.closeErrors, err)
	network.mu.Unlock()
}

// stop joins actual owned dials rather than assuming a canceled RoundTrip joined
// them. Late transport callbacks cannot acquire new IO after stopped is set.
func (network *networkOwner) stop(ctx context.Context) (bool, error) {
	if network == nil {
		return true, nil
	}
	network.mu.Lock()
	network.stopped = true
	dials := make([]*dialUse, 0, len(network.dials))
	for use := range network.dials {
		dials = append(dials, use)
	}
	connections := make([]*ownedConnection, 0, len(network.connections))
	for connection := range network.connections {
		connections = append(connections, connection)
	}
	network.mu.Unlock()
	for _, use := range dials {
		use.cancel(failure(ErrState, "network-stopped"))
	}
	for _, connection := range connections {
		_ = connection.Close()
	}
	for {
		network.mu.Lock()
		done := len(network.dials) == 0 && len(network.connections) == 0
		changed := network.changed
		causes := append([]error(nil), network.closeErrors...)
		network.mu.Unlock()
		if done {
			return true, joined(ErrCleanup, "network-close", causes...)
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return false, joined(ErrCleanup, "network-join", append(causes, ctx.Err(), context.Cause(ctx))...)
		}
	}
}
