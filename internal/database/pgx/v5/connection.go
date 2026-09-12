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

package pgx

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"strconv"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	sdk "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// nativeContext deliberately omits caller values, including net/http trace hooks.
// The outer operation retains the original context's cancellation cause.
type nativeContext struct{ context.Context }

func (nativeContext) Value(any) any { return nil }

// wireScope is a per-connection acquisition fence, not another connection pool.
// Failed startup can launch pgconn cleanup without returning a PgConn. Sealing
// prevents that cleanup (or a retained ConnectError.Config callback) from opening
// another socket, and joins every dial and socket it already owns.
type wireScope struct {
	mu       sync.Mutex
	sealed   bool
	address  string
	dials    sync.WaitGroup
	lifetime context.Context
	cancel   context.CancelFunc
	sockets  map[*wireSocket]struct{}
	errs     []error
	dialer   func(context.Context, string, string) (net.Conn, error)
}
type wireSocket struct {
	net.Conn
	scope *wireScope
	once  sync.Once
	err   error
}

func newWireScope(address string) *wireScope {
	ctx, cancel := context.WithCancel(context.Background())
	return &wireScope{address: address, lifetime: ctx, cancel: cancel, sockets: make(map[*wireSocket]struct{}),
		dialer: (&net.Dialer{}).DialContext}
}
func (scope *wireScope) dial(ctx context.Context, network, address string) (net.Conn, error) {
	scope.mu.Lock()
	if scope.sealed || network != "tcp" && network != "tcp4" && network != "tcp6" || address != scope.address || len(scope.sockets) >= 2 {
		scope.mu.Unlock()
		return nil, failure(ErrState, "dial")
	}
	scope.dials.Add(1)
	scope.mu.Unlock()
	defer scope.dials.Done()
	work, cancel := context.WithCancel(nativeContext{ctx})
	stop := context.AfterFunc(scope.lifetime, cancel)
	defer stop()
	defer cancel()
	if scope.lifetime.Err() != nil {
		cancel()
	}
	socket, err := scope.dialer(work, network, address)
	if err != nil {
		return nil, err
	}
	owned := &wireSocket{Conn: socket, scope: scope}
	scope.mu.Lock()
	sealed := scope.sealed
	if !sealed {
		scope.sockets[owned] = struct{}{}
	}
	scope.mu.Unlock()
	if sealed {
		return nil, failure(ErrState, "dial", owned.Close())
	}
	return owned, nil
}
func (socket *wireSocket) Close() error {
	socket.once.Do(func() {
		socket.err = socket.Conn.Close()
		socket.scope.mu.Lock()
		delete(socket.scope.sockets, socket)
		if socket.err != nil {
			socket.scope.errs = append(socket.scope.errs, socket.err)
		}
		socket.scope.mu.Unlock()
	})
	return socket.err
}
func (scope *wireScope) close() error {
	scope.mu.Lock()
	scope.sealed = true
	scope.cancel()
	sockets := make([]*wireSocket, 0, len(scope.sockets))
	for socket := range scope.sockets {
		sockets = append(sockets, socket)
	}
	scope.mu.Unlock()
	for _, socket := range sockets {
		_ = socket.Close()
	}
	scope.dials.Wait()
	scope.mu.Lock()
	defer scope.mu.Unlock()
	return errors.Join(scope.errs...)
}

type connection struct {
	native   *sdk.Conn
	wire     *wireScope
	sequence uint64
}

func (connection *connection) nextName(prefix string) (string, error) {
	if connection.sequence == ^uint64(0) {
		return "", failure(ErrLimit, "name-sequence")
	}
	connection.sequence++
	return prefix + strconv.FormatUint(connection.sequence, 10), nil
}

func isolatedTLS(config *tls.Config, rootCAPEM string) (*tls.Config, error) {
	if config == nil {
		return nil, nil
	}
	copy := config.Clone()
	copy.RootCAs = x509.NewCertPool()
	if !copy.RootCAs.AppendCertsFromPEM([]byte(rootCAPEM)) {
		return nil, failure(ErrInput, "roots")
	}
	return copy, nil
}

func connect(ctx context.Context, config *sdk.ConnConfig, rootCAPEM string) (*connection, error) {
	config = config.Copy()
	var err error
	// CertPool.Clone shares parsed certificate pointers exposed by x509 errors.
	// Rebuild from immutable PEM so failed attempts cannot alter future trust.
	config.TLSConfig, err = isolatedTLS(config.TLSConfig, rootCAPEM)
	if err != nil {
		return nil, err
	}
	wire := newWireScope(net.JoinHostPort(config.Host, strconv.Itoa(int(config.Port))))
	config.DialFunc = wire.dial
	config.LookupFunc = func(_ context.Context, host string) ([]string, error) { return []string{host}, nil }
	native, err := sdk.ConnectConfig(nativeContext{ctx}, config)
	if err != nil {
		cleanup := wire.close()
		var snapshotError error
		if original, ok := err.(*pgconn.ConnectError); ok && original.Config != nil {
			// asyncClose may still read its private config after startup fails.
			// Preserve the native error identity but expose an independent snapshot.
			snapshot := original.Config.Copy()
			snapshot.TLSConfig, snapshotError = isolatedTLS(snapshot.TLSConfig, rootCAPEM)
			original.Config = snapshot
		}
		return nil, failure(ErrConnect, "connect", nativeFailure(ErrConnect, "connect", ctx, err), cleanup, snapshotError)
	}
	return &connection{native: native, wire: wire}, nil
}
func (connection *connection) close(ctx context.Context) error {
	nativeErr := connection.native.Close(nativeContext{ctx})
	// IsClosed and a repeated Close are not completion evidence. The selected
	// native transport has no application callbacks; wait for its real cleanup.
	<-connection.native.PgConn().CleanupDone()
	return failureOrNil(ErrCleanup, "connection-close", nativeErr, connection.wire.close())
}
func failureOrNil(kind fault.Kind, operation string, causes ...error) error {
	for _, cause := range causes {
		if cause != nil {
			return failure(kind, operation, causes...)
		}
	}
	return nil
}

func nativeFailure(kind fault.Kind, operation string, ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return failure(kind, operation, err, context.Cause(ctx))
	}
	return failure(kind, operation, err)
}
