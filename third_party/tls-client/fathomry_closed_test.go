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
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/bogdanfinn/fhttp"
)

type closedErrorBranches []error

func (closedErrorBranches) Error() string            { return "synthetic cleanup branches" }
func (branches closedErrorBranches) Unwrap() []error { return []error(branches) }

func TestFathomryClosedErrorRequiresOnlyClosedCauses(t *testing.T) {
	sentinel := errors.New("synthetic independent cleanup")
	wrapped := fmt.Errorf("native close: %w", net.ErrClosed)
	for _, entry := range []struct {
		name   string
		value  error
		closed bool
	}{
		{"nil", nil, false},
		{"closed", net.ErrClosed, true},
		{"wrapped", wrapped, true},
		{"native-operation", &net.OpError{Op: "close", Net: "tcp", Err: net.ErrClosed}, true},
		{"closed-join", errors.Join(net.ErrClosed, wrapped), true},
		{"closed-nil-join", errors.Join(nil, net.ErrClosed), true},
		{"mixed", errors.Join(wrapped, sentinel), false},
		{"wrapped-mixed", fmt.Errorf("native cleanup: %w", errors.Join(net.ErrClosed, sentinel)), false},
		{"empty-branches", closedErrorBranches{}, false},
		{"nil-branch", closedErrorBranches{nil}, false},
		{"closed-nil-branch", closedErrorBranches{net.ErrClosed, nil}, false},
		{"closed-empty-branch", closedErrorBranches{net.ErrClosed, closedErrorBranches{}}, false},
		{"empty-single-wrapper", &net.OpError{Op: "close"}, false},
	} {
		t.Run(entry.name, func(t *testing.T) {
			if compatClosedError(entry.value) != entry.closed {
				t.Fatal("cleanup classification conflated all-closed and mixed causes")
			}
		})
	}
}

type mixedCleanupConn struct {
	first     error
	calls     atomic.Int64
	confirmed atomic.Bool
}

func (conn *mixedCleanupConn) Read([]byte) (int, error)         { return 0, net.ErrClosed }
func (conn *mixedCleanupConn) Write([]byte) (int, error)        { return 0, net.ErrClosed }
func (conn *mixedCleanupConn) LocalAddr() net.Addr              { return compatAddress{} }
func (conn *mixedCleanupConn) RemoteAddr() net.Addr             { return compatAddress{} }
func (conn *mixedCleanupConn) SetDeadline(time.Time) error      { return net.ErrClosed }
func (conn *mixedCleanupConn) SetReadDeadline(time.Time) error  { return net.ErrClosed }
func (conn *mixedCleanupConn) SetWriteDeadline(time.Time) error { return net.ErrClosed }
func (conn *mixedCleanupConn) Close() error {
	if conn.calls.Add(1) == 1 && conn.first != nil {
		return conn.first
	}
	conn.confirmed.Store(true)
	return nil
}

func TestFathomryMixedClosedErrorRetainsOwnershipAndHistory(t *testing.T) {
	state := newCompatibilityState()
	defer state.cancel()
	sentinel := errors.New("synthetic pending native cleanup")
	mixed := errors.Join(net.ErrClosed, sentinel)
	raw := &mixedCleanupConn{first: mixed}
	var held atomic.Int64
	state.control.AcquireTCP = func() (func(), error) {
		held.Add(1)
		return func() { held.Add(-1) }, nil
	}
	tracked, err := state.dial(context.Background(), "synthetic", "owned", func(context.Context, string, string) (net.Conn, error) { return raw, nil })
	if err != nil || held.Load() != 1 {
		t.Fatal("synthetic native acquisition failed", err)
	}
	first := tracked.Close()
	state.mu.Lock()
	handles, history := len(state.conns), state.cleanup
	state.mu.Unlock()
	if first != mixed || history != mixed || !errors.Is(first, sentinel) || held.Load() != 1 || handles != 1 || raw.confirmed.Load() {
		t.Fatal("mixed cleanup failure released ownership or lost original evidence")
	}
	transport := &roundTripper{compat: state, cachedTransports: make(map[string]http.RoundTripper), cachedConnections: make(map[string]net.Conn)}
	client := &httpClient{Client: http.Client{Transport: transport}}
	if FathomryQuiescent(client) {
		t.Fatal("pending cleanup certified quiescent")
	}
	if err := Close(client); !errors.Is(err, sentinel) || !errors.Is(err, net.ErrClosed) {
		t.Fatal("successful retry erased mixed cleanup history")
	}
	if raw.calls.Load() != 2 || !raw.confirmed.Load() || held.Load() != 0 || !FathomryQuiescent(client) {
		t.Fatal("retry did not confirm actual native cleanup")
	}
	if err := Close(client); !errors.Is(err, sentinel) || raw.calls.Load() != 2 || held.Load() != 0 {
		t.Fatal("repeated source cleanup changed original evidence or ownership")
	}
}

func TestFathomryWrappedClosedErrorsConfirmRelease(t *testing.T) {
	for _, entry := range []struct {
		name  string
		value error
	}{
		{"plain", net.ErrClosed},
		{"wrapped", fmt.Errorf("native close: %w", net.ErrClosed)},
		{"native-operation", &net.OpError{Op: "close", Net: "tcp", Err: net.ErrClosed}},
		{"all-closed-join", errors.Join(net.ErrClosed, fmt.Errorf("another close: %w", net.ErrClosed))},
	} {
		t.Run(entry.name, func(t *testing.T) {
			state := newCompatibilityState()
			defer state.cancel()
			raw := &mixedCleanupConn{first: entry.value}
			raw.confirmed.Store(true)
			var held atomic.Int64
			state.control.AcquireTCP = func() (func(), error) {
				held.Add(1)
				return func() { held.Add(-1) }, nil
			}
			tracked, err := state.dial(context.Background(), "synthetic", "owned", func(context.Context, string, string) (net.Conn, error) { return raw, nil })
			if err != nil {
				t.Fatal(err)
			}
			if err := tracked.Close(); err != nil || held.Load() != 0 || raw.calls.Load() != 1 {
				t.Fatal("benign wrapped closed error retained native ownership")
			}
			transport := &roundTripper{compat: state, cachedTransports: make(map[string]http.RoundTripper), cachedConnections: make(map[string]net.Conn)}
			client := &httpClient{Client: http.Client{Transport: transport}}
			if err := Close(client); err != nil || !FathomryQuiescent(client) || raw.calls.Load() != 1 {
				t.Fatal("benign closed error became failed source cleanup")
			}
		})
	}
}
