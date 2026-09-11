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

package nacos

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/frost-leo/fathomry/internal/resource"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
)

// Client owns its transports, sessions and subscriptions. Do not copy it.
// Open's context owns the whole lifetime. Open performs no readiness request.
type Client struct {
	private
	settings settings
	trust    *tls.Config
	lifetime context.Context
	cancel   context.CancelCauseFunc
	assembly *resource.Assembly
	access   *resource.Access
	http     *http.Client
	// mu protects admission/lifetime accounting, registered sessions/sockets,
	// token entries and cleanup errors; native I/O must run after releasing it.
	mu            sync.Mutex
	closing       bool
	active        int
	subscriptions int
	drained       chan struct{}
	sessions      map[*session]struct{}
	sockets       map[*ownedSocket]struct{}
	closeErrors   []error
	// Native acquisition can outlive an RPC wait, so it has a separate allowance.
	nativeSlots chan struct{}
	// authGate serializes network logins; token invalidation uses mu, not this gate.
	authGate  chan struct{}
	tokens    []token
	sequence  atomic.Uint64
	preferred atomic.Uint64
}

// Open validates and freezes bootstrap before acquiring local transport ownership.
// It does not contact a server. Close remains required after lifetime cancellation.
func Open(ctx context.Context, options OptionsV1) (*Client, error) {
	if ctx == nil {
		return nil, fail(ErrInput, "open")
	}
	value, trust, err := prepareOptions(options)
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, fail(ErrClosed, "open", ctx.Err(), context.Cause(ctx))
	}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: value},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: value.Name}, Format: 1})
	if err != nil {
		return nil, err
	}
	client := &Client{settings: value, trust: trust, drained: make(chan struct{}),
		sessions: make(map[*session]struct{}), sockets: make(map[*ownedSocket]struct{}),
		nativeSlots: make(chan struct{}, value.Active), authGate: make(chan struct{}, 1), tokens: make([]token, len(value.Servers))}
	client.lifetime, client.cancel = context.WithCancelCause(ctx)
	limits := resource.Limits{Active: value.Active, Queued: value.Queued, Bytes: int64(value.Active) * reservationBytes, MaxLeases: 1}
	if value.Queued > 0 {
		limits.QueuedBytes = int64(value.Queued) * reservationBytes
	}
	selected := resource.WithLimits(resource.Select(prepared, func(context.Context, settings) (resource.Resource[struct{}], error) {
		transport := client.newHTTPTransport()
		client.http = &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		return resource.Resource[struct{}]{Acquired: true, Capability: struct{}{}, Release: func(context.Context) resource.ReleaseResult {
			transport.CloseIdleConnections()
			client.mu.Lock()
			defer client.mu.Unlock()
			return resource.ReleaseResult{Quiescent: true, Released: true, Err: errors.Join(client.closeErrors...)}
		}}, nil
	}), limits)
	assembly, err := resource.Assemble(ctx, context.Background(), value.Name, selected)
	if err != nil {
		client.cancel(err)
		return nil, err
	}
	client.assembly = assembly
	client.access, err = resource.AccessFor(assembly, selected)
	if err != nil {
		client.cancel(err)
		_ = assembly.Close(context.Background())
		return nil, err
	}
	return client, nil
}

// Info returns independently owned, redacted preparation metadata, not service
// readiness, actual SDK build evidence or a certificate of compatibility.
func (client *Client) Info() resource.Info {
	if client == nil || client.access == nil {
		return resource.Info{}
	}
	return client.access.Info()
}

// enter retains local lifetime responsibility, not a logical request permit.
// The returned end function must run once, after the admitted local user exits;
// cancellation requests do not decrement that ownership count.
func (client *Client) enter(ctx context.Context) (context.Context, func(), error) {
	if client == nil || client.cancel == nil || ctx == nil {
		return nil, nil, fail(ErrInput, "enter")
	}
	client.mu.Lock()
	if client.closing {
		client.mu.Unlock()
		return nil, nil, fail(ErrClosed, "enter")
	}
	client.active++
	client.mu.Unlock()
	work, cancel := context.WithCancelCause(ctx)
	propagate := func() { cancel(errors.Join(ErrClosed, client.lifetime.Err(), context.Cause(client.lifetime))) }
	stop := context.AfterFunc(client.lifetime, propagate)
	if client.lifetime.Err() != nil {
		propagate()
	}
	return work, func() {
		stop()
		cancel(nil)
		client.mu.Lock()
		defer client.mu.Unlock()
		client.active--
		if client.closing && client.active == 0 {
			close(client.drained)
		}
	}, nil
}

// native separately bounds dial/TLS work, including work whose initiating RPC
// already stopped waiting. It retains the same client's cleanup responsibility.
func (client *Client) native(ctx context.Context) (context.Context, func(), error) {
	work, done, err := client.enter(ctx)
	if err != nil {
		return nil, nil, err
	}
	select {
	case client.nativeSlots <- struct{}{}:
		if work.Err() != nil {
			<-client.nativeSlots
			done()
			return nil, nil, fail(ErrClosed, "native", work.Err(), context.Cause(work))
		}
		return work, func() { <-client.nativeSlots; done() }, nil
	case <-work.Done():
		err := fail(ErrClosed, "native", work.Err(), context.Cause(work))
		done()
		return nil, nil, err
	}
}

// closingCause preserves the authoritative owner reason when socket closure wins
// the race against asynchronous propagation of lifetime cancellation.
func (client *Client) closingCause(err error) error {
	if err != nil && client.lifetime.Err() != nil {
		return fail(ErrRead, "lifetime", err, ErrClosed, client.lifetime.Err(), context.Cause(client.lifetime))
	}
	return err
}

// Close stops admission, cancels sessions and closes sockets before joining
// local users. A timeout retains the same owner; retry Close with a new budget.
func (client *Client) Close(ctx context.Context) error {
	if client == nil || client.cancel == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	client.mu.Lock()
	if !client.closing {
		client.closing = true
		client.cancel(ErrClosed)
		if client.active == 0 {
			close(client.drained)
		}
	}
	sessions := make([]*session, 0, len(client.sessions))
	for current := range client.sessions {
		sessions = append(sessions, current)
	}
	sockets := make([]*ownedSocket, 0, len(client.sockets))
	for current := range client.sockets {
		sockets = append(sockets, current)
	}
	client.mu.Unlock()
	// Stop native progress before joining users; placing this behind final resource
	// release would wait on calls whose only termination action had not run yet.
	for _, current := range sessions {
		current.cancel(ErrClosed)
	}
	for _, current := range sockets {
		_ = current.Close()
	}
	client.http.CloseIdleConnections()
	select {
	case <-client.drained:
		return client.assembly.Close(ctx)
	case <-ctx.Done():
		return fail(ErrState, "close", ctx.Err(), context.Cause(ctx))
	}
}

// ownedSocket transfers a successful native dial into the client's registered
// socket set. Close is idempotent across transport and owner shutdown paths.
type ownedSocket struct {
	net.Conn
	client *Client
	once   sync.Once
	err    error
}

func (socket *ownedSocket) Close() error {
	socket.once.Do(func() {
		socket.err = socket.Conn.Close()
		socket.client.mu.Lock()
		defer socket.client.mu.Unlock()
		delete(socket.client.sockets, socket)
		if socket.err != nil && socket.client.closing {
			socket.client.closeErrors = append(socket.client.closeErrors, socket.err)
		}
	})
	return socket.err
}

// own atomically registers a socket or closes it if shutdown already won. A late
// successful dial must not hand an untracked connection back to the transport.
func (client *Client) own(socket net.Conn) (net.Conn, error) {
	owned := &ownedSocket{Conn: socket, client: client}
	client.mu.Lock()
	closing := client.closing
	if !closing {
		client.sockets[owned] = struct{}{}
	}
	client.mu.Unlock()
	if closing {
		return nil, fail(ErrClosed, "socket", owned.Close())
	}
	return owned, nil
}
