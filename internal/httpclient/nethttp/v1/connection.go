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

package nethttp

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Connection is an explicitly owned direct native connection, independent of the
// native transport pool. It permits one outstanding child response at a time;
// child calls borrow the session reservation rather than multiply source quotas.
// Close interrupts native work, closes any child stream and joins its cleanup.
// The parent receipt is terminal only after Close; Timeout bounds its lifetime.
type Connection struct {
	private
	client     *Client
	op         *operation
	native     *http.ClientConn
	address    string
	scheme     string
	mu         sync.Mutex
	closing    bool
	busy       bool
	child      *Stream
	childOp    *operation
	childReady chan struct{}
	once       sync.Once
	done       chan struct{}
	cleanup    error
}

// Connect uses Go's native NewClientConn with separate pool-free transport state.
// Source-wide connection admission still applies, including during construction.
// Raw ClientConn, its Reserve/Release authority and state hooks are not exposed.
// Optional RequestOptionsV1 selects the route at establishment; child calls
// cannot replace the proxy of this already-established connection.
func (client *Client) Connect(ctx context.Context, id fault.Correlation, scheme, address string, options ...RequestOptionsV1) (*Connection, *invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, nil, err
	}
	if scheme != "http" && scheme != "https" || len(address) > 8192 {
		return nil, nil, failure(ErrInput, "connect")
	}
	target, err := url.Parse(scheme + "://" + address)
	if err != nil || target == nil || target.User != nil || target.Path != "" || target.RawQuery != "" || target.Fragment != "" ||
		len(address) > 8192 || httptrace.ContextClientTrace(ctx) != nil {
		return nil, nil, failure(ErrInput, "connect", err)
	}
	if err := validateRequest(ctx, &http.Request{Method: "GET", URL: target}, client.owner.settings); err != nil {
		return nil, nil, err
	}
	address = authority(target)
	if _, _, err := net.SplitHostPort(address); err != nil {
		return nil, nil, failure(ErrInput, "address", err)
	}
	proxy, err := requestOptions(options, client.owner.settings.RoutingLocked)
	if err != nil {
		return nil, nil, err
	}
	op, err := client.begin(ctx, id, invocation.Session, nil, proxy)
	if err != nil {
		if op == nil {
			return nil, nil, err
		}
		return nil, op.call.Receipt(), err
	}
	ready := make(chan struct{})
	var deliveryMu sync.Mutex
	var connection *Connection
	var nativeErr error
	available, abandoned := false, false
	op.callbacks.enter()
	go func() {
		defer op.callbacks.leave()
		_, err := op.call.Attempt()
		var native *http.ClientConn
		if err == nil {
			// Native NewClientConn supplies a proxy preview without the target port.
			// Resolve against the full authority once, then freeze establishment.
			preview := (&http.Request{Method: "GET", URL: &url.URL{Scheme: scheme, Host: address, Path: "/"}, Host: address}).WithContext(op.ctx)
			var route *url.URL
			route, err = client.owner.proxy(preview)
			if err == nil {
				bound := proxyChoice{selection: ProxyDirect}
				if route != nil {
					bound = proxyChoice{selection: ProxyAddress, address: route}
				}
				native, err = client.owner.direct.NewClientConn(context.WithValue(op.ctx, boundProxyKey{}, bound), scheme, address)
			}
		}
		if err != nil {
			err = failure(ErrTransport, "connect", err, op.ctx.Err(), context.Cause(op.ctx))
			op.fail(err)
		} else {
			op.mu.Lock()
			op.data.connected = true
			op.mu.Unlock()
		}
		deliveryMu.Lock()
		if abandoned {
			deliveryMu.Unlock()
			if native != nil {
				cleanup := native.Close()
				op.mu.Lock()
				op.extraCleanup = cleanup
				op.mu.Unlock()
			}
			op.finish()
			return
		}
		if native != nil {
			connection = &Connection{client: client, op: op, native: native, scheme: scheme, address: address, done: make(chan struct{})}
		}
		nativeErr, available = err, true
		if err != nil {
			op.finish()
		}
		close(ready)
		deliveryMu.Unlock()
	}()
	select {
	case <-ready:
	case <-op.ctx.Done():
		deliveryMu.Lock()
		if !available {
			abandoned = true
			deliveryMu.Unlock()
			op.finish()
			return nil, op.call.Receipt(), invocation.ErrWait.New(fault.Context{Provider: ProviderID, Operation: "connect"}, op.ctx.Err(), context.Cause(op.ctx))
		}
		deliveryMu.Unlock()
	}
	deliveryMu.Lock()
	defer deliveryMu.Unlock()
	return connection, op.call.Receipt(), nativeErr
}

func authority(address *url.URL) string {
	port := address.Port()
	if port == "" {
		if address.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return net.JoinHostPort(address.Hostname(), port)
}

// Open sends to this connection's exact URL authority. Host remains a supported
// explicit per-request input. Cross-authority redirects are refused rather than
// being silently sent on a connection established for another destination.
// Correlation.Parent must identify the Connect call.
func (connection *Connection) Open(ctx context.Context, id fault.Correlation, request *http.Request) (*Stream, *invocation.Receipt[Result], error) {
	return connection.open(ctx, id, request, invocation.Stream)
}
func (connection *Connection) open(ctx context.Context, id fault.Correlation, request *http.Request, shape invocation.Shape) (*Stream, *invocation.Receipt[Result], error) {
	if connection == nil || connection.op == nil {
		return nil, nil, failure(ErrState, "connection")
	}
	if err := connection.client.valid(ctx); err != nil {
		return nil, nil, err
	}
	if err := connection.op.ctx.Err(); err != nil {
		return nil, nil, failure(ErrState, "connection", err, context.Cause(connection.op.ctx))
	}
	if err := validateRequest(ctx, request, connection.client.owner.settings); err != nil {
		return nil, nil, err
	}
	if request.URL.Scheme != connection.scheme || !strings.EqualFold(authority(request.URL), connection.address) {
		return nil, nil, failure(ErrInput, "authority")
	}
	connection.mu.Lock()
	if connection.closing || connection.busy {
		connection.mu.Unlock()
		return nil, nil, failure(ErrState, "connection-busy")
	}
	connection.busy = true
	ready := make(chan struct{})
	connection.childReady = ready
	connection.mu.Unlock()
	work, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(connection.op.ctx, func() { cancel(context.Cause(connection.op.ctx)) })
	op, err := connection.client.begin(work, id, shape, connection.op, connection.op.proxy)
	if err != nil {
		stop()
		cancel(nil)
		connection.mu.Lock()
		connection.childOp = op
		connection.child = nil
		connection.busy = false
		close(ready)
		connection.mu.Unlock()
		if op == nil {
			return nil, nil, err
		}
		return nil, op.call.Receipt(), err
	}
	connection.mu.Lock()
	connection.childOp = op
	connection.child = nil
	connection.mu.Unlock()
	op.onDone = func() {
		stop()
		cancel(nil)
		<-ready
		connection.mu.Lock()
		connection.busy = false
		connection.mu.Unlock()
	}
	stream, receipt, err := op.open(request, directExchange{connection: connection})
	connection.mu.Lock()
	connection.child = stream
	close(ready)
	connection.mu.Unlock()
	return stream, receipt, err
}

// Do retains and closes a bounded child response. Its terminal evidence is
// independent of the connection-lifetime receipt.
func (connection *Connection) Do(ctx, cleanupCtx context.Context, id fault.Correlation, request *http.Request) (*invocation.Receipt[Result], error) {
	if cleanupCtx == nil {
		return nil, failure(ErrInput, "cleanup-context")
	}
	stream, receipt, err := connection.open(ctx, id, request, invocation.Finite)
	if stream == nil {
		return receipt, err
	}
	return consume(stream, cleanupCtx, receipt)
}

type directExchange struct{ connection *Connection }

func (exchange directExchange) RoundTrip(request *http.Request) (*http.Response, error) {
	connection := exchange.connection
	if request.URL.Scheme != connection.scheme || !strings.EqualFold(authority(request.URL), connection.address) {
		return nil, failure(ErrUnsupported, "redirect-authority")
	}
	return connection.native.RoundTrip(request)
}

func (connection *Connection) Close(ctx context.Context) error {
	if connection == nil || connection.op == nil || ctx == nil {
		return failure(ErrInput, "close")
	}
	connection.once.Do(func() {
		connection.mu.Lock()
		connection.closing = true
		ready := connection.childReady
		connection.mu.Unlock()
		connection.op.cancel()
		go func() {
			nativeErr := connection.native.Close()
			if ready != nil {
				<-ready
				connection.mu.Lock()
				child := connection.child
				childOp := connection.childOp
				connection.mu.Unlock()
				if child != nil {
					_ = child.Close(ctx)
				}
				if childOp != nil {
					<-childOp.done
				}
			}
			connection.op.callbacks.stop()
			<-connection.op.callbacks.done
			connection.op.mu.Lock()
			sockets := append([]*socket(nil), connection.op.sockets...)
			connection.op.mu.Unlock()
			var causes []error
			if nativeErr != nil {
				causes = append(causes, nativeErr)
			}
			for _, socket := range sockets {
				socket.closeMu.Lock()
				if socket.lastError != nil {
					causes = append(causes, socket.lastError)
				}
				socket.closeMu.Unlock()
			}
			if len(causes) > 0 {
				connection.cleanup = failure(ErrCleanup, "connection", causes...)
			}
			connection.op.mu.Lock()
			data := *connection.op.data
			data.complete = true
			connection.op.mu.Unlock()
			connection.op.call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: &data}, Cleanup: connection.cleanup})
			close(connection.op.done)
			close(connection.done)
		}()
	})
	select {
	case <-connection.done:
		return connection.cleanup
	default:
	}
	select {
	case <-connection.done:
		return connection.cleanup
	case <-ctx.Done():
		return invocation.ErrWait.New(fault.Context{Provider: ProviderID, Operation: "close"}, errors.Join(ctx.Err(), context.Cause(ctx)))
	}
}
