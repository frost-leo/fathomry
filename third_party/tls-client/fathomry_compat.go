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
	"io"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/bogdanfinn/quic-go-utls/http3"
)

// FathomryCompatibilityRevision identifies local corrections, not the SDK version.
const FathomryCompatibilityRevision = "v2"

var (
	ErrClientClosed      = errors.New("tls-client: client is closed")
	ErrBodyNotReplayable = errors.New("tls-client: protocol racing requires independent replayable bodies")
)

// Close stops a configuration-frozen client and joins managed racing, dial and
// proxy-cleanup work. Callers must close response bodies and must not concurrently
// mutate native configuration. Opaque callbacks remain cooperative. Failed socket
// cleanup can be retried; earlier cleanup causes remain inspectable.
// Close must not be called from a native hook or a body callback on this client.
func Close(client HttpClient) error {
	native, ok := client.(*httpClient)
	if !ok || native == nil {
		return errors.New("tls-client: unsupported client ownership")
	}
	rt, ok := native.Transport.(*roundTripper)
	if !ok || rt.compat == nil {
		return errors.New("tls-client: unsupported transport ownership")
	}
	return rt.compat.close(rt)
}

type compatibilityState struct {
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	closed     bool
	finished   bool
	active     int
	configured bool
	control    FathomryControlV1
	work       sync.WaitGroup
	conns      map[*compatConn]struct{}
	h3         map[*http3.Transport]func()
	closeMu    sync.Mutex
	cleanup    error
}

type compatibilityContextKey struct{}

func newCompatibilityState() *compatibilityState {
	ctx, cancel := context.WithCancel(context.Background())
	return &compatibilityState{ctx: ctx, cancel: cancel, conns: make(map[*compatConn]struct{}), h3: make(map[*http3.Transport]func())}
}
func (state *compatibilityState) begin(ctx context.Context) (context.Context, func(), error) {
	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		return nil, nil, ErrClientClosed
	}
	state.work.Add(1)
	state.active++
	state.mu.Unlock()
	work, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(state.ctx, cancel)
	if state.ctx.Err() != nil {
		cancel()
	}
	return work, func() { stop(); cancel(); state.end() }, nil
}

func (state *compatibilityState) end() {
	state.mu.Lock()
	state.active--
	state.mu.Unlock()
	state.work.Done()
}

// A child is reserved only while its parent owns a work token. Cleanup can
// therefore remain owned after new admission has been sealed.
func (state *compatibilityState) child() func() {
	state.mu.Lock()
	state.work.Add(1)
	state.active++
	state.mu.Unlock()
	return state.end
}
func (state *compatibilityState) failed(err error) {
	if err == nil || compatClosedError(err) {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.cleanup == nil {
		state.cleanup = err
	}
}

func compatClosedError(err error) bool {
	switch value := err.(type) {
	case interface{ Unwrap() []error }:
		causes := value.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !compatClosedError(cause) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return compatClosedError(value.Unwrap())
	default:
		return errors.Is(err, net.ErrClosed)
	}
}

func (state *compatibilityState) track(conn net.Conn) (net.Conn, error) {
	return state.trackReserved(conn, func() {})
}

func (state *compatibilityState) trackReserved(conn net.Conn, release func()) (net.Conn, error) {
	if nilInterface(conn) {
		release()
		return nil, errors.New("tls-client: nil native connection")
	}
	if tracked, ok := conn.(*compatConn); ok && tracked.owner == state {
		release()
		return tracked, nil
	}
	tracked := &compatConn{native: conn, owner: state, uses: newCompatUse(), release: release}
	state.mu.Lock()
	state.conns[tracked] = struct{}{}
	closed := state.closed
	state.mu.Unlock()
	if closed {
		return nil, errors.Join(ErrClientClosed, tracked.Close())
	}
	return tracked, nil
}
func (state *compatibilityState) dial(ctx context.Context, network, addr string, dial func(context.Context, string, string) (net.Conn, error)) (net.Conn, error) {
	work, done, err := state.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err = work.Err(); err != nil {
		return nil, err
	}
	release, err := state.reserve(false)
	if err != nil {
		return nil, err
	}
	conn, err := dial(work, network, addr)
	if conn == nil && err != nil {
		release()
		return nil, err
	}
	tracked, trackErr := state.trackReserved(conn, release)
	if err != nil || trackErr != nil || work.Err() != nil {
		if tracked != nil {
			err = errors.Join(err, tracked.Close())
		}
		return nil, errors.Join(err, trackErr, work.Err())
	}
	return tracked, nil
}
func (state *compatibilityState) registerH3(transport *http3.Transport) error {
	release, err := state.reserve(true)
	if err != nil {
		return errors.Join(err, transport.Close())
	}
	state.mu.Lock()
	closed := state.closed
	if !closed {
		state.h3[transport] = release
	}
	state.mu.Unlock()
	if closed {
		release()
		return errors.Join(ErrClientClosed, transport.Close())
	}
	return nil
}
func (state *compatibilityState) closeTCP() {
	state.mu.Lock()
	conns := make([]*compatConn, 0, len(state.conns))
	for conn := range state.conns {
		conns = append(conns, conn)
	}
	state.mu.Unlock()
	for _, conn := range conns {
		state.failed(conn.Close())
	}
}
func (state *compatibilityState) close(rt *roundTripper) error {
	state.closeMu.Lock()
	defer state.closeMu.Unlock()
	state.mu.Lock()
	state.closed = true
	transports := make([]*http3.Transport, 0, len(state.h3))
	for transport := range state.h3 {
		transports = append(transports, transport)
	}
	state.mu.Unlock()
	state.cancel()
	state.closeTCP()
	var retired []*http3.Transport
	for _, transport := range transports {
		err := transport.Close()
		state.failed(err)
		if err == nil {
			retired = append(retired, transport)
		}
	}
	state.work.Wait()
	for _, transport := range retired {
		state.mu.Lock()
		release := state.h3[transport]
		delete(state.h3, transport)
		state.mu.Unlock()
		if release != nil {
			release()
		}
	}
	state.closeTCP()
	rt.CloseIdleConnections()
	rt.Lock()
	for address, conn := range rt.cachedConnections {
		state.failed(conn.Close())
		delete(rt.cachedConnections, address)
	}
	rt.Unlock()
	state.mu.Lock()
	defer state.mu.Unlock()
	state.finished = state.active == 0 && len(state.conns) == 0 && len(state.h3) == 0
	return state.cleanup
}
func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return reflected.IsNil()
	}
	return false
}

type compatUse struct {
	mu     sync.Mutex
	closed bool
	active int
	done   chan struct{}
}

func newCompatUse() *compatUse { return &compatUse{done: make(chan struct{})} }
func (use *compatUse) enter() bool {
	use.mu.Lock()
	defer use.mu.Unlock()
	if use.closed {
		return false
	}
	use.active++
	return true
}
func (use *compatUse) leave() {
	use.mu.Lock()
	defer use.mu.Unlock()
	use.active--
	if use.closed && use.active == 0 {
		close(use.done)
	}
}
func (use *compatUse) stop() <-chan struct{} {
	use.mu.Lock()
	defer use.mu.Unlock()
	if !use.closed {
		use.closed = true
		if use.active == 0 {
			close(use.done)
		}
	}
	return use.done
}

type compatConn struct {
	native  net.Conn
	owner   *compatibilityState
	uses    *compatUse
	mu      sync.Mutex
	closed  bool
	release func()
}

func (conn *compatConn) Read(data []byte) (int, error) {
	if !conn.uses.enter() {
		return 0, net.ErrClosed
	}
	defer conn.uses.leave()
	return conn.native.Read(data)
}
func (conn *compatConn) Write(data []byte) (int, error) {
	if !conn.uses.enter() {
		return 0, net.ErrClosed
	}
	defer conn.uses.leave()
	return conn.native.Write(data)
}
func (conn *compatConn) Close() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.closed {
		return nil
	}
	done := conn.uses.stop()
	err := conn.native.Close()
	<-done
	if err == nil || compatClosedError(err) {
		conn.closed = true
		conn.owner.mu.Lock()
		delete(conn.owner.conns, conn)
		conn.owner.mu.Unlock()
		if conn.release != nil {
			conn.release()
		}
		return nil
	}
	conn.owner.failed(err)
	return err
}

type compatAddress struct{ network, address string }

func (address compatAddress) Network() string { return address.network }
func (address compatAddress) String() string  { return address.address }
func freezeAddress(address net.Addr) net.Addr {
	if nilInterface(address) {
		return compatAddress{}
	}
	return compatAddress{address.Network(), address.String()}
}
func (conn *compatConn) LocalAddr() net.Addr {
	if !conn.uses.enter() {
		return compatAddress{}
	}
	defer conn.uses.leave()
	return freezeAddress(conn.native.LocalAddr())
}
func (conn *compatConn) RemoteAddr() net.Addr {
	if !conn.uses.enter() {
		return compatAddress{}
	}
	defer conn.uses.leave()
	return freezeAddress(conn.native.RemoteAddr())
}
func (conn *compatConn) SetDeadline(until time.Time) error {
	if !conn.uses.enter() {
		return net.ErrClosed
	}
	defer conn.uses.leave()
	return conn.native.SetDeadline(until)
}
func (conn *compatConn) SetReadDeadline(until time.Time) error {
	if !conn.uses.enter() {
		return net.ErrClosed
	}
	defer conn.uses.leave()
	return conn.native.SetReadDeadline(until)
}
func (conn *compatConn) SetWriteDeadline(until time.Time) error {
	if !conn.uses.enter() {
		return net.ErrClosed
	}
	defer conn.uses.leave()
	return conn.native.SetWriteDeadline(until)
}

type compatBody struct {
	io.ReadCloser
	uses  *compatUse
	once  sync.Once
	after func() error
	err   error
}

func newCompatBody(raw io.ReadCloser, after func() error) *compatBody {
	return &compatBody{ReadCloser: raw, uses: newCompatUse(), after: after}
}

func (body *compatBody) Read(data []byte) (int, error) {
	if !body.uses.enter() {
		return 0, net.ErrClosed
	}
	defer body.uses.leave()
	return body.ReadCloser.Read(data)
}

func (body *compatBody) Close() error {
	body.once.Do(func() {
		done := body.uses.stop()
		body.err = body.ReadCloser.Close()
		<-done
		if body.after != nil {
			body.err = errors.Join(body.err, body.after())
		}
	})
	return body.err
}
