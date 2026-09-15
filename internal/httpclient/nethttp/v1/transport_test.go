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
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExplicitDirectTransportIgnoresAmbientProxy(t *testing.T) {
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "direct") })
	options.ServerName = "127.0.0.1"
	var observed string
	options.Native.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		observed = address
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	f := bindFixture(t, options, 1)
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("direct"), newRequest(t, "GET", "https://logical.invalid/", nil))
	if err != nil {
		t.Fatal(err)
	}
	result := settle(t, f, receipt)
	if observed != "logical.invalid:443" || string(result.Outcome.Value.DataCopy()) != "direct" {
		t.Fatal("ambient proxy changed explicit transport")
	}
}

func TestConfiguredConnectProxy(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		origin, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "tunneled") })
		var tunnels atomic.Int64
		var bridging sync.WaitGroup
		proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != "CONNECT" || request.Host != origin.Listener.Addr().String() {
				t.Error("proxy target changed")
				writer.WriteHeader(400)
				return
			}
			if !strings.HasPrefix(request.Header.Get("Proxy-Authorization"), "Basic ") {
				t.Error("configured proxy authentication missing")
				writer.WriteHeader(407)
				return
			}
			target, err := net.DialTimeout("tcp", request.Host, time.Second)
			if err != nil {
				t.Error(err)
				writer.WriteHeader(502)
				return
			}
			hijacker := writer.(http.Hijacker)
			client, buffer, err := hijacker.Hijack()
			if err != nil {
				_ = target.Close()
				t.Error(err)
				return
			}
			tunnels.Add(1)
			bridging.Add(1)
			defer bridging.Done()
			defer client.Close()
			defer target.Close()
			_, _ = io.WriteString(buffer, "HTTP/1.1 200 Connection Established\r\n\r\n")
			_ = buffer.Flush()
			done := make(chan struct{})
			go func() { defer close(done); _, _ = io.Copy(client, target); _ = client.Close() }()
			_, _ = io.Copy(target, buffer.Reader)
			_ = target.Close()
			<-done
		}))
		t.Cleanup(func() {
			proxy.Close()
			done := make(chan struct{})
			go func() { bridging.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("proxy bridge did not stop")
			}
		})
		options.ProxyURL = strings.Replace(proxy.URL, "://", "://synthetic-user:synthetic-password@", 1)
		f := bindFixture(t, options, 1)
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation("proxy"), newRequest(t, "GET", origin.URL, nil))
		if err != nil {
			t.Fatal(err)
		}
		result := settle(t, f, receipt)
		if tunnels.Load() != 1 || string(result.Outcome.Value.DataCopy()) != "tunneled" || !result.Outcome.Value.Complete() {
			t.Fatal("native CONNECT capability failed")
		}
	})
}

func TestSourceWideConnectionCapacity(t *testing.T) {
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "data") })
	options.MaxConnections = 1
	f := bindFixture(t, options, 2)
	connection, parent, err := f.client.Connect(deadline(t), correlation("held"), "https", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(deadline(t))
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("limited"), newRequest(t, "GET", server.URL, nil))
	if err == nil || !errors.Is(err, ErrLimit) {
		t.Fatal("direct and pooled resources bypassed shared connection capacity", err)
	}
	if _, err := receipt.WaitReleased(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := parent.WaitReleased(deadline(t)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		delivery, err := f.inbox.Next(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestUpgradeDoesNotExposeOwningSocket(t *testing.T) {
	var bridge sync.WaitGroup
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) {
		connection, buffer, err := writer.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		bridge.Add(1)
		defer bridge.Done()
		defer connection.Close()
		_, _ = io.WriteString(buffer, "HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: synthetic\r\n\r\n")
		_ = buffer.Flush()
		_, _ = io.Copy(io.Discard, bufio.NewReader(connection))
	})
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { bridge.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("upgraded peer leaked")
		}
	})
	f := bindFixture(t, options, 1)
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("upgrade"), newRequest(t, "GET", server.URL, nil))
	result := settle(t, f, receipt)
	if !errors.Is(err, ErrUnsupported) || !errors.Is(result.Err(), ErrUnsupported) || result.Outcome.Value.Complete() {
		t.Fatal("raw upgrade was silently accepted", err)
	}
}

type blockedConnMethod struct {
	method  string
	entered chan struct{}
	resume  chan struct{}
	closed  chan struct{}
	once    sync.Once
	close   sync.Once
	calls   atomic.Int64
}

func (conn *blockedConnMethod) call(method string) {
	conn.calls.Add(1)
	if method == conn.method {
		conn.once.Do(func() { close(conn.entered); <-conn.resume })
	}
}
func (conn *blockedConnMethod) Read([]byte) (int, error) {
	conn.call("read")
	return 0, io.EOF
}
func (conn *blockedConnMethod) Write(data []byte) (int, error) {
	conn.call("write")
	return len(data), nil
}
func (conn *blockedConnMethod) Close() error {
	conn.close.Do(func() { close(conn.closed) })
	return nil
}
func (conn *blockedConnMethod) LocalAddr() net.Addr {
	conn.call("local-address")
	return blockedConnAddress{conn}
}
func (conn *blockedConnMethod) RemoteAddr() net.Addr {
	conn.call("remote-address")
	return blockedConnAddress{conn}
}

type blockedConnAddress struct{ conn *blockedConnMethod }

func (address blockedConnAddress) Network() string {
	address.conn.call("address-network")
	return "tcp"
}
func (address blockedConnAddress) String() string {
	address.conn.call("address-string")
	return "127.0.0.1:1234"
}
func (conn *blockedConnMethod) SetDeadline(time.Time) error {
	conn.call("deadline")
	return nil
}
func (conn *blockedConnMethod) SetReadDeadline(time.Time) error {
	conn.call("read-deadline")
	return nil
}
func (conn *blockedConnMethod) SetWriteDeadline(time.Time) error {
	conn.call("write-deadline")
	return nil
}

func TestSocketCloseJoinsEveryNativeMethod(t *testing.T) {
	methods := []struct {
		name string
		call func(net.Conn) error
	}{
		{"read", func(conn net.Conn) error { _, err := conn.Read(nil); return err }},
		{"write", func(conn net.Conn) error { _, err := conn.Write(nil); return err }},
		{"local-address", func(conn net.Conn) error { _ = conn.LocalAddr().String(); return nil }},
		{"remote-address", func(conn net.Conn) error { _ = conn.RemoteAddr().String(); return nil }},
		{"address-network", func(conn net.Conn) error { _ = conn.LocalAddr().Network(); return nil }},
		{"address-string", func(conn net.Conn) error { _ = conn.RemoteAddr().String(); return nil }},
		{"deadline", func(conn net.Conn) error { return conn.SetDeadline(time.Time{}) }},
		{"read-deadline", func(conn net.Conn) error { return conn.SetReadDeadline(time.Time{}) }},
		{"write-deadline", func(conn net.Conn) error { return conn.SetWriteDeadline(time.Time{}) }},
	}
	for _, method := range methods {
		t.Run(method.name, func(t *testing.T) {
			raw := &blockedConnMethod{method: method.name, entered: make(chan struct{}), resume: make(chan struct{}), closed: make(chan struct{})}
			var once sync.Once
			unblock := func() { once.Do(func() { close(raw.resume) }) }
			defer unblock()
			own := &owner{sockets: make(map[*socket]struct{})}
			conn := &socket{native: raw, owner: own, io: newActivity()}
			own.sockets[conn] = struct{}{}
			returned := make(chan struct{})
			go func() { _ = method.call(conn); close(returned) }()
			select {
			case <-raw.entered:
			case <-time.After(time.Second):
				t.Fatal("native method not reached")
			}
			closed := make(chan error, 1)
			go func() { closed <- conn.Close() }()
			<-raw.closed
			select {
			case <-closed:
				unblock()
				<-returned
				t.Fatal("socket released before its native method returned")
			case <-time.After(20 * time.Millisecond):
			}
			own.mu.Lock()
			retained := len(own.sockets)
			own.mu.Unlock()
			if retained != 1 {
				t.Fatal("live native method lost source accounting")
			}
			unblock()
			<-returned
			if err := <-closed; err != nil {
				t.Fatal(err)
			}
			if len(own.sockets) != 0 {
				t.Fatal("completed native method retained socket capacity")
			}
			before := raw.calls.Load()
			for _, late := range methods {
				err := late.call(conn)
				if !strings.Contains(late.name, "address") && !errors.Is(err, net.ErrClosed) {
					t.Fatal("closed method did not reject work", late.name, err)
				}
			}
			if raw.calls.Load() != before {
				t.Fatal("closed socket invoked its native dependency")
			}
		})
	}
}

func TestNativeSocketAddressesAreFrozen(t *testing.T) {
	for _, address := range []net.Addr{nil, (*net.TCPAddr)(nil)} {
		copy := copyAddress(address)
		if copy == nil || copy.String() != "" || copy.Network() != "" {
			t.Fatal("absent native address did not become safe empty metadata")
		}
	}
	original := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
	copy := copyAddress(original)
	original.Port = 4321
	if copy.Network() != "tcp" || copy.String() != "127.0.0.1:1234" {
		t.Fatal("socket address retained mutable native state")
	}
}

func TestClaimAndPendingSocketCleanupHaveOneOwnershipOrder(t *testing.T) {
	for _, first := range []string{"claim", "cancel", "cancel-before-registration"} {
		t.Run(first, func(t *testing.T) {
			raw := &blockedConnMethod{closed: make(chan struct{})}
			own := &owner{sockets: make(map[*socket]struct{})}
			conn := &socket{native: raw, owner: own, io: newActivity()}
			own.sockets[conn] = struct{}{}
			if first != "cancel-before-registration" {
				conn.cancel = func() bool { return false }
			}
			if first == "claim" {
				claimConnection(conn)
			}
			conn.closeUnclaimed()
			closed := false
			select {
			case <-raw.closed:
				closed = true
			default:
			}
			if closed == (first == "claim") {
				t.Fatal("queued cleanup ignored the established ownership order")
			}
			claimConnection(conn)
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			<-raw.closed
			if len(own.sockets) != 0 {
				t.Fatal("explicit close failed after claiming")
			}
		})
	}
}
