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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

type lifetimeSOCKSPeer struct {
	address      string
	targets      chan string
	connections  atomic.Int64
	originDials  atomic.Int64
	authObserved atomic.Bool
}

func lifetimeNewSOCKSPeer(t *testing.T, origin string, auth bool, failure string, alternatives ...string) *lifetimeSOCKSPeer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer := &lifetimeSOCKSPeer{address: listener.Addr().String(), targets: make(chan string, 16)}
	var workers sync.WaitGroup
	var socketsMu sync.Mutex
	sockets := make(map[net.Conn]struct{})
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			peer.connections.Add(1)
			socketsMu.Lock()
			sockets[conn] = struct{}{}
			socketsMu.Unlock()
			workers.Go(func() {
				defer func() {
					_ = conn.Close()
					socketsMu.Lock()
					delete(sockets, conn)
					socketsMu.Unlock()
				}()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				var greeting [2]byte
				if _, err := io.ReadFull(conn, greeting[:]); err != nil {
					return
				}
				if greeting[0] != 5 || greeting[1] == 0 {
					t.Error("invalid SOCKS greeting")
					return
				}
				methods := make([]byte, int(greeting[1]))
				if _, err := io.ReadFull(conn, methods); err != nil {
					return
				}
				if failure == "greeting-refusal" {
					_, _ = conn.Write([]byte{5, 255})
					return
				}
				method := byte(0)
				if auth {
					method = 2
				}
				if _, err := conn.Write([]byte{5, method}); err != nil {
					return
				}
				if auth {
					var authPrefix [2]byte
					if _, err := io.ReadFull(conn, authPrefix[:]); err != nil {
						return
					}
					username := make([]byte, int(authPrefix[1]))
					if _, err := io.ReadFull(conn, username); err != nil {
						return
					}
					var passwordSize [1]byte
					if _, err := io.ReadFull(conn, passwordSize[:]); err != nil {
						return
					}
					password := make([]byte, int(passwordSize[0]))
					if _, err := io.ReadFull(conn, password); err != nil {
						return
					}
					if authPrefix[0] != 1 || string(username) != "synthetic-user" || string(password) != "synthetic-pass" {
						t.Error("SOCKS username/password negotiation changed")
						return
					}
					peer.authObserved.Store(true)
					if _, err := conn.Write([]byte{1, 0}); err != nil {
						return
					}
				}
				var request [4]byte
				if _, err := io.ReadFull(conn, request[:]); err != nil {
					return
				}
				if request[0] != 5 || request[1] != 1 || request[2] != 0 {
					t.Error("invalid SOCKS CONNECT request")
					return
				}
				var host string
				switch request[3] {
				case 1, 4:
					size := net.IPv4len
					if request[3] == 4 {
						size = net.IPv6len
					}
					ip := make(net.IP, size)
					if _, err := io.ReadFull(conn, ip); err != nil {
						return
					}
					host = ip.String()
				case 3:
					var size [1]byte
					if _, err := io.ReadFull(conn, size[:]); err != nil {
						return
					}
					name := make([]byte, int(size[0]))
					if _, err := io.ReadFull(conn, name); err != nil {
						return
					}
					host = string(name)
				default:
					t.Error("unsupported test SOCKS address type")
					return
				}
				var port [2]byte
				if _, err := io.ReadFull(conn, port[:]); err != nil {
					return
				}
				peer.targets <- net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(port[:]))))
				if failure == "truncated" {
					_, _ = conn.Write([]byte{5, 0})
					return
				}
				if failure == "connect-refusal" {
					_, _ = conn.Write([]byte{5, 1, 0, 1, 127, 0, 0, 1, 0, 0})
					return
				}
				if failure == "stall" {
					_, _ = io.Copy(io.Discard, conn)
					return
				}
				selected := origin
				for _, candidate := range alternatives {
					_, candidatePort, _ := net.SplitHostPort(candidate)
					if candidatePort == strconv.Itoa(int(binary.BigEndian.Uint16(port[:]))) {
						selected = candidate
					}
				}
				target, err := net.DialTimeout("tcp", selected, time.Second)
				if err != nil {
					t.Error(err)
					return
				}
				peer.originDials.Add(1)
				defer target.Close()
				if _, err := conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0}); err != nil {
					return
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					_, _ = io.Copy(conn, target)
					_ = conn.Close()
				}()
				_, _ = io.Copy(target, conn)
				_ = target.Close()
				<-done
			})
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-accepted
		socketsMu.Lock()
		for conn := range sockets {
			_ = conn.Close()
		}
		socketsMu.Unlock()
		done := make(chan struct{})
		go func() { workers.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("test SOCKS bridge did not terminate")
		}
	})
	return peer
}

func TestSOCKSLocalProtocolParityAndFailure(t *testing.T) {
	for _, scheme := range []string{"socks5", "socks5h"} {
		t.Run(scheme, func(t *testing.T) {
			eachProtocol(t, func(t *testing.T, h2 bool) {
				for _, auth := range []bool{false, true} {
					t.Run(fmt.Sprintf("auth-%t", auth), func(t *testing.T) {
						origin, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
							if request.Header.Get("Proxy-Authorization") != "" {
								t.Error("SOCKS authentication escaped into HTTP headers")
							}
							_, _ = io.WriteString(writer, request.Proto)
						})
						peer := lifetimeNewSOCKSPeer(t, origin.Listener.Addr().String(), auth, "")
						proxyURL := &url.URL{Scheme: scheme, Host: peer.address}
						if auth {
							proxyURL.User = url.UserPassword("synthetic-user", "synthetic-pass")
						}
						options.ProxyURL, options.ServerName = proxyURL.String(), "127.0.0.1"
						f := bindFixture(t, options, 1)
						_, port, _ := net.SplitHostPort(origin.Listener.Addr().String())
						address := net.JoinHostPort("target.synthetic.invalid", port)
						for index := range 2 {
							receipt, err := f.client.Do(deadline(t), deadline(t), correlation(fmt.Sprintf("socks-%d", index)), newRequest(t, "GET", "https://"+address+"/", nil))
							if err != nil {
								t.Fatal(err)
							}
							result := settle(t, f, receipt)
							want := "HTTP/1.1"
							if h2 {
								want = "HTTP/2.0"
							}
							if !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != want {
								t.Fatal("SOCKS origin protocol/result changed")
							}
						}
						select {
						case target := <-peer.targets:
							if target != address {
								t.Fatal("SOCKS did not preserve remote target DNS/port")
							}
						case <-deadline(t).Done():
							t.Fatal("SOCKS peer did not receive CONNECT")
						}
						if peer.connections.Load() != 1 || peer.originDials.Load() != 1 || peer.authObserved.Load() != auth {
							t.Fatal("SOCKS authentication or native pool reuse changed")
						}
					})
				}
			})
		})
	}
	for _, failure := range []string{"greeting-refusal", "connect-refusal", "truncated"} {
		t.Run(failure, func(t *testing.T) {
			peer := lifetimeNewSOCKSPeer(t, "127.0.0.1:1", false, failure)
			options := OptionsV1{Name: "socks-failure", HTTP1: true, ProxyURL: "socks5://" + peer.address}
			f := bindFixture(t, options, 1)
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation(failure), newRequest(t, "GET", "https://target.synthetic.invalid/", nil))
			if err == nil || peer.originDials.Load() != 0 {
				t.Fatal("failed SOCKS setup reached origin or lost error", err)
			}
			result := settle(t, f, receipt)
			if result.Outcome.Value.Complete() || !errors.Is(result.Err(), ErrTransport) {
				t.Fatal("failed SOCKS setup evidence changed", result.Err())
			}
			f.client.owner.mu.Lock()
			remaining := len(f.client.owner.sockets) + f.client.owner.pending
			f.client.owner.mu.Unlock()
			if remaining != 0 {
				t.Fatal("failed SOCKS setup lost physical socket cleanup")
			}
		})
	}
}

type lifetimeCloseSignal struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (conn *lifetimeCloseSignal) Close() error {
	conn.once.Do(func() { close(conn.closed) })
	return conn.Conn.Close()
}

func TestTLSPlannedLifetimeBeforeNativeStart(t *testing.T) {
	for _, route := range []string{"direct", "https-proxy-inner", "socks", "direct-timeout", "socks-timeout"} {
		t.Run(route, func(t *testing.T) {
			origin, options := newPeer(t, false, func(http.ResponseWriter, *http.Request) {})
			wantStart := int64(1)
			switch strings.TrimSuffix(route, "-timeout") {
			case "https-proxy-inner":
				var routes sync.Map
				proxy := newRouteProxyTransport(t, true, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "test" }, nil)
				options.ProxyURL = proxy.server.URL
				wantStart = 2
			case "socks":
				proxy := lifetimeNewSOCKSPeer(t, origin.Listener.Addr().String(), false, "")
				options.ProxyURL = "socks5://" + proxy.address
			}
			if strings.HasSuffix(route, "-timeout") {
				options.TLSHandshakeTimeout = 10 * time.Millisecond
			}
			nativeClosed := make(chan struct{})
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var starts atomic.Int64
			options.Native.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				trace := httptrace.ContextClientTrace(ctx)
				if trace == nil {
					return nil, errors.New("missing private trace in test dial")
				}
				original := trace.TLSHandshakeStart
				// Test-only scheduling gate: installed before DialContext returns,
				// so no handshake can yet read this private trace field.
				trace.TLSHandshakeStart = func() {
					if starts.Add(1) == wantStart {
						close(entered)
						<-release
					}
					if original != nil {
						original()
					}
				}
				conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return &lifetimeCloseSignal{Conn: conn, closed: nativeClosed}, nil
			}
			f := bindFixture(t, options, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type completion struct {
				receipt *invocation.Receipt[Result]
				err     error
			}
			done := make(chan completion, 1)
			cleanup := deadline(t)
			go func() {
				receipt, err := f.client.Do(ctx, cleanup, correlation(strings.ReplaceAll(route, "-", "")), newRequest(t, "GET", origin.URL, nil))
				done <- completion{receipt: receipt, err: err}
			}()
			select {
			case <-entered:
			case <-deadline(t).Done():
				t.Fatal("TLS scheduling gate did not enter")
			}
			if strings.HasSuffix(route, "-timeout") {
				select {
				case <-nativeClosed:
				case <-deadline(t).Done():
					t.Fatal("native TLS timeout did not close socket while Start was still gated")
				}
			}
			cancel()
			var completed completion
			select {
			case completed = <-done:
			case <-deadline(t).Done():
				t.Fatal("cancellation did not return the waiter")
			}
			if completed.receipt == nil || !errors.Is(completed.err, context.Canceled) {
				t.Fatal("canceled native handshake lost its receipt", completed.err)
			}
			short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer stop()
			if result, err := completed.receipt.WaitReleased(short); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("root released before scheduled TLS start: Final=%t Released=%t wait=%v", result.Final, result.Released, err)
			}
			unblock()
			result := settle(t, f, completed.receipt)
			if result.Outcome.Value.Complete() || !errors.Is(result.Err(), context.Canceled) {
				t.Fatal("canceled planned handshake falsely completed", result.Err())
			}
		})
	}
}

func TestSOCKSPlainAndDirectConnections(t *testing.T) {
	for _, mode := range []string{"http-pooled", "http-direct", "https-direct"} {
		t.Run(mode, func(t *testing.T) {
			handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Header.Get("Proxy-Authorization") != "" {
					t.Error("SOCKS authentication appeared in origin request")
				}
				_, _ = io.WriteString(writer, "socks-origin")
			})
			scheme := "http"
			var origin *httptest.Server
			var options OptionsV1
			if mode == "https-direct" {
				scheme = "https"
				origin, options = newPeer(t, false, handler)
				options.ServerName = "127.0.0.1"
			} else {
				origin = httptest.NewServer(handler)
				t.Cleanup(origin.Close)
				options = OptionsV1{Name: "socks-plain", HTTP1: true}
			}
			peer := lifetimeNewSOCKSPeer(t, origin.Listener.Addr().String(), true, "")
			proxyURL := &url.URL{Scheme: "socks5h", Host: peer.address, User: url.UserPassword("synthetic-user", "synthetic-pass")}
			options.ProxyURL, options.MaxConnections = proxyURL.String(), 1
			f := bindFixture(t, options, 3)
			_, port, _ := net.SplitHostPort(origin.Listener.Addr().String())
			address := net.JoinHostPort("target.synthetic.invalid", port)
			endpoint := scheme + "://" + address + "/"
			var parent *invocation.Receipt[Result]
			var connection *Connection
			if strings.HasSuffix(mode, "direct") {
				var err error
				connection, parent, err = f.client.Connect(deadline(t), correlation("parent"), scheme, address)
				if err != nil {
					t.Fatal(err)
				}
				defer connection.Close(deadline(t))
			}
			var children []*invocation.Receipt[Result]
			for index := range 2 {
				var receipt *invocation.Receipt[Result]
				var err error
				if connection == nil {
					receipt, err = f.client.Do(deadline(t), deadline(t), correlation(fmt.Sprintf("plain-%d", index)), newRequest(t, "GET", endpoint, nil))
				} else {
					receipt, err = connection.Do(deadline(t), deadline(t), fault.Correlation{Call: fmt.Sprintf("child-%d", index), Parent: "parent"}, newRequest(t, "GET", endpoint, nil))
				}
				if err != nil {
					t.Fatal(err)
				}
				result, err := receipt.WaitReleased(deadline(t))
				if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "socks-origin" {
					t.Fatal("SOCKS plain/direct response differs", err)
				}
				children = append(children, receipt)
			}
			if connection != nil {
				if err := connection.Close(deadline(t)); err != nil {
					t.Fatal(err)
				}
				if result := settle(t, f, parent); !result.Outcome.Value.Connected() || !result.Outcome.Value.Complete() {
					t.Fatal("SOCKS direct connection lifetime incomplete")
				}
			}
			for _, receipt := range children {
				settle(t, f, receipt)
			}
			if peer.connections.Load() != 1 || peer.originDials.Load() != 1 || !peer.authObserved.Load() {
				t.Fatal("SOCKS plain/direct requests did not keep one authenticated connection")
			}
		})
	}
}

func TestCanceledSOCKSSetupLeavesNoPlannedTLS(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("direct-%t", direct), func(t *testing.T) {
			peer := lifetimeNewSOCKSPeer(t, "127.0.0.1:1", false, "stall")
			f := bindFixture(t, OptionsV1{Name: "socks-cancel", HTTP1: true, ProxyURL: "socks5://" + peer.address}, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type completion struct {
				receipt *invocation.Receipt[Result]
				err     error
			}
			done := make(chan completion, 1)
			cleanup := deadline(t)
			go func() {
				var receipt *invocation.Receipt[Result]
				var err error
				if direct {
					var connection *Connection
					connection, receipt, err = f.client.Connect(ctx, correlation("socks-cancel"), "https", "target.synthetic.invalid:443")
					if connection != nil {
						_ = connection.Close(cleanup)
						t.Error("stalled SOCKS handshake returned a direct connection")
					}
				} else {
					receipt, err = f.client.Do(ctx, cleanup, correlation("socks-cancel"), newRequest(t, "GET", "https://target.synthetic.invalid/", nil))
				}
				done <- completion{receipt: receipt, err: err}
			}()
			select {
			case <-peer.targets:
			case <-deadline(t).Done():
				t.Fatal("SOCKS request did not reach the blocking peer")
			}
			cancel()
			var completed completion
			select {
			case completed = <-done:
			case <-deadline(t).Done():
				t.Fatal("canceled SOCKS waiter did not return")
			}
			if completed.receipt == nil || !errors.Is(completed.err, context.Canceled) {
				t.Fatal("canceled SOCKS work lost its receipt", completed.err)
			}
			result := settle(t, f, completed.receipt)
			if result.Outcome.Value.Complete() || !errors.Is(result.Err(), context.Canceled) || peer.originDials.Load() != 0 {
				t.Fatal("canceled SOCKS setup reached origin or falsely completed", result.Err())
			}
			f.client.owner.mu.Lock()
			remaining := len(f.client.owner.sockets) + f.client.owner.pending
			f.client.owner.mu.Unlock()
			if remaining != 0 {
				t.Fatal("canceled SOCKS setup lost its physical socket")
			}
		})
	}
}

func TestNanosecondTLSDeadlineReleasesPlannedWork(t *testing.T) {
	for _, socks := range []bool{false, true} {
		t.Run(fmt.Sprintf("socks-%t", socks), func(t *testing.T) {
			origin, options := newPeer(t, false, func(http.ResponseWriter, *http.Request) {})
			if socks {
				peer := lifetimeNewSOCKSPeer(t, origin.Listener.Addr().String(), false, "")
				options.ProxyURL = "socks5://" + peer.address
			}
			options.TLSHandshakeTimeout = time.Nanosecond
			f := bindFixture(t, options, 1)
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation("tiny-timeout"), newRequest(t, "GET", origin.URL, nil))
			var timeout net.Error
			if !errors.As(err, &timeout) || !timeout.Timeout() {
				t.Fatal("tiny native TLS deadline did not produce timeout", err)
			}
			result := settle(t, f, receipt)
			if result.Outcome.Value.Complete() || !result.Released {
				t.Fatal("tiny TLS timeout leaked or falsely completed its root")
			}
		})
	}
}

func TestSOCKSBindingPreservesPerExchangeTLSPlanning(t *testing.T) {
	for _, first := range []string{"http", "https"} {
		t.Run(first+"-first", func(t *testing.T) {
			plain := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(writer, "plain") }))
			t.Cleanup(plain.Close)
			secure, options := newPeer(t, false, func(writer http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(writer, "tls") })
			peer := lifetimeNewSOCKSPeer(t, secure.Listener.Addr().String(), false, "", plain.Listener.Addr().String())
			options.ProxyURL, options.MaxConnections = "socks5://"+peer.address, 2
			f := bindFixture(t, options, 1)
			endpoints := []string{plain.URL, secure.URL, plain.URL, secure.URL}
			if first == "https" {
				endpoints = []string{secure.URL, plain.URL, secure.URL, plain.URL}
			}
			for index, endpoint := range endpoints {
				receipt, err := f.client.Do(deadline(t), deadline(t), correlation(fmt.Sprintf("mixed-%d", index)), newRequest(t, "GET", endpoint, nil))
				if err != nil {
					t.Fatal(err)
				}
				result := settle(t, f, receipt)
				want := "plain"
				if strings.HasPrefix(endpoint, "https:") {
					want = "tls"
				}
				if !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != want {
					t.Fatal("same route binding changed later exchange protocol")
				}
			}
			if peer.connections.Load() != 2 || peer.originDials.Load() != 2 {
				t.Fatal("same route binding did not preserve separate native HTTP/TLS pools")
			}
		})
	}
}

func TestIdleConnectionWinnerRetainsLateTLSHandshake(t *testing.T) {
	for _, socks := range []bool{false, true} {
		t.Run(fmt.Sprintf("socks-%t", socks), func(t *testing.T) {
			finishFirst := make(chan struct{})
			var firstOnce sync.Once
			finish := func() { firstOnce.Do(func() { close(finishFirst) }) }
			defer finish()
			origin, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path == "/first" {
					writer.Header().Set("Content-Length", "1")
					writer.WriteHeader(http.StatusOK)
					writer.(http.Flusher).Flush()
					<-finishFirst
					_, _ = io.WriteString(writer, "1")
					return
				}
				_, _ = io.WriteString(writer, "reused")
			})
			if socks {
				peer := lifetimeNewSOCKSPeer(t, origin.Listener.Addr().String(), false, "")
				options.ProxyURL = "socks5://" + peer.address
			}
			options.MaxActive, options.MaxConnections = 2, 2
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var dials atomic.Int64
			options.Native.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				if dials.Add(1) == 2 {
					trace := httptrace.ContextClientTrace(ctx)
					if trace == nil {
						return nil, errors.New("missing private trace")
					}
					original := trace.TLSHandshakeStart
					trace.TLSHandshakeStart = func() {
						close(entered)
						<-release
						if original != nil {
							original()
						}
					}
				}
				return (&net.Dialer{}).DialContext(ctx, network, address)
			}
			f := bindFixture(t, options, 2)
			first, firstReceipt, err := f.client.Open(deadline(t), correlation("busy"), newRequest(t, "GET", origin.URL+"/first", nil))
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close(deadline(t))
			type completion struct {
				stream  *Stream
				receipt *invocation.Receipt[Result]
				err     error
			}
			done := make(chan completion, 1)
			ctx := deadline(t)
			go func() {
				stream, receipt, err := f.client.Open(ctx, correlation("winner"), newRequest(t, "GET", origin.URL+"/second", nil))
				done <- completion{stream: stream, receipt: receipt, err: err}
			}()
			select {
			case <-entered:
			case <-deadline(t).Done():
				t.Fatal("second socket did not reach gated TLS setup")
			}
			finish()
			if _, err := io.Copy(io.Discard, first); err != nil {
				t.Fatal(err)
			}
			if err := first.Close(deadline(t)); err != nil {
				t.Fatal(err)
			}
			settle(t, f, firstReceipt)
			var completed completion
			select {
			case completed = <-done:
			case <-deadline(t).Done():
				t.Fatal("native pool did not hand idle connection to pending request")
			}
			if completed.err != nil || completed.stream == nil {
				t.Fatal("idle connection winner failed", completed.err)
			}
			body, err := io.ReadAll(completed.stream)
			if err != nil || string(body) != "reused" {
				t.Fatal("idle winner did not complete its independent response", err)
			}
			short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer stop()
			if err := completed.stream.Close(short); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("late detached TLS work did not retain cleanup", err)
			}
			if result, _ := completed.receipt.Result(); result.Final || result.Released {
				t.Fatal("successful idle winner released late native TLS work")
			}
			unblock()
			result := settle(t, f, completed.receipt)
			if result.Err() != nil || !result.Outcome.Value.Complete() || dials.Load() != 2 {
				t.Fatal("cleanup of unused TLS dial changed completed HTTP evidence", result.Err())
			}
		})
	}
}
