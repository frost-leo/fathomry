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
package surf

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/enetx/http"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestReviewSimultaneousRoutesKeepOriginalIdentity(t *testing.T) {
	for _, kind := range []string{"http", "https-h2", "socks4", "socks4a"} {
		t.Run(kind, func(t *testing.T) {
			var arrived atomic.Int32
			both := make(chan struct{})
			target := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				id := r.Header.Get("X-Call")
				data, err := io.ReadAll(r.Body)
				if err != nil || string(data) != id || r.Header.Get("Cookie") != "value="+id ||
					r.Header.Get("Proxy-Authorization") != "" || r.Header.Get("X-Route") != "" {
					t.Error("concurrent route changed runtime input or leaked proxy headers")
				}
				if arrived.Add(1) == 2 {
					close(both)
				}
				select {
				case <-both:
					_, _ = w.Write(data)
				case <-r.Context().Done():
				}
			}, false)
			roots := x509.NewCertPool()
			routes := make([]RequestOptionsV1, 2)
			if kind == "socks4" || kind == "socks4a" {
				route := newReviewSOCKS4(t, target.tcp.Listener.Addr().String(), kind == "socks4a")
				for index, id := range []string{"first", "second"} {
					value := (&url.URL{Scheme: kind, Host: route, User: url.User(id)}).String()
					routes[index].ProxyURL = &value
				}
			} else {
				for index, id := range []string{"first", "second"} {
					proxy := newProxy(t, kind == "https-h2", "Bearer "+id)
					if kind == "https-h2" {
						roots.AddCert(proxy.server.Certificate())
					}
					value := proxy.server.URL
					routes[index] = RequestOptionsV1{ProxyURL: &value, ConnectHeaders: http.Header{"proxy-authorization": {proxy.auth}, "X-Route": {"frozen"}}}
				}
			}
			fix := newFixture(t, OptionsV1{Name: "concurrent-routes", Mode: HTTP1Only,
				MaxActive: 2, MaxRoutes: 2, MaxTCPConnections: 2,
				Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: target.roots},
					ProxyTLSConfig: &tls.Config{RootCAs: roots, NextProtos: []string{"h2"}}},
			}, 2)
			ctx := testContext(t)
			var group sync.WaitGroup
			for index, id := range []string{"first", "second"} {
				group.Go(func() {
					input := request(t, "POST", target.tcp.URL, id)
					input.Header.Set("X-Call", id)
					input.Header.Set("Cookie", "value="+id)
					receipt, err := fix.client.Do(ctx, ctx, fault.Correlation{Call: id}, input, routes[index])
					if err != nil || receipt == nil {
						t.Error("concurrent route failed", err)
						return
					}
					value, err := receipt.WaitReleased(ctx)
					if err != nil || value.Err() != nil || value.Context.Source != "concurrent-routes" ||
						!value.Outcome.Value.Complete() || string(value.Outcome.Value.DataCopy()) != id {
						t.Error("direct route evidence changed", err)
					}
				})
			}
			group.Wait()
			if arrived.Load() != 2 {
				t.Fatal("test did not reach two simultaneous origin operations")
			}
			seen := make(map[string]bool)
			for range 2 {
				delivery, err := fix.inbox.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				value, err := delivery.Receipt().WaitReleased(ctx)
				id := value.Context.Correlation.Call
				if err != nil || value.Err() != nil || seen[id] || !value.Released ||
					value.Context.Source != "concurrent-routes" || string(value.Outcome.Value.DataCopy()) != id {
					t.Fatal("independent concurrent route evidence changed", err)
				}
				seen[id] = true
				if err := delivery.Release(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func newReviewSOCKS4(t *testing.T, target string, remoteDNS bool) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	var mu sync.Mutex
	var sockets []net.Conn
	identities := make(map[string]bool)
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			sockets = append(sockets, conn)
			mu.Unlock()
			group.Go(func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
				reader := bufio.NewReader(conn)
				var header [8]byte
				if _, err := io.ReadFull(reader, header[:]); err != nil {
					t.Error(err)
					return
				}
				identity, err := reader.ReadString(0)
				if err != nil {
					t.Error(err)
					return
				}
				identity = identity[:len(identity)-1]
				host := net.IP(header[4:]).String()
				if remoteDNS {
					if host != "0.0.0.1" {
						t.Error("SOCKS4a address marker changed")
					}
					host, err = reader.ReadString(0)
					if err != nil {
						t.Error(err)
						return
					}
					host = host[:len(host)-1]
				}
				if header[0] != 4 || header[1] != 1 || net.JoinHostPort(host, fmt.Sprint(binary.BigEndian.Uint16(header[2:4]))) != target {
					t.Error("SOCKS4 target changed")
					return
				}
				mu.Lock()
				if identities[identity] || identity != "first" && identity != "second" {
					t.Error("SOCKS4 USERID lost isolation")
				}
				identities[identity] = true
				mu.Unlock()
				upstream, err := net.DialTimeout("tcp", target, time.Second)
				if err != nil {
					t.Error(err)
					return
				}
				defer upstream.Close()
				mu.Lock()
				sockets = append(sockets, upstream)
				mu.Unlock()
				for _, value := range []byte{0, 0x5a, 0, 0, 0, 0, 0, 0} {
					if _, err := conn.Write([]byte{value}); err != nil {
						return
					}
				}
				done := make(chan struct{})
				go func() { _, _ = io.Copy(upstream, reader); upstream.Close(); close(done) }()
				_, _ = io.Copy(conn, upstream)
				conn.Close()
				upstream.Close()
				<-done
			})
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		<-acceptDone
		mu.Lock()
		owned := append([]net.Conn(nil), sockets...)
		mu.Unlock()
		for _, conn := range owned {
			conn.Close()
		}
		group.Wait()
		if len(identities) != 2 {
			t.Error("SOCKS4 peer did not observe both identities")
		}
	})
	return listener.Addr().String()
}
