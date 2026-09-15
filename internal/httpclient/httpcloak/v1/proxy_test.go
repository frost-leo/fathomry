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

package httpcloak

import (
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	nativehttp "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/transport"
)

func connectProxy(t *testing.T, target, credential string, secure bool) (string, *x509.CertPool) {
	t.Helper()
	var copies sync.WaitGroup
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "CONNECT" || r.Host != target || r.Header.Get("Proxy-Authorization") != credential {
			t.Error("proxy routing or credentials changed")
			w.WriteHeader(403)
			return
		}
		upstream, err := net.Dial("tcp", target)
		if err != nil {
			t.Error(err)
			w.WriteHeader(502)
			return
		}
		client, buffer, err := w.(http.Hijacker).Hijack()
		if err != nil {
			_ = upstream.Close()
			t.Error(err)
			return
		}
		_, _ = buffer.WriteString("HTTP/1.1 200 Connection established\r\n\r\n")
		_ = buffer.Flush()
		closeBoth := func() { _ = client.Close(); _ = upstream.Close() }
		copies.Add(2)
		go func() { defer copies.Done(); defer closeBoth(); _, _ = io.Copy(upstream, buffer) }()
		go func() { defer copies.Done(); defer closeBoth(); _, _ = io.Copy(client, upstream) }()
	})
	var server *httptest.Server
	if secure {
		server = httptest.NewTLSServer(handler)
	} else {
		server = httptest.NewServer(handler)
	}
	t.Cleanup(func() { server.Close(); copies.Wait() })
	roots := x509.NewCertPool()
	if secure {
		roots.AddCert(server.Certificate())
	}
	return server.URL, roots
}
func TestDynamicProxyInputsAndDirectOverride(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1, HTTP2} {
		t.Run(string(mode), func(t *testing.T) {
			address, options := peer(t, mode, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Proxy-Authorization") != "" {
					t.Error("proxy credentials reached origin")
				}
				_, _ = io.WriteString(w, r.Header.Get("X-Request"))
			})
			target, _ := url.Parse(address)
			proxyA, _ := connectProxy(t, target.Host, "route-a", false)
			proxyB, roots := connectProxy(t, target.Host, "route-b", true)
			options.Native.ProxyVerify = &transport.TLSVerify{RootCAs: roots}
			options.MaxActive = 2
			options.MaxBindings = 2
			options.MaxConnections = 2
			fixture := bindFixture(t, options, 4)
			var workers sync.WaitGroup
			for index, route := range []string{proxyA, proxyB} {
				workers.Add(1)
				go func(index int, route string) {
					defer workers.Done()
					value := []string{"route-a", "route-b"}[index]
					input := request(t, "GET", address, nil)
					input.Header.Set("X-Request", value)
					receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: value}, input, RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: route, ConnectHeaders: nativehttp.Header{"Proxy-Authorization": {value}}})
					if err != nil {
						t.Error(err)
						return
					}
					result, err := receipt.WaitReleased(testContext(t))
					if err != nil || string(result.Outcome.Value.DataCopy()) != value || result.Source.Configuration.Identity.Name != "source" {
						t.Error("per-call route isolation failed", err)
					}
				}(index, route)
			}
			workers.Wait()
			for range 2 {
				delivery, err := fixture.inbox.Next(testContext(t))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := delivery.Receipt().WaitReleased(testContext(t)); err != nil {
					t.Fatal(err)
				}
				if err := delivery.Release(); err != nil {
					t.Fatal(err)
				}
			}
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "direct"}, request(t, "GET", address, nil), RequestOptionsV1{Proxy: ProxyDirect})
			if err != nil {
				t.Fatal(err)
			}
			result := settle(t, fixture, receipt)
			if result.Outcome.Value.ProxyMode() != "direct" || !result.Outcome.Value.Complete() {
				t.Fatal("direct override changed identity or result")
			}
		})
	}
}

func TestSOCKS5RuntimeRoute(t *testing.T) {
	address, options := peer(t, HTTP2, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "socks") })
	target, _ := url.Parse(address)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("SOCKS peer did not exit")
		}
	})
	go func() {
		defer close(done)
		client, err := listener.Accept()
		if err != nil {
			return
		}
		defer client.Close()
		_ = client.SetDeadline(time.Now().Add(5 * time.Second))
		var greeting [2]byte
		if _, err := io.ReadFull(client, greeting[:]); err != nil {
			return
		}
		methods := make([]byte, int(greeting[1]))
		if _, err := io.ReadFull(client, methods); err != nil {
			return
		}
		_, _ = client.Write([]byte{5, 0})
		var command [4]byte
		if _, err := io.ReadFull(client, command[:]); err != nil {
			return
		}
		length := 0
		switch command[3] {
		case 1:
			length = 4
		case 4:
			length = 16
		case 3:
			var size [1]byte
			if _, err := io.ReadFull(client, size[:]); err != nil {
				return
			}
			length = int(size[0])
		default:
			return
		}
		skip := make([]byte, length+2)
		if _, err := io.ReadFull(client, skip); err != nil {
			return
		}
		upstream, err := net.Dial("tcp", target.Host)
		if err != nil {
			return
		}
		defer upstream.Close()
		_, _ = client.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0})
		copyDone := make(chan struct{})
		go func() { defer close(copyDone); _, _ = io.Copy(upstream, client); _ = upstream.Close() }()
		_, _ = io.Copy(client, upstream)
		_ = client.Close()
		<-copyDone
	}()
	fixture := bindFixture(t, options, 1)
	receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "socks"}, request(t, "GET", address, nil), RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: "socks5://" + listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	if string(settle(t, fixture, receipt).Outcome.Value.DataCopy()) != "socks" {
		t.Fatal("SOCKS response changed")
	}
}
