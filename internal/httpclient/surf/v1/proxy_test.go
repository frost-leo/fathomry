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
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/enetx/http"
	"github.com/frost-leo/fathomry/internal/fault"
)

type flushingWriter struct{ writer stdhttp.ResponseWriter }

func (writer flushingWriter) Write(data []byte) (int, error) {
	count, err := writer.writer.Write(data)
	writer.writer.(stdhttp.Flusher).Flush()
	return count, err
}

type proxyPeer struct {
	server   *httptest.Server
	connects atomic.Int32
	protocol atomic.Int32
	auth     string
}

func newProxy(t *testing.T, tlsProxy bool, auth string, extraHeaderBytes ...int) *proxyPeer {
	t.Helper()
	peer := &proxyPeer{auth: auth}
	var mu sync.Mutex
	var owned []net.Conn
	handler := func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if len(extraHeaderBytes) > 0 {
			w.Header().Set("X-Large", strings.Repeat("x", extraHeaderBytes[0]))
		}
		peer.connects.Add(1)
		peer.protocol.Store(int32(r.ProtoMajor))
		if r.Method != "CONNECT" || r.Header.Get("Proxy-Authorization") != auth || r.Header.Get("X-Route") != "frozen" {
			t.Error("proxy route/header changed")
			w.WriteHeader(403)
			return
		}
		remote, err := net.DialTimeout("tcp", r.Host, time.Second)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		mu.Lock()
		owned = append(owned, remote)
		mu.Unlock()
		defer remote.Close()
		if r.ProtoMajor == 2 {
			w.WriteHeader(200)
			w.(stdhttp.Flusher).Flush()
			up := make(chan struct{})
			go func() { _, _ = io.Copy(remote, r.Body); close(up) }()
			_, _ = io.Copy(flushingWriter{w}, remote)
			_ = remote.Close()
			_ = r.Body.Close()
			<-up
			return
		}
		conn, buffer, err := w.(stdhttp.Hijacker).Hijack()
		if err != nil {
			return
		}
		mu.Lock()
		owned = append(owned, conn)
		mu.Unlock()
		defer conn.Close()
		_, _ = buffer.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		_ = buffer.Flush()
		up := make(chan struct{})
		go func() { _, _ = io.Copy(remote, buffer); _ = remote.Close(); close(up) }()
		_, _ = io.Copy(conn, remote)
		_ = conn.Close()
		_ = remote.Close()
		<-up
	}
	peer.server = httptest.NewUnstartedServer(stdhttp.HandlerFunc(handler))
	peer.server.EnableHTTP2 = tlsProxy
	if tlsProxy {
		peer.server.StartTLS()
	} else {
		peer.server.Start()
	}
	t.Cleanup(func() {
		mu.Lock()
		connections := append([]net.Conn(nil), owned...)
		mu.Unlock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		peer.server.Close()
	})
	return peer
}
func TestRuntimeProxyIsolationAndNativeH2TunnelLifetime(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "http-connect", true: "https-h2-connect"}[secure], func(t *testing.T) {
			var seenMu sync.Mutex
			seen := map[string]string{}
			target := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				data, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				if r.Header.Get("Proxy-Authorization") != "" || r.Header.Get("X-Route") != "" {
					t.Error("proxy credentials leaked to origin")
				}
				if r.Header.Get("Cookie") != "cookie="+r.Header.Get("X-Call") {
					t.Error("runtime cookies crossed calls")
				}
				seenMu.Lock()
				seen[r.Header.Get("X-Call")] = string(data)
				seenMu.Unlock()
				_, _ = w.Write(data)
			}, false)
			first := newProxy(t, secure, "Bearer first")
			second := newProxy(t, secure, "Bearer second")
			roots := x509.NewCertPool()
			if secure {
				roots.AddCert(first.server.Certificate())
				roots.AddCert(second.server.Certificate())
			}
			options := OptionsV1{Name: "routed", Mode: HTTP1Only, MaxActive: 4, QueuedCalls: 16,
				Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: target.roots}, ProxyTLSConfig: &tls.Config{RootCAs: roots, NextProtos: []string{"h2", "http/1.1"}}}}
			fix := newFixture(t, options, 4)
			for _, id := range []string{"first-a", "second-a", "first-b", "second-b"} {
				proxy := first
				if id[:6] == "second" {
					proxy = second
				}
				route := proxy.server.URL
				input := request(t, "POST", target.tcp.URL, id)
				input.Header.Set("X-Call", id)
				input.Header.Set("Cookie", "cookie="+id)
				receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: id}, input,
					RequestOptionsV1{ProxyURL: &route, ConnectHeaders: http.Header{"Proxy-Authorization": {proxy.auth}, "X-Route": {"frozen"}}})
				if err != nil {
					t.Fatal(err)
				}
				if value := settle(t, fix, receipt); !value.Outcome.Value.Complete() || string(value.Outcome.Value.DataCopy()) != id || value.Context.Source != "routed" {
					t.Fatal("routed result changed")
				}
			}
			if first.connects.Load() != 1 || second.connects.Load() != 1 {
				t.Fatal("route pooling or CONNECT lifetime changed", first.connects.Load(), second.connects.Load())
			}
			if secure && (first.protocol.Load() != 2 || second.protocol.Load() != 2) {
				t.Fatal("test did not use native H2 CONNECT")
			}
			seenMu.Lock()
			defer seenMu.Unlock()
			if len(seen) != 4 {
				t.Fatal("server observed missing or repeated calls")
			}
			for id, body := range seen {
				if id != body {
					t.Fatal("body routing crossed requests")
				}
			}
		})
	}
}
