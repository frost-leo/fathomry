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

package nuki

import (
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	nativehttp "github.com/nukilabs/http"
)

type connectPeer struct {
	peer        *httptest.Server
	calls       atomic.Int32
	mu          sync.Mutex
	connections []net.Conn
	workers     sync.WaitGroup
	wantAuth    string
	lastHeader  string
}

func newConnectPeer(t *testing.T, auth string) *connectPeer {
	t.Helper()
	proxy := &connectPeer{wantAuth: auth}
	proxy.peer = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != "CONNECT" {
			t.Error("native proxy did not use CONNECT")
			writer.WriteHeader(400)
			return
		}
		if request.Header.Get("Proxy-Authorization") != proxy.wantAuth {
			t.Error("proxy credential encoding changed")
			writer.WriteHeader(407)
			return
		}
		target, err := net.Dial("tcp", request.Host)
		if err != nil {
			t.Error(err)
			writer.WriteHeader(502)
			return
		}
		conn, _, err := writer.(http.Hijacker).Hijack()
		if err != nil {
			target.Close()
			t.Error(err)
			return
		}
		proxy.mu.Lock()
		proxy.connections = append(proxy.connections, conn, target)
		proxy.lastHeader = request.Header.Get("X-Route")
		proxy.mu.Unlock()
		proxy.calls.Add(1)
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
		proxy.workers.Add(2)
		go func() { defer proxy.workers.Done(); _, _ = io.Copy(target, conn); target.Close(); conn.Close() }()
		go func() { defer proxy.workers.Done(); _, _ = io.Copy(conn, target); target.Close(); conn.Close() }()
	}))
	t.Cleanup(func() {
		proxy.peer.Close()
		proxy.mu.Lock()
		connections := append([]net.Conn(nil), proxy.connections...)
		proxy.mu.Unlock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		proxy.workers.Wait()
	})
	return proxy
}

func TestProviderRuntimeProxyIdentityHeadersAndCredentials(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Proxy-Authorization") != "" || request.Header.Get("X-Route") != "" {
			t.Error("CONNECT headers escaped to origin")
		}
		_, _ = io.WriteString(writer, "origin")
	}))
	defer origin.Close()
	auth := "Basic " + base64.StdEncoding.EncodeToString([]byte("u@ser:p%word"))
	first, second := newConnectPeer(t, auth), newConnectPeer(t, "")
	options := providerOptions()
	options.ProxyURL = "http://u%40ser:p%25word@" + first.peer.Listener.Addr().String()
	options.MaxConnections = 3
	fixture := bindProvider(t, options)
	for _, choice := range []RequestOptionsV1{
		{},
		{ProxyMode: ProxyAddress, ProxyURL: second.peer.URL, ConnectHeaders: nativehttp.Header{"X-Route": {"second"}}},
		{ProxyMode: ProxyDirect},
	} {
		receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "route"}, nativeRequest(t, "GET", origin.URL, nil), choice)
		if err != nil {
			t.Fatal(err)
		}
		result := outcome(t, fixture, receipt)
		if result.Err() != nil || string(result.Outcome.Value.DataCopy()) != "origin" {
			t.Fatal("routing failed", result.Err())
		}
		mode := choice.ProxyMode
		if mode == "" {
			mode = ProxyFromProvider
		}
		if result.Outcome.Value.ProxyMode() != mode {
			t.Fatal("runtime route changed source semantics")
		}
	}
	if first.calls.Load() != 1 || second.calls.Load() != 1 {
		t.Fatal("route setter or client isolation failed")
	}
	second.mu.Lock()
	header := second.lastHeader
	second.mu.Unlock()
	if header != "second" {
		t.Fatal("CONNECT extension disappeared")
	}
	fixture.client.owner.mu.Lock()
	sockets := len(fixture.client.owner.sockets)
	fixture.client.owner.mu.Unlock()
	if sockets > options.MaxConnections {
		t.Fatal("routes multiplied physical allowance")
	}
}

func TestProviderLockedRoutingAndSharedOriginCeiling(t *testing.T) {
	for _, locked := range []bool{false, true} {
		options := providerOptions()
		options.RoutingLocked = locked
		options.MaxOrigins = 1
		fixture := bindProvider(t, options)
		first := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { writer.WriteHeader(204) }))
		second := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { writer.WriteHeader(204) }))
		defer first.Close()
		defer second.Close()
		if locked {
			receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "locked"}, nativeRequest(t, "GET", first.URL, nil), RequestOptionsV1{ProxyMode: ProxyDirect})
			if receipt != nil || !errors.Is(err, ErrUnsupported) {
				t.Fatal("locked route bypassed", err)
			}
			continue
		}
		receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "first"}, nativeRequest(t, "GET", first.URL, nil))
		if err != nil {
			t.Fatal(err)
		}
		if result := outcome(t, fixture, receipt); result.Err() != nil {
			t.Fatal(result.Err())
		}
		receipt, err = fixture.client.Do(testContext(t), fault.Correlation{Call: "second"}, nativeRequest(t, "GET", second.URL, nil))
		if err != nil {
			t.Fatal(err)
		}
		if result := outcome(t, fixture, receipt); !errors.Is(result.Err(), ErrCapacity) || result.Attempts.Observed != 0 {
			t.Fatal("source-wide origin ceiling bypassed", result.Err())
		}
	}
}

func TestProviderHeaderDigestHasContainerBoundaries(t *testing.T) {
	first := nativehttp.Header{"A": {"B", "C"}}
	second := nativehttp.Header{"A": {"B"}, "C": {}}
	if headerDigest(first) == headerDigest(second) {
		t.Fatal("different CONNECT configurations share a route key")
	}
}
