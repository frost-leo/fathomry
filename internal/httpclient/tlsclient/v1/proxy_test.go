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

package tlsclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	nativehttp "github.com/bogdanfinn/fhttp"
	sdk "github.com/bogdanfinn/tls-client"
)

type localProxy struct {
	url  string
	mu   sync.Mutex
	auth map[string]int
}

func connectPeer(t *testing.T, target string) *localProxy {
	t.Helper()
	proxy := &localProxy{auth: make(map[string]int)}
	var workers sync.WaitGroup
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "CONNECT" || r.Host != target {
			http.Error(w, "target refused", 400)
			return
		}
		proxy.mu.Lock()
		proxy.auth[r.Header.Get("Proxy-Authorization")]++
		proxy.mu.Unlock()
		upstream, err := net.DialTimeout("tcp", target, time.Second)
		if err != nil {
			http.Error(w, "local target unavailable", 502)
			return
		}
		client, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			_ = upstream.Close()
			return
		}
		workers.Add(1)
		defer workers.Done()
		defer client.Close()
		defer upstream.Close()
		if _, err = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			return
		}
		if err = buffered.Flush(); err != nil {
			return
		}
		done := make(chan struct{})
		go func() { _, _ = io.Copy(upstream, buffered); _ = upstream.Close(); _ = client.Close(); close(done) }()
		_, _ = io.Copy(client, upstream)
		_ = client.Close()
		_ = upstream.Close()
		<-done
	}))
	proxy.url = server.URL
	t.Cleanup(func() {
		server.Close()
		done := make(chan struct{})
		go func() { workers.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("local CONNECT handlers did not stop")
		}
	})
	return proxy
}
func TestProviderDynamicProxyCredentialsDoNotSharePools(t *testing.T) {
	endpoint, options := providerPeer(t, Negotiated, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Proxy-Authorization") != "" {
			t.Error("CONNECT credentials reached origin")
		}
		_, _ = io.WriteString(w, r.Header.Get("Cookie"))
	})
	target := strings.TrimPrefix(endpoint, "https://")
	proxyA, proxyB := connectPeer(t, target), connectPeer(t, target)
	options.ProxyURL = proxyA.url
	options.QueuedCalls = 16
	fixture := bindProvider(t, options, 16)
	ctx := testContext(t)
	var group sync.WaitGroup
	for index := range 12 {
		group.Go(func() {
			id := fmt.Sprintf("route-%02d", index)
			selected := RequestOptionsV1{ConnectHeaders: nativehttp.Header{"proxy-authorization": {id}}}
			if index%3 == 1 {
				selected.Proxy, selected.ProxyURL = ProxyAddress, proxyB.url
			}
			if index%3 == 2 {
				selected.Proxy = ProxyDirect
			}
			request := providerRequest(t, "GET", endpoint, nil)
			request.Header.Set("Cookie", id)
			receipt, err := fixture.client.Do(ctx, ctx, providerID(id), request, selected)
			if err != nil || receipt == nil {
				t.Error("dynamic proxy failed", err)
				return
			}
			result, err := receipt.WaitReleased(ctx)
			if err != nil || string(result.Outcome.Value.DataCopy()) != id || result.Context.Source != options.Name {
				t.Error("route/request identity mixed", err)
			}
		})
	}
	group.Wait()
	for range 12 {
		delivery, err := fixture.inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := delivery.Receipt().WaitReleased(ctx); err != nil {
			t.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	for index := range 12 {
		id := fmt.Sprintf("route-%02d", index)
		proxyA.mu.Lock()
		a := proxyA.auth[id]
		proxyA.mu.Unlock()
		proxyB.mu.Lock()
		b := proxyB.auth[id]
		proxyB.mu.Unlock()
		wantA, wantB := 0, 0
		if index%3 == 0 {
			wantA = 1
		}
		if index%3 == 1 {
			wantB = 1
		}
		if a != wantA || b != wantB {
			t.Fatalf("credential pool isolation failed for %s: %d/%d", id, a, b)
		}
	}
	if fixture.client.owner.settings.ProxyURL != proxyA.url {
		t.Fatal("runtime choice mutated provider default")
	}
}
func TestProviderNativeConnectHeaderInputIsCopiedAndOverridesAuth(t *testing.T) {
	options := providerOptions()
	options.ProxyURL = "http://name:pass@localhost:8080"
	fixture := bindProvider(t, options, 1)
	extra := nativehttp.Header{"proxy-authorization": {"override"}}
	ctx := context.WithValue(context.Background(), sdk.ContextKeyHeader{}, extra)
	route, err := fixture.client.route(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	extra["proxy-authorization"][0] = "mutated"
	if len(route.headers) != 1 || route.headers["proxy-authorization"][0] != "override" {
		t.Fatal("native context input aliased or duplicate Basic credentials")
	}
	if _, err := fixture.client.route(ctx, []RequestOptionsV1{{ConnectHeaders: nativehttp.Header{}}}); !errors.Is(err, ErrInput) {
		t.Fatal("ambiguous native/per-call headers accepted", err)
	}
}

func TestProviderConnectHeaderDigestKeepsOpaqueBytes(t *testing.T) {
	fixture := bindProvider(t, providerOptions(), 1)
	first, err := fixture.client.route(context.Background(), []RequestOptionsV1{{ConnectHeaders: nativehttp.Header{"Proxy-Authorization": {string([]byte{0xff})}}}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.client.route(context.Background(), []RequestOptionsV1{{ConnectHeaders: nativehttp.Header{"Proxy-Authorization": {string([]byte{0xfe})}}}})
	if err != nil {
		t.Fatal(err)
	}
	if first.digest == second.digest {
		t.Fatal("different opaque CONNECT credentials share a binding digest")
	}
}
func TestProviderProxyHandshakeCancellationAndParserBound(t *testing.T) {
	for _, kind := range []string{"http", "socks4", "socks5"} {
		t.Run(kind, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			entered, closed := make(chan struct{}), make(chan struct{})
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				data := make([]byte, 1)
				_, _ = io.ReadFull(conn, data)
				close(entered)
				_, _ = io.Copy(io.Discard, conn)
				close(closed)
			}()
			options := providerOptions()
			options.Mode = HTTP1Only
			options.ProxyURL = kind + "://" + listener.Addr().String()
			fixture := bindProvider(t, options, 1)
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			request := providerRequest(t, "GET", "https://127.0.0.1:1/", nil)
			returned := make(chan struct{})
			go func() {
				receipt, _ := fixture.client.Do(ctx, testContext(t), providerID("proxy-cancel"), request)
				if receipt != nil {
					_, _ = receipt.WaitReleased(testContext(t))
				}
				close(returned)
			}()
			select {
			case <-entered:
			case <-testContext(t).Done():
				t.Fatal("proxy handshake not entered")
			}
			cancel()
			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Fatal("canceled handshake retained native request")
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("proxy socket survived cancellation")
			}
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
		})
	}
	t.Run("header-bound", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Too-Large", strings.Repeat("x", 4096))
			w.WriteHeader(200)
		}))
		defer server.Close()
		options := providerOptions()
		options.Mode = HTTP1Only
		options.ProxyURL = server.URL
		options.MaxHeaderBytes = 1024
		fixture := bindProvider(t, options, 1)
		receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("oversized-proxy"), providerRequest(t, "GET", "https://127.0.0.1:1/", nil))
		if err == nil {
			t.Fatal("unbounded CONNECT response accepted")
		}
		settleProvider(t, fixture, receipt)
	})
}
