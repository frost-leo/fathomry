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
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
)

type routeProxy struct {
	server   *httptest.Server
	requests atomic.Int64
}

// Local HTTP requests terminate at the proxy; CONNECT requests reach the TLS
// origin. The origin independently identifies the tunnel's upstream socket.
func newRouteProxy(t *testing.T, target string, routes *sync.Map, label func(*http.Request) string, intercept func(http.ResponseWriter, *http.Request) bool) *routeProxy {
	t.Helper()
	return newRouteProxyTransport(t, false, target, routes, label, intercept)
}

func newRouteProxyTransport(t *testing.T, secure bool, target string, routes *sync.Map, label func(*http.Request) string, intercept func(http.ResponseWriter, *http.Request) bool) *routeProxy {
	t.Helper()
	peer := &routeProxy{}
	var bridges sync.WaitGroup
	peer.server = httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		peer.requests.Add(1)
		if request.URL.Host != target && request.Host != target {
			t.Error("proxy target changed")
			writer.WriteHeader(400)
			return
		}
		if intercept != nil && intercept(writer, request) {
			return
		}
		route := label(request)
		if request.Method != "CONNECT" {
			if !request.URL.IsAbs() {
				t.Error("request did not use native forward-proxy form")
			}
			_, _ = io.Copy(io.Discard, request.Body)
			_, _ = io.WriteString(writer, route)
			return
		}
		targetConn, err := net.DialTimeout("tcp", target, time.Second)
		if err != nil {
			t.Error(err)
			writer.WriteHeader(502)
			return
		}
		routes.Store(targetConn.LocalAddr().String(), route)
		client, buffer, err := writer.(http.Hijacker).Hijack()
		if err != nil {
			_ = targetConn.Close()
			t.Error(err)
			return
		}
		bridges.Add(1)
		defer bridges.Done()
		defer client.Close()
		defer targetConn.Close()
		_, _ = io.WriteString(buffer, "HTTP/1.1 200 Connection Established\r\n\r\n")
		_ = buffer.Flush()
		done := make(chan struct{})
		go func() { defer close(done); _, _ = io.Copy(client, targetConn); _ = client.Close() }()
		_, _ = io.Copy(targetConn, buffer.Reader)
		_ = targetConn.Close()
		<-done
	}))
	if secure {
		peer.server.StartTLS()
	} else {
		peer.server.Start()
	}
	t.Cleanup(func() {
		peer.server.Close()
		done := make(chan struct{})
		go func() { bridges.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("test-owned proxy bridge did not end")
		}
	})
	return peer
}

func TestConnectNativeProxyReceivesFullAuthority(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var routes sync.Map
		origin, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			route := "direct"
			if value, ok := routes.Load(request.RemoteAddr); ok {
				route = value.(string)
			}
			_, _ = io.WriteString(writer, route)
		})
		target := origin.Listener.Addr().String()
		peer := newRouteProxy(t, target, &routes, func(*http.Request) string { return "proxy" }, nil)
		proxy, err := url.Parse(peer.server.URL)
		if err != nil {
			t.Fatal(err)
		}
		var callbacks atomic.Int64
		options.RoutingLocked = true
		options.Native.Proxy = func(request *http.Request) (*url.URL, error) {
			callbacks.Add(1)
			if request.URL.Host == target && request.Host == target {
				return proxy, nil
			}
			return nil, nil
		}
		f := bindFixture(t, options, 2)
		baseline, err := f.client.Do(deadline(t), deadline(t), correlation("ordinary"), newRequest(t, "GET", origin.URL, nil))
		if err != nil {
			t.Fatal(err)
		}
		if actual := string(settle(t, f, baseline).Outcome.Value.DataCopy()); actual != "proxy" {
			t.Fatal("ordinary request did not reach selected proxy")
		}
		connection, parent, err := f.client.Connect(deadline(t), correlation("connect"), "https", target)
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close(deadline(t))
		child, err := connection.Do(deadline(t), deadline(t), fault.Correlation{Call: "child", Parent: "connect"}, newRequest(t, "GET", origin.URL, nil))
		if err != nil {
			t.Fatal(err)
		}
		if err := connection.Close(deadline(t)); err != nil {
			t.Fatal(err)
		}
		parentResult := settle(t, f, parent)
		childResult := settle(t, f, child)
		if callbacks.Load() != 2 || string(childResult.Outcome.Value.DataCopy()) != "proxy" || parentResult.Outcome.Value.ProxyMode() != "provider" || childResult.Outcome.Value.ProxyMode() != "provider" {
			t.Fatal("connection lost the selected full-authority route or selection evidence")
		}
	})
}

func TestHTTPSProxyReusesNestedTLSConnections(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var routes sync.Map
		origin, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "complete") })
		proxy := newRouteProxyTransport(t, true, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "proxy" }, nil)
		options.HTTP1 = true
		options.ProxyURL = proxy.server.URL
		options.RootCAPEM += string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: proxy.server.Certificate().Raw}))
		f := bindFixture(t, options, 1)
		for _, id := range []string{"first", "second"} {
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation(id), newRequest(t, "GET", origin.URL, nil))
			if err != nil {
				t.Fatal(err)
			}
			data := settle(t, f, receipt).Outcome.Value
			wantProtocol := "HTTP/1.1"
			if h2 {
				wantProtocol = "HTTP/2.0"
			}
			if !data.Complete() || data.Metadata().Protocol() != wantProtocol || string(data.DataCopy()) != "complete" {
				t.Fatal("nested TLS response differed")
			}
		}
		if proxy.requests.Load() != 1 {
			t.Fatal("request completion discarded the shared nested TLS connection")
		}
	})
}

func TestHTTPSProxyH2CompletionDoesNotInterruptAnotherStream(t *testing.T) {
	resume := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	defer unblock()
	var routes sync.Map
	origin, options := newPeer(t, true, func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/held" {
			writer.Header().Set("Content-Length", "2")
			_, _ = io.WriteString(writer, "a")
			writer.(http.Flusher).Flush()
			<-resume
			_, _ = io.WriteString(writer, "b")
			return
		}
		_, _ = io.WriteString(writer, "first")
	})
	proxy := newRouteProxyTransport(t, true, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "proxy" }, nil)
	options.HTTP1 = true
	options.ProxyURL = proxy.server.URL
	options.RootCAPEM += string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: proxy.server.Certificate().Raw}))
	f := bindFixture(t, options, 2)
	first, firstReceipt, err := f.client.Open(deadline(t), correlation("first"), newRequest(t, "GET", origin.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(deadline(t))
	held, heldReceipt, err := f.client.Open(deadline(t), correlation("held"), newRequest(t, "GET", origin.URL+"/held", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close(deadline(t))
	if proxy.requests.Load() != 1 || held.Metadata().Protocol() != "HTTP/2.0" {
		t.Fatal("test did not establish two streams on one nested TLS tunnel")
	}
	if _, err := io.Copy(io.Discard, first); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	settle(t, f, firstReceipt)
	type readResult struct {
		body []byte
		err  error
	}
	read := make(chan readResult, 1)
	go func() { body, err := io.ReadAll(held); read <- readResult{body, err} }()
	select {
	case <-read:
		t.Fatal("first request completion interrupted the unrelated held stream")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case result := <-read:
		if result.err != nil || string(result.body) != "ab" {
			t.Fatal("held response was incomplete", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("held stream did not resume")
	}
	if err := held.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	settle(t, f, heldReceipt)
}

func TestProxySelectionStates(t *testing.T) {
	for _, mode := range []ProxySelection{ProxyFromProvider, ProxyDirect, ProxyAddress} {
		option := RequestOptionsV1{Proxy: mode}
		if mode == ProxyAddress {
			option.ProxyURL = "http://user:secret@proxy.invalid:8080"
		}
		choice, err := requestOptions([]RequestOptionsV1{option}, false)
		if err != nil || choice.selection != mode {
			t.Fatal("valid route rejected", err)
		}
	}
	for _, option := range []RequestOptionsV1{
		{Proxy: ProxyAddress}, {Proxy: ProxyAddress, ProxyURL: " "},
		{Proxy: ProxyAddress, ProxyURL: "ftp://proxy.invalid"},
		{Proxy: ProxyFromProvider, ProxyURL: "http://proxy.invalid"},
		{Proxy: ProxyDirect, ProxyURL: "http://proxy.invalid"}, {Proxy: ProxySelection(99)},
	} {
		if _, err := requestOptions([]RequestOptionsV1{option}, false); !errors.Is(err, ErrInput) {
			t.Fatal("ambiguous/invalid route accepted", err)
		}
	}
	if _, err := requestOptions([]RequestOptionsV1{{}, {}}, false); !errors.Is(err, ErrInput) {
		t.Fatal("multiple runtime choices accepted")
	}
	for _, mode := range []ProxySelection{ProxyDirect, ProxyAddress} {
		if _, err := requestOptions([]RequestOptionsV1{{Proxy: mode, ProxyURL: "http://proxy.invalid"}}, true); !errors.Is(err, ErrInput) {
			t.Fatal("routing lock bypassed")
		}
	}
	secret := "proxy-credential-canary"
	conformance.Runtime(t, RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: "http://user:" + secret + "@proxy.invalid"}, new(RequestOptionsV1), secret)
}

func TestRuntimeProxyIsolationAndStableProviderIdentity(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var routes sync.Map
		origin, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			route := "direct"
			if value, ok := routes.Load(request.RemoteAddr); ok {
				route = value.(string)
			}
			_, _ = io.WriteString(writer, route)
		})
		first := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "a" }, nil)
		second := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "b" }, nil)
		options.ProxyURL = first.server.URL
		options.MaxActive, options.QueuedCalls = 4, 16
		f := bindFixture(t, options, 16)
		type expected struct{ route, mode string }
		wants := make(map[string]expected)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var group sync.WaitGroup
		for index := range 12 {
			id := fmt.Sprintf("route-%02d", index)
			option := RequestOptionsV1{}
			want := expected{route: "a", mode: "provider"}
			switch index % 3 {
			case 1:
				option = RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: second.server.URL}
				want = expected{route: "b", mode: "proxy"}
			case 2:
				option = RequestOptionsV1{Proxy: ProxyDirect}
				want = expected{route: "direct", mode: "direct"}
			}
			wants[id] = want
			group.Go(func() {
				request, _ := http.NewRequest("GET", origin.URL, nil)
				receipt, err := f.client.Do(ctx, ctx, correlation(id), request, option)
				if err != nil {
					t.Error("concurrent proxy call failed", err)
					return
				}
				result, err := receipt.WaitReleased(ctx)
				if err != nil || string(result.Outcome.Value.DataCopy()) != want.route || result.Outcome.Value.ProxyMode() != want.mode {
					t.Error("runtime route crossed into another call", err)
				}
			})
		}
		group.Wait()
		for range 12 {
			delivery, err := f.inbox.Next(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			result, err := delivery.Receipt().WaitReleased(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			want, ok := wants[result.Context.Correlation.Call]
			if !ok || result.Source.Configuration.Identity.Name != options.Name || result.Context.Provider != ProviderID ||
				string(result.Outcome.Value.DataCopy()) != want.route || result.Outcome.Value.ProxyMode() != want.mode {
				t.Fatal("independent route/source evidence differs")
			}
			delete(wants, result.Context.Correlation.Call)
			if err := delivery.Release(); err != nil {
				t.Fatal(err)
			}
		}
		if len(wants) != 0 || f.client.owner.settings.ProxyURL != first.server.URL {
			t.Fatal("runtime route mutated the configured provider")
		}
	})
}

func TestProxyCredentialsSeparateAuthenticatedTunnels(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var routes sync.Map
		origin, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			route, ok := routes.Load(request.RemoteAddr)
			if !ok {
				t.Error("request bypassed the proxy")
				writer.WriteHeader(500)
				return
			}
			_, _ = io.WriteString(writer, route.(string))
		})
		peer := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(request *http.Request) string {
			for _, name := range []string{"one", "two"} {
				if request.Header.Get("Proxy-Authorization") == "Basic "+base64.StdEncoding.EncodeToString([]byte(name+":synthetic")) {
					return name
				}
			}
			t.Error("proxy authentication changed")
			return "invalid"
		}, nil)
		f := bindFixture(t, options, 1)
		for _, name := range []string{"one", "two", "one", "two"} {
			address, _ := url.Parse(peer.server.URL)
			address.User = url.UserPassword(name, "synthetic")
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation(name), newRequest(t, "GET", origin.URL, nil), RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: address.String()})
			if err != nil {
				t.Fatal(err)
			}
			result := settle(t, f, receipt)
			if string(result.Outcome.Value.DataCopy()) != name {
				t.Fatal("authenticated native tunnel was reused across credentials")
			}
		}
		if peer.requests.Load() != 2 {
			t.Fatal("native pool did not retain two independently authenticated tunnels")
		}
	})
}

func TestExplicitProxyFrozenThroughRedirect(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("request unexpectedly became direct") }))
	t.Cleanup(origin.Close)
	var routes sync.Map
	first := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "a" },
		func(writer http.ResponseWriter, request *http.Request) bool {
			if request.URL.Path == "/start" {
				writer.Header().Set("Location", origin.URL+"/end")
				writer.WriteHeader(307)
				return true
			}
			return false
		})
	second := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "b" }, nil)
	option := RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: first.server.URL}
	config := OptionsV1{Name: "redirect-proxy", HTTP1: true}
	config.Native.CheckRedirect = func(*http.Request, []*http.Request) error { option.ProxyURL = second.server.URL; return nil }
	f := bindFixture(t, config, 1)
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("redirect-proxy"), newRequest(t, "POST", origin.URL+"/start", strings.NewReader("payload")), option)
	if err != nil {
		t.Fatal(err)
	}
	result := settle(t, f, receipt)
	if string(result.Outcome.Value.DataCopy()) != "a" || first.requests.Load() != 2 || second.requests.Load() != 0 || result.Outcome.Value.Exchanges() != 2 {
		t.Fatal("redirect reread mutable proxy input")
	}
}

func TestExplicitProxyFrozenThroughNativeRetry(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("request unexpectedly became direct") }))
	t.Cleanup(origin.Close)
	var routes sync.Map
	second := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "b" }, nil)
	var attempts atomic.Int64
	option := RequestOptionsV1{Proxy: ProxyAddress}
	first := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "a" },
		func(writer http.ResponseWriter, request *http.Request) bool {
			if attempts.Add(1) != 2 {
				return false
			}
			option.ProxyURL = second.server.URL
			connection, _, err := writer.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return true
			}
			_ = connection.Close()
			return true
		})
	option.ProxyURL = first.server.URL
	f := bindFixture(t, OptionsV1{Name: "retry-proxy", HTTP1: true}, 1)
	for _, id := range []string{"warm", "retried"} {
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation(id), newRequest(t, "GET", origin.URL, nil), option)
		if err != nil {
			t.Fatal(err)
		}
		result := settle(t, f, receipt)
		if string(result.Outcome.Value.DataCopy()) != "a" || result.Attempts.Exact || result.Outcome.Value.Exchanges() != 1 {
			t.Fatal("native retry changed route or count semantics")
		}
	}
	if first.requests.Load() != 3 || second.requests.Load() != 0 {
		t.Fatal("native retry reread mutable route")
	}
}

func TestRoutingLockAndFailedProxyNeverFallBack(t *testing.T) {
	var received atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { received.Add(1) }))
	t.Cleanup(origin.Close)
	var routes sync.Map
	peer := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "fixed" }, nil)
	options := OptionsV1{Name: "locked", HTTP1: true, ProxyURL: peer.server.URL, RoutingLocked: true}
	f := bindFixture(t, options, 1)
	for _, option := range []RequestOptionsV1{{Proxy: ProxyDirect}, {Proxy: ProxyAddress, ProxyURL: peer.server.URL}} {
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation("refused"), newRequest(t, "GET", origin.URL, nil), option)
		if receipt != nil || !errors.Is(err, ErrInput) || received.Load() != 0 || peer.requests.Load() != 0 {
			t.Fatal("routing constraint bypassed")
		}
	}
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("configured"), newRequest(t, "GET", origin.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	settle(t, f, receipt)
	unavailable, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	badProxy := "http://" + unavailable.Addr().String()
	_ = unavailable.Close()
	another := bindFixture(t, OptionsV1{Name: "no-fallback", HTTP1: true, ProxyURL: peer.server.URL}, 1)
	receipt, err = another.client.Do(deadline(t), deadline(t), correlation("failed"), newRequest(t, "GET", origin.URL, nil), RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: badProxy})
	if err == nil {
		t.Fatal("unavailable proxy unexpectedly succeeded")
	}
	result := settle(t, another, receipt)
	if result.Outcome.Value.Complete() || received.Load() != 0 || peer.requests.Load() != 1 {
		t.Fatal("failed runtime route fell back to configured or direct access")
	}
}

func TestNewRouteDoesNotRetireOldStream(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var routes sync.Map
		release := make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		origin, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			route, ok := routes.Load(request.RemoteAddr)
			if !ok {
				t.Error("proxy bypass")
				return
			}
			label := route.(string)
			if request.URL.Path == "/hold" {
				writer.Header().Set("Content-Length", "2")
				_, _ = io.WriteString(writer, label)
				writer.(http.Flusher).Flush()
				<-release
				_, _ = io.WriteString(writer, label)
				return
			}
			_, _ = io.WriteString(writer, label)
		})
		first := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "a" }, nil)
		second := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "b" }, nil)
		f := bindFixture(t, options, 2)
		stream, held, err := f.client.Open(deadline(t), correlation("held-route"), newRequest(t, "GET", origin.URL+"/hold", nil), RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: first.server.URL})
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close(deadline(t))
		changed, err := f.client.Do(deadline(t), deadline(t), correlation("new-route"), newRequest(t, "GET", origin.URL, nil), RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: second.server.URL})
		if err != nil {
			t.Fatal(err)
		}
		got, err := changed.WaitReleased(deadline(t))
		if err != nil || string(got.Outcome.Value.DataCopy()) != "b" {
			t.Fatal("new route failed", err)
		}
		if result, available := held.Result(); available && result.Released {
			t.Fatal("new route released the old stream")
		}
		unblock()
		body, err := io.ReadAll(stream)
		if err != nil || string(body) != "aa" {
			t.Fatal("old stream was rerouted or retired", err)
		}
		if err := stream.Close(deadline(t)); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			delivery, err := f.inbox.Next(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := delivery.Receipt().WaitReleased(deadline(t)); err != nil {
				t.Fatal(err)
			}
			if err := delivery.Release(); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestDirectConnectionKeepsRuntimeProxyForChildren(t *testing.T) {
	var routes sync.Map
	origin, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) {
		route, ok := routes.Load(request.RemoteAddr)
		if !ok {
			t.Error("connection bypassed proxy")
			return
		}
		_, _ = io.WriteString(writer, route.(string))
	})
	peer := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "bound" }, nil)
	f := bindFixture(t, options, 2)
	connection, parent, err := f.client.Connect(deadline(t), correlation("proxy-connection"), "https", origin.Listener.Addr().String(), RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: peer.server.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(deadline(t))
	child, err := connection.Do(deadline(t), deadline(t), fault.Correlation{Call: "child", Parent: "proxy-connection"}, newRequest(t, "GET", origin.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	result, err := child.WaitReleased(deadline(t))
	if err != nil || string(result.Outcome.Value.DataCopy()) != "bound" || result.Outcome.Value.ProxyMode() != "proxy" {
		t.Fatal("child lost its fixed connection route", err)
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
	if err := f.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
}

func TestRouteTransportReuseHasSourceWideBounds(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var routes sync.Map
		origin, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			value, ok := routes.Load(request.RemoteAddr)
			if !ok {
				t.Error("proxy bypass")
				return
			}
			_, _ = io.WriteString(writer, value.(string))
		})
		peers := make([]*routeProxy, 3)
		for index := range peers {
			label := fmt.Sprintf("route-%d", index)
			peers[index] = newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return label }, nil)
		}
		options.MaxConnections = 1
		f := bindFixture(t, options, 1)
		for index := range 12 {
			selected := index % len(peers)
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation(fmt.Sprintf("bounded-%d", index)), newRequest(t, "GET", origin.URL, nil), RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: peers[selected].server.URL})
			if err != nil {
				t.Fatal(err)
			}
			result := settle(t, f, receipt)
			if string(result.Outcome.Value.DataCopy()) != fmt.Sprintf("route-%d", selected) {
				t.Fatal("retired binding affected the new route")
			}
			f.client.owner.mu.Lock()
			valid := len(f.client.owner.bindings)+f.client.owner.retiring <= 1 && len(f.client.owner.sockets)+f.client.owner.pending <= 1
			for _, binding := range f.client.owner.bindings {
				valid = valid && binding.users == 0
			}
			f.client.owner.mu.Unlock()
			if !valid {
				t.Fatal("route changes multiplied source resources")
			}
		}
	})
}

func TestNativeRoutingHookStillRunsBeforeH2PoolSelection(t *testing.T) {
	var routes sync.Map
	origin, options := newPeer(t, true, func(writer http.ResponseWriter, request *http.Request) {
		value, ok := routes.Load(request.RemoteAddr)
		if !ok {
			t.Error("proxy bypass")
			return
		}
		_, _ = io.WriteString(writer, value.(string))
	})
	first := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "a" }, nil)
	second := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "b" }, nil)
	var callbacks atomic.Int64
	options.Native.Proxy = func(request *http.Request) (*url.URL, error) {
		callbacks.Add(1)
		if request.Header.Get("X-Routing") == "b" {
			return url.Parse(second.server.URL)
		}
		return url.Parse(first.server.URL)
	}
	options.RoutingLocked = true
	f := bindFixture(t, options, 1)
	for index, route := range []string{"a", "b", "a", "b"} {
		request := newRequest(t, "GET", origin.URL, nil)
		request.Header.Set("X-Routing", route)
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation(fmt.Sprintf("hook-%d", index)), request)
		if err != nil {
			t.Fatal(err)
		}
		result := settle(t, f, receipt)
		if string(result.Outcome.Value.DataCopy()) != route || result.Outcome.Value.ProxyMode() != "provider" {
			t.Fatal("native routing hook was bypassed by cached H2 state")
		}
	}
	if callbacks.Load() != 4 {
		t.Fatal("native routing callback invocation changed")
	}
}
