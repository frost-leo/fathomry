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
	"encoding/base64"
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

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func preparedFixture(t *testing.T, options OptionsV1, capacity int, layers ...resource.Layer) *fixture {
	t.Helper()
	prepared, err := PrepareV1(options, layers...)
	if err != nil {
		t.Fatal(err)
	}
	metadata := prepared.Metadata()
	selected := resource.WithLimits(prepared.Select(), metadata.Limits)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "prepared", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](capacity, int64(capacity)*metadata.EvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{client: client, selected: selected, assembly: assembly, inbox: inbox}
	t.Cleanup(func() {
		if err := assembly.Close(deadline(t)); err != nil {
			t.Error("assembly cleanup remains incomplete", err)
		}
	})
	return f
}

type preparedBodyObservation struct {
	bytes int
	err   error
}

type preparedContinuePeer struct {
	endpoint string
	headers  chan struct{}
	body     chan preparedBodyObservation
	done     chan struct{}
	stop     chan struct{}
	mu       sync.Mutex
	conn     net.Conn
}

func preparedNoContinuePeer(t *testing.T, headerDelay time.Duration) *preparedContinuePeer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer := &preparedContinuePeer{
		endpoint: "http://" + listener.Addr().String() + "/",
		headers:  make(chan struct{}), body: make(chan preparedBodyObservation, 1),
		done: make(chan struct{}), stop: make(chan struct{}),
	}
	go func() {
		defer close(peer.done)
		conn, err := listener.Accept()
		if err != nil {
			peer.body <- preparedBodyObservation{err: err}
			return
		}
		peer.mu.Lock()
		peer.conn = conn
		peer.mu.Unlock()
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		request, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			peer.body <- preparedBodyObservation{err: err}
			return
		}
		close(peer.headers)
		body, err := io.ReadAll(request.Body)
		_ = request.Body.Close()
		peer.body <- preparedBodyObservation{bytes: len(body), err: err}
		if err != nil {
			return
		}
		timer := time.NewTimer(headerDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-peer.stop:
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
	}()
	t.Cleanup(func() {
		close(peer.stop)
		_ = listener.Close()
		peer.mu.Lock()
		if peer.conn != nil {
			_ = peer.conn.Close()
		}
		peer.mu.Unlock()
		select {
		case <-peer.done:
		case <-time.After(time.Second):
			t.Error("raw continue peer did not stop")
		}
	})
	return peer
}

func (peer *preparedContinuePeer) observed(t *testing.T) preparedBodyObservation {
	t.Helper()
	select {
	case observed := <-peer.body:
		return observed
	case <-deadline(t).Done():
		t.Fatal("raw peer did not finish observing the request body")
		return preparedBodyObservation{}
	}
}

func TestContinueNativeDifferential(t *testing.T) {
	for _, test := range []struct {
		name string
		wait time.Duration
		want int
	}{
		{name: "independent", wait: 10 * time.Millisecond, want: 17},
		{name: "native-zero", wait: 0, want: 17},
		{name: "coupled-rejecting-control", wait: time.Second, want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			peer := preparedNoContinuePeer(t, 0)
			transport := &http.Transport{ExpectContinueTimeout: test.wait, ResponseHeaderTimeout: time.Second}
			defer transport.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			request := newRequest(t, "POST", peer.endpoint, strings.NewReader("synthetic-payload"))
			request.Header.Set("Expect", "100-continue")
			response, err := transport.RoundTrip(request.WithContext(ctx))
			if response != nil {
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
			}
			observed := peer.observed(t)
			if observed.bytes != test.want || (test.want > 0 && (err != nil || observed.err != nil)) ||
				(test.want == 0 && !errors.Is(err, context.DeadlineExceeded)) {
				t.Fatalf("native continue control: bytes=%d, read=%v, call=%v", observed.bytes, observed.err, err)
			}
		})
	}
}

func TestContinueIndependentWaitAndExplicitZero(t *testing.T) {
	for _, test := range []struct {
		name   string
		wait   time.Duration
		layers []resource.Layer
	}{
		{name: "independent", wait: 10 * time.Millisecond},
		{name: "resolved-zero", wait: time.Second, layers: []resource.Layer{{Kind: resource.Local, Content: []byte("expect_continue_timeout_ns: 0\n")}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			peer := preparedNoContinuePeer(t, 80*time.Millisecond)
			options := OptionsV1{Name: "continue", HTTP1: true, ExpectContinueTimeout: test.wait, ResponseHeaderTimeout: time.Second}
			f := preparedFixture(t, options, 1, test.layers...)
			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()
			request := newRequest(t, "POST", peer.endpoint, strings.NewReader("synthetic-payload"))
			request.Header.Set("Expect", "100-continue")
			receipt, err := f.client.Do(ctx, deadline(t), correlation(test.name), request)
			if err != nil {
				t.Fatal("independent continue wait lost native behavior", err)
			}
			result := settle(t, f, receipt)
			observed := peer.observed(t)
			if observed.bytes != 17 || observed.err != nil || result.Err() != nil ||
				!result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "ok" {
				t.Fatal("continue/body/independent evidence differs", observed, result.Err())
			}
		})
	}
}

func TestContinuePreservesIndependentHeaderDeadline(t *testing.T) {
	peer := preparedNoContinuePeer(t, time.Second)
	options := OptionsV1{Name: "header-deadline", HTTP1: true, ResponseHeaderTimeout: 40 * time.Millisecond}
	f := preparedFixture(t, options, 1, resource.Layer{Kind: resource.Local, Content: []byte("expect_continue_timeout_ns: 0\n")})
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	request := newRequest(t, "POST", peer.endpoint, strings.NewReader("synthetic-payload"))
	request.Header.Set("Expect", "100-continue")
	receipt, err := f.client.Do(ctx, deadline(t), correlation("header-deadline"), request)
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() || ctx.Err() != nil {
		t.Fatal("response-header deadline did not independently stop native work", err, ctx.Err())
	}
	result := settle(t, f, receipt)
	observed := peer.observed(t)
	if observed.bytes != 17 || observed.err != nil || result.Outcome.Value.Complete() {
		t.Fatal("header deadline changed immediate-send or falsely completed", observed)
	}
}

func TestContinueCancellationDoesNotSendBody(t *testing.T) {
	peer := preparedNoContinuePeer(t, 0)
	f := preparedFixture(t, OptionsV1{Name: "continue-cancel", HTTP1: true, ExpectContinueTimeout: 2 * time.Second}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := newRequest(t, "POST", peer.endpoint, strings.NewReader("synthetic-payload"))
	request.Header.Set("Expect", "100-continue")
	type completion struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	done := make(chan completion, 1)
	go func() {
		receipt, err := f.client.Do(ctx, deadline(t), correlation("continue-cancel"), request)
		done <- completion{receipt: receipt, err: err}
	}()
	select {
	case <-peer.headers:
	case <-deadline(t).Done():
		t.Fatal("peer did not receive request headers")
	}
	cancel()
	var completed completion
	select {
	case completed = <-done:
	case <-deadline(t).Done():
		t.Fatal("canceled continue waiter did not return")
	}
	if !errors.Is(completed.err, context.Canceled) {
		t.Fatal("method cancellation was not preserved", completed.err)
	}
	result := settle(t, f, completed.receipt)
	if observed := peer.observed(t); observed.bytes != 0 || result.Outcome.Value.Complete() {
		t.Fatal("cancellation sent the body or falsely completed", observed)
	}
}

type preparedCredentialKey struct{}

func TestConnectStaticMetadataFrozenAndProxyOnly(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var routes sync.Map
		origin, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			if request.Header.Get("Proxy-Authorization") != "" || request.Header.Get("X-Connect-Only") != "" ||
				request.Header.Get("Authorization") != "Bearer origin" {
				t.Error("proxy metadata escaped to the origin, or origin authorization changed")
			}
			route, ok := routes.Load(request.RemoteAddr)
			if !ok {
				t.Error("origin request bypassed the selected proxy")
				return
			}
			_, _ = io.WriteString(writer, route.(string))
		})
		proxy := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(request *http.Request) string {
			if request.Header.Get("Authorization") != "" || request.Header.Get("X-Connect-Only") != "synthetic" {
				t.Error("origin authorization leaked to CONNECT, or static metadata changed")
			}
			return request.Header.Get("Proxy-Authorization")
		}, nil)
		headers := http.Header{"Proxy-Authorization": {"Bearer frozen"}, "X-Connect-Only": {"synthetic"}}
		options.ProxyURL, options.ProxyConnectHeader = proxy.server.URL, headers
		f := preparedFixture(t, options, 1)
		headers.Set("Proxy-Authorization", "Bearer changed")
		request := newRequest(t, "GET", origin.URL, nil)
		request.Header.Set("Authorization", "Bearer origin")
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation("static"), request)
		if err != nil {
			t.Fatal(err)
		}
		result := settle(t, f, receipt)
		if string(result.Outcome.Value.DataCopy()) != "Bearer frozen" || proxy.requests.Load() != 1 {
			t.Fatal("prepared static CONNECT metadata was not frozen")
		}
	})
}

func TestConnectDynamicMetadataIsolatesAuthenticatedPools(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var routes sync.Map
		origin, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			if request.Header.Get("Proxy-Authorization") != "" || request.Header.Get("X-Connect-Only") != "" {
				t.Error("dynamic CONNECT metadata escaped to origin")
			}
			route, ok := routes.Load(request.RemoteAddr)
			if !ok {
				t.Error("origin request bypassed selected proxy")
				return
			}
			_, _ = io.WriteString(writer, route.(string))
		})
		proxy := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(request *http.Request) string {
			if request.Header.Get("X-Static-Ignored") != "" || request.Header.Get("X-Connect-Only") != "synthetic" {
				t.Error("dynamic metadata did not replace static metadata")
			}
			return request.Header.Get("Proxy-Authorization")
		}, nil)
		options.ProxyURL, options.MaxConnections = proxy.server.URL, 2
		options.ProxyConnectHeader = http.Header{"X-Static-Ignored": {"must-not-send"}}
		var selectors, observations atomic.Int64
		options.Native.GetProxyConnectHeader = func(ctx context.Context, address *url.URL, target string) (http.Header, error) {
			selectors.Add(1)
			if address.String() != proxy.server.URL || target != origin.Listener.Addr().String() {
				t.Error("dynamic callback received the wrong proxy or target authority")
			}
			credential, _ := ctx.Value(preparedCredentialKey{}).(string)
			return http.Header{"Proxy-Authorization": {"Bearer " + credential}, "X-Connect-Only": {"synthetic"}}, nil
		}
		options.Native.OnProxyConnectResponse = func(ctx context.Context, response ConnectResponse) error {
			observations.Add(1)
			if ctx.Value(preparedCredentialKey{}) == nil || response.StatusCode() != 200 || response.Protocol() != "HTTP/1.1" ||
				response.Target() != origin.Listener.Addr().String() || response.ProxyURLCopy().String() != proxy.server.URL {
				t.Error("CONNECT observer context or bounded metadata differs")
			}
			copy := response.HeadersCopy()
			copy.Set("X-Mutated", "copy")
			address := response.ProxyURLCopy()
			address.Host = "changed.invalid"
			if response.HeadersCopy().Get("X-Mutated") != "" || response.ProxyURLCopy().String() != proxy.server.URL {
				t.Error("CONNECT metadata accessors alias their immutable snapshot")
			}
			return nil
		}
		f := preparedFixture(t, options, 1)
		for index, credential := range []string{"a", "b", "a", "b"} {
			ctx := context.WithValue(deadline(t), preparedCredentialKey{}, credential)
			receipt, err := f.client.Do(ctx, deadline(t), correlation(fmt.Sprintf("dynamic-%d", index)), newRequest(t, "GET", origin.URL, nil))
			if err != nil {
				t.Fatal(err)
			}
			result := settle(t, f, receipt)
			if string(result.Outcome.Value.DataCopy()) != "Bearer "+credential {
				t.Fatal("a tunnel authenticated with different metadata was reused")
			}
		}
		if selectors.Load() != 4 || observations.Load() != 2 || proxy.requests.Load() != 2 {
			t.Fatal("selector did not precede pool lookup, or equivalent metadata failed to reuse a tunnel", selectors.Load(), observations.Load(), proxy.requests.Load())
		}
	})
}

func TestConnectDynamicResultFrozenBeforeDial(t *testing.T) {
	var routes sync.Map
	origin, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) {
		route, ok := routes.Load(request.RemoteAddr)
		if !ok {
			t.Error("request bypassed proxy")
			return
		}
		_, _ = io.WriteString(writer, route.(string))
	})
	proxy := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(request *http.Request) string {
		return request.Header.Get("Proxy-Authorization")
	}, nil)
	options.ProxyURL = proxy.server.URL
	header := http.Header{"Proxy-Authorization": {"Bearer frozen"}}
	options.Native.GetProxyConnectHeader = func(context.Context, *url.URL, string) (http.Header, error) { return header, nil }
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	options.Native.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		close(entered)
		<-release
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	f := preparedFixture(t, options, 1)
	type completion struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	done := make(chan completion, 1)
	ctx, cleanup := deadline(t), deadline(t)
	go func() {
		receipt, err := f.client.Do(ctx, cleanup, correlation("frozen"), newRequest(t, "GET", origin.URL, nil))
		done <- completion{receipt: receipt, err: err}
	}()
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("controlled dial did not start")
	}
	header.Set("Proxy-Authorization", "Bearer mutated-after-snapshot")
	unblock()
	completed := <-done
	if completed.err != nil {
		t.Fatal(completed.err)
	}
	if result := settle(t, f, completed.receipt); string(result.Outcome.Value.DataCopy()) != "Bearer frozen" {
		t.Fatal("callback-owned map remained borrowed through CONNECT dispatch")
	}
}

func TestConnectMetadataRejectionPrecedesNativeDial(t *testing.T) {
	for _, name := range []string{"callback-error", "dynamic-oversize", "url-auth-oversize"} {
		t.Run(name, func(t *testing.T) {
			var dialed atomic.Int64
			refusal := errors.New("synthetic selector refusal")
			options := OptionsV1{Name: "connect-reject", HTTP1: true, ProxyURL: "http://127.0.0.1:1", MaxHeaderBytes: 1024}
			options.Native.DialContext = func(context.Context, string, string) (net.Conn, error) {
				dialed.Add(1)
				return nil, errors.New("unexpected dial")
			}
			switch name {
			case "callback-error":
				options.Native.GetProxyConnectHeader = func(context.Context, *url.URL, string) (http.Header, error) { return nil, refusal }
			case "dynamic-oversize":
				options.Native.GetProxyConnectHeader = func(context.Context, *url.URL, string) (http.Header, error) {
					return http.Header{"X-Too-Large": {strings.Repeat("x", 2048)}}, nil
				}
			case "url-auth-oversize":
				address, _ := url.Parse(options.ProxyURL)
				address.User = url.UserPassword(strings.Repeat("x", 1200), "synthetic")
				options.ProxyURL = address.String()
			}
			f := preparedFixture(t, options, 1)
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation("reject"), newRequest(t, "GET", "https://synthetic.invalid/", nil))
			if err == nil || dialed.Load() != 0 {
				t.Fatal("CONNECT metadata rejection occurred after native dial", err, dialed.Load())
			}
			result := settle(t, f, receipt)
			if name == "callback-error" && !errors.Is(result.Err(), refusal) ||
				name != "callback-error" && !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.Complete() {
				t.Fatal("CONNECT refusal evidence lost its cause or falsely completed", result.Err())
			}
		})
	}
}

func TestConnectStaticMetadataRejectedOffline(t *testing.T) {
	for _, header := range []http.Header{
		{"X-Too-Large": {strings.Repeat("x", 2048)}},
		{"X-Invalid": {"contains\r\na-header"}},
		{"Host": {"wrong.invalid"}},
		{"Transfer-Encoding": {"chunked"}},
	} {
		var invoked atomic.Int64
		options := OptionsV1{Name: "static-reject", MaxHeaderBytes: 1024, ProxyConnectHeader: header}
		options.Native.GetProxyConnectHeader = func(context.Context, *url.URL, string) (http.Header, error) {
			invoked.Add(1)
			return nil, nil
		}
		if _, err := PrepareV1(options); err == nil || invoked.Load() != 0 {
			t.Fatal("invalid static CONNECT metadata passed offline preparation, or invoked dependency", err)
		}
	}
}

func TestConnectResponseFailureAndBounds(t *testing.T) {
	for _, mode := range []string{"407-observed", "wire-bound", "snapshot-bound", "observer-refusal"} {
		t.Run(mode, func(t *testing.T) {
			var origins, observations atomic.Int64
			origin, options := newPeer(t, false, func(http.ResponseWriter, *http.Request) { origins.Add(1) })
			var routes sync.Map
			proxy := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(*http.Request) string { return "unused" }, func(writer http.ResponseWriter, _ *http.Request) bool {
				writer.Header().Set("X-Proxy-Observation", "synthetic")
				if mode == "wire-bound" {
					writer.Header().Set("X-Oversize", strings.Repeat("x", 2048))
				}
				if mode == "snapshot-bound" {
					for index := range 18 {
						writer.Header().Set(fmt.Sprintf("X-%02d", index), "x")
					}
				}
				writer.WriteHeader(http.StatusProxyAuthRequired)
				return true
			})
			refusal := errors.New("synthetic observer refusal")
			options.ProxyURL, options.MaxHeaderBytes = proxy.server.URL, 1024
			options.Native.OnProxyConnectResponse = func(_ context.Context, response ConnectResponse) error {
				observations.Add(1)
				if response.StatusCode() != 407 || response.Target() != origin.Listener.Addr().String() ||
					response.HeadersCopy().Get("X-Proxy-Observation") != "synthetic" {
					t.Error("failed CONNECT metadata was lost or attributed to origin")
				}
				if mode == "observer-refusal" {
					return refusal
				}
				return nil
			}
			f := preparedFixture(t, options, 1)
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation(mode), newRequest(t, "GET", origin.URL, nil))
			if err == nil || origins.Load() != 0 || proxy.requests.Load() != 1 {
				t.Fatal("failed CONNECT fell back to direct origin or lost failure", err)
			}
			result := settle(t, f, receipt)
			want := int64(1)
			if mode == "wire-bound" || mode == "snapshot-bound" {
				want = 0
			}
			if observations.Load() != want || result.Outcome.Value.Complete() ||
				mode == "observer-refusal" && !errors.Is(result.Err(), refusal) ||
				mode == "snapshot-bound" && !errors.Is(result.Err(), ErrLimit) {
				t.Fatal("CONNECT observer bounds/refusal evidence differs", observations.Load(), result.Err())
			}
		})
	}
}

func TestConnectResponseCallbackRetainsCanceledWork(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusProxyAuthRequired)
	}))
	t.Cleanup(proxy.Close)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	options := OptionsV1{Name: "observer-lifetime", HTTP1: true, ProxyURL: proxy.URL, MaxActive: 1}
	options.Native.OnProxyConnectResponse = func(ctx context.Context, _ ConnectResponse) error {
		close(entered)
		<-release
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Error("observer received detached native dial context instead of method context")
		}
		return ctx.Err()
	}
	f := preparedFixture(t, options, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type completion struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	done := make(chan completion, 1)
	cleanup := deadline(t)
	go func() {
		receipt, err := f.client.Do(ctx, cleanup, correlation("observer-lifetime"), newRequest(t, "GET", "https://synthetic.invalid/", nil))
		done <- completion{receipt: receipt, err: err}
	}()
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("CONNECT observer did not start")
	}
	cancel()
	var completed completion
	select {
	case completed = <-done:
	case <-deadline(t).Done():
		t.Fatal("canceled observer waiter did not return")
	}
	if completed.receipt == nil || !errors.Is(completed.err, context.Canceled) {
		t.Fatal("accepted canceled CONNECT lost receipt/cancellation", completed.err)
	}
	if result, _ := completed.receipt.Result(); result.Final || result.Released {
		t.Fatal("blocked observer released native lifetime early")
	}
	if next, err := f.client.Do(deadline(t), deadline(t), correlation("saturated"), newRequest(t, "GET", "https://synthetic.invalid/", nil)); next != nil || !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("blocked callback returned root capacity", err)
	}
	short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := f.assembly.Close(short); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("source shutdown discarded borrowed observer lifetime", err)
	}
	unblock()
	result := settle(t, f, completed.receipt)
	if result.Outcome.Value.Complete() || !errors.Is(result.Err(), context.Canceled) {
		t.Fatal("late CONNECT completion lost canceled-work evidence", result.Err())
	}
}

func TestDirectConnectFreezesMetadataForChildren(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var routes sync.Map
		origin, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			if request.Header.Get("Proxy-Authorization") != "" {
				t.Error("direct connection child received proxy-only authorization")
			}
			route, ok := routes.Load(request.RemoteAddr)
			if !ok {
				t.Error("direct connection bypassed selected proxy")
				return
			}
			_, _ = io.WriteString(writer, route.(string))
		})
		proxy := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(request *http.Request) string {
			return request.Header.Get("Proxy-Authorization")
		}, nil)
		options.ProxyURL = proxy.server.URL
		var selectors atomic.Int64
		options.Native.GetProxyConnectHeader = func(ctx context.Context, _ *url.URL, target string) (http.Header, error) {
			selectors.Add(1)
			if target != origin.Listener.Addr().String() {
				t.Error("direct selector lost full target authority")
			}
			credential, _ := ctx.Value(preparedCredentialKey{}).(string)
			return http.Header{"Proxy-Authorization": {"Bearer " + credential}}, nil
		}
		f := preparedFixture(t, options, 3)
		ctx := context.WithValue(deadline(t), preparedCredentialKey{}, "parent")
		connection, parent, err := f.client.Connect(ctx, correlation("parent"), "https", origin.Listener.Addr().String())
		if err != nil || connection == nil {
			t.Fatal("controlled direct CONNECT failed", err)
		}
		defer connection.Close(deadline(t))
		var children []*invocation.Receipt[Result]
		for _, child := range []string{"first", "second"} {
			childCtx := context.WithValue(deadline(t), preparedCredentialKey{}, "different-child")
			receipt, err := connection.Do(childCtx, deadline(t), fault.Correlation{Call: child, Parent: "parent"}, newRequest(t, "GET", origin.URL, nil))
			if err != nil {
				t.Fatal(err)
			}
			result, waitErr := receipt.WaitReleased(deadline(t))
			if waitErr != nil || result.Err() != nil || !result.Nested || string(result.Outcome.Value.DataCopy()) != "Bearer parent" {
				t.Fatal("child metadata replaced its established tunnel")
			}
			children = append(children, receipt)
		}
		if err := connection.Close(deadline(t)); err != nil {
			t.Fatal(err)
		}
		result := settle(t, f, parent)
		if !result.Outcome.Value.Connected() || !result.Outcome.Value.Complete() || selectors.Load() != 1 || proxy.requests.Load() != 1 {
			t.Fatal("direct connection lifetime/selector evidence differs")
		}
		for _, receipt := range children {
			if result := settle(t, f, receipt); !result.Nested || string(result.Outcome.Value.DataCopy()) != "Bearer parent" {
				t.Fatal("independent child evidence differs from direct return")
			}
		}
	})
}

func TestConnectMetadataOnlyAppliesToActualHTTPTunnels(t *testing.T) {
	for _, mode := range []string{"direct-https", "http-forward-proxy"} {
		t.Run(mode, func(t *testing.T) {
			var origins, proxies, callbacks atomic.Int64
			origin, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) {
				origins.Add(1)
				if request.Header.Get("X-Connect-Only") != "" || request.Header.Get("Proxy-Authorization") != "" {
					t.Error("CONNECT metadata leaked into direct origin request")
				}
				_, _ = io.WriteString(writer, "origin")
			})
			endpoint, want := origin.URL, "origin"
			if mode == "http-forward-proxy" {
				proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					proxies.Add(1)
					if request.Method == "CONNECT" || !request.URL.IsAbs() || request.Header.Get("X-Connect-Only") != "" {
						t.Error("ordinary HTTP forwarding used CONNECT-only metadata")
					}
					_, _ = io.WriteString(writer, "forward")
				}))
				t.Cleanup(proxy.Close)
				options.ProxyURL = proxy.URL
				endpoint, want = "http://synthetic.invalid/", "forward"
			}
			options.ProxyConnectHeader = http.Header{"X-Connect-Only": {"synthetic"}, "Proxy-Authorization": {"Bearer tunnel"}}
			options.Native.GetProxyConnectHeader = func(context.Context, *url.URL, string) (http.Header, error) {
				callbacks.Add(1)
				return nil, errors.New("CONNECT selector should not apply")
			}
			options.Native.OnProxyConnectResponse = func(context.Context, ConnectResponse) error {
				callbacks.Add(1)
				return errors.New("CONNECT observer should not apply")
			}
			f := preparedFixture(t, options, 1)
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation(mode), newRequest(t, "GET", endpoint, nil))
			if err != nil {
				t.Fatal(err)
			}
			result := settle(t, f, receipt)
			if callbacks.Load() != 0 || string(result.Outcome.Value.DataCopy()) != want ||
				mode == "direct-https" && (origins.Load() != 1 || proxies.Load() != 0) ||
				mode == "http-forward-proxy" && (origins.Load() != 0 || proxies.Load() != 1) {
				t.Fatal("CONNECT applicability or independent route observation differs")
			}
		})
	}
}

func TestProxyURLAuthenticationPreservesNativePrecedence(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var routes sync.Map
		origin, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			route, ok := routes.Load(request.RemoteAddr)
			if !ok {
				t.Error("authenticated request bypassed proxy")
				return
			}
			_, _ = io.WriteString(writer, route.(string))
		})
		proxy := newRouteProxy(t, origin.Listener.Addr().String(), &routes, func(request *http.Request) string {
			return request.Header.Get("Proxy-Authorization")
		}, nil)
		address, _ := url.Parse(proxy.server.URL)
		address.User = url.UserPassword("synthetic-user", "synthetic-pass")
		options.ProxyURL = address.String()
		var selectors atomic.Int64
		options.Native.GetProxyConnectHeader = func(context.Context, *url.URL, string) (http.Header, error) {
			value := selectors.Add(1)
			return http.Header{"Proxy-Authorization": {fmt.Sprintf("Bearer ignored-%d", value)}}, nil
		}
		f := preparedFixture(t, options, 1)
		want := "Basic " + base64.StdEncoding.EncodeToString([]byte("synthetic-user:synthetic-pass"))
		for index := range 2 {
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation(fmt.Sprintf("native-auth-%d", index)), newRequest(t, "GET", origin.URL, nil))
			if err != nil {
				t.Fatal(err)
			}
			if result := settle(t, f, receipt); string(result.Outcome.Value.DataCopy()) != want {
				t.Fatal("CONNECT headers overrode native proxy-URL authentication precedence")
			}
		}
		if proxy.requests.Load() != 1 || selectors.Load() != 2 {
			t.Fatal("pool key differs despite identical effective native authentication")
		}
	})
}

func TestCanceledConnectSelectorRetainsPooledAndDirectWork(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("direct-%t", direct), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			var dialed atomic.Int64
			options := OptionsV1{Name: "selector-lifetime", HTTP1: true, ProxyURL: "http://127.0.0.1:1", MaxActive: 1}
			options.Native.DialContext = func(context.Context, string, string) (net.Conn, error) {
				dialed.Add(1)
				return nil, errors.New("unexpected native dial")
			}
			options.Native.GetProxyConnectHeader = func(ctx context.Context, _ *url.URL, _ string) (http.Header, error) {
				close(entered)
				<-release
				if !errors.Is(ctx.Err(), context.Canceled) {
					t.Error("selector did not retain method-context cancellation")
				}
				return http.Header{"X-Late": {"synthetic"}}, nil
			}
			f := preparedFixture(t, options, 1)
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
					connection, receipt, err = f.client.Connect(ctx, correlation("selector-lifetime"), "https", "synthetic.invalid:443")
					if connection != nil {
						_ = connection.Close(cleanup)
						t.Error("canceled direct selector unexpectedly returned a connection")
					}
				} else {
					receipt, err = f.client.Do(ctx, cleanup, correlation("selector-lifetime"), newRequest(t, "GET", "https://synthetic.invalid/", nil))
				}
				done <- completion{receipt: receipt, err: err}
			}()
			select {
			case <-entered:
			case <-deadline(t).Done():
				t.Fatal("CONNECT selector did not enter")
			}
			cancel()
			var completed completion
			select {
			case completed = <-done:
			case <-deadline(t).Done():
				t.Fatal("canceled selector waiter did not return")
			}
			if completed.receipt == nil || !errors.Is(completed.err, context.Canceled) || dialed.Load() != 0 {
				t.Fatal("canceled predispatch selector lost evidence or dialed", completed.err)
			}
			if result, _ := completed.receipt.Result(); result.Final || result.Released {
				t.Fatal("blocked selector lifetime was released early")
			}
			unblock()
			result := settle(t, f, completed.receipt)
			if result.Outcome.Value.Complete() || !errors.Is(result.Err(), context.Canceled) || dialed.Load() != 0 {
				t.Fatal("late selector result reached native transport or lost cancellation", result.Err())
			}
		})
	}
}
