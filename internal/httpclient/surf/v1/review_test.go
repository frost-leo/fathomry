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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/enetx/http"
	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/profiles/chrome"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

func TestReviewCanceledReadGateKeepsFinalCause(t *testing.T) {
	peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = io.WriteString(w, "body") }))
	defer peer.Close()
	fix := newFixture(t, OptionsV1{Name: "gate", Mode: HTTP1Only}, 1)
	ctx, cancel := context.WithCancelCause(testContext(t))
	stream, receipt, err := fix.client.Open(ctx, fault.Correlation{Call: "gate"}, request(t, "GET", peer.URL, ""))
	if err != nil {
		t.Fatal(err)
	}
	stream.gate <- struct{}{}
	cause := errors.New("gate cancellation")
	cancel(cause)
	_, readErr := stream.Read(make([]byte, 1))
	<-stream.gate
	if !errors.Is(readErr, context.Canceled) {
		t.Fatal("read gate did not exercise cancellation")
	}
	_ = stream.Close(testContext(t))
	value := settle(t, fix, receipt)
	if !errors.Is(value.Outcome.Primary, cause) || !errors.Is(value.Outcome.Primary, context.Canceled) {
		t.Fatal("cancelled read cause absent from required evidence")
	}
}

func TestReviewNativeViewHasNoRawDialer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var escapes atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			escapes.Add(1)
			_ = conn.Close()
		}
	}()
	defer func() { listener.Close(); <-done }()
	peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = io.WriteString(w, "ok") }))
	defer peer.Close()
	options := OptionsV1{Name: "raw-view", Mode: HTTP1Only, MaxTCPConnections: 1, Native: NativeOptionsV1{
		ResponseMiddleware: []func(*sdk.Response) error{func(view *sdk.Response) error {
			if dialer := view.GetDialer(); dialer != nil {
				conn, err := dialer.DialContext(context.Background(), "tcp", listener.Addr().String())
				if err == nil {
					conn.Close()
				}
				t.Error("metadata view exported an ungoverned dialer")
			}
			if view.GetClient() != nil || view.GetTransport() != nil || view.GetTLSConfig() != nil {
				t.Error("metadata view exported native resource/configuration handles")
			}
			return nil
		}},
	}}
	fix := newFixture(t, options, 1)
	receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "view"}, request(t, "GET", peer.URL, ""))
	if err != nil {
		t.Fatal(err)
	}
	settle(t, fix, receipt)
	if escapes.Load() != 0 {
		t.Fatal("view opened socket beyond original physical quota")
	}
}

func TestReviewJAVerificationClockIsNotIgnored(t *testing.T) {
	var clocks atomic.Int32
	peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		t.Error("expired fixture passed custom verification clock")
	}, false)
	profile := chrome.Desktop
	options := OptionsV1{Name: "clock", Mode: HTTP1Only, Native: NativeOptionsV1{Profile: &profile, TLSConfig: &tls.Config{
		RootCAs: peer.roots, Time: func() time.Time { clocks.Add(1); return time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC) },
	}}}
	fix := newFixture(t, options, 1)
	receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "clock"}, request(t, "GET", peer.tcp.URL, ""))
	value := settle(t, fix, receipt)
	if err == nil || value.Err() == nil || clocks.Load() == 0 {
		t.Fatal("JA path ignored native verification clock")
	}
}

type reviewRoundTrip func(*http.Request) (*http.Response, error)

func (fn reviewRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }

type reviewCloseReader struct {
	io.Reader
	cause error
	calls atomic.Int32
}

func (reader *reviewCloseReader) Close() error { reader.calls.Add(1); return reader.cause }
func TestReviewResponseFailureSeparatesCleanupEvidence(t *testing.T) {
	primary, cleanup := errors.New("response middleware"), errors.New("response close")
	options := OptionsV1{Name: "cleanup", Mode: HTTP1Only, Native: NativeOptionsV1{ResponseMiddleware: []func(*sdk.Response) error{func(*sdk.Response) error { return primary }}}}
	fix := newFixture(t, options, 1)
	binding, err := fix.client.owner.binding(testContext(t), routeChoice{})
	if err != nil {
		t.Fatal(err)
	}
	body := &reviewCloseReader{Reader: strings.NewReader("body"), cause: cleanup}
	binding.client.GetClient().Transport = routeTransport{raw: reviewRoundTrip(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, ContentLength: 4, Request: r, Proto: "HTTP/1.1"}, nil
	})}
	receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "cleanup"}, request(t, "GET", "http://fixture.invalid", ""))
	if err == nil {
		t.Fatal("injected native failure disappeared")
	}
	value := settle(t, fix, receipt)
	if !errors.Is(value.Outcome.Primary, primary) || !errors.Is(value.Outcome.Cleanup, cleanup) || body.calls.Load() != 1 {
		t.Fatal("primary and cleanup occurrence fields were conflated")
	}
}
func TestReviewEncodedOverflowJoinedWithEOFIsNotComplete(t *testing.T) {
	payload := encoded(t, "deflate", nil)
	peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Encoding", "deflate")
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		_, _ = w.Write(payload)
	}, false)
	fix := newFixture(t, OptionsV1{Name: "encoded-overflow", Mode: HTTP1Only, MaxResponseBytes: int64(len(payload) - 1),
		Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots}}}, 1)
	receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "encoded"}, request(t, "GET", peer.tcp.URL, ""))
	value := settle(t, fix, receipt)
	if err == nil || value.Err() == nil || value.Outcome.Value.Complete() {
		t.Fatal("encoded limit joined with EOF was certified complete")
	}
}

func TestReviewFailedConstructionDoesNotPoisonFutureRoute(t *testing.T) {
	entered := make(chan struct{})
	var calls atomic.Int32
	peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = io.WriteString(w, "ok") }, true)
	options := OptionsV1{Name: "retry-construction", Mode: PreferHTTP3, MaxRoutes: 1, Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots},
		ListenPacket: func(ctx context.Context, network, address string) (net.PacketConn, error) {
			if calls.Add(1) == 1 {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return (&net.ListenConfig{}).ListenPacket(ctx, network, address)
		}}}
	fix := newFixture(t, options, 2)
	ctx, cancel := context.WithCancel(testContext(t))
	first := make(chan *invocation.Receipt[Result], 1)
	go func() {
		receipt, _ := fix.client.Do(ctx, testContext(t), fault.Correlation{Call: "first"}, request(t, "GET", peer.tcp.URL, ""))
		first <- receipt
	}()
	<-entered
	cancel()
	settle(t, fix, <-first)
	receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "second"}, request(t, "GET", peer.tcp.URL, ""))
	value := settle(t, fix, receipt)
	if err != nil || !value.Outcome.Value.Complete() || calls.Load() < 2 {
		t.Fatal("failed first-call construction poisoned future same-route request", err)
	}
}
func TestReviewRetryCodesDistinguishCompatibility(t *testing.T) {
	first := newFixture(t, OptionsV1{Name: "first-codes", NativeRetries: 1, RetryCodes: []int{503}}, 1)
	second := newFixture(t, OptionsV1{Name: "second-codes", NativeRetries: 1, RetryCodes: []int{429}}, 1)
	if reflect.DeepEqual(first.client.Profile().Options, second.client.Profile().Options) {
		t.Fatal("different native retry policy has identical compatibility profile")
	}
}
func TestReviewConnectHeaderOverrideIsCaseInsensitive(t *testing.T) {
	target := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = io.WriteString(w, "ok") }, false)
	proxy := newProxy(t, false, "Bearer override")
	address, _ := url.Parse(proxy.server.URL)
	address.User = url.UserPassword("owner", "old")
	route := address.String()
	fix := newFixture(t, OptionsV1{Name: "header-case", Mode: HTTP1Only, Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: target.roots}}}, 1)
	receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "case"}, request(t, "GET", target.tcp.URL, ""),
		RequestOptionsV1{ProxyURL: &route, ConnectHeaders: http.Header{"proxy-authorization": {"Bearer override"}, "X-Route": {"frozen"}}})
	value := settle(t, fix, receipt)
	if err != nil || !value.Outcome.Value.Complete() {
		t.Fatal("CONNECT header override duplicated case-insensitive authorization", err)
	}
}
func TestReviewH2ConnectReceiveHeaderLimit(t *testing.T) {
	var origins atomic.Int32
	target := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) { origins.Add(1); _, _ = io.WriteString(w, "ok") }, false)
	proxy := newProxy(t, true, "Bearer", 2048)
	roots := x509.NewCertPool()
	roots.AddCert(proxy.server.Certificate())
	route := proxy.server.URL
	fix := newFixture(t, OptionsV1{Name: "proxy-headers", Mode: HTTP1Only, MaxHeaderBytes: 1024,
		Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: target.roots}, ProxyTLSConfig: &tls.Config{RootCAs: roots, NextProtos: []string{"h2"}}}}, 1)
	receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "limit"}, request(t, "GET", target.tcp.URL, ""),
		RequestOptionsV1{ProxyURL: &route, ConnectHeaders: http.Header{"Proxy-Authorization": {"Bearer"}, "X-Route": {"frozen"}}})
	value := settle(t, fix, receipt)
	if err == nil || value.Err() == nil || origins.Load() != 0 || proxy.protocol.Load() != 2 {
		t.Fatal("H2 CONNECT bypassed configured receive-header limit")
	}
}

type blockingTLSCache struct {
	entered, release chan struct{}
	once             sync.Once
	failure          error
}

func (*blockingTLSCache) Get(string) (*tls.ClientSessionState, bool) { return nil, false }
func (cache *blockingTLSCache) Put(string, *tls.ClientSessionState) {
	cache.once.Do(func() { close(cache.entered) })
	<-cache.release
	if cache.failure != nil {
		panic(cache.failure)
	}
}
func TestReviewPostHandshakeCacheBlocksOwnedRelease(t *testing.T) {
	cache := &blockingTLSCache{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(cache.release) }) }
	defer unblock()
	peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = io.WriteString(w, "ok") }, false)
	fix := newFixture(t, OptionsV1{Name: "cache", Mode: HTTP1Only, Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots, ClientSessionCache: cache}}}, 1)
	fix.cleanupCause = context.DeadlineExceeded
	ctx, cancel := context.WithCancel(testContext(t))
	type answer struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	done := make(chan answer, 1)
	go func() {
		_, receipt, err := fix.client.Open(ctx, fault.Correlation{Call: "cache"}, request(t, "GET", peer.tcp.URL, ""))
		done <- answer{receipt, err}
	}()
	select {
	case <-cache.entered:
	case <-time.After(time.Second):
		t.Fatal("peer did not invoke session cache")
	}
	cancel()
	got := <-done
	if got.err == nil {
		t.Fatal("canceled Open did not stop waiting")
	}
	if _, err := got.receipt.WaitReleased(testContext(t)); err != nil {
		t.Fatal("canceled call did not finish before resource-release probe", err)
	}
	cleanup, cancelCleanup := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelCleanup()
	if err := fix.assembly.Close(cleanup); err == nil {
		t.Error("owned resource released while borrowed cache callback remained active")
	}
	unblock()
	settle(t, fix, got.receipt)
	if err := fix.assembly.Close(testContext(t)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("eventual release erased earlier cleanup deadline", err)
	}
}
