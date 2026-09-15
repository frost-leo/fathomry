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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
)

type countingCache struct {
	native tls.ClientSessionCache
	gets   atomic.Int64
	puts   atomic.Int64
}

func (cache *countingCache) Get(key string) (*tls.ClientSessionState, bool) {
	cache.gets.Add(1)
	return cache.native.Get(key)
}
func (cache *countingCache) Put(key string, state *tls.ClientSessionState) {
	cache.puts.Add(1)
	cache.native.Put(key, state)
}

func TestNativeTLSCallbacksAndCacheRetainTheirContracts(t *testing.T) {
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "native") })
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(options.RootCAPEM)) {
		t.Fatal("bad roots")
	}
	options.RootCAPEM = ""
	options.DisableKeepAlives = true
	cache := &countingCache{native: tls.NewLRUClientSessionCache(4)}
	var times, verified atomic.Int64
	var keylog bytes.Buffer
	var logMu sync.Mutex
	options.Native.TLS = &tls.Config{RootCAs: roots, Rand: rand.Reader, ClientSessionCache: cache,
		Time:                  func() time.Time { times.Add(1); return time.Now() },
		VerifyPeerCertificate: func([][]byte, [][]*x509.Certificate) error { verified.Add(1); return nil },
		KeyLogWriter:          writerFunc(func(value []byte) (int, error) { logMu.Lock(); defer logMu.Unlock(); return keylog.Write(value) }),
	}
	f := bindFixture(t, options, 1)
	for _, id := range []string{"first", "second"} {
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation(id), newRequest(t, "GET", server.URL, nil))
		if err != nil {
			t.Fatal(err)
		}
		settle(t, f, receipt)
	}
	if times.Load() == 0 || verified.Load() == 0 || cache.gets.Load() == 0 || cache.puts.Load() == 0 {
		t.Fatal("native extension was silently discarded")
	}
	if err := f.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	reads := cache.gets.Load()
	if _, ok := f.client.owner.transport.TLSClientConfig.ClientSessionCache.Get("after-close"); ok || cache.gets.Load() != reads {
		t.Fatal("released native cache was invoked")
	}
	conformance.Private(t, f.client, "CLIENT_HANDSHAKE_TRAFFIC_SECRET")
}

type writerFunc func([]byte) (int, error)

func (write writerFunc) Write(value []byte) (int, error) { return write(value) }

func TestNativeProxyCallbackDoesNotExposeRequestBody(t *testing.T) {
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) { _, _ = io.Copy(io.Discard, request.Body) })
	var called atomic.Int64
	options.Native.Proxy = func(request *http.Request) (*url.URL, error) {
		called.Add(1)
		if request.Body != nil || request.GetBody != nil || request.Response != nil {
			t.Error("native owning request escaped")
		}
		return nil, nil
	}
	f := bindFixture(t, options, 1)
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("native-proxy"), newRequest(t, "POST", server.URL, bytes.NewReader([]byte("runtime"))))
	if err != nil {
		t.Fatal(err)
	}
	settle(t, f, receipt)
	if called.Load() != 1 {
		t.Fatal("proxy extension was not used")
	}
}

func TestTypedNilNativeDependencyRejected(t *testing.T) {
	var jar *cookiejar.Jar
	if _, err := Select(OptionsV1{Name: "nil", Native: NativeOptionsV1{Jar: jar}}); !errors.Is(err, ErrInput) {
		t.Fatal("typed nil jar accepted", err)
	}
}

func TestCanceledDirectDialKeepsReceiptAndNativeUse(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	options := OptionsV1{Name: "direct-dial", Timeout: 30 * time.Millisecond, Native: NativeOptionsV1{DialContext: func(context.Context, string, string) (net.Conn, error) {
		close(entered)
		<-release
		return nil, context.DeadlineExceeded
	}}}
	f := bindFixture(t, options, 1)
	connection, receipt, err := f.client.Connect(deadline(t), correlation("direct-dial"), "https", "synthetic.invalid:443")
	if connection != nil || receipt == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("direct setup failed to yield an owned wait", err)
	}
	<-entered
	if result, ok := receipt.Result(); ok && result.Released {
		t.Fatal("direct native callback released early")
	}
	unblock()
	result := settle(t, f, receipt)
	if !errors.Is(result.Err(), context.DeadlineExceeded) {
		t.Fatal("direct native cause lost")
	}
}

func TestUnencryptedHTTP2IsExplicit(t *testing.T) {
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "h2c") }))
	server.Config.Protocols = protocols
	server.Start()
	t.Cleanup(server.Close)
	f := bindFixture(t, OptionsV1{Name: "h2c", UnencryptedHTTP2: true}, 1)
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("h2c"), newRequest(t, "GET", server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	result := settle(t, f, receipt)
	if result.Outcome.Value.Metadata().Protocol() != "HTTP/2.0" || string(result.Outcome.Value.DataCopy()) != "h2c" {
		t.Fatal("cleartext protocol silently changed")
	}
}
