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

package tls_client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nativehttp "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/tls-client/profiles"
)

func compatH2Proxy(t *testing.T, handler http.HandlerFunc, cleanupCauses ...error) (*connectDialer, *roundTripper, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	peer := httptest.NewUnstartedServer(handler)
	peer.EnableHTTP2 = true
	peer.StartTLS()
	t.Cleanup(peer.Close)
	roots := x509.NewCertPool()
	roots.AddCert(peer.Certificate())
	state := newCompatibilityState()
	rt := &roundTripper{compat: state, cachedTransports: make(map[string]nativehttp.RoundTripper), cachedConnections: make(map[string]net.Conn)}
	t.Cleanup(func() {
		closed := make(chan error, 1)
		go func() { closed <- state.close(rt) }()
		select {
		case err := <-closed:
			accepted := err == nil
			for _, cause := range cleanupCauses {
				accepted = accepted || errors.Is(err, cause)
			}
			if !accepted {
				t.Error("proxy cleanup failed", err)
			}
		case <-time.After(time.Second):
			t.Error("proxy cleanup did not join owned work")
		}
	})
	address, err := url.Parse(peer.URL)
	if err != nil {
		t.Fatal(err)
	}
	dialer := &connectDialer{compat: state, ProxyUrl: *address, Timeout: time.Second, logger: NewNoopLogger(), EnableH2ConnReuse: true}
	dialer.DialTLS = func(network, address string) (net.Conn, string, error) {
		raw, err := state.dial(ctx, network, address, (&net.Dialer{}).DialContext)
		if err != nil {
			return nil, "", err
		}
		secured := tls.Client(raw, &tls.Config{RootCAs: roots, ServerName: "127.0.0.1", NextProtos: []string{"h2"}})
		if err := secured.HandshakeContext(ctx); err != nil {
			_ = secured.Close()
			return nil, "", err
		}
		return secured, secured.ConnectionState().NegotiatedProtocol, nil
	}
	rt.dialer = dialer
	return dialer, rt, ctx
}

func compatEchoTunnel(writer http.ResponseWriter, request *http.Request) {
	writer.WriteHeader(http.StatusOK)
	writer.(http.Flusher).Flush()
	buffer := make([]byte, 32)
	for {
		count, err := request.Body.Read(buffer)
		if count > 0 {
			_, _ = writer.Write(buffer[:count])
			writer.(http.Flusher).Flush()
		}
		if err != nil {
			return
		}
	}
}

func compatCheckTunnel(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, "echo"); err != nil {
		t.Fatal("handed-off tunnel cannot write", err)
	}
	data := make([]byte, 4)
	if _, err := io.ReadFull(conn, data); err != nil || string(data) != "echo" {
		t.Fatal("handed-off tunnel cannot read the echoed payload", err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func TestFathomryH2ConnectRetainsEstablishedTunnel(t *testing.T) {
	var connects atomic.Int64
	dialer, rt, ctx := compatH2Proxy(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != "CONNECT" || request.ProtoMajor != 2 {
			t.Error("native H2 CONNECT not reached")
			return
		}
		connects.Add(1)
		compatEchoTunnel(writer, request)
	})
	conn, err := rt.compat.dial(ctx, "tcp", "origin.invalid:443", dialer.DialContext)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for range 3 {
		compatCheckTunnel(t, conn)
	}
	if connects.Load() != 1 {
		t.Fatal("tunnel echo replaced the original CONNECT stream")
	}
}

func TestFathomryH2ConnectCancellationJoinsOwnedPipe(t *testing.T) {
	for _, mode := range []string{"caller", "source", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{})
			dialer, rt, ctx := compatH2Proxy(t, func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != "CONNECT" || request.ProtoMajor != 2 {
					t.Error("native H2 CONNECT not reached")
					return
				}
				close(entered)
				<-request.Context().Done()
			})
			if mode == "timeout" {
				dialer.Timeout = 50 * time.Millisecond
			}
			work, cancel := context.WithCancel(ctx)
			defer cancel()
			returned := make(chan error, 1)
			go func() {
				conn, err := rt.compat.dial(work, "tcp", "origin.invalid:443", dialer.DialContext)
				if conn != nil {
					_ = conn.Close()
				}
				returned <- err
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("CONNECT did not reach the waiting proxy")
			}
			var shutdown chan error
			switch mode {
			case "caller":
				cancel()
			case "source":
				shutdown = make(chan error, 1)
				go func() { shutdown <- rt.compat.close(rt) }()
			}
			select {
			case err := <-returned:
				want := context.Canceled
				if mode == "timeout" {
					want = context.DeadlineExceeded
				}
				if !errors.Is(err, want) {
					t.Fatal("CONNECT lost its interruption cause", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation did not join the SDK's own CONNECT pipe")
			}
			if shutdown == nil {
				shutdown = make(chan error, 1)
				go func() { shutdown <- rt.compat.close(rt) }()
			}
			select {
			case err := <-shutdown:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("source shutdown retained internal CONNECT workers")
			}
			rt.compat.mu.Lock()
			complete := rt.compat.finished && rt.compat.active == 0 && len(rt.compat.conns) == 0
			rt.compat.mu.Unlock()
			if !complete {
				t.Fatal("CONNECT cleanup did not confirm native release")
			}
		})
	}
}

func TestFathomryH2ConnectCancelPreservesOtherTunnel(t *testing.T) {
	var connects atomic.Int64
	secondEntered := make(chan struct{})
	dialer, rt, ctx := compatH2Proxy(t, func(writer http.ResponseWriter, request *http.Request) {
		if connects.Add(1) == 2 {
			close(secondEntered)
			<-request.Context().Done()
			return
		}
		compatEchoTunnel(writer, request)
	})
	first, err := rt.compat.dial(ctx, "tcp", "origin.invalid:443", dialer.DialContext)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	compatCheckTunnel(t, first)
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	returned := make(chan error, 1)
	go func() {
		conn, err := rt.compat.dial(work, "tcp", "origin.invalid:443", dialer.DialContext)
		if conn != nil {
			_ = conn.Close()
		}
		returned <- err
	}()
	select {
	case <-secondEntered:
	case <-ctx.Done():
		t.Fatal("cached CONNECT was not submitted")
	}
	cancel()
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("second CONNECT lost cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second CONNECT retained its input pipe")
	}
	compatCheckTunnel(t, first)
	if connects.Load() != 2 {
		t.Fatal("canceled CONNECT silently submitted another tunnel")
	}
}

func TestFathomryControlledH2ProxyIsolationAndQuotas(t *testing.T) {
	var peerMu sync.Mutex
	sessions := make(map[string]struct{})
	dialer, rt, ctx := compatH2Proxy(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != "CONNECT" || request.ProtoMajor != 2 {
			t.Error("controlled proxy did not retain H2 CONNECT")
			return
		}
		peerMu.Lock()
		sessions[request.RemoteAddr] = struct{}{}
		peerMu.Unlock()
		compatEchoTunnel(writer, request)
	})
	if !dialer.EnableH2ConnReuse {
		t.Fatal("unconfigured SDK proxy reuse was changed")
	}
	var held, dials atomic.Int64
	capacityErr := errors.New("synthetic source TCP capacity")
	native := &httpClient{Client: nativehttp.Client{Transport: rt}}
	if err := ConfigureFathomry(native, FathomryControlV1{AcquireTCP: func() (func(), error) {
		for {
			current := held.Load()
			if current >= 4 {
				return nil, capacityErr
			}
			if held.CompareAndSwap(current, current+1) {
				return func() { held.Add(-1) }, nil
			}
		}
	}, MaxProxyHeaderBytes: 4096}); err != nil {
		t.Fatal(err)
	}
	if dialer.EnableH2ConnReuse {
		t.Fatal("controlled proxy retained unsafe physical-session sharing")
	}
	originalDial := dialer.DialTLS
	dialer.DialTLS = func(network, address string) (net.Conn, string, error) {
		dials.Add(1)
		return originalDial(network, address)
	}
	first, err := rt.compat.dial(ctx, "tcp", "origin.invalid:443", dialer.DialContext)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := rt.compat.dial(ctx, "tcp", "origin.invalid:443", dialer.DialContext)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	peerMu.Lock()
	physical := len(sessions)
	peerMu.Unlock()
	if physical != 2 || dials.Load() != 2 || held.Load() != 4 {
		t.Fatal("tunnels did not retain independent physical connections and shared quotas", physical, dials.Load(), held.Load())
	}
	third, err := rt.compat.dial(ctx, "tcp", "origin.invalid:443", dialer.DialContext)
	if third != nil || !errors.Is(err, capacityErr) || dials.Load() != 2 || held.Load() != 4 {
		t.Fatal("source capacity permitted another native connection", err)
	}
	compatCheckTunnel(t, first)
	compatCheckTunnel(t, second)
	if err := first.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if held.Load() != 2 {
		t.Fatal("dedicated tunnel close did not return both managed handles", held.Load())
	}
	compatCheckTunnel(t, second)
	third, err = rt.compat.dial(ctx, "tcp", "origin.invalid:443", dialer.DialContext)
	if err != nil || held.Load() != 4 {
		t.Fatal("released source capacity could not be reused", err, held.Load())
	}
	compatCheckTunnel(t, third)
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Close(native); err != nil || !FathomryQuiescent(native) || held.Load() != 0 {
		t.Fatal("controlled source cleanup did not confirm release", err, held.Load())
	}
}

type compatRetryProxyClose struct {
	net.Conn
	allow *atomic.Bool
	cause error
}

func (conn compatRetryProxyClose) Close() error {
	err := conn.Conn.Close()
	if !conn.allow.Load() {
		return conn.cause
	}
	return err
}

func TestFathomryControlledH2ProxyRetriesOwnedPhysicalClose(t *testing.T) {
	cause := errors.New("synthetic unconfirmed proxy close")
	dialer, rt, ctx := compatH2Proxy(t, compatEchoTunnel, cause)
	var held atomic.Int64
	var allow atomic.Bool
	original := dialer.DialTLS
	dialer.DialTLS = func(network, address string) (net.Conn, string, error) {
		conn, protocol, err := original(network, address)
		if err != nil {
			return nil, "", err
		}
		return compatRetryProxyClose{Conn: conn, allow: &allow, cause: cause}, protocol, nil
	}
	native := &httpClient{Client: nativehttp.Client{Transport: rt}}
	if err := ConfigureFathomry(native, FathomryControlV1{AcquireTCP: func() (func(), error) {
		held.Add(1)
		return func() { held.Add(-1) }, nil
	}}); err != nil {
		t.Fatal(err)
	}
	conn, err := rt.compat.dial(ctx, "tcp", "origin.invalid:443", dialer.DialContext)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { allow.Store(true); _ = conn.Close() })
	compatCheckTunnel(t, conn)
	if err := conn.Close(); !errors.Is(err, cause) || held.Load() == 0 {
		t.Fatal("unconfirmed physical cleanup lost its quota/cause", err, held.Load())
	}
	if err := Close(native); !errors.Is(err, cause) || FathomryQuiescent(native) || held.Load() == 0 {
		t.Fatal("source certified failed physical cleanup", err, held.Load())
	}
	allow.Store(true)
	if err := Close(native); !errors.Is(err, cause) || !FathomryQuiescent(native) || held.Load() != 0 {
		t.Fatal("retry did not confirm release while retaining original error", err, held.Load())
	}
}

func TestFathomryControlledH2ProxyPreservesOriginPooling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	origin := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "pooled") }))
	defer origin.Close()
	var connects atomic.Int64
	proxy := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != "CONNECT" || request.ProtoMajor != 2 || request.Host != origin.Listener.Addr().String() {
			t.Error("unexpected H2 proxy request")
			writer.WriteHeader(400)
			return
		}
		upstream, err := (&net.Dialer{}).DialContext(request.Context(), "tcp", request.Host)
		if err != nil {
			t.Error(err)
			writer.WriteHeader(502)
			return
		}
		defer upstream.Close()
		stop := context.AfterFunc(request.Context(), func() { _ = upstream.Close() })
		defer stop()
		connects.Add(1)
		writer.WriteHeader(200)
		writer.(http.Flusher).Flush()
		inputDone := make(chan struct{})
		go func() { defer close(inputDone); _, _ = io.Copy(upstream, request.Body); _ = upstream.Close() }()
		buffer := make([]byte, 32<<10)
		for {
			count, err := upstream.Read(buffer)
			if count > 0 {
				if _, writeErr := writer.Write(buffer[:count]); writeErr != nil {
					break
				}
				writer.(http.Flusher).Flush()
			}
			if err != nil {
				break
			}
		}
		_ = upstream.Close()
		_ = request.Body.Close()
		<-inputDone
	}))
	proxy.EnableHTTP2 = true
	proxy.StartTLS()
	defer proxy.Close()
	originRoots := x509.NewCertPool()
	originRoots.AddCert(origin.Certificate())
	client, err := NewHttpClient(NewNoopLogger(), WithClientProfile(profiles.Chrome_144), WithForceHttp1(), WithDisableHttp3(), WithProxyUrl(proxy.URL), WithTransportOptions(&TransportOptions{RootCAs: originRoots}))
	if err != nil {
		t.Fatal(err)
	}
	defer Close(client)
	rt := client.(*httpClient).Transport.(*roundTripper)
	dialer := rt.dialer.(*connectDialer)
	proxyRoots := x509.NewCertPool()
	proxyRoots.AddCert(proxy.Certificate())
	dialer.DialTLS = func(network, address string) (net.Conn, string, error) {
		raw, err := rt.compat.dial(ctx, network, address, (&net.Dialer{}).DialContext)
		if err != nil {
			return nil, "", err
		}
		secured := tls.Client(raw, &tls.Config{RootCAs: proxyRoots, ServerName: "127.0.0.1", NextProtos: []string{"h2"}})
		if err := secured.HandshakeContext(ctx); err != nil {
			_ = secured.Close()
			return nil, "", err
		}
		return secured, secured.ConnectionState().NegotiatedProtocol, nil
	}
	if err := ConfigureFathomry(client, FathomryControlV1{MaxProxyHeaderBytes: 4096}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		request, err := nativehttp.NewRequestWithContext(ctx, "GET", origin.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil || string(data) != "pooled" || response.ProtoMajor != 1 {
			t.Fatal("origin response through H2 CONNECT changed", readErr, closeErr)
		}
	}
	if connects.Load() != 1 {
		t.Fatal("managed proxy isolation disabled origin connection pooling", connects.Load())
	}
	if err := Close(client); err != nil || !FathomryQuiescent(client) {
		t.Fatal("origin pool cleanup did not confirm release", err)
	}
}
