// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package nuki

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	nativehttp "github.com/nukilabs/http"
	nativeproxy "github.com/nukilabs/tlsclient/proxy"
	nativetls "github.com/nukilabs/utls"
	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

func masqueCertificate(t *testing.T, name string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, roots
}

func TestMASQUETunnelSiblingReadDeadlineAndTerminalClose(t *testing.T) {
	certificate, roots := masqueCertificate(t, "proxy-sibling")
	echo, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	echoDone := make(chan struct{})
	go func() {
		defer close(echoDone)
		data := make([]byte, 65535)
		for {
			count, address, err := echo.ReadFrom(data)
			if err != nil {
				return
			}
			if _, err = echo.WriteTo(data[:count], address); err != nil {
				return
			}
		}
	}()
	defer func() { _ = echo.Close(); <-echoDone }()
	var workers sync.WaitGroup
	endpoint, stopProxy := masqueServer(t, certificate, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		workers.Add(1)
		defer workers.Done()
		if r.Method != "CONNECT" || r.Proto != "connect-udp" {
			w.WriteHeader(400)
			return
		}
		target, err := net.Dial("udp", echo.LocalAddr().String())
		if err != nil {
			w.WriteHeader(502)
			return
		}
		defer target.Close()
		w.Header().Set(http3.CapsuleProtocolHeader, "?1")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		stream := w.(http3.HTTPStreamer).HTTPStream()
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		var loops sync.WaitGroup
		loops.Add(2)
		go func() {
			defer loops.Done()
			for {
				data, err := stream.ReceiveDatagram(ctx)
				if err != nil {
					return
				}
				if len(data) > 0 && data[0] == 0 {
					if _, err := target.Write(data[1:]); err != nil {
						return
					}
				}
			}
		}()
		go func() {
			defer loops.Done()
			data := make([]byte, 65535)
			for {
				count, err := target.Read(data)
				if err != nil {
					return
				}
				if err := stream.SendDatagram(append([]byte{0}, data[:count]...)); err != nil {
					return
				}
			}
		}()
		_, _ = io.Copy(io.Discard, stream)
		cancel()
		_ = target.Close()
		stream.CancelRead(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
		_ = stream.Close()
		loops.Wait()
	}), &quic.Config{InitialPacketSize: 1400})
	defer stopProxy()
	address, _ := url.Parse(endpoint + "/udp/{target_host}/{target_port}/")
	dialer, err := nativeproxy.NewWithDialer(address, time.Second, &nativetls.Config{RootCAs: roots}, nativeproxy.Direct(nil, time.Second), nil, 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.(io.Closer).Close()
	first, err := dialer.ListenPacket(testContext(t), "udp", echo.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := dialer.ListenPacket(testContext(t), "udp", echo.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := first.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if count, err := first.WriteTo([]byte("refused"), echo.LocalAddr()); count != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("past write deadline", count, err)
	}
	if err := first.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.WriteTo([]byte("first"), echo.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if err := first.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	if _, _, err := first.ReadFrom(buffer); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("past read deadline", err)
	}
	if err := first.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	count, target, err := first.ReadFrom(buffer)
	if err != nil || string(buffer[:count]) != "first" || target.String() != echo.LocalAddr().String() {
		t.Fatal("deadline recovery/target", count, target, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := second.WriteTo([]byte("sibling"), echo.LocalAddr()); err != nil {
		t.Fatal("sibling write", err)
	}
	if err := second.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	count, _, err = second.ReadFrom(buffer)
	if err != nil || string(buffer[:count]) != "sibling" {
		t.Fatal("closing one tunnel killed sibling", count, err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := dialer.(io.Closer).Close(); err != nil {
		t.Fatal(err)
	}
	stopProxy()
	workers.Wait()
}

func TestMASQUESetupCancellationAndTimeoutJoinSource(t *testing.T) {
	for _, cancelRequest := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "cancel"}[cancelRequest], func(t *testing.T) {
			certificate, roots := masqueCertificate(t, "proxy-setup")
			entered, ended := make(chan struct{}), make(chan struct{})
			endpoint, stop := masqueServer(t, certificate, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				defer close(ended)
				<-r.Context().Done()
			}))
			defer stop()
			options := providerOptions()
			options.Mode = HTTP3Only
			options.Timeout = time.Second
			options.Native.ProxyTLS = &nativetls.Config{RootCAs: roots}
			options.ProxyURL = endpoint + "/udp/{target_host}/{target_port}/"
			fix := bindProvider(t, options)
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			receipt, err := fix.client.Do(ctx, fault.Correlation{Call: "setup-stop"}, nativeRequest(t, "GET", "https://127.0.0.1:1", nil))
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-testContext(t).Done():
				t.Fatal("CONNECT-UDP setup never entered")
			}
			if cancelRequest {
				cancel()
			}
			result := outcome(t, fix, receipt)
			if result.Err() == nil || result.Outcome.Value.Complete() {
				t.Fatal("stalled setup succeeded")
			}
			if cancelRequest && !errors.Is(result.Err(), context.Canceled) {
				t.Fatal("request cancellation cause lost", result.Err())
			}
			if err := fix.assembly.Close(testContext(t)); err != nil {
				t.Fatal("source could not terminate actual setup", err)
			}
			select {
			case <-ended:
			case <-testContext(t).Done():
				t.Fatal("proxy control stream survived source release")
			}
			fix.client.owner.mu.Lock()
			sockets, tunnels, pending := len(fix.client.owner.sockets), fix.client.owner.tunnels, fix.client.owner.nativeDials
			fix.client.owner.mu.Unlock()
			if sockets != 0 || tunnels != 0 || pending != 0 {
				t.Fatal("canceled setup retained physical work", sockets, tunnels, pending)
			}
		})
	}
}

func masqueServer(t *testing.T, certificate tls.Certificate, handler http.Handler, configurations ...*quic.Config) (string, func()) {
	t.Helper()
	socket, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http3.Server{TLSConfig: &tls.Config{Certificates: []tls.Certificate{certificate}}, EnableDatagrams: true, Handler: handler}
	if len(configurations) > 0 {
		server.QUICConfig = configurations[0]
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(socket) }()
	stop := sync.OnceFunc(func() { _ = server.Close(); _ = socket.Close(); <-done })
	t.Cleanup(stop)
	return "https://" + socket.LocalAddr().String(), stop
}

func TestMASQUEOwnedRoutingTrustAndTunnelQuota(t *testing.T) {
	for _, scenario := range []string{"success", "proxy-trust-refusal", "origin-trust-refusal", "auth-refusal", "negotiation-refusal", "quota"} {
		t.Run(scenario, func(t *testing.T) {
			originCert, originRoots := masqueCertificate(t, "origin")
			proxyCert, proxyRoots := masqueCertificate(t, "proxy")
			var originCalls, connects, workers, upstream, downstream atomic.Int64
			origin, stopOrigin := masqueServer(t, originCert, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				originCalls.Add(1)
				if r.Header.Get("Proxy-Authorization") != "" || r.Header.Get("X-Route") != "" || r.Header.Get("Authorization") != "Bearer origin-only" {
					t.Error("proxy/origin credential association changed")
				}
				_, _ = io.WriteString(w, "masque-origin")
			}))
			defer stopOrigin()
			other, _ := masqueServer(t, originCert, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { originCalls.Add(1); _, _ = io.WriteString(w, "other") }))
			proxy, stopProxy := masqueServer(t, proxyCert, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				connects.Add(1)
				workers.Add(1)
				defer workers.Add(-1)
				if r.Method != "CONNECT" || r.Proto != "connect-udp" || r.Header.Get("Proxy-Authorization") != "Basic dXNlcjpwYXNz" || r.Header.Get("X-Route") != "proxy-only" || r.Header.Get("Authorization") != "" || scenario == "auth-refusal" {
					w.WriteHeader(407)
					return
				}
				path := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
				if len(path) != 3 || path[0] != "udp" {
					w.WriteHeader(400)
					return
				}
				remote, err := net.ResolveUDPAddr("udp", net.JoinHostPort(path[1], path[2]))
				if err != nil {
					w.WriteHeader(400)
					return
				}
				relay, err := net.DialUDP("udp", nil, remote)
				if err != nil {
					w.WriteHeader(502)
					return
				}
				defer relay.Close()
				t.Logf("relay target=%s origin=%s", remote, origin)
				if scenario != "negotiation-refusal" {
					w.Header().Set(http3.CapsuleProtocolHeader, "?1")
				}
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				stream := w.(http3.HTTPStreamer).HTTPStream()
				ctx, cancel := context.WithCancel(r.Context())
				defer cancel()
				var group sync.WaitGroup
				group.Add(2)
				go func() {
					defer group.Done()
					defer relay.Close()
					for {
						data, err := stream.ReceiveDatagram(ctx)
						if err != nil {
							return
						}
						if len(data) == 0 || data[0] != 0 {
							continue
						}
						if upstream.Load() == 0 {
							t.Logf("first tunneled datagram length=%d prefix=%x", len(data), data[:min(6, len(data))])
						}
						if _, err := relay.Write(data[1:]); err != nil {
							return
						}
						upstream.Add(1)
					}
				}()
				go func() {
					defer group.Done()
					buffer := make([]byte, 65535)
					for {
						count, err := relay.Read(buffer)
						if err != nil {
							t.Logf("relay read ended: %v", err)
							return
						}
						if err := stream.SendDatagram(append([]byte{0}, buffer[:count]...)); err != nil {
							t.Logf("proxy datagram send failed size=%d: %v", count, err)
							return
						}
						downstream.Add(1)
					}
				}()
				_, _ = io.Copy(io.Discard, stream)
				cancel()
				_ = relay.Close()
				stream.CancelRead(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
				stream.CancelWrite(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
				_ = stream.Close()
				group.Wait()
			}), &quic.Config{InitialPacketSize: 1400})
			defer stopProxy()
			proxyURL, _ := url.Parse(proxy + "/udp/{target_host}/{target_port}/")
			proxyURL.User = url.UserPassword("user", "pass")
			options := providerOptions()
			options.Mode = HTTP3Only
			options.ProxyURL = proxyURL.String()
			options.Native.TLS = &nativetls.Config{RootCAs: originRoots}
			options.Native.ProxyTLS = &nativetls.Config{RootCAs: proxyRoots}
			if scenario == "proxy-trust-refusal" {
				options.Native.ProxyTLS.RootCAs = originRoots
			}
			if scenario == "origin-trust-refusal" {
				options.Native.TLS.RootCAs = proxyRoots
			}
			if scenario == "quota" {
				options.MaxProxyTunnels = 1
			}
			fix := bindProvider(t, options)
			invoke := func(address string) (Result, error) {
				input := nativeRequest(t, "GET", address, nil)
				input.Header.Set("Authorization", "Bearer origin-only")
				receipt, err := fix.client.Do(testContext(t), fault.Correlation{Call: "masque-" + scenario}, input, RequestOptionsV1{ConnectHeaders: nativehttp.Header{"X-Route": {"proxy-only"}}})
				if err != nil {
					return Result{}, err
				}
				result := outcome(t, fix, receipt)
				return result.Outcome.Value, result.Err()
			}
			result, err := invoke(origin)
			if err != nil {
				logMASQUECauses(t, err, 0)
				t.Logf("proxy=%d origin=%d datagrams=%d/%d workers=%d", connects.Load(), originCalls.Load(), upstream.Load(), downstream.Load(), workers.Load())
			}
			if scenario == "success" || scenario == "quota" {
				if err != nil || !result.Complete() || result.Metadata().Protocol() != "HTTP/3.0" || string(result.DataCopy()) != "masque-origin" {
					t.Fatal("native MASQUE positive", err, result.Metadata().Protocol())
				}
				if scenario == "quota" {
					_, err = invoke(other)
					if !errors.Is(err, ErrCapacity) || connects.Load() != 1 || originCalls.Load() != 1 {
						t.Fatal("tunnel quota bypassed", err, connects.Load(), originCalls.Load())
					}
					result, err = invoke(origin)
					if err != nil || !result.Complete() {
						t.Fatal("refused tunnel poisoned healthy sibling", err)
					}
				}
			} else if err == nil || result.Complete() || originCalls.Load() != 0 {
				t.Fatal("refusal bypassed proxy/trust/negotiation", scenario, err, originCalls.Load())
			}
			if err := fix.assembly.Close(testContext(t)); err != nil {
				t.Fatal("source close", err)
			}
			fix.client.owner.mu.Lock()
			sockets, tunnels := len(fix.client.owner.sockets), fix.client.owner.tunnels
			fix.client.owner.mu.Unlock()
			if sockets != 0 || tunnels != 0 {
				t.Fatal("physical/tunnel source charge retained", sockets, tunnels)
			}
			stopProxy()
			if workers.Load() != 0 {
				t.Fatal("peer control stream still running", workers.Load())
			}
		})
	}
}

func logMASQUECauses(t *testing.T, err error, depth int) {
	if err == nil || depth > 12 {
		return
	}
	t.Logf("cause[%d] %T: %s", depth, err, err.Error())
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			logMASQUECauses(t, cause, depth+1)
		}
	} else if cause := errors.Unwrap(err); cause != nil {
		logMASQUECauses(t, cause, depth+1)
	}
}
