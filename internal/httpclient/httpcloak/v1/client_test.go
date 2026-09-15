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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	officialh3 "github.com/quic-go/quic-go/http3"
	nativehttp "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/transport"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type fixture struct {
	client   *Client
	assembly *resource.Assembly
	selected resource.Selection[Source]
	inbox    *invocation.Inbox[Result]
}

func bindFixture(t *testing.T, options OptionsV1, slots int) *fixture {
	t.Helper()
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	limits, err := LimitsV1(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "fixture", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](slots, int64(slots)*defaults(options).evidenceBytes())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assembly.Close(testContext(t)); err != nil {
			client.owner.mu.Lock()
			t.Logf("release accounting: connections=%d sockets=%d cleanup=%T", client.owner.connections, len(client.owner.sockets), client.owner.cleanup)
			client.owner.mu.Unlock()
			t.Error("source release", err)
		}
	})
	return &fixture{client, assembly, selected, inbox}
}
func peer(t *testing.T, mode ProtocolMode, handler http.HandlerFunc) (string, OptionsV1) {
	t.Helper()
	tcp := httptest.NewUnstartedServer(handler)
	tcp.EnableHTTP2 = mode == HTTP2
	tcp.StartTLS()
	t.Cleanup(tcp.Close)
	roots := x509.NewCertPool()
	roots.AddCert(tcp.Certificate())
	options := OptionsV1{Name: "source", PresetName: "chrome-148", Protocol: mode, DisableECH: true, Native: NativeOptionsV1{Verify: &transport.TLSVerify{RootCAs: roots}}}
	if mode != HTTP3 {
		return tcp.URL, options
	}
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &officialh3.Server{TLSConfig: &tls.Config{Certificates: tcp.TLS.Certificates}, Handler: handler, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	done := make(chan error, 1)
	go func() { done <- server.Serve(packet) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = packet.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("H3 peer did not exit")
		}
	})
	return "https://" + packet.LocalAddr().String(), options
}
func request(t *testing.T, method, address string, body io.Reader) *nativehttp.Request {
	t.Helper()
	value, err := nativehttp.NewRequest(method, address, body)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func settle(t *testing.T, fixture *fixture, receipt *invocation.Receipt[Result]) invocation.Result[Result] {
	t.Helper()
	if receipt == nil {
		t.Fatal("missing receipt")
	}
	result, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := fixture.inbox.Next(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	independent, err := delivery.Receipt().WaitReleased(testContext(t))
	if err != nil || independent.Context != result.Context || independent.Outcome.Primary != result.Outcome.Primary || independent.Outcome.Cleanup != result.Outcome.Cleanup {
		t.Fatal("independent evidence changed", err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	return result
}
func TestRequestResponseAndReuse(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1, HTTP2, HTTP3} {
		t.Run(string(mode), func(t *testing.T) {
			address, options := peer(t, mode, func(w http.ResponseWriter, r *http.Request) {
				data, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				if string(data) != "input" || r.Header.Get("Cookie") != "session=caller" || r.Header.Get("X-Call") != "native" {
					t.Error("native request input changed")
				}
				w.Header().Set("X-Native", "response")
				_, _ = io.WriteString(w, "output")
			})
			options.MaxBindings = 1
			options.MaxConnections = 1
			fixture := bindFixture(t, options, 1)
			for index := 0; index < 2; index++ {
				input := request(t, "POST", address, strings.NewReader("input"))
				input.Header.Set("Cookie", "session=caller")
				input.Header.Set("X-Call", "native")
				receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "request"}, input)
				if err != nil {
					t.Fatal(err)
				}
				got := settle(t, fixture, receipt)
				if got.Err() != nil || !got.Released || !got.Final || !got.Outcome.Value.Complete() || string(got.Outcome.Value.DataCopy()) != "output" || got.Outcome.Value.Metadata().StatusCode() != 200 {
					t.Fatal("response evidence changed", got.Err())
				}
				expected := map[ProtocolMode]string{HTTP1: "HTTP/1.1", HTTP2: "HTTP/2.0", HTTP3: "HTTP/3.0"}[mode]
				if got.Outcome.Value.Metadata().Protocol() != expected {
					t.Fatal("protocol fallback")
				}
			}
		})
	}
}
func TestDeclaredShortResponseAndBodylessResponses(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1, HTTP2, HTTP3} {
		t.Run(string(mode), func(t *testing.T) {
			address, options := peer(t, mode, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "64")
				if r.URL.Path == "/304" {
					w.WriteHeader(304)
					return
				}
				if r.Method == "HEAD" {
					return
				}
				_, _ = io.WriteString(w, "short")
			})
			fixture := bindFixture(t, options, 1)
			for _, path := range []string{"/head", "/304", "/short"} {
				method := "GET"
				if path == "/head" {
					method = "HEAD"
				}
				receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "body"}, request(t, method, address+path, nil))
				got := settle(t, fixture, receipt)
				if path == "/short" {
					if err == nil || !errors.Is(got.Err(), io.ErrUnexpectedEOF) || got.Outcome.Value.Complete() {
						t.Fatal("short response accepted", err, got.Err())
					}
				} else if err != nil || !got.Outcome.Value.Complete() || len(got.Outcome.Value.DataCopy()) != 0 {
					t.Fatal("bodyless response rejected", path, err, "unexpected-EOF", errors.Is(got.Err(), io.ErrUnexpectedEOF), "canceled", errors.Is(got.Err(), context.Canceled))
				}
			}
		})
	}
}
