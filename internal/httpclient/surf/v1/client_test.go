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
	"log"
	"log/slog"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/enetx/http"
	"github.com/enetx/surf/profiles/chrome"
	"github.com/enetx/surf/profiles/firefox"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

type fixture struct {
	cleanupCause error
	client       *Client
	assembly     *resource.Assembly
	selected     resource.Selection[Source]
	inbox        *invocation.Inbox[Result]
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func newFixture(t *testing.T, options OptionsV1, capacity int) *fixture {
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
	assembly, err := resource.Assemble(testContext(t), testContext(t), "test", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](capacity, int64(capacity)*defaults(options).evidenceBytes())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &fixture{client: client, assembly: assembly, selected: selected, inbox: inbox}
	t.Cleanup(func() {
		if err := assembly.Close(testContext(t)); err != nil && (fixture.cleanupCause == nil || !errors.Is(err, fixture.cleanupCause)) {
			t.Error("owned release", err)
		}
	})
	return fixture
}
func request(t *testing.T, method, uri, body string) *http.Request {
	t.Helper()
	value, err := http.NewRequest(method, uri, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func settle(t *testing.T, fixture *fixture, receipt *invocation.Receipt[Result]) invocation.Result[Result] {
	t.Helper()
	if receipt == nil {
		t.Fatal("accepted evidence absent")
	}
	direct, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := fixture.inbox.Next(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	independent, err := delivery.Receipt().WaitReleased(testContext(t))
	if err != nil || independent.Context != direct.Context || independent.Outcome.Primary != direct.Outcome.Primary || independent.Outcome.Cleanup != direct.Outcome.Cleanup || !independent.Final || !independent.Released {
		t.Fatal("independent result changed", err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	return direct
}

type peers struct {
	tcp   *httptest.Server
	roots *x509.CertPool
	mu    sync.Mutex
	quic  []*quic.Conn
}

func newPeers(t *testing.T, handler stdhttp.HandlerFunc, h3 bool, configuration ...*tls.Config) *peers {
	t.Helper()
	result := &peers{roots: x509.NewCertPool()}
	result.tcp = httptest.NewUnstartedServer(handler)
	result.tcp.EnableHTTP2 = true
	if len(configuration) > 0 {
		result.tcp.TLS = configuration[0]
	}
	result.tcp.Config.ErrorLog = log.New(io.Discard, "", 0)
	result.tcp.StartTLS()
	t.Cleanup(result.tcp.Close)
	result.roots.AddCert(result.tcp.Certificate())
	if h3 {
		packet, err := net.ListenPacket("udp", result.tcp.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		server := &http3.Server{TLSConfig: &tls.Config{Certificates: result.tcp.TLS.Certificates}, Handler: handler,
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			ConnContext: func(ctx context.Context, conn *quic.Conn) context.Context {
				result.mu.Lock()
				result.quic = append(result.quic, conn)
				result.mu.Unlock()
				return ctx
			}}
		done := make(chan error, 1)
		go func() { done <- server.Serve(packet) }()
		t.Cleanup(func() {
			_ = server.Close()
			_ = packet.Close()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("H3 fixture did not stop")
			}
		})
	}
	return result
}
func TestNativeProtocolsAndIndependentEvidence(t *testing.T) {
	for _, name := range []string{"standard-h1", "standard-h2", "ja-h1", "ja-h2", "h3"} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				calls.Add(1)
				data, err := io.ReadAll(r.Body)
				if err != nil || string(data) != "payload" || r.Header.Get("Cookie") != "synthetic=caller" || r.Header.Get("X-Call") != "one" {
					t.Error("peer input differs", err)
				}
				w.Header().Set("Trailer", "X-End")
				w.WriteHeader(429)
				_, _ = w.Write(data)
				w.Header().Set("X-End", "done")
			}, name == "h3")
			options := OptionsV1{Name: "external", Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots}}}
			want := "HTTP/1.1"
			switch name {
			case "standard-h1":
				options.Mode = HTTP1Only
			case "standard-h2":
				options.Mode = HTTP2Only
				want = "HTTP/2.0"
			case "ja-h1":
				options.Mode = HTTP1Only
				profile := chrome.Desktop
				options.Native.Profile = &profile
			case "ja-h2":
				options.Mode = HTTP2Only
				profile := firefox.Desktop
				options.Native.Profile = &profile
				want = "HTTP/2.0"
			case "h3":
				options.Mode = PreferHTTP3
				profile := chrome.Desktop
				options.Native.Profile = &profile
				want = "HTTP/3.0"
			}
			fix := newFixture(t, options, 1)
			input := request(t, "PATCH", peer.tcp.URL, "payload")
			input.Header.Set("Cookie", "synthetic=caller")
			input.Header.Set("X-Call", "one")
			receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "one"}, input)
			if err != nil {
				t.Fatalf("Do: %+v", err)
			}
			result := settle(t, fix, receipt)
			data := result.Outcome.Value
			if result.Err() != nil || !data.Complete() || data.Metadata().Protocol() != want || data.Metadata().StatusCode() != 429 ||
				string(data.DataCopy()) != "payload" || data.TrailersCopy().Get("X-End") != "done" || calls.Load() != 1 ||
				result.Context.Source != "external" || result.Attempts.Exact || result.Attempts.Observed != 1 {
				t.Errorf("wrong evidence: protocol=%q complete=%v status=%d bytes=%q attempts=%+v calls=%d err=%v",
					data.Metadata().Protocol(), data.Complete(), data.Metadata().StatusCode(), data.DataCopy(), result.Attempts, calls.Load(), result.Err())
			}
			copy := data.DataCopy()
			copy[0] = '!'
			headers := data.Metadata().HeadersCopy()
			headers.Set("X-New", "changed")
			if string(data.DataCopy()) != "payload" || data.Metadata().HeadersCopy().Get("X-New") != "" {
				t.Fatal("mutable result escaped")
			}
			if err := fix.assembly.Close(testContext(t)); err != nil {
				t.Fatal(err)
			}
			peer.mu.Lock()
			connections := append([]*quic.Conn(nil), peer.quic...)
			peer.mu.Unlock()
			for _, conn := range connections {
				select {
				case <-conn.Context().Done():
				case <-time.After(2 * time.Second):
					t.Fatal("original QUIC connection survived release")
				}
			}
		})
	}
}
func TestMalformedInputAndEvidenceSaturationPrecedeNativeEntry(t *testing.T) {
	var calls atomic.Int32
	peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) { calls.Add(1); _, _ = io.WriteString(w, "ok") }))
	defer peer.Close()
	fix := newFixture(t, OptionsV1{Name: "guard", Mode: HTTP1Only}, 1)
	input := request(t, "GET", peer.URL, "")
	input.Header["Invalid\nHeader"] = []string{"x"}
	if receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "bad"}, input); receipt != nil || err == nil {
		t.Fatal("malformed input admitted")
	}
	input = request(t, "GET", peer.URL, "")
	first, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "first"}, input)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "overflow"}, input)
	if receipt != nil || !errors.Is(err, invocation.ErrEvidence) || calls.Load() != 1 {
		t.Fatal("saturated evidence entered native work", err)
	}
	settle(t, fix, first)
}
