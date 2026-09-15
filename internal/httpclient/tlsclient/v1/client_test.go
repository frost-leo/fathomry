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
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	nativehttp "github.com/bogdanfinn/fhttp"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type providerFixture struct {
	client       *Client
	selected     resource.Selection[Source]
	assembly     *resource.Assembly
	inbox        *invocation.Inbox[Result]
	cleanupCause error
}

func providerOptions() OptionsV1 {
	profile := profiles.Chrome_144
	return OptionsV1{Name: "synthetic", Native: NativeOptionsV1{Profile: &profile}}
}
func providerPeer(t *testing.T, mode ProtocolMode, handler http.HandlerFunc) (string, OptionsV1) {
	t.Helper()
	options := providerOptions()
	options.Mode = mode
	if mode == HTTP3Racing {
		peers := newNativePeers(t, handler, handler)
		options.Native.Transport = &sdk.TransportOptions{RootCAs: peers.roots}
		return peers.tcp.URL, options
	}
	peer := httptest.NewUnstartedServer(handler)
	peer.EnableHTTP2 = mode == Negotiated
	peer.StartTLS()
	t.Cleanup(peer.Close)
	roots := x509.NewCertPool()
	roots.AddCert(peer.Certificate())
	options.Native.Transport = &sdk.TransportOptions{RootCAs: roots}
	return peer.URL, options
}
func bindProvider(t *testing.T, options OptionsV1, capacity int, layers ...resource.Layer) *providerFixture {
	t.Helper()
	selected, err := Select(options, layers...)
	if err != nil {
		t.Fatal(err)
	}
	limits, err := LimitsV1(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "local", selected)
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
	fixture := &providerFixture{client: client, selected: selected, assembly: assembly, inbox: inbox}
	t.Cleanup(func() {
		if err := assembly.Close(testContext(t)); err != nil && (fixture.cleanupCause == nil || !errors.Is(err, fixture.cleanupCause)) {
			t.Error("assembly not released", err)
		}
	})
	return fixture
}
func providerRequest(t *testing.T, method, endpoint string, body io.Reader) *nativehttp.Request {
	t.Helper()
	request, err := nativehttp.NewRequest(method, endpoint, body)
	if err != nil {
		t.Fatal(err)
	}
	return request
}
func providerID(name string) fault.Correlation { return fault.Correlation{Call: name} }
func settleProvider(t *testing.T, fixture *providerFixture, receipt *invocation.Receipt[Result]) invocation.Result[Result] {
	t.Helper()
	if receipt == nil {
		t.Fatal("missing accepted evidence")
	}
	value, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := fixture.inbox.Next(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	independent, err := delivery.Receipt().WaitReleased(testContext(t))
	if err != nil || independent.Context != value.Context || !independent.Final || !independent.Released ||
		independent.Outcome.Primary != value.Outcome.Primary || independent.Outcome.Cleanup != value.Outcome.Cleanup {
		t.Fatal("independent evidence differs", err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	return value
}
func providerProtocols(t *testing.T, run func(*testing.T, ProtocolMode)) {
	t.Helper()
	for _, mode := range []ProtocolMode{HTTP1Only, Negotiated, HTTP3Racing} {
		t.Run(string(mode), func(t *testing.T) { run(t, mode) })
	}
}

func TestProviderNativeProtocolsAndEvidence(t *testing.T) {
	providerProtocols(t, func(t *testing.T, mode ProtocolMode) {
		var requests atomic.Int64
		endpoint, options := providerPeer(t, mode, func(w http.ResponseWriter, request *http.Request) {
			requests.Add(1)
			data, err := io.ReadAll(request.Body)
			if err != nil {
				t.Error(err)
			}
			w.Header().Set("X-Method", request.Method)
			w.Header().Set("Trailer", "X-Final")
			w.WriteHeader(429)
			_, _ = w.Write(data)
			w.Header().Set("X-Final", "done")
		})
		fixture := bindProvider(t, options, 2)
		input := providerRequest(t, "PATCH", endpoint, strings.NewReader("native-payload"))
		input.Header.Set("Cookie", "synthetic=caller")
		receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("one"), input)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		result := settleProvider(t, fixture, receipt)
		data := result.Outcome.Value
		expected := map[ProtocolMode]string{HTTP1Only: "HTTP/1.1", Negotiated: "HTTP/2.0", HTTP3Racing: "HTTP/3.0"}[mode]
		if result.Err() != nil || !data.Complete() || string(data.DataCopy()) != "native-payload" ||
			data.Metadata().StatusCode() != 429 || data.Metadata().Protocol() != expected ||
			data.Metadata().HeadersCopy().Get("X-Method") != "PATCH" || data.TrailersCopy().Get("X-Final") != "done" {
			t.Fatalf("wrong result: error=%v protocol=%s complete=%v payload=%q trailers=%v", result.Err(), data.Metadata().Protocol(), data.Complete(), data.DataCopy(), data.TrailersCopy())
		}
		if requests.Load() < 1 || result.Attempts.Exact || result.Attempts.Observed != 1 || data.Exchanges() != 1 {
			t.Fatal("attempt evidence claims hidden racing attempts")
		}
		copy := data.DataCopy()
		copy[0] = 'x'
		headers := data.Metadata().HeadersCopy()
		headers.Set("X-Method", "changed")
		trailers := data.TrailersCopy()
		trailers.Set("X-Final", "changed")
		if string(data.DataCopy()) != "native-payload" || data.Metadata().HeadersCopy().Get("X-Method") != "PATCH" || data.TrailersCopy().Get("X-Final") != "done" {
			t.Fatal("result aliases inspection copies")
		}
		if input.Header.Get("Cookie") != "synthetic=caller" || input.URL.String() != endpoint {
			t.Fatal("caller metadata mutated")
		}
		if err := fixture.assembly.Close(testContext(t)); err != nil {
			t.Fatal(err)
		}
		owner := fixture.client.owner
		owner.mu.Lock()
		defer owner.mu.Unlock()
		if owner.tcp != 0 || owner.h3 != 0 || owner.retiring != 0 {
			t.Fatal("native quota not released")
		}
	})
}

type countedInput struct{ reads, closes atomic.Int64 }

func (input *countedInput) Read([]byte) (int, error) { input.reads.Add(1); return 0, io.EOF }
func (input *countedInput) Close() error             { input.closes.Add(1); return nil }

func TestProviderNoAdmissionNoEffect(t *testing.T) {
	var effects atomic.Int64
	endpoint, options := providerPeer(t, HTTP1Only, func(w http.ResponseWriter, request *http.Request) { effects.Add(1) })
	fixture := bindProvider(t, options, 1)
	input := &countedInput{}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	receipt, err := fixture.client.Do(canceled, testContext(t), providerID("canceled"), providerRequest(t, "POST", endpoint, input))
	if receipt != nil || !errors.Is(err, context.Canceled) || input.reads.Load() != 0 || input.closes.Load() != 0 || effects.Load() != 0 {
		t.Fatal("preadmission canceled request acquired input")
	}
	receipt, err = fixture.client.Do(testContext(t), testContext(t), providerID("first"), providerRequest(t, "GET", endpoint, nil))
	if err != nil {
		t.Fatal(err)
	}
	refused, err := fixture.client.Do(testContext(t), testContext(t), providerID("full"), providerRequest(t, "POST", endpoint, input))
	if refused != nil || !errors.Is(err, invocation.ErrEvidence) || input.reads.Load() != 0 || input.closes.Load() != 0 || effects.Load() != 1 {
		t.Fatal("saturated evidence allowed I/O", err)
	}
	settleProvider(t, fixture, receipt)
}
