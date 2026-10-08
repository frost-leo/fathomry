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
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/enetx/g"
	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/profiles"
	"github.com/enetx/surf/profiles/chrome"
	"github.com/frost-leo/fathomry/internal/fault"
	utls "github.com/refraction-networking/utls"
	peerh2 "golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func TestH2CInternalAuthorityAndRedirect(t *testing.T) {
	var requests, dials atomic.Int64
	peer := httptest.NewUnstartedServer(nil)
	peer.Config.Handler = h2c.NewHandler(stdhttp.HandlerFunc(func(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
		requests.Add(1)
		if request.URL.Path == "/redirect" {
			writer.Header().Set("Location", "https://"+request.Host+"/target")
			writer.WriteHeader(302)
			return
		}
		_, _ = io.WriteString(writer, request.Proto)
	}), &peerh2.Server{})
	peer.Start()
	t.Cleanup(peer.Close)
	profile := chrome.Desktop
	profile.HelloSpec, profile.HelloID, profile.ShuffleExtensions = nil, utls.ClientHelloID{}, false
	options := OptionsV1{Name: "owned-h2c", Mode: H2C, Native: NativeOptionsV1{Profile: &profile,
		ProxyTLSConfig: &tls.Config{},
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}}}
	fixture := newFixture(t, options, 1)
	receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "normal"}, request(t, "GET", peer.URL, ""))
	if err != nil {
		t.Fatal(err)
	}
	normal := settle(t, fixture, receipt)
	if !normal.Outcome.Value.Complete() || string(normal.Outcome.Value.DataCopy()) != "HTTP/2.0" || dials.Load() != 1 || requests.Load() != 1 {
		t.Fatal("controlled Internal h2c control failed")
	}
	refused, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "https"}, request(t, "GET", strings.Replace(peer.URL, "http:", "https:", 1), ""))
	if err == nil || refused != nil || dials.Load() != 1 || requests.Load() != 1 {
		t.Fatal("h2c accepted an HTTPS initial target", err)
	}
	receipt, err = fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "redirect"}, request(t, "GET", peer.URL+"/redirect", ""))
	if err == nil || receipt == nil {
		t.Fatal("h2c redirect did not produce a rejecting receipt", err)
	}
	redirected := settle(t, fixture, receipt)
	if redirected.Outcome.Value.Complete() || requests.Load() != 2 || dials.Load() != 1 || redirected.Outcome.Value.Metadata().StatusCode() != 302 {
		t.Fatal("h2c HTTPS redirect was dispatched or lost original response metadata")
	}
}

func TestH2CRejectsOnlyActualOriginTLS(t *testing.T) {
	for _, selection := range []string{"std-tls", "ja-tls", "hello-factory", "hello-spec", "hello-id", "shuffle"} {
		t.Run(selection, func(t *testing.T) {
			var calls atomic.Int64
			profile := chrome.Desktop
			profile.HelloSpec, profile.HelloID, profile.ShuffleExtensions = nil, utls.ClientHelloID{}, false
			options := OptionsV1{Name: "h2c-rejection", Mode: H2C, Native: NativeOptionsV1{Profile: &profile}}
			switch selection {
			case "std-tls":
				options.Native.TLSConfig = &tls.Config{}
			case "ja-tls":
				options.Native.JAConfig = &utls.Config{}
			case "hello-factory":
				options.Native.HelloSpecFactory = func(context.Context) (utls.ClientHelloSpec, error) { calls.Add(1); return utls.ClientHelloSpec{}, nil }
			case "hello-spec":
				profile.HelloSpec = &utls.ClientHelloSpec{}
			case "hello-id":
				profile.HelloID = utls.HelloGolang
			case "shuffle":
				profile.ShuffleExtensions = true
			}
			if _, err := PrepareV1(options); err == nil || calls.Load() != 0 {
				t.Fatal("actual origin TLS was ignored or eagerly invoked", err)
			}
		})
	}
}

func TestLazyProfileBoundsBeforeNativeDial(t *testing.T) {
	for _, selection := range []string{"stream", "headers"} {
		t.Run(selection, func(t *testing.T) {
			var callbacks, dials atomic.Int64
			profile := chrome.Desktop
			profile.HelloSpec, profile.HelloID, profile.ShuffleExtensions = nil, utls.ClientHelloID{}, false
			if selection == "stream" {
				profile.ConfigureH2 = func(config profiles.H2Config) { callbacks.Add(1); config.InitialWindowSize(8<<20 + 1) }
			} else {
				profile.BuildHeaders = func(profiles.OSKey) *g.MapOrd[g.String, g.String] {
					callbacks.Add(1)
					headers := g.NewMapOrd[g.String, g.String]()
					headers.Insert("X-Large", g.String(strings.Repeat("x", 65537)))
					return &headers
				}
			}
			options := OptionsV1{Name: "profile-limit", Mode: H2C, Native: NativeOptionsV1{Profile: &profile,
				DialContext: func(context.Context, string, string) (net.Conn, error) {
					dials.Add(1)
					return nil, errors.New("unexpected native dial")
				}}}
			fixture := newFixture(t, options, 1)
			if callbacks.Load() != 0 {
				t.Fatal("offline selection invoked profile output")
			}
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "lazy-limit"}, request(t, "GET", "http://127.0.0.1:1/", ""))
			if !errors.Is(err, sdk.ErrFathomryProfileLimit) || receipt == nil {
				t.Fatal("lazy output limit/cause not preserved", err)
			}
			result := settle(t, fixture, receipt)
			if callbacks.Load() != 1 || dials.Load() != 0 || result.Outcome.Value.Complete() || !errors.Is(result.Err(), sdk.ErrFathomryProfileLimit) {
				t.Fatal("oversized lazy profile installed or released incorrectly")
			}
		})
	}
}

func TestCustomHTTP2StreamCeilingHasExactWireBoundary(t *testing.T) {
	const ceiling = 4 << 20
	for _, excessive := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact", true: "one-over"}[excessive], func(t *testing.T) {
			endpoint := "http://127.0.0.1:1"
			var observed <-chan independentH2Result
			if !excessive {
				endpoint, observed = independentH2Peer(t, 1)
			}
			var callbacks, dials atomic.Int64
			profile := chrome.Desktop
			profile.HelloSpec, profile.HelloID, profile.ShuffleExtensions = nil, utls.ClientHelloID{}, false
			profile.ConfigureH2 = func(config profiles.H2Config) {
				callbacks.Add(1)
				window := uint32(ceiling)
				if excessive {
					window++
				}
				config.InitialWindowSize(window)
			}
			fixture := newFixture(t, OptionsV1{Name: "exact-stream-limit", Mode: H2C, MaxHTTP2StreamBytes: ceiling,
				Native: NativeOptionsV1{Profile: &profile, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
					dials.Add(1)
					return (&net.Dialer{}).DialContext(ctx, network, address)
				}}}, 1)
			if callbacks.Load() != 0 || dials.Load() != 0 {
				t.Fatal("offline preparation acquired lazy profile authority")
			}
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "stream-limit"}, request(t, "GET", endpoint, ""))
			result := settle(t, fixture, receipt)
			if excessive {
				if !errors.Is(err, sdk.ErrFathomryProfileLimit) || dials.Load() != 0 || result.Outcome.Value.Complete() {
					t.Fatal("one-over custom stream limit reached native dialing", err)
				}
				return
			}
			if err != nil || !result.Outcome.Value.Complete() || dials.Load() != 1 {
				t.Fatal("exact custom stream limit refused", err)
			}
			select {
			case wire := <-observed:
				if wire.err != nil || wire.window != ceiling || !wire.processed {
					t.Fatal("accepted stream limit differs on the wire", wire)
				}
			case <-testContext(t).Done():
				t.Fatal("independent H2 observation did not finish")
			}
		})
	}
}
