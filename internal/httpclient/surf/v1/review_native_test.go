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
	"crypto/rand"
	"crypto/tls"
	"errors"
	"io"
	stdhttp "net/http"
	"strings"
	"sync/atomic"
	"testing"

	http "github.com/enetx/http"
	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/profiles/chrome"
	"github.com/frost-leo/fathomry/internal/fault"
	utls "github.com/refraction-networking/utls"
)

type reviewUTLSCache struct {
	utls.ClientSessionCache
	gets, puts atomic.Int32
}

func (cache *reviewUTLSCache) Get(key string) (*utls.ClientSessionState, bool) {
	cache.gets.Add(1)
	return cache.ClientSessionCache.Get(key)
}
func (cache *reviewUTLSCache) Put(key string, value *utls.ClientSessionState) {
	cache.puts.Add(1)
	cache.ClientSessionCache.Put(key, value)
}

type reviewTLSCache struct{ gets, puts atomic.Int32 }

func (cache *reviewTLSCache) Get(string) (*tls.ClientSessionState, bool) {
	cache.gets.Add(1)
	return nil, false
}
func (cache *reviewTLSCache) Put(string, *tls.ClientSessionState) { cache.puts.Add(1) }

type reviewRandom struct{ calls atomic.Int32 }

func (random *reviewRandom) Read(data []byte) (int, error) {
	random.calls.Add(1)
	return rand.Read(data)
}

func TestReviewJAExplicitCachePrecedenceAndRandomSource(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "enabled", true: "disabled"}[disabled], func(t *testing.T) {
			var resumed atomic.Int32
			peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				if r.TLS.DidResume {
					resumed.Add(1)
				}
				_, _ = io.WriteString(w, "ok")
			}, false)
			nativeCache := &reviewUTLSCache{ClientSessionCache: utls.NewLRUClientSessionCache(4)}
			goCache := &reviewTLSCache{}
			random := &reviewRandom{}
			profile := chrome.Desktop
			fix := newFixture(t, OptionsV1{Name: "explicit-cache", Mode: HTTP1Only, Native: NativeOptionsV1{
				Profile: &profile, TLSConfig: &tls.Config{RootCAs: peer.roots, ClientSessionCache: goCache},
				JAConfig: &utls.Config{RootCAs: peer.roots, ClientSessionCache: nativeCache, SessionTicketsDisabled: disabled,
					OmitEmptyPsk: true, PreferSkipResumptionOnNilExtension: true, Rand: random},
			}}, 2)
			for index := range 2 {
				input := request(t, "GET", peer.tcp.URL, "")
				input.Close = true
				receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: []string{"first", "second"}[index]}, input)
				if err != nil {
					t.Fatal(err)
				}
				if !settle(t, fix, receipt).Outcome.Value.Complete() {
					t.Fatal("native JA request incomplete")
				}
			}
			if goCache.gets.Load() != 0 || goCache.puts.Load() != 0 || random.calls.Load() == 0 {
				t.Fatal("explicit JA configuration did not take precedence")
			}
			if disabled {
				if nativeCache.gets.Load() != 0 || nativeCache.puts.Load() != 0 || resumed.Load() != 0 {
					t.Fatal("disabled session tickets were enabled")
				}
			} else if nativeCache.gets.Load() == 0 || nativeCache.puts.Load() == 0 || resumed.Load() == 0 {
				t.Fatal("explicit native cache was replaced or ignored")
			}
		})
	}
}

func TestReviewEarlyCleanupFieldsRemainIndependent(t *testing.T) {
	for _, phase := range []string{"request-view", "response-headers", "upgrade", "retry"} {
		t.Run(phase, func(t *testing.T) {
			cleanup := errors.New("synthetic early close")
			body := &reviewCloseReader{Reader: strings.NewReader("body"), cause: cleanup}
			options := OptionsV1{Name: "early-" + phase, Mode: HTTP1Only, MaxHeaderBytes: 1024}
			if phase == "request-view" {
				options.Native.RequestMiddleware = []func(*sdk.Request) error{func(view *sdk.Request) error {
					view.GetRequest().Body = body
					return nil
				}}
			}
			if phase == "retry" {
				options.NativeRetries = 1
				options.RetryCodes = []int{503}
			}
			fix := newFixture(t, options, 1)
			binding, err := fix.client.owner.binding(testContext(t), routeChoice{})
			if err != nil {
				t.Fatal(err)
			}
			binding.client.GetClient().Transport = routeTransport{raw: reviewRoundTrip(func(r *http.Request) (*http.Response, error) {
				if phase == "request-view" {
					t.Error("rejected body reached transport")
				}
				response := &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, ContentLength: 4, Request: r, Proto: "HTTP/1.1"}
				switch phase {
				case "response-headers":
					response.Header.Set("X-Too-Large", strings.Repeat("x", 2048))
				case "upgrade":
					response.StatusCode = 101
				case "retry":
					response.StatusCode = 503
				}
				return response, nil
			})}
			receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: phase}, request(t, "GET", "http://fixture.invalid", ""))
			value := settle(t, fix, receipt)
			if err == nil || !errors.Is(value.Outcome.Cleanup, cleanup) || body.calls.Load() != 1 || value.Outcome.Value.Complete() {
				t.Fatal("early cleanup lost its own field, close-once or failure", err)
			}
		})
	}
}
