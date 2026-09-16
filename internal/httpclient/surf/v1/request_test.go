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
	"errors"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/enetx/http"
	"github.com/enetx/http/cookiejar"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestNativeRetriesCookiesAndRuntimeInputRemainDistinct(t *testing.T) {
	var calls atomic.Int32
	peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil || string(data) != "body" {
			t.Error("retry changed native body", err)
		}
		if calls.Add(1) == 1 {
			stdhttp.SetCookie(w, &stdhttp.Cookie{Name: "retry", Value: "yes", Path: "/"})
			w.WriteHeader(503)
			_, _ = io.WriteString(w, "retry")
			return
		}
		cookie, err := r.Cookie("retry")
		if err != nil || cookie.Value != "yes" {
			t.Error("native jar not retained between native attempts")
		}
		_, _ = io.WriteString(w, "final")
	}))
	defer peer.Close()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	fix := newFixture(t, OptionsV1{Name: "retry", Mode: HTTP1Only, NativeRetries: 1, RetryCodes: []int{503}, Native: NativeOptionsV1{Jar: jar}}, 1)
	receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "retry"}, request(t, "POST", peer.URL, "body"))
	if err != nil {
		t.Fatal(err)
	}
	value := settle(t, fix, receipt)
	if !value.Outcome.Value.Complete() || string(value.Outcome.Value.DataCopy()) != "final" || calls.Load() != 2 || value.Attempts.Exact ||
		value.Attempts.Observed != 2 || value.Outcome.Value.RoundTrips() != 2 {
		t.Fatal("native retries became false exact-attempt or result evidence")
	}
}
func TestNativeRedirectReplaysOriginalFactoryAndRetainsCause(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "replay", true: "factory-error"}[fail], func(t *testing.T) {
			var calls, replays atomic.Int32
			cause := errors.New("replay factory")
			peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				calls.Add(1)
				data, _ := io.ReadAll(r.Body)
				if string(data) != "body" {
					t.Error("replay body changed")
				}
				if r.URL.Path == "/" {
					w.Header().Set("Location", "/next")
					w.WriteHeader(307)
					return
				}
				if r.Header.Get("X-Redirect") != "retained" {
					t.Error("native redirect extension disappeared")
				}
				_, _ = w.Write(data)
			}))
			defer peer.Close()
			options := OptionsV1{Name: "redirect", Mode: HTTP1Only, Native: NativeOptionsV1{CheckRedirect: func(r *http.Request, via []*http.Request) error {
				if r.Body != nil || r.GetBody != nil || r.Response != nil {
					t.Error("redirect callback has owning handles")
				}
				r.Header.Set("X-Redirect", "retained")
				return nil
			}}}
			fix := newFixture(t, options, 1)
			input := request(t, "POST", peer.URL, "body")
			input.GetBody = func() (io.ReadCloser, error) {
				replays.Add(1)
				if fail {
					return nil, cause
				}
				return io.NopCloser(strings.NewReader("body")), nil
			}
			receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "redirect"}, input)
			value := settle(t, fix, receipt)
			if replays.Load() != 1 {
				t.Fatal("caller replay factory replaced or duplicated")
			}
			if fail {
				if !errors.Is(value.Err(), cause) || err == nil || calls.Load() != 1 {
					t.Fatal("factory cause or remote-count evidence changed")
				}
			} else if err != nil || !value.Outcome.Value.Complete() || calls.Load() != 2 || string(value.Outcome.Value.DataCopy()) != "body" {
				t.Fatal("native redirect replay failed", err)
			}
		})
	}
}
func TestNativeRetryAfterCannotExtendCallerBudget(t *testing.T) {
	var calls atomic.Int32
	peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(503)
	}))
	defer peer.Close()
	fix := newFixture(t, OptionsV1{Name: "delay", Mode: HTTP1Only, NativeRetries: 1}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	receipt, err := fix.client.Do(ctx, testContext(t), fault.Correlation{Call: "delay"}, request(t, "GET", peer.URL, ""))
	if err == nil {
		t.Fatal("retry delay escaped caller lifetime")
	}
	value := settle(t, fix, receipt)
	if !errors.Is(value.Err(), context.DeadlineExceeded) || calls.Load() != 1 || value.Outcome.Value.Complete() {
		t.Fatal("retry cancellation fabricated completion or another attempt")
	}
}
func TestVersionLayerAndNativeInputBoundaries(t *testing.T) {
	for _, version := range []uint32{2, 99} {
		if _, err := Select(OptionsV1{Name: "version", Version: version}); err == nil {
			t.Fatal("unknown configuration version accepted")
		}
	}
	options := OptionsV1{Name: "layers", MaxActive: 2}
	selected, err := Select(options, resource.Layer{Kind: resource.Base, Content: []byte("max_active: 1\n")})
	if err != nil {
		t.Fatal(err)
	}
	limits, err := LimitsV1(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "layers", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(testContext(t))
	other := newFixture(t, OptionsV1{Name: "other"}, 1)
	if _, err := Bind(assembly, selected, other.inbox, nil); err == nil {
		t.Fatal("binding enlarged layered limit")
	}
	if _, err := Select(options, resource.Layer{Kind: resource.Base, Content: []byte("unknown: true\n")}); err == nil {
		t.Fatal("unknown field ignored")
	}
	fix := newFixture(t, OptionsV1{Name: "runtime", Mode: HTTP1Only}, 1)
	input := request(t, "GET", "http://127.0.0.1:1", "")
	if receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "runtime"}, input, RequestOptionsV1{Version: 2}); receipt != nil || err == nil {
		t.Fatal("unknown runtime input admitted")
	}
}
