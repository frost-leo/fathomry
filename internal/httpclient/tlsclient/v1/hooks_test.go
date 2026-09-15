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
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	nativehttp "github.com/bogdanfinn/fhttp"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
)

type hookPanicError struct{ formatted atomic.Int64 }

func (value *hookPanicError) Error() string {
	value.formatted.Add(1)
	return "hook-private-canary"
}

type hookPanicValue struct{ formatted atomic.Int64 }

func (value *hookPanicValue) String() string {
	value.formatted.Add(1)
	return "hook-private-canary"
}

func TestProviderHookPanicsRetainNativeContainment(t *testing.T) {
	for _, phase := range []string{"pre", "post"} {
		for _, kind := range []string{"error", "opaque", "nil", "continue"} {
			t.Run(phase+"/"+kind, func(t *testing.T) {
				nativeError := &hookPanicError{}
				opaque := &hookPanicValue{}
				var panicValue any
				var cause error
				switch kind {
				case "error":
					panicValue, cause = nativeError, nativeError
				case "opaque":
					panicValue = opaque
				case "continue":
					panicValue, cause = sdk.ErrContinueHooks, sdk.ErrContinueHooks
				}
				var seen, subsequent atomic.Int64
				endpoint, options := providerPeer(t, HTTP1Only, func(w http.ResponseWriter, r *http.Request) {
					seen.Add(1)
					_, _ = io.WriteString(w, "native-response")
				})
				if phase == "pre" {
					options.Native.PreHooks = []sdk.PreRequestHookFunc{
						func(*nativehttp.Request) error { panic(panicValue) },
						func(*nativehttp.Request) error { subsequent.Add(1); return nil },
					}
				} else {
					options.Native.PostHooks = []sdk.PostResponseHookFunc{
						func(*sdk.PostResponseContext) error { panic(panicValue) },
						func(*sdk.PostResponseContext) error { subsequent.Add(1); return nil },
					}
				}
				fixture := bindProvider(t, options, 1)
				receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("hook-panic"), providerRequest(t, "GET", endpoint, nil))
				result := settleProvider(t, fixture, receipt)
				var observed error
				if phase == "pre" {
					observed = result.Outcome.Primary
					if !errors.Is(err, ErrState) || !errors.Is(observed, ErrState) || result.Outcome.Value.Complete() || seen.Load() != 0 {
						t.Fatal("pre-hook panic did not abort with retained evidence")
					}
				} else {
					notices := result.Outcome.Value.HookErrorsCopy()
					if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "native-response" || seen.Load() != 1 ||
						len(notices) != 1 || !errors.Is(notices[0], ErrState) {
						t.Fatal("post-hook panic changed native response or lost its notice")
					}
					observed = notices[0]
				}
				if cause != nil && !errors.Is(observed, cause) {
					t.Fatal("panic error cause was lost")
				}
				if subsequent.Load() != 0 {
					t.Fatal("panic incorrectly continued native hook processing")
				}
				conformance.Private(t, observed, "hook-private-canary")
				if nativeError.formatted.Load() != 0 || opaque.formatted.Load() != 0 {
					t.Fatal("panic recovery invoked caller presentation code")
				}
				if err := fixture.assembly.Close(testContext(t)); err != nil {
					t.Fatal("panic retained callback ownership", err)
				}
			})
		}
	}
}

func TestProviderContinueHookNoticesDoNotAbort(t *testing.T) {
	for _, phase := range []string{"pre", "post"} {
		t.Run(phase, func(t *testing.T) {
			var subsequent atomic.Int64
			cause := errors.New("synthetic continue notice")
			notice := errors.Join(sdk.ErrContinueHooks, cause)
			endpoint, options := providerPeer(t, HTTP1Only, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "continued") })
			if phase == "pre" {
				options.Native.PreHooks = []sdk.PreRequestHookFunc{
					func(*nativehttp.Request) error { return notice },
					func(*nativehttp.Request) error { subsequent.Add(1); return nil },
				}
			} else {
				options.Native.PostHooks = []sdk.PostResponseHookFunc{
					func(*sdk.PostResponseContext) error { return notice },
					func(*sdk.PostResponseContext) error { subsequent.Add(1); return nil },
				}
			}
			fixture := bindProvider(t, options, 1)
			receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("continue"), providerRequest(t, "GET", endpoint, nil))
			if err != nil {
				t.Fatal(err)
			}
			result := settleProvider(t, fixture, receipt)
			notices := result.Outcome.Value.HookErrorsCopy()
			if result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "continued" || subsequent.Load() != 1 ||
				len(notices) != 1 || !errors.Is(notices[0], cause) || !errors.Is(notices[0], sdk.ErrContinueHooks) {
				t.Fatal("ordinary native continue-hook semantics changed")
			}
		})
	}
}

func TestProviderPostHookCapacityRefusalHasEvidence(t *testing.T) {
	const blockedCalls = 8
	entered, release := make(chan struct{}, blockedCalls), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	started, headers := make(chan struct{}, 1), make(chan struct{}, 1)
	var postCalls atomic.Int64
	endpoint, options := providerPeer(t, HTTP1Only, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Blocked") == "" {
			started <- struct{}{}
			select {
			case <-headers:
			case <-r.Context().Done():
				return
			}
		}
		_, _ = io.WriteString(w, "observed")
	})
	options.MaxActive = blockedCalls + 1
	options.MaxTCPConnections, options.MaxHTTP3Transports = 1, 1
	options.Native.PreHooks = []sdk.PreRequestHookFunc{func(request *nativehttp.Request) error {
		if request.Header.Get("X-Blocked") != "" {
			entered <- struct{}{}
			<-release
		}
		return nil
	}}
	options.Native.PostHooks = []sdk.PostResponseHookFunc{func(observed *sdk.PostResponseContext) error {
		if observed.Request.Header.Get("X-Blocked") == "" {
			postCalls.Add(1)
		}
		return nil
	}}
	fixture := bindProvider(t, options, blockedCalls+1)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	type outcome struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	first := make(chan outcome, 1)
	go func() {
		receipt, err := fixture.client.Do(ctx, ctx, providerID("first"), providerRequest(t, "GET", endpoint, nil))
		first <- outcome{receipt, err}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first native request did not reach peer")
	}
	remaining := make(chan outcome, blockedCalls)
	for index := range blockedCalls {
		go func() {
			request := providerRequest(t, "GET", endpoint, nil)
			request.Header.Set("X-Blocked", "yes")
			receipt, err := fixture.client.Do(ctx, ctx, providerID("blocked-"+strconv.Itoa(index)), request)
			remaining <- outcome{receipt, err}
		}()
	}
	for range blockedCalls {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("source callback ceiling not reached")
		}
	}
	headers <- struct{}{}
	var completed outcome
	select {
	case completed = <-first:
	case <-ctx.Done():
		t.Fatal("unblocked native response did not complete")
	}
	result := settleProvider(t, fixture, completed.receipt)
	notices := result.Outcome.Value.HookErrorsCopy()
	if completed.err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || postCalls.Load() != 0 ||
		len(notices) != 1 || !errors.Is(notices[0], ErrState) {
		t.Error("post-hook admission refusal was not separate retained evidence")
	}
	unblock()
	for range blockedCalls {
		select {
		case completed := <-remaining:
			if completed.receipt == nil {
				t.Error("blocking fixture lost accepted receipt")
			}
		case <-ctx.Done():
			t.Fatal("released hook did not terminate")
		}
	}
	for range blockedCalls {
		delivery, err := fixture.inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := delivery.Receipt().WaitReleased(ctx); err != nil {
			t.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.assembly.Close(testContext(t)); err != nil {
		t.Fatal("callback fixture did not release", err)
	}
}
