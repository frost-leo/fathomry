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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/fhttp/cookiejar"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/frost-leo/fathomry/internal/invocation"
)

type controlledObservation struct {
	Peer, Method, Path, Host, Authorization, Body string
	Cookies                                       map[string]string
}

type controlledOrigins struct {
	addresses map[string]*url.URL
	dials     map[string]string
	mu        sync.Mutex
	seen      []controlledObservation
	location  string
	status    int
}

func controlledNewOrigins(t *testing.T) *controlledOrigins {
	t.Helper()
	peers := &controlledOrigins{addresses: make(map[string]*url.URL), dials: make(map[string]string)}
	for _, name := range []string{"a", "b"} {
		peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			body, err := io.ReadAll(io.LimitReader(request.Body, 1<<20))
			if err != nil {
				http.Error(writer, "fixture request read failed", http.StatusBadRequest)
				return
			}
			observation := controlledObservation{Peer: name, Method: request.Method, Path: request.URL.Path, Host: request.Host,
				Authorization: request.Header.Get("Authorization"), Body: string(body), Cookies: make(map[string]string)}
			for _, cookie := range request.Cookies() {
				observation.Cookies[cookie.Name] = cookie.Value
			}
			peers.mu.Lock()
			peers.seen = append(peers.seen, observation)
			location, status := peers.location, peers.status
			peers.mu.Unlock()
			if request.URL.Path == "/start" {
				writer.Header().Set("Location", location)
				writer.WriteHeader(status)
				return
			}
			http.SetCookie(writer, &http.Cookie{Name: "response_" + name, Value: "received", Path: "/"})
			writer.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(writer).Encode(observation)
		}))
		t.Cleanup(peer.Close)
		parsed, err := url.Parse(peer.URL)
		if err != nil {
			t.Fatal(err)
		}
		physical := parsed.Host
		parsed.Host = net.JoinHostPort("origin-"+name+".invalid", parsed.Port())
		peers.addresses[name] = parsed
		peers.dials[parsed.Host] = physical
	}
	return peers
}

func (peers *controlledOrigins) endpoint(name, path string) *url.URL {
	copy := *peers.addresses[name]
	copy.Path = path
	return &copy
}

func (peers *controlledOrigins) dial(ctx context.Context, network, address string) (net.Conn, error) {
	physical, present := peers.dials[address]
	if !present {
		return nil, errors.New("fixture refuses non-loopback route")
	}
	return (&net.Dialer{}).DialContext(ctx, network, physical)
}

func (peers *controlledOrigins) observations() []controlledObservation {
	peers.mu.Lock()
	defer peers.mu.Unlock()
	return append([]controlledObservation(nil), peers.seen...)
}

func controlledJar(t *testing.T, peers *controlledOrigins) *cookiejar.Jar {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		jar.SetCookies(peers.addresses[name], []*fhttp.Cookie{{Name: "jar_origin", Value: name, Path: "/"}})
	}
	return jar
}

func controlledHasCookie(jar *cookiejar.Jar, address *url.URL, name string) bool {
	for _, cookie := range jar.Cookies(address) {
		if cookie.Name == name {
			return true
		}
	}
	return false
}

func controlledAssertCookieDestination(t *testing.T, peers *controlledOrigins, jar *cookiejar.Jar, target string) {
	t.Helper()
	for _, name := range []string{"a", "b"} {
		present := controlledHasCookie(jar, peers.addresses[name], "response_"+target)
		if present != (name == target) {
			t.Errorf("response cookie destination differs: target=%s jar=%s present=%t", target, name, present)
		}
	}
}

func controlledPerform(t *testing.T, driver string, options OptionsV1, request *fhttp.Request, perHopNative bool) controlledObservation {
	t.Helper()
	var payload []byte
	if driver == "internal" {
		originalURL, originalHeader := request.URL.String(), request.Header.Clone()
		fixture := bindProvider(t, options, 1)
		receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("controlled-evidence"), request)
		if err != nil {
			t.Fatal("Internal request failed", err)
		}
		result := settleProvider(t, fixture, receipt)
		if result.Err() != nil || !result.Outcome.Value.Complete() {
			t.Fatal("Internal result incomplete", result.Err())
		}
		if request.URL.String() != originalURL || !reflect.DeepEqual(request.Header, originalHeader) {
			t.Fatal("Internal hooks mutated caller request metadata")
		}
		payload = result.Outcome.Value.DataCopy()
	} else {
		configured := []sdk.HttpClientOption{sdk.WithClientProfile(*options.Native.Profile), sdk.WithForceHttp1(), sdk.WithDisableHttp3(),
			sdk.WithTimeoutSeconds(5), sdk.WithDialContext(options.Native.DialContext), sdk.WithCookieJar(options.Native.Jar)}
		for _, hook := range options.Native.PreHooks {
			configured = append(configured, sdk.WithPreHook(hook))
		}
		if options.Native.CheckRedirect != nil || perHopNative {
			configured = append(configured, sdk.WithCustomRedirectFunc(func(request *fhttp.Request, previous []*fhttp.Request) error {
				if options.Native.CheckRedirect != nil {
					if err := options.Native.CheckRedirect(request, previous); err != nil {
						return err
					}
				}
				if perHopNative {
					for _, hook := range options.Native.PreHooks {
						if err := hook(request); err != nil {
							return err
						}
					}
				}
				return nil
			}))
		}
		client, err := sdk.NewHttpClient(sdk.NewNoopLogger(), configured...)
		if err != nil {
			t.Fatal("native client construction", err)
		}
		t.Cleanup(func() {
			if err := sdk.Close(client); err != nil {
				t.Error("native client cleanup", err)
			}
		})
		response, err := client.Do(request.WithContext(testContext(t)))
		if err != nil {
			t.Fatal("native request", err)
		}
		payload, err = io.ReadAll(response.Body)
		if closeErr := response.Body.Close(); err != nil || closeErr != nil {
			t.Fatal("native body cleanup", err, closeErr)
		}
	}
	var observation controlledObservation
	if err := json.Unmarshal(payload, &observation); err != nil {
		t.Fatal("independent peer result", err)
	}
	return observation
}

func TestInitialCredentialScopeMatrix(t *testing.T) {
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("fixture-user:fixture-password"))
	for _, test := range []struct {
		name                                    string
		rewrite, userinfo, preserveUser, caller bool
		hookCredentials, hostOnly               bool
	}{
		{name: "unchanged-jar"},
		{name: "rewritten-jar", rewrite: true},
		{name: "unchanged-userinfo", userinfo: true},
		{name: "rewritten-drop-userinfo", rewrite: true, userinfo: true},
		{name: "rewritten-retain-explicit-userinfo", rewrite: true, userinfo: true, preserveUser: true},
		{name: "unchanged-caller-headers", caller: true},
		{name: "rewritten-caller-headers-native-parity", rewrite: true, caller: true},
		{name: "rewritten-explicit-hook-credentials", rewrite: true, caller: true, hookCredentials: true},
		{name: "host-only-edit-does-not-change-url-jar", hostOnly: true},
	} {
		for _, driver := range []string{"native", "internal"} {
			t.Run(test.name+"/"+driver, func(t *testing.T) {
				peers := controlledNewOrigins(t)
				jar := controlledJar(t, peers)
				options := providerOptions()
				options.Mode, options.Native.Jar, options.Native.DialContext = HTTP1Only, jar, peers.dial
				var hooks atomic.Int64
				options.Native.PreHooks = []sdk.PreRequestHookFunc{func(request *fhttp.Request) error {
					hooks.Add(1)
					if driver == "internal" && (request.Body != nil || request.GetBody != nil || request.Response != nil) {
						return errors.New("owning request authority escaped into a hook")
					}
					if test.rewrite {
						target := peers.endpoint("b", "/final")
						if test.preserveUser {
							target.User = request.URL.User
						}
						request.URL, request.Host = target, target.Host
					}
					if test.hostOnly {
						request.Host = peers.addresses["b"].Host
					}
					if test.hookCredentials {
						request.Header.Set("Authorization", "Bearer hook-approved")
						request.Header.Set("Cookie", "hook_cookie=approved")
					}
					return nil
				}}
				initial := peers.endpoint("a", "/final")
				if test.userinfo {
					initial.User = url.UserPassword("fixture-user", "fixture-password")
				}
				request := providerRequest(t, "PATCH", initial.String(), strings.NewReader("initial-payload"))
				if test.caller {
					request.Header.Set("Authorization", "Bearer caller-explicit")
					request.Header.Set("Cookie", "caller_cookie=explicit")
				}
				observation := controlledPerform(t, driver, options, request, false)
				target := "a"
				if test.rewrite {
					target = "b"
				}
				wantAuth := ""
				if test.userinfo && (!test.rewrite || test.preserveUser) {
					wantAuth = basic
				}
				if test.caller {
					wantAuth = "Bearer caller-explicit"
				}
				if test.hookCredentials {
					wantAuth = "Bearer hook-approved"
				}
				if observation.Peer != target || observation.Cookies["jar_origin"] != target || observation.Authorization != wantAuth ||
					observation.Method != "PATCH" || observation.Body != "initial-payload" || hooks.Load() != 1 || len(peers.observations()) != 1 {
					t.Errorf("initial credential scope differs: observation=%+v target=%s auth-match=%t hooks=%d", observation, target, observation.Authorization == wantAuth, hooks.Load())
				}
				if test.caller && !test.hookCredentials && observation.Cookies["caller_cookie"] != "explicit" ||
					test.hookCredentials && (observation.Cookies["hook_cookie"] != "approved" || observation.Cookies["caller_cookie"] != "") {
					t.Error("explicit credential authority was changed")
				}
				if test.hostOnly && observation.Host != peers.addresses["b"].Host {
					t.Error("legal Host edit was removed")
				}
				controlledAssertCookieDestination(t, peers, jar, target)
				t.Logf("peer=%s jar-origin=%s caller-cookie=%t hook-cookie=%t auth-present=%t response-cookie-at=%s", observation.Peer,
					observation.Cookies["jar_origin"], observation.Cookies["caller_cookie"] != "", observation.Cookies["hook_cookie"] != "", observation.Authorization != "", target)
			})
		}
	}
}

type controlledReplayBody struct {
	*strings.Reader
	closed, bytes atomic.Int64
}

func (body *controlledReplayBody) Read(output []byte) (int, error) {
	count, err := body.Reader.Read(output)
	body.bytes.Add(int64(count))
	return count, err
}

func (body *controlledReplayBody) Close() error { body.closed.Add(1); return nil }

func TestRedirectCredentialScopeMatrix(t *testing.T) {
	for _, status := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		for _, test := range []struct {
			name, location, rewrite string
			credentials             string
			lowercase               bool
		}{
			{name: "same-origin", location: "a"},
			{name: "cross-origin", location: "b"},
			{name: "redirect-callback-rewrite", location: "a", rewrite: "callback"},
			{name: "per-hop-prehook-rewrite", location: "a", rewrite: "prehook"},
			{name: "explicit-prehook-credentials-after-rewrite", location: "a", rewrite: "prehook", credentials: "pre-change"},
			{name: "explicit-callback-credentials-after-rewrite", location: "a", rewrite: "callback", credentials: "callback-change"},
			{name: "same-value-prehook-reassertion-after-rewrite", location: "a", rewrite: "prehook", credentials: "pre-same"},
			{name: "same-value-callback-reassertion-after-rewrite", location: "a", rewrite: "callback", credentials: "callback-same"},
			{name: "explicit-prehook-credentials-on-cross-origin-redirect", location: "b", credentials: "pre-change"},
			{name: "lowercase-same-origin", location: "a", lowercase: true},
			{name: "lowercase-redirect-callback-rewrite", location: "a", rewrite: "callback", lowercase: true},
			{name: "lowercase-per-hop-prehook-rewrite", location: "a", rewrite: "prehook", lowercase: true},
		} {
			for _, driver := range []string{"native-per-hop-composition", "internal"} {
				t.Run(fmt.Sprintf("%d/%s/%s", status, test.name, driver), func(t *testing.T) {
					peers := controlledNewOrigins(t)
					peers.mu.Lock()
					peers.location, peers.status = peers.endpoint(test.location, "/redirect").String(), status
					peers.mu.Unlock()
					jar := controlledJar(t, peers)
					options := providerOptions()
					options.Mode, options.Native.Jar, options.Native.DialContext = HTTP1Only, jar, peers.dial
					var events []string
					var eventMu sync.Mutex
					event := func(value string) { eventMu.Lock(); events = append(events, value); eventMu.Unlock() }
					rewrite := func(request *fhttp.Request) {
						target := peers.endpoint("b", "/final")
						request.URL, request.Host = target, target.Host
					}
					options.Native.CheckRedirect = func(request *fhttp.Request, previous []*fhttp.Request) error {
						event("redirect")
						if len(previous) != 1 {
							return errors.New("unexpected redirect history")
						}
						if test.rewrite == "callback" {
							rewrite(request)
						}
						if test.credentials == "callback-change" {
							request.Header.Set("Authorization", "Bearer hook-approved")
							request.Header.Set("Cookie", "hook_cookie=approved")
						}
						if test.credentials == "callback-same" {
							request.Header.Set("Authorization", "Bearer caller-explicit")
							request.Header.Set("Cookie", "caller_cookie=explicit")
						}
						return nil
					}
					options.Native.PreHooks = []sdk.PreRequestHookFunc{func(request *fhttp.Request) error {
						event("pre")
						if driver == "internal" && (request.Body != nil || request.GetBody != nil || request.Response != nil) {
							return errors.New("owning input escaped through per-hop hook")
						}
						if request.URL.Path != "/start" && test.rewrite == "prehook" {
							rewrite(request)
						}
						if request.URL.Path != "/start" && test.credentials == "pre-change" {
							request.Header.Set("Authorization", "Bearer hook-approved")
							request.Header.Set("Cookie", "hook_cookie=approved")
						}
						if request.URL.Path != "/start" && test.credentials == "pre-same" {
							request.Header.Set("Authorization", "Bearer caller-explicit")
							request.Header.Set("Cookie", "caller_cookie=explicit")
						}
						return nil
					}}
					initial := &controlledReplayBody{Reader: strings.NewReader("redirect-payload")}
					inputs := []*controlledReplayBody{initial}
					var inputMu sync.Mutex
					request := providerRequest(t, "POST", peers.endpoint("a", "/start").String(), initial)
					request.ContentLength = int64(len("redirect-payload"))
					request.GetBody = func() (io.ReadCloser, error) {
						copy := &controlledReplayBody{Reader: strings.NewReader("redirect-payload")}
						inputMu.Lock()
						inputs = append(inputs, copy)
						inputMu.Unlock()
						return copy, nil
					}
					request.Header.Set("Authorization", "Bearer caller-explicit")
					request.Header.Set("Cookie", "caller_cookie=explicit")
					if test.lowercase {
						request.Header = fhttp.Header{"authorization": {"Bearer caller-explicit"}, "cookie": {"caller_cookie=explicit"}}
					}
					observation := controlledPerform(t, driver, options, request, true)
					target := test.location
					if test.rewrite != "" {
						target = "b"
					}
					if observation.Peer != target || observation.Cookies["jar_origin"] != target || len(peers.observations()) != 2 {
						t.Errorf("redirect jar origin differs: %+v target=%s", observation, target)
					}
					wantMethod, wantBody := "GET", ""
					if status == http.StatusTemporaryRedirect {
						wantMethod, wantBody = "POST", "redirect-payload"
					}
					if observation.Method != wantMethod || observation.Body != wantBody {
						t.Error("native redirect method/replay behavior changed")
					}
					changed := strings.HasSuffix(test.credentials, "-change")
					if changed {
						if observation.Authorization != "Bearer hook-approved" || observation.Cookies["hook_cookie"] != "approved" || observation.Cookies["caller_cookie"] != "" {
							t.Error("explicit per-hop credential edits were removed or inherited cookies remained")
						}
					} else {
						wantAuth, wantCookie := "", ""
						if test.location == "a" && (driver != "internal" || test.rewrite == "") {
							wantAuth, wantCookie = "Bearer caller-explicit", "explicit"
						}
						if observation.Authorization != wantAuth || observation.Cookies["caller_cookie"] != wantCookie {
							t.Errorf("final-origin inherited credential policy differs: driver=%s rewrite=%s edit=%s inherited-auth=%t inherited-cookie=%t",
								driver, test.rewrite, test.credentials, observation.Authorization == "Bearer caller-explicit", observation.Cookies["caller_cookie"] != "")
						}
					}
					controlledAssertCookieDestination(t, peers, jar, target)
					eventMu.Lock()
					if !reflect.DeepEqual(events, []string{"pre", "redirect", "pre"}) {
						t.Errorf("hook ordering changed: %v", events)
					}
					eventMu.Unlock()
					inputMu.Lock()
					wantInputs := 1
					if status == http.StatusTemporaryRedirect {
						wantInputs = 2
						if driver == "internal" {
							wantInputs = 3
						}
					}
					if len(inputs) != wantInputs {
						t.Errorf("replay-reader count differs: got=%d want=%d", len(inputs), wantInputs)
					}
					var inputBytes int64
					for _, input := range inputs {
						inputBytes += input.bytes.Load()
						if input.closed.Load() == 0 || driver == "internal" && input.closed.Load() != 1 {
							t.Error("input cleanup lost or duplicated Internal ownership")
						}
					}
					wantBytes := int64(len("redirect-payload"))
					if status == http.StatusTemporaryRedirect {
						wantBytes *= 2
					}
					if inputBytes != wantBytes {
						t.Errorf("replay bytes differ: got=%d want=%d", inputBytes, wantBytes)
					}
					inputMu.Unlock()
					t.Logf("peer=%s jar-origin=%s inherited-caller-cookie=%t hook-cookie=%t inherited-caller-auth=%t hook-auth=%t", observation.Peer,
						observation.Cookies["jar_origin"], observation.Cookies["caller_cookie"] != "", observation.Cookies["hook_cookie"] != "",
						observation.Authorization == "Bearer caller-explicit", observation.Authorization == "Bearer hook-approved")
				})
			}
		}
	}
}

func TestMovedPreHookCancellationRetainsOwnedInput(t *testing.T) {
	peers := controlledNewOrigins(t)
	options := providerOptions()
	options.Mode, options.Native.DialContext = HTTP1Only, peers.dial
	entered, resume := make(chan struct{}), make(chan struct{})
	resumeHook := sync.OnceFunc(func() { close(resume) })
	defer resumeHook()
	sentinel := errors.New("fixture late hook error")
	options.Native.PreHooks = []sdk.PreRequestHookFunc{func(request *fhttp.Request) error {
		if request.Body != nil || request.GetBody != nil || request.Response != nil {
			return errors.New("moved pre-hook exposes owning request input")
		}
		close(entered)
		<-resume
		return sentinel
	}}
	fixture := bindProvider(t, options, 1)
	input := &controlledReplayBody{Reader: strings.NewReader("owned-input")}
	request := providerRequest(t, "POST", peers.endpoint("a", "/final").String(), input)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan *invocation.Receipt[Result], 1)
	go func() {
		receipt, _ := fixture.client.Do(ctx, context.Background(), providerID("controlled-hook-cancel"), request)
		done <- receipt
	}()
	select {
	case <-entered:
	case <-testContext(t).Done():
		t.Fatal("pre-hook was not entered")
	}
	cancel()
	var receipt *invocation.Receipt[Result]
	select {
	case receipt = <-done:
	case <-testContext(t).Done():
		t.Fatal("canceled header waiter did not return")
	}
	if receipt == nil {
		t.Fatal("accepted hook lost its receipt")
	}
	short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if _, err := receipt.WaitReleased(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("noncooperative hook was released prematurely", err)
	}
	if len(peers.observations()) != 0 {
		t.Fatal("canceled pre-dispatch hook sent a request")
	}
	resumeHook()
	result := settleProvider(t, fixture, receipt)
	if !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), sentinel) || result.Outcome.Value.Complete() || input.closed.Load() != 1 || len(peers.observations()) != 0 {
		t.Fatal("late hook failure lost cancellation, cleanup or no-dispatch facts", result.Err())
	}
}
