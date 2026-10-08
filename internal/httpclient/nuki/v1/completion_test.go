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
	"crypto/x509"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
	nativehttp "github.com/nukilabs/http"
	"github.com/nukilabs/http/cookiejar"
	sdk "github.com/nukilabs/tlsclient"
	nativetls "github.com/nukilabs/utls"
)

// The native HTTP client must see the final target before jar selection and redirect resolution.
func TestPreparedTargetCookieAndRedirectIdentity(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.URL.Path+"|"+r.Header.Get("Cookie")+"|"+r.Header.Get("Authorization"))
		mu.Unlock()
		http.SetCookie(w, &http.Cookie{Name: "issued", Value: "target", Path: "/"})
		if r.URL.Path == "/first" {
			http.Redirect(w, r, "/last", 302)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer peer.Close()
	origin, _ := url.Parse("http://original.invalid/")
	target, _ := url.Parse(peer.URL)
	jar, _ := cookiejar.New(nil)
	jar.SetCookies(origin, []*nativehttp.Cookie{{Name: "private", Value: "original"}})
	jar.SetCookies(target, []*nativehttp.Cookie{{Name: "target", Value: "correct"}})
	options := providerOptions()
	options.Mode = HTTP1Only
	options.FollowRedirects = true
	options.Native.Jar = jar
	options.Native.Before = []func(context.Context, *nativehttp.Request) error{func(_ context.Context, r *nativehttp.Request) error {
		if r.URL.Host == origin.Host {
			r.URL.Scheme, r.URL.Host, r.Host = target.Scheme, target.Host, target.Host
		}
		return nil
	}}
	fix := bindProvider(t, options)
	input := nativeRequest(t, "GET", origin.String()+"first", nil)
	input.Header.Set("Authorization", "Bearer origin")
	receipt, err := fix.client.Do(testContext(t), fault.Correlation{Call: "identity"}, input)
	if err != nil {
		t.Fatal(err)
	}
	result := outcome(t, fix, receipt)
	if result.Err() != nil || !result.Outcome.Value.Complete() || result.Outcome.Value.Metadata().URL() != peer.URL+"/last" {
		t.Fatal("effective target evidence", result.Err(), result.Outcome.Value.Metadata().URL())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || !strings.Contains(seen[0], "target=correct") || strings.Contains(strings.Join(seen, " "), "original") || !strings.HasPrefix(seen[1], "/last|") {
		t.Fatal("target/cookie/credential scope", seen)
	}
	for _, cookie := range jar.Cookies(origin) {
		if cookie.Name == "issued" {
			t.Fatal("target response cookie stored at original authority")
		}
	}
}

func TestUserinfoCredentialsFollowPreparedAuthority(t *testing.T) {
	for _, phase := range []string{"before", "redirect"} {
		for _, choice := range []string{"inherited", "replaced", "same"} {
			t.Run(phase+"/"+choice, func(t *testing.T) {
				observed := make(chan string, 4)
				target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					observed <- r.Header.Get("Authorization")
					_, _ = io.WriteString(w, "ok")
				}))
				defer target.Close()
				location, _ := url.Parse(target.URL)
				options := providerOptions()
				options.Mode = HTTP1Only
				options.FollowRedirects = true
				mutate := func(r *nativehttp.Request) {
					r.URL.Scheme, r.URL.Host, r.Host = location.Scheme, location.Host, location.Host
					if choice == "replaced" {
						r.URL.User = url.UserPassword("destination", "fixture")
					}
				}
				initial := "http://original.invalid/"
				if phase == "before" {
					options.Native.Before = []func(context.Context, *nativehttp.Request) error{func(_ context.Context, r *nativehttp.Request) error { mutate(r); return nil }}
					if choice == "same" {
						initial = target.URL
					}
				} else {
					first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/next", 302) }))
					defer first.Close()
					initial = first.URL
					options.Native.CheckRedirect = func(r *nativehttp.Request, _ []*nativehttp.Request) error { mutate(r); return nil }
					if choice == "same" {
						first.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							if r.URL.Path == "/" {
								http.Redirect(w, r, "/next", 302)
								return
							}
							observed <- r.Header.Get("Authorization")
							_, _ = io.WriteString(w, "ok")
						})
						options.Native.CheckRedirect = nil
					}
				}
				input := nativeRequest(t, "GET", initial, nil)
				input.URL.User = url.UserPassword("original", "fixture")
				fix := bindProvider(t, options)
				receipt, err := fix.client.Do(testContext(t), fault.Correlation{Call: "userinfo"}, input)
				if err != nil {
					t.Fatal(err)
				}
				result := outcome(t, fix, receipt)
				if result.Err() != nil || !result.Outcome.Value.Complete() {
					t.Fatal("userinfo request", result.Err())
				}
				want := ""
				if choice == "replaced" {
					want = "Basic " + base64.StdEncoding.EncodeToString([]byte("destination:fixture"))
				}
				if choice == "same" {
					want = "Basic " + base64.StdEncoding.EncodeToString([]byte("original:fixture"))
				}
				select {
				case got := <-observed:
					if got != want {
						t.Fatal("userinfo crossed prepared authority", phase, choice)
					}
				case <-testContext(t).Done():
					t.Fatal("target did not observe request")
				}
			})
		}
	}
}

func TestPreparedMetadataUsesFrozenLayeredNativeSelection(t *testing.T) {
	options := providerOptions()
	h2 := *options.Native.Profile.H2
	options.Native.Profile.H2 = &h2
	options.FollowRedirects = true
	var factories int
	factory := options.Native.Profile.ClientHelloSpec
	options.Native.Profile.ClientHelloSpec = func() *nativetls.ClientHelloSpec { factories++; return factory() }
	prepared, err := PrepareV1(options, resource.Layer{Kind: resource.Local, Content: []byte(`{"follow_redirects":false,"queued_calls":0,"max_response_bytes":131072,"max_proxy_tunnels":3}`)})
	if err != nil {
		t.Fatal(err)
	}
	metadata := prepared.Metadata()
	options.Native.Profile.H2.Settings = nil
	if factories != 0 || metadata.SourceBytes <= 0 || metadata.WorkBytes <= metadata.EvidenceBytes || metadata.MaxProxyTunnels != 3 {
		t.Fatal("offline prepared bounds", metadata)
	}
	selection := resource.WithLimits(prepared.Select(), metadata.Limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "prepared", selection)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(testContext(t))
	selected, _, err := resource.Bind(assembly, selection)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(selected.owner.budget, metadata) || selected.owner.settings.FollowRedirects || factories != 0 {
		t.Fatal("construction changed selection")
	}
}

func TestPreparationSeparatesOwnedSourceAndBorrowedCache(t *testing.T) {
	options := providerOptions()
	options.MaxRoutes = 1
	options.MaxConnections = 1
	options.MaxOrigins = 1
	options.MaxProxyTunnels = 1
	baseline, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	options.MaxProxyTunnels = 2
	tunnels, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	if tunnels.Metadata().SourceBytes <= baseline.Metadata().SourceBytes || tunnels.Metadata().WorkBytes != baseline.Metadata().WorkBytes {
		t.Fatal("virtual tunnels not source-accounted")
	}
	options.MaxProxyTunnels = 1
	options.Native.TLS = &nativetls.Config{ClientSessionCache: nativetls.NewLRUClientSessionCache(1)}
	borrowed, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	if baseline.Metadata().SourceBytes-borrowed.Metadata().SourceBytes < int64(sdk.FathomrySessionCacheCapacity)*(16<<20) || baseline.Metadata().WorkBytes != borrowed.Metadata().WorkBytes {
		t.Fatal("owned and borrowed session caches conflated")
	}
	options.MaxRoutes = 2
	routes, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	if routes.Metadata().SourceBytes <= borrowed.Metadata().SourceBytes {
		t.Fatal("route residence missing")
	}
}

func TestPreparedTrustPoolsRemainBorrowedWithFrozenSelection(t *testing.T) {
	peer := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {}))
	defer peer.Close()
	originRoots := testRoots(peer).RootCAs
	proxyRoots := x509.NewCertPool()
	options := providerOptions()
	options.Native.TLS = &nativetls.Config{RootCAs: originRoots, ClientCAs: originRoots}
	options.Native.ProxyTLS = &nativetls.Config{RootCAs: proxyRoots, ClientCAs: proxyRoots}
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.native.TLS == options.Native.TLS || prepared.native.ProxyTLS == options.Native.ProxyTLS ||
		prepared.native.TLS.RootCAs != originRoots || prepared.native.TLS.ClientCAs != originRoots ||
		prepared.native.ProxyTLS.RootCAs != proxyRoots || prepared.native.ProxyTLS.ClientCAs != proxyRoots {
		t.Fatal("trust authority was cloned, conflated or not frozen")
	}
	options.Native.TLS.RootCAs = proxyRoots
	options.Native.ProxyTLS.RootCAs = originRoots
	if prepared.native.TLS.RootCAs != originRoots || prepared.native.ProxyTLS.RootCAs != proxyRoots {
		t.Fatal("caller pointer replacement changed prepared trust")
	}
	if allocations := testing.AllocsPerRun(3, func() { _ = nativeDataBytes(prepared.native) }); allocations != 0 {
		t.Fatal("source accounting enumerated foreign trust state", allocations)
	}
	withoutPools := prepared.native
	origin, proxy := withoutPools.TLS.Clone(), withoutPools.ProxyTLS.Clone()
	origin.RootCAs, origin.ClientCAs, proxy.RootCAs, proxy.ClientCAs = nil, nil, nil, nil
	withoutPools.TLS, withoutPools.ProxyTLS = origin, proxy
	if nativeDataBytes(prepared.native) != nativeDataBytes(withoutPools) {
		t.Fatal("borrowed trust memory included in owned declaration")
	}
}

func TestConfigurationStringEnvelopePrecedesSerialization(t *testing.T) {
	for _, field := range []string{"mode", "proxy"} {
		t.Run(field, func(t *testing.T) {
			options := providerOptions()
			if field == "mode" {
				options.Mode = ProtocolMode(strings.Repeat("x", 65))
			} else {
				options.ProxyURL = strings.Repeat("x", 8193)
			}
			if err := ValidateDataV1(options); !errors.Is(err, ErrLimit) {
				t.Fatal("data envelope", err)
			}
			options.Native.Profile = nil
			if _, err := PrepareV1(options); !errors.Is(err, ErrLimit) {
				t.Fatal("native copy preceded data envelope", err)
			}
		})
	}
	if err := ValidateDataV1(OptionsV1{Name: "data-only"}); err != nil {
		t.Fatal("valid no-profile data control", err)
	}
}

func TestLazyProfileCeilingRefusesBeforeNativeDial(t *testing.T) {
	options := providerOptions()
	options.MaxProfileBytes = 1024
	var factories, dials int
	options.Native.Profile.ClientHelloSpec = func() *nativetls.ClientHelloSpec {
		factories++
		return &nativetls.ClientHelloSpec{Extensions: []nativetls.TLSExtension{&nativetls.GenericExtension{Id: 1234, Data: make([]byte, 2048)}}}
	}
	options.Native.DialContext = func(context.Context, string, string) (net.Conn, error) {
		dials++
		return nil, errors.New("unexpected dial")
	}
	fix := bindProvider(t, options)
	if factories != 0 || dials != 0 {
		t.Fatal("offline factory executed")
	}
	receipt, err := fix.client.Do(testContext(t), fault.Correlation{Call: "profile-bound"}, nativeRequest(t, "GET", "https://127.0.0.1:1", nil))
	if err != nil {
		t.Fatal(err)
	}
	result := outcome(t, fix, receipt)
	if factories != 1 || dials != 0 || !errors.Is(result.Err(), ErrLimit) || !errors.Is(result.Err(), sdk.ErrFathomryProfileLimit) {
		t.Fatal("profile bound", result.Err(), factories, dials)
	}
}

func TestLazyProfileBoundsPrecedeALPNTransform(t *testing.T) {
	for _, excessive := range []bool{false, true} {
		options := providerOptions()
		options.Mode = HTTP1Only
		alpn := &nativetls.ALPNExtension{AlpnProtocols: []string{"h2"}}
		count := 1
		if excessive {
			count = 257
		}
		extensions := make([]nativetls.TLSExtension, count)
		for index := range extensions {
			extensions[index] = alpn
		}
		options.Native.Profile.ClientHelloSpec = func() *nativetls.ClientHelloSpec { return &nativetls.ClientHelloSpec{Extensions: extensions} }
		own, err := newOwner(defaults(options), options.Native)
		if err != nil {
			t.Fatal(err)
		}
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_ = own.protectNative(own.native).Profile.ClientHelloSpec()
		}()
		if excessive {
			cause, ok := recovered.(error)
			if !ok || !errors.Is(cause, sdk.ErrFathomryProfileLimit) {
				t.Fatal("oversized lazy output not rejected", recovered)
			}
			if !reflect.DeepEqual(alpn.AlpnProtocols, []string{"h2"}) {
				t.Fatal("rejected lazy output was transformed before its count bound")
			}
		} else if recovered != nil || !reflect.DeepEqual(alpn.AlpnProtocols, []string{"http/1.1"}) {
			t.Fatal("valid native H1 ALPN transform changed", recovered, alpn.AlpnProtocols)
		}
		if released := own.release(testContext(t)); !released.Released {
			t.Fatal("profile fixture owner not released", released.Err)
		}
	}
}

func TestProfileCeilingIncludesControlledALPNRewrite(t *testing.T) {
	for _, excessive := range []bool{false, true} {
		options := providerOptions()
		options.Mode = HTTP1Only
		options.MaxProfileBytes = 1024
		options.Native.Profile.ClientHelloSpec = func() *nativetls.ClientHelloSpec {
			alpn := &nativetls.ALPNExtension{}
			padding := &nativetls.GenericExtension{Id: 1234}
			length := int(options.MaxProfileBytes) - 512 - alpn.Len() - padding.Len() - 9
			if excessive {
				length++
			}
			padding.Data = make([]byte, length)
			return &nativetls.ClientHelloSpec{Extensions: []nativetls.TLSExtension{alpn, padding}}
		}
		own, err := newOwner(defaults(options), options.Native)
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer func() {
				cause := recover()
				if excessive {
					err, ok := cause.(error)
					if !ok || !errors.Is(err, sdk.ErrFathomryProfileLimit) {
						t.Fatal("controlled rewrite exceeded ceiling", cause)
					}
				} else if cause != nil {
					t.Fatal("exact controlled rewrite rejected", cause)
				}
			}()
			spec := own.protectNative(own.native).Profile.ClientHelloSpec()
			if excessive {
				t.Fatal("one-over rewritten profile accepted")
			}
			if err := sdk.ValidateFathomryProfile(spec, 1024); err != nil {
				t.Fatal("exact rewritten profile", err)
			}
		}()
		if result := own.release(testContext(t)); !result.Released || result.Err != nil {
			t.Fatal("profile callback retained source", result.Err)
		}
	}
}

func TestRequestExtensionsBoundBeforeSnapshot(t *testing.T) {
	input := nativeRequest(t, "GET", "http://127.0.0.1:1/", nil)
	input.Priority = nativehttp.PriorityLowest
	input.DiscardResponseCookies = true
	input.ExcludedCookies = map[string]struct{}{"Cookie Name Exact": {}}
	input.SetPathValue("unused", strings.Repeat("x", 8192))
	copied, err := SnapshotRequestV1(context.Background(), input, RequestPolicyV1{1024, 1024})
	if err != nil || copied.Priority != input.Priority || !copied.DiscardResponseCookies || copied.PathValue("unused") != "" {
		t.Fatal("native snapshot", err)
	}
	delete(input.ExcludedCookies, "Cookie Name Exact")
	if _, ok := copied.ExcludedCookies["Cookie Name Exact"]; !ok {
		t.Fatal("exclusions alias caller")
	}
	input.ExcludedCookies = map[string]struct{}{strings.Repeat("x", 1<<20): {}}
	if err := ValidateRequestV1(context.Background(), input); !errors.Is(err, ErrLimit) {
		t.Fatal("unbounded preflight", err)
	}
	input.ExcludedCookies = nil
	input.Form = url.Values{"server": {strings.Repeat("x", 8192)}}
	if _, err := SnapshotRequestV1(context.Background(), input, RequestPolicyV1{1024, 1024}); !errors.Is(err, ErrUnsupported) {
		t.Fatal("server state", err)
	}
}

func TestConsumeReturnInterruptsEnteredReadBeforeEOFInspection(t *testing.T) {
	peerEntered := make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "8")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		close(peerEntered)
		<-r.Context().Done()
	}))
	defer peer.Close()
	options := providerOptions()
	options.Mode = HTTP1Only
	options.Timeout = 3 * time.Second
	fix := bindProvider(t, options)
	var saved *Response
	readDone := make(chan struct{})
	receipt, err := fix.client.Consume(testContext(t), fault.Correlation{Call: "entered-read"}, nativeRequest(t, "GET", peer.URL, nil),
		func(_ context.Context, response *Response) error {
			saved = response
			go func() { defer close(readDone); _, _ = response.Read(make([]byte, 1)) }()
			<-peerEntered
			deadline := time.Now().Add(time.Second)
			for {
				if !response.reader.budget.mu.TryLock() {
					break
				}
				response.reader.budget.mu.Unlock()
				if time.Now().After(deadline) {
					t.Error("read did not enter")
					break
				}
				time.Sleep(time.Millisecond)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := receipt.WaitReleased(ctx); err != nil {
		t.Fatal("callback return left entered Read waiting for source timeout", err)
	}
	<-readDone
	result := outcome(t, fix, receipt)
	if result.Outcome.Value.Complete() {
		t.Fatal("interrupted response became complete")
	}
	if _, err := saved.Read(make([]byte, 1)); !errors.Is(err, ErrState) {
		t.Fatal("callback response retained authority", err)
	}
}

func TestRedirectErrorRetainsAvailableResponse(t *testing.T) {
	cause := errors.New("redirect policy")
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Observed", "yes")
		http.Redirect(w, r, "/next", 302)
	}))
	defer peer.Close()
	options := providerOptions()
	options.Mode = HTTP1Only
	options.FollowRedirects = true
	options.Native.CheckRedirect = func(*nativehttp.Request, []*nativehttp.Request) error { return cause }
	fix := bindProvider(t, options)
	receipt, err := fix.client.Do(testContext(t), fault.Correlation{Call: "redirect-error"}, nativeRequest(t, "GET", peer.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	result := outcome(t, fix, receipt)
	if !errors.Is(result.Err(), cause) || !result.Outcome.Present || result.Outcome.Value.Metadata().StatusCode() != 302 || result.Outcome.Value.Complete() {
		t.Fatal("partial redirect response lost", result.Err())
	}
}
