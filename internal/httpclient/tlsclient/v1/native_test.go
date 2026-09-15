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
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	nativehttp "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/fhttp/cookiejar"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	"github.com/tam7t/hpkp"
)

func TestProviderNativeProfileFamiliesAndAnonymousFactory(t *testing.T) {
	for name, profile := range map[string]profiles.ClientProfile{"chrome": profiles.Chrome_150, "firefox": profiles.Firefox_148, "safari": profiles.Safari_16_0} {
		t.Run(name, func(t *testing.T) {
			endpoint, options := providerPeer(t, Negotiated, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "profile") })
			options.Native.Profile = &profile
			fixture := bindProvider(t, options, 1)
			receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("profile"), providerRequest(t, "GET", endpoint, nil))
			if err != nil {
				t.Fatal("native profile family refused", err)
			}
			result := settleProvider(t, fixture, receipt)
			if result.Outcome.Value.Metadata().Protocol() != "HTTP/2.0" || string(result.Outcome.Value.DataCopy()) != "profile" {
				t.Fatal("native profile operation differs")
			}
		})
	}
	t.Run("anonymous-factory", func(t *testing.T) {
		endpoint, options := providerPeer(t, Negotiated, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "custom") })
		profile := *options.Native.Profile
		id := profile.GetClientHelloId()
		id.Client, id.Version = "", ""
		custom := profiles.NewClientProfile(id, profile.GetSettings(), profile.GetSettingsOrder(), profile.GetPseudoHeaderOrder(), profile.GetConnectionFlow(), profile.GetPriorities(), profile.GetHeaderPriority(),
			profile.GetStreamID(), profile.GetAllowHTTP(), profile.GetHttp3Settings(), profile.GetHttp3SettingsOrder(), profile.GetHttp3PriorityParam(), profile.GetHttp3PseudoHeaderOrder(), profile.GetHttp3SendGreaseFrames())
		options.Native.Profile = &custom
		fixture := bindProvider(t, options, 1)
		receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("custom"), providerRequest(t, "GET", endpoint, nil))
		if err != nil {
			t.Fatal("native factory required a catalog identity", err)
		}
		settleProvider(t, fixture, receipt)
	})
}

func TestProviderCertificatePinsAreInstanceLocal(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "pinned") }))
	defer server.Close()
	endpoint, base := server.URL, providerOptions()
	base.Mode = HTTP1Only
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	base.Native.Transport = &sdk.TransportOptions{RootCAs: roots}
	parsed, _ := url.Parse(endpoint)
	good := base
	good.Name = "good-pins"
	good.Native.CertificatePins = map[string][]string{parsed.Hostname(): {hpkp.Fingerprint(server.Certificate())}}
	valid := bindProvider(t, good, 2)
	bad := base
	bad.Name = "bad-pins"
	bad.Native.CertificatePins = map[string][]string{parsed.Hostname(): {"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}}
	invalid := bindProvider(t, bad, 1)
	first, err := valid.client.Do(testContext(t), testContext(t), providerID("first"), providerRequest(t, "GET", endpoint, nil))
	if err != nil {
		t.Fatal("valid native pin refused", err)
	}
	settleProvider(t, valid, first)
	failed, err := invalid.client.Do(testContext(t), testContext(t), providerID("bad"), providerRequest(t, "GET", endpoint, nil))
	if !errors.Is(err, sdk.ErrBadPinDetected) {
		t.Fatal("bad native pin accepted", err)
	}
	settleProvider(t, invalid, failed)
	// A new route binding forces a new pinner/handshake on the original source.
	next, err := valid.client.Do(testContext(t), testContext(t), providerID("next"), providerRequest(t, "GET", endpoint, nil), RequestOptionsV1{ConnectHeaders: nativehttp.Header{"X-Binding": {"two"}}})
	if err != nil {
		t.Fatal("other instance overwrote native pin", err)
	}
	settleProvider(t, valid, next)
	again, err := invalid.client.Do(testContext(t), testContext(t), providerID("bad-again"), providerRequest(t, "GET", endpoint, nil))
	if !errors.Is(err, sdk.ErrBadPinDetected) {
		t.Fatal("other source's valid pin made bad instance pass", err)
	}
	settleProvider(t, invalid, again)
}
func TestProviderRedirectJarAndMetadataHooks(t *testing.T) {
	var seen atomic.Int64
	endpoint, options := providerPeer(t, Negotiated, func(w http.ResponseWriter, r *http.Request) {
		seen.Add(1)
		if r.URL.Path == "/start" {
			http.SetCookie(w, &http.Cookie{Name: "native", Value: "caller-jar", Path: "/"})
			http.Redirect(w, r, "/end", http.StatusTemporaryRedirect)
			return
		}
		data, _ := io.ReadAll(r.Body)
		if r.Header.Get("X-Hook") != "yes" || !strings.Contains(r.Header.Get("Cookie"), "native=caller-jar") {
			t.Error("native hook/jar input lost")
		}
		_, _ = w.Write(data)
	})
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	options.Native.Jar = jar
	var before, after atomic.Int64
	notice := errors.New("synthetic post notice")
	options.Native.PreHooks = []sdk.PreRequestHookFunc{func(r *nativehttp.Request) error {
		before.Add(1)
		if r.Body != nil || r.GetBody != nil || r.Response != nil || r.TLS != nil {
			t.Error("native hook obtained owning handles")
		}
		r.Header.Set("X-Hook", "yes")
		return nil
	}}
	options.Native.PostHooks = []sdk.PostResponseHookFunc{func(observed *sdk.PostResponseContext) error {
		after.Add(1)
		if observed.Response != nil && (observed.Response.Body != nil || observed.Response.TLS != nil) {
			t.Error("post hook obtained native handles")
		}
		return notice
	}}
	fixture := bindProvider(t, options, 1)
	receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("redirect"), providerRequest(t, "POST", endpoint+"/start", strings.NewReader("replayed")))
	if err != nil {
		t.Fatal(err)
	}
	result := settleProvider(t, fixture, receipt)
	data := result.Outcome.Value
	if string(data.DataCopy()) != "replayed" || data.Exchanges() != 2 || data.RequestBytesRead() != 16 || seen.Load() != 2 || before.Load() != 2 || after.Load() != 2 ||
		len(data.HookErrorsCopy()) != 2 || !errors.Is(data.HookErrorsCopy()[0], notice) {
		t.Fatal("redirect/replay/native hook semantics differ", result.Err(), data.RequestBytesRead())
	}
}
func TestProviderNativeOwningHookAndAliasedReplayAreRefused(t *testing.T) {
	var sent atomic.Int64
	endpoint, options := providerPeer(t, HTTP1Only, func(w http.ResponseWriter, r *http.Request) { sent.Add(1) })
	options.Native.PreHooks = []sdk.PreRequestHookFunc{func(r *nativehttp.Request) error { r.Body = io.NopCloser(strings.NewReader("hidden")); return nil }}
	fixture := bindProvider(t, options, 1)
	receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("bad-hook"), providerRequest(t, "GET", endpoint, nil))
	if !errors.Is(err, ErrUnsupported) || sent.Load() != 0 {
		t.Fatal("owning hook bypassed controlled request", err)
	}
	settleProvider(t, fixture, receipt)
	racing := providerOptions()
	racing.Mode = HTTP3Racing
	other := bindProvider(t, racing, 1)
	body := &countedInput{}
	request := providerRequest(t, "POST", "https://127.0.0.1:1", body)
	request.GetBody = func() (io.ReadCloser, error) { return body, nil }
	receipt, err = other.client.Do(context.Background(), testContext(t), providerID("alias"), request)
	if !errors.Is(err, ErrInput) || body.reads.Load() != 0 {
		t.Fatal("racing reused original consuming reader", err)
	}
	settleProvider(t, other, receipt)
	if body.closes.Load() != 1 {
		t.Fatal("aliased replay caused duplicate input cleanup")
	}
}
