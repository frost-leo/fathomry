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
	"net"
	"strings"
	"testing"
	"time"

	nativehttp "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/fhttp/http2"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestProviderNativeSnapshotAndCustomProfile(t *testing.T) {
	options := providerOptions()
	id := options.Native.Profile.GetClientHelloId()
	id.Client = "external-custom-profile"
	original := *options.Native.Profile
	profile := profiles.NewClientProfile(id, original.GetSettings(), original.GetSettingsOrder(), original.GetPseudoHeaderOrder(), original.GetConnectionFlow(), original.GetPriorities(), original.GetHeaderPriority(),
		original.GetStreamID(), original.GetAllowHTTP(), original.GetHttp3Settings(), original.GetHttp3SettingsOrder(), original.GetHttp3PriorityParam(), original.GetHttp3PseudoHeaderOrder(), original.GetHttp3SendGreaseFrames())
	copied, err := copyNative(NativeOptionsV1{Profile: &profile})
	if err != nil {
		t.Fatal(err)
	}
	options.Native = copied
	options.Native.DefaultHeaders = nativehttp.Header{"Cookie": {"external-secret"}}
	limits, err := LimitsV1(options)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	options.Native.Profile.GetSettings()[http2.SettingHeaderTableSize] = 17
	options.Native.DefaultHeaders.Set("Cookie", "changed")
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "first", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(testContext(t))
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	if source.owner.native.Profile.GetClientHelloId().Client != "external-custom-profile" || source.owner.native.Profile.GetSettings()[http2.SettingHeaderTableSize] == 17 ||
		source.owner.native.DefaultHeaders.Get("Cookie") != "external-secret" {
		t.Fatal("native containers aliased or custom profile refused")
	}
	second, err := resource.Assemble(testContext(t), testContext(t), "second", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(testContext(t))
	other, _, err := resource.Bind(second, selected)
	if err != nil {
		t.Fatal(err)
	}
	if source.owner == other.owner || source.owner.native.Profile == other.owner.native.Profile {
		t.Fatal("reused selection shared an owning native source")
	}
}
func TestProviderInvalidOptionsAndIgnoredNativeSettings(t *testing.T) {
	cases := map[string]func(*OptionsV1){
		"profile":       func(o *OptionsV1) { o.Native.Profile = nil },
		"version":       func(o *OptionsV1) { o.Version = 2 },
		"mode":          func(o *OptionsV1) { o.Mode = "invented" },
		"active":        func(o *OptionsV1) { o.MaxActive = -1 },
		"queue":         func(o *OptionsV1) { o.QueuedCalls = -1 },
		"timeout":       func(o *OptionsV1) { o.Timeout = time.Microsecond },
		"header-parser": func(o *OptionsV1) { o.Native.Transport = &sdk.TransportOptions{MaxResponseHeaderBytes: -1} },
		"native-idle": func(o *OptionsV1) {
			d := time.Duration(0)
			o.Native.Transport = &sdk.TransportOptions{IdleConnTimeout: &d}
		},
		"h2-unbounded": func(o *OptionsV1) {
			n, _ := copyNative(o.Native)
			o.Native = n
			o.Native.Profile.GetSettings()[http2.SettingMaxHeaderListSize] = 0xffffffff
		},
		"proxy-port": func(o *OptionsV1) { o.ProxyURL = "http://localhost:65536" },
		"proxy-conflict": func(o *OptionsV1) {
			o.ProxyURL = "http://localhost:8080"
			o.Native.DialContext = func(context.Context, string, string) (net.Conn, error) { return nil, nil }
		},
		"h3-tcp-dial": func(o *OptionsV1) {
			o.Mode = HTTP3Racing
			o.Native.DialContext = func(context.Context, string, string) (net.Conn, error) { return nil, nil }
		},
		"h3-ip-filter": func(o *OptionsV1) { o.Mode = HTTP3Racing; o.DisableIPV6 = true },
		"h3-pins": func(o *OptionsV1) {
			o.Mode = HTTP3Racing
			o.Native.CertificatePins = map[string][]string{"localhost": {"synthetic"}}
		},
		"h3-tcp-proxy": func(o *OptionsV1) { o.Mode = HTTP3Racing; o.ProxyURL = "http://localhost:8080" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			options := providerOptions()
			mutate(&options)
			if _, err := Select(options); err == nil {
				t.Fatal("invalid or ignored native option silently accepted")
			}
			if _, err := LimitsV1(options); err == nil {
				t.Fatal("limits accepted invalid settings")
			}
		})
	}
}
func TestProviderLayeringAndRoutingValidation(t *testing.T) {
	options := providerOptions()
	fixture := bindProvider(t, options, 1, resource.Layer{Kind: resource.Local, Content: []byte(`{"routing_locked":true}`)})
	if !fixture.client.owner.settings.RoutingLocked {
		t.Fatal("layer was ignored")
	}
	if _, err := fixture.client.route(context.Background(), []RequestOptionsV1{{Proxy: ProxyDirect}}); !errors.Is(err, ErrInput) {
		t.Fatal("locked routing overridden", err)
	}
	for _, url := range []string{"http://localhost:0", "http://localhost:-1", "http://localhost:99999", "http://localhost:80/path", "http://localhost:80?", "socks5://localhost"} {
		if _, err := parseProxy(url, Negotiated); err == nil {
			t.Fatal("invalid proxy accepted", url)
		}
	}
	if _, err := parseProxy("socks4://user:pass@localhost:1080", Negotiated); !errors.Is(err, ErrUnsupported) {
		t.Fatal("ignored native SOCKS4 credentials accepted", err)
	}
	if connectHeadersFit(nativehttp.Header{"Proxy-Authorization": {"one"}, "proxy-authorization": {"two"}}, 1024) {
		t.Fatal("ambiguous CONNECT overlay accepted")
	}
	address, err := parseProxy("http://[::1]", Negotiated)
	if err != nil || address.Host != "[::1]:80" {
		t.Fatal("IPv6 proxy authority differs", err)
	}
	if _, err := Select(options, resource.Layer{Kind: resource.Local, Content: []byte(`{"unknown":true}`)}); err == nil {
		t.Fatal("unknown plain setting accepted")
	}
}
func FuzzProviderRequestMetadata(f *testing.F) {
	f.Add("GET", "http://localhost:8080/path", "X-Input", "value")
	f.Add("bad method", "http://localhost:0/", "X-Invalid\r", "bad\n")
	f.Fuzz(func(t *testing.T, method, endpoint, key, value string) {
		if len(method)+len(endpoint)+len(key)+len(value) > 1<<20 {
			return
		}
		request, err := nativehttp.NewRequest("GET", endpoint, nil)
		if err != nil {
			return
		}
		request.Method = method
		request.Header = nativehttp.Header{key: {value}}
		if err := validateRequest(context.Background(), request, defaults(providerOptions())); err == nil {
			copy := copyRequest(request, context.Background())
			copy.Header.Set("X-Copy", "changed")
			if strings.Contains(request.Header.Get("X-Copy"), "changed") {
				t.Fatal("request clone aliases metadata")
			}
		}
	})
}
