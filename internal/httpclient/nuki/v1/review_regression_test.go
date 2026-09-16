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

package nuki

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/nukilabs/http/http2"
	sdk "github.com/nukilabs/tlsclient"
	nativetls "github.com/nukilabs/utls"
)

func TestProviderHandledStreamReadFailureRemainsRequiredEvidence(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Length", "10")
		_, _ = io.WriteString(writer, "short")
	}))
	defer peer.Close()
	fixture := bindProvider(t, providerOptions())
	var observed error
	receipt, err := fixture.client.Consume(testContext(t), fault.Correlation{Call: "handled-read"}, nativeRequest(t, "GET", peer.URL, nil), func(_ context.Context, response *Response) error { _, observed = io.ReadAll(response); return nil })
	if err != nil {
		t.Fatal(err)
	}
	result := outcome(t, fixture, receipt)
	if !errors.Is(observed, io.ErrUnexpectedEOF) || !errors.Is(result.Err(), io.ErrUnexpectedEOF) || result.Outcome.Value.Complete() {
		t.Fatal("handled native read error disappeared", result.Err())
	}
}

func TestProviderBodylessRepresentationAndMissingEncodedEntity(t *testing.T) {
	for _, test := range []struct {
		method, encoding string
		status           int
		length           string
		bodyless         bool
	}{
		{"HEAD", "gzip", 200, "123", true}, {"HEAD", "deflate", 200, "123", true},
		{"GET", "deflate", 204, "", true}, {"GET", "deflate", 304, "", true}, {"GET", "gzip", 200, "0", false},
	} {
		t.Run(test.method+"-"+test.encoding+"-"+test.length, func(t *testing.T) {
			peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Encoding", test.encoding)
				if test.length != "" {
					writer.Header().Set("Content-Length", test.length)
				}
				writer.WriteHeader(test.status)
			}))
			defer peer.Close()
			fixture := bindProvider(t, providerOptions())
			receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "representation"}, nativeRequest(t, test.method, peer.URL, nil))
			if err != nil {
				t.Fatal(err)
			}
			result := outcome(t, fixture, receipt)
			value := result.Outcome.Value
			if test.bodyless {
				if result.Err() != nil || !value.Complete() || value.Metadata().Uncompressed() {
					t.Fatal("bodyless representation was decoded", result.Err())
				}
				if test.method == "HEAD" && value.Metadata().ContentLength() != 123 {
					t.Fatal("HEAD representation length lost")
				}
			} else if result.Err() == nil || value.Complete() {
				t.Fatal("missing gzip header/trailer certified complete")
			}
		})
	}
}

type observedSessionCache struct {
	native nativetls.ClientSessionCache
	reads  atomic.Int32
}

func (cache *observedSessionCache) Get(key string) (*nativetls.ClientSessionState, bool) {
	cache.reads.Add(1)
	return cache.native.Get(key)
}
func (cache *observedSessionCache) Put(key string, state *nativetls.ClientSessionState) {
	cache.native.Put(key, state)
}
func TestProviderNativeSessionCacheAndServerNameAreEffective(t *testing.T) {
	seen := make(chan string, 2)
	peer := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		seen <- request.TLS.ServerName
		_, _ = io.WriteString(writer, "native")
	}))
	defer peer.Close()
	cache := &observedSessionCache{native: nativetls.NewLRUClientSessionCache(4)}
	options := providerOptions()
	options.Mode = HTTP1Only
	options.Native.TLS = &nativetls.Config{InsecureSkipVerify: true, ServerName: "requested.invalid", ClientSessionCache: cache}
	options.Native.Transport = &sdk.TransportOptions{DisableKeepAlives: true}
	fixture := bindProvider(t, options)
	for range 2 {
		receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "native-options"}, nativeRequest(t, "GET", peer.URL, nil))
		if err != nil {
			t.Fatal(err)
		}
		if result := outcome(t, fixture, receipt); result.Err() != nil {
			t.Fatal(result.Err())
		}
		if actual := <-seen; actual != "requested.invalid" {
			t.Fatal("explicit SNI was discarded")
		}
	}
	if cache.reads.Load() == 0 {
		t.Fatal("supplied session cache was not used")
	}
}

func TestProviderPinsUseCanonicalDestinationIdentity(t *testing.T) {
	peer := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { writer.WriteHeader(204) }))
	defer peer.Close()
	for _, host := range []string{"xn--tst-qla.invalid:443", "täst.invalid:443", "TÄST.invalid。:00443"} {
		options := providerOptions()
		options.Mode = HTTP1Only
		options.Native.TLS = &nativetls.Config{InsecureSkipVerify: true}
		options.Native.Pinner = sdk.NewPinner(false)
		options.Native.Pinner.AddPins(host, []string{base64.StdEncoding.EncodeToString(make([]byte, 32))})
		options.Native.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, peer.Listener.Addr().String())
		}
		fixture := bindProvider(t, options)
		receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "pin"}, nativeRequest(t, "GET", "https://täst.invalid", nil))
		if err != nil {
			t.Fatal(err)
		}
		if result := outcome(t, fixture, receipt); !errors.Is(result.Err(), sdk.ErrCertificatePinningFailed) {
			t.Fatal("equivalent hostname bypassed pin", result.Err())
		}
	}
	first, err := canonicalEndpoint("[0:0:0:0:0:0:0:1]:00443")
	if err != nil {
		t.Fatal(err)
	}
	second, err := canonicalEndpoint("[::1]:443")
	if err != nil || first != second {
		t.Fatal("IPv6 aliases differ", err)
	}
	options := providerOptions()
	options.Native.Pinner = sdk.NewPinner(false)
	options.Native.Pinner.AddPins("täst.invalid:443", []string{base64.StdEncoding.EncodeToString(make([]byte, 32))})
	options.Native.Pinner.AddPins("xn--tst-qla.invalid.:443", []string{base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 32)))})
	if _, err := Select(options); !errors.Is(err, ErrInput) {
		t.Fatal("conflicting canonical pins accepted", err)
	}
}

func TestProviderRejectsUnboundedHPACKTable(t *testing.T) {
	options := providerOptions()
	profile := *options.Native.Profile
	h2 := *profile.H2
	h2.Settings = append([]http2.Setting(nil), h2.Settings...)
	h2.Settings = append(h2.Settings, http2.Setting{ID: http2.SettingHeaderTableSize, Val: ^uint32(0)})
	profile.H2 = &h2
	options.Native.Profile = &profile
	if _, err := Select(options); !errors.Is(err, ErrLimit) {
		t.Fatal("unbounded persistent HPACK memory accepted", err)
	}
}

type inputWithUnrelatedUploadMethod struct {
	io.Reader
	calls atomic.Int32
}

func (input *inputWithUnrelatedUploadMethod) Close() error { return nil }
func (input *inputWithUnrelatedUploadMethod) UploadError() error {
	input.calls.Add(1)
	return errors.New("unrelated-method-error")
}

func TestProviderUploadDiagnosticsDoNotInvokeInputMethods(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(request.Body)
		if err != nil || string(data) != "input" {
			t.Error("request input changed", err)
		}
		_, _ = io.WriteString(writer, "accepted")
	}))
	defer peer.Close()
	fixture := bindProvider(t, providerOptions())
	input := &inputWithUnrelatedUploadMethod{Reader: strings.NewReader("input")}
	receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "diagnostic-scope"}, nativeRequest(t, "POST", peer.URL, input))
	if err != nil {
		t.Fatal(err)
	}
	result := outcome(t, fixture, receipt)
	if result.Err() != nil || !result.Outcome.Value.Complete() || input.calls.Load() != 0 || len(result.Outcome.Value.InputErrorsCopy()) != 0 {
		t.Fatal("undocumented input callback or invented failure evidence", result.Err())
	}
}
