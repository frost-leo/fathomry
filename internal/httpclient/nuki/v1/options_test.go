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
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
	nativehttp "github.com/nukilabs/http"
	"github.com/nukilabs/tlsclient/profiles"
	nativetls "github.com/nukilabs/utls"
)

func TestProviderConfigurationVersionsLayersAndFactoryOwnership(t *testing.T) {
	options := providerOptions()
	var calls atomic.Int32
	original := options.Native.Profile.ClientHelloSpec
	options.Native.Profile.ClientHelloSpec = func() *nativetls.ClientHelloSpec { calls.Add(1); return original() }
	if _, err := Select(options); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("preparation invoked runtime factory")
	}
	invalid := options
	invalid.Version = 2
	if _, err := Select(invalid); err == nil {
		t.Fatal("unknown configuration version accepted")
	}
	missing := options
	missing.Native.Profile = nil
	if _, err := Select(missing); !errors.Is(err, ErrInput) {
		t.Fatal("implicit default profile selected", err)
	}
	for _, data := range []string{"max_active: 0", "unknown: true", "timeout_ns: -1", "max_origins: 0"} {
		if _, err := Select(options, resource.Layer{Kind: resource.Base, Content: []byte(data)}); err == nil {
			t.Fatal("invalid layer accepted")
		}
	}
	cause := errors.New("synthetic-profile-factory")
	options.Native.Profile.ClientHelloSpec = func() *nativetls.ClientHelloSpec { panic(cause) }
	fixture := bindProvider(t, options)
	receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "factory"}, nativeRequest(t, "GET", "http://127.0.0.1:1", nil))
	if err != nil {
		t.Fatal(err)
	}
	result := outcome(t, fixture, receipt)
	if !errors.Is(result.Err(), cause) || result.Attempts.Observed != 0 {
		t.Fatal("native factory failure lost", result.Err())
	}
}

func TestProviderRuntimeGuardsAndFacade(t *testing.T) {
	fixture := bindProvider(t, providerOptions())
	conformance.Facade(t, fixture.client, "Do", "Consume", "Profile", "Assess", "EvidenceBytes", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
	source, _, err := resource.Bind(fixture.assembly, fixture.selected)
	if err != nil {
		t.Fatal(err)
	}
	conformance.Facade(t, source, "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
	value := Result{data: &resultData{body: []byte("private-payload"), retained: true, metadata: Metadata{present: true, url: "https://private-endpoint.invalid"}}}
	conformance.Runtime(t, value, &Result{}, "private-payload", "private-endpoint")
	conformance.Runtime(t, value.Metadata(), &Metadata{}, "private-endpoint")
	options := providerOptions()
	options.ProxyURL = "http://user:private-credential@127.0.0.1:99"
	conformance.Runtime(t, options, &OptionsV1{}, "private-credential")
	cause := errors.New("private-native-cause")
	err = failure(ErrTransport, "request", cause)
	if !errors.Is(err, cause) || strings.Contains(fmt.Sprintf("%+v", err), "private-native-cause") {
		t.Fatal("causal privacy contract changed")
	}
}

func TestProviderH3WarmupIsSourceOwnedAndJoined(t *testing.T) {
	peer := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Alt-Svc", "h3=\":443\"; ma=60")
		writer.WriteHeader(204)
	}))
	defer peer.Close()
	packet, err := net.ListenPacket("udp", peer.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer packet.Close()
	opened := make(chan struct{}, 1)
	options := providerOptions()
	options.Mode = Native
	options.Native.TLS = testRoots(peer)
	options.Native.ListenPacket = func(ctx context.Context, network, address string) (net.PacketConn, error) {
		conn, err := (&net.ListenConfig{}).ListenPacket(ctx, network, ":0")
		if err == nil {
			opened <- struct{}{}
		}
		return conn, err
	}
	fixture := bindProvider(t, options)
	receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "warmup"}, nativeRequest(t, "GET", peer.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	if result := outcome(t, fixture, receipt); result.Err() != nil {
		t.Fatal(result.Err())
	}
	select {
	case <-opened:
	case <-testContext(t).Done():
		t.Fatal("native warming was not exercised")
	}
	closing, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fixture.assembly.Close(closing); err != nil {
		t.Fatal("warming survived terminal Close", err)
	}
	fixture.client.owner.mu.Lock()
	sockets := len(fixture.client.owner.sockets)
	fixture.client.owner.mu.Unlock()
	if sockets != 0 {
		t.Fatal("warmup retained physical sockets")
	}
}

func TestProviderCopiedNativeContainersRemainIndependent(t *testing.T) {
	options := providerOptions()
	profile := profiles.Chrome152
	profile.PseudoHeaderOrder = append([]string(nil), profile.PseudoHeaderOrder...)
	options.Native.Profile = &profile
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	profile.PseudoHeaderOrder[0] = "mutated"
	limits, _ := LimitsV1(options)
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "copy", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(testContext(t))
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	if source.owner.native.Profile.PseudoHeaderOrder[0] == "mutated" {
		t.Fatal("native container alias survived preparation")
	}
}

func FuzzProviderRequestMetadata(f *testing.F) {
	f.Add("X-Input", "value")
	f.Add("Content-Length", "invalid")
	f.Fuzz(func(t *testing.T, name, value string) {
		if len(name) > 2048 || len(value) > 8192 {
			return
		}
		err := validateHeaders(nativehttp.Header{name: {value}}, 4096)
		if err != nil && strings.Contains(fmt.Sprint(err), value) && len(value) > 32 {
			t.Fatal("raw rejected metadata formatted")
		}
	})
}
