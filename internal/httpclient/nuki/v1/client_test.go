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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	nativehttp "github.com/nukilabs/http"
	"github.com/nukilabs/tlsclient/profiles"
	nativetls "github.com/nukilabs/utls"
	"github.com/quic-go/quic-go/http3"
)

type fixture struct {
	client   *Client
	assembly *resource.Assembly
	selected resource.Selection[Source]
	inbox    *invocation.Inbox[Result]
	info     resource.Info
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func providerOptions() OptionsV1 {
	profile := profiles.Chrome150
	return OptionsV1{Name: "nuki-instance", Mode: HTTP2Negotiated, Native: NativeOptionsV1{Profile: &profile},
		Timeout: 3 * time.Second, MaxRequestBytes: 64 << 10, MaxResponseBytes: 64 << 10, MaxEncodedBytes: 64 << 10}
}

func testRoots(peer *httptest.Server) *nativetls.Config {
	roots := x509.NewCertPool()
	roots.AddCert(peer.Certificate())
	return &nativetls.Config{RootCAs: roots}
}
func bindProvider(t *testing.T, options OptionsV1) *fixture {
	t.Helper()
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	limits, err := LimitsV1(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "nuki-tests", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](32, 32*defaults(options).evidenceBytes())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, info, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assembly.Close(testContext(t)); err != nil {
			t.Error("resource cleanup", err)
		}
	})
	return &fixture{client: client, assembly: assembly, selected: selected, inbox: inbox, info: info}
}
func outcome(t *testing.T, fixture *fixture, receipt *invocation.Receipt[Result]) invocation.Result[Result] {
	t.Helper()
	result, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Final || !result.Released || !reflect.DeepEqual(result.Source, fixture.info) {
		t.Fatal("completion or source attribution changed")
	}
	delivery, err := fixture.inbox.Next(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	other, err := delivery.Receipt().WaitReleased(testContext(t))
	if err != nil || other.Context != result.Context || other.Outcome.Value.data != result.Outcome.Value.data {
		t.Fatal("independent evidence changed", err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	return result
}
func nativeRequest(t *testing.T, method, address string, reader io.Reader) *nativehttp.Request {
	t.Helper()
	request, err := nativehttp.NewRequest(method, address, reader)
	if err != nil {
		t.Fatal(err)
	}
	return request
}
func TestProviderNativeProtocolsAndOwnedCleanup(t *testing.T) {
	for _, protocol := range []int{1, 2, 3} {
		t.Run([]string{"", "h1", "h2", "h3"}[protocol], func(t *testing.T) {
			handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Header.Get("X-Input") != "private-value" {
					t.Error("request changed")
				}
				writer.Header().Set("Content-Length", "7")
				writer.Header().Set("X-Output", "isolated")
				_, _ = io.WriteString(writer, "payload")
			})
			peer := httptest.NewUnstartedServer(handler)
			peer.EnableHTTP2 = protocol != 1
			peer.StartTLS()
			defer peer.Close()
			address := peer.URL
			var packet net.PacketConn
			if protocol == 3 {
				var err error
				packet, err = net.ListenPacket("udp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				server := &http3.Server{TLSConfig: &tls.Config{Certificates: peer.TLS.Certificates}, Handler: handler}
				done := make(chan error, 1)
				go func() { done <- server.Serve(packet) }()
				defer func() { _ = server.Close(); _ = packet.Close(); <-done }()
				address = "https://" + packet.LocalAddr().String()
			}
			roots := x509.NewCertPool()
			roots.AddCert(peer.Certificate())
			options := providerOptions()
			options.Native.TLS = &nativetls.Config{RootCAs: roots}
			if protocol == 1 {
				options.Mode = HTTP1Only
			}
			if protocol == 3 {
				options.Mode = HTTP3Only
			}
			fixture := bindProvider(t, options)
			request := nativeRequest(t, "GET", address, nil)
			request.Header.Set("X-Input", "private-value")
			receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "protocol"}, request)
			if err != nil {
				t.Fatal(err)
			}
			result := outcome(t, fixture, receipt)
			if result.Err() != nil {
				t.Fatal("request", result.Err(), errors.Unwrap(result.Err()))
			}
			value := result.Outcome.Value
			if !value.Complete() || string(value.DataCopy()) != "payload" || value.Exchanges() != 1 ||
				value.EncodedBytesRead() != 7 || value.DecodedBytesRead() != 7 ||
				value.Metadata().HeadersCopy().Get("X-Output") != "isolated" {
				t.Fatalf("protocol %d response evidence mismatch", protocol)
			}
			if protocol == 3 && value.Metadata().Protocol() != "HTTP/3.0" || protocol == 2 && value.Metadata().Protocol() != "HTTP/2.0" || protocol == 1 && value.Metadata().Protocol() != "HTTP/1.1" {
				t.Fatal("negotiated protocol mismatch", value.Metadata().Protocol())
			}
			data := value.DataCopy()
			data[0] = 'x'
			header := value.Metadata().HeadersCopy()
			header.Set("X-Output", "changed")
			if string(value.DataCopy()) != "payload" || value.Metadata().HeadersCopy().Get("X-Output") != "isolated" {
				t.Fatal("shared mutable result")
			}
			if err := fixture.assembly.Close(testContext(t)); err != nil {
				t.Fatal("terminal cleanup", err)
			}
			fixture.client.owner.mu.Lock()
			sockets := len(fixture.client.owner.sockets)
			fixture.client.owner.mu.Unlock()
			if sockets != 0 {
				t.Fatal("source retained sockets", sockets)
			}
		})
	}
}
