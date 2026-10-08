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
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	nativehttp "github.com/nukilabs/http"
	nativequic "github.com/nukilabs/quic-go"
	sdk "github.com/nukilabs/tlsclient"
	nativetls "github.com/nukilabs/utls"
)

type observedBandwidth struct{ read, written atomic.Int64 }

func (tracker *observedBandwidth) AddReadBytes(count int64)  { tracker.read.Add(count) }
func (tracker *observedBandwidth) AddWriteBytes(count int64) { tracker.written.Add(count) }

func TestProviderH1ExactHeadersTrailersAndTracker(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	type observation struct {
		header, body, trailer string
		err                   error
	}
	observed := make(chan observation, 1)
	go func() {
		var result observation
		defer func() { observed <- result }()
		conn, err := listener.Accept()
		if err != nil {
			result.err = err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(conn)
		var header strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				result.err = err
				return
			}
			header.WriteString(line)
			if line == "\r\n" {
				break
			}
		}
		result.header = header.String()
		request, err := http.ReadRequest(bufio.NewReader(io.MultiReader(strings.NewReader(result.header), reader)))
		if err != nil {
			result.err = err
			return
		}
		body, err := io.ReadAll(request.Body)
		result.err = errors.Join(err, request.Body.Close())
		result.body, result.trailer = string(body), request.Trailer.Get("X-Trailer")
		_, err = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
		result.err = errors.Join(result.err, err)
	}()
	tracker := &observedBandwidth{}
	options := providerOptions()
	options.Mode, options.Native.Tracker = HTTP1Only, tracker
	fixture := bindProvider(t, options)
	request := nativeRequest(t, "POST", "http://"+listener.Addr().String(), strings.NewReader("payload"))
	request.Header = nativehttp.Header{"x-fIRst": {"one", "two"}, "X-lAst": {"last"}, nativehttp.HeaderOrderKey: {"x-first", "x-last"}}
	request.ContentLength = -1
	request.TransferEncoding = []string{"chunked"}
	request.Trailer = nativehttp.Header{"X-Trailer": {"complete"}}
	receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "native-wire-fields"}, request)
	if err != nil {
		t.Fatal(err)
	}
	result := outcome(t, fixture, receipt)
	if result.Err() != nil || string(result.Outcome.Value.DataCopy()) != "ok" || !result.Outcome.Value.Complete() {
		t.Fatal("request result", result.Err())
	}
	peer := <-observed
	first, last := strings.Index(peer.header, "x-fIRst: one\r\nx-fIRst: two\r\n"), strings.Index(peer.header, "X-lAst: last\r\n")
	if peer.err != nil || first < 0 || last <= first || strings.Contains(peer.header, nativehttp.HeaderOrderKey+":") || peer.body != "payload" || peer.trailer != "complete" {
		t.Fatal("independent H1 wire fields", peer)
	}
	if tracker.read.Load() <= 2 || tracker.written.Load() <= int64(len("payload")) {
		t.Fatal("borrowed tracker was inert", tracker.read.Load(), tracker.written.Load())
	}
	input := &closeReader{Reader: strings.NewReader("untouched")}
	invalid := nativeRequest(t, "POST", "http://"+listener.Addr().String(), input)
	invalid.Header["X-Invalid\r\n"] = []string{"refused"}
	if receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "invalid-wire-fields"}, invalid); receipt != nil || !errors.Is(err, ErrInput) || input.closed.Load() != 0 {
		t.Fatal("invalid metadata acquired input", receipt, err, input.closed.Load())
	}
}

func TestProviderTLSCallbacksAndManualPinHaveNativeEffect(t *testing.T) {
	for _, refusing := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid-certificate-and-pin", true: "callback-refusal"}[refusing], func(t *testing.T) {
			var accepted, peerChecks, connectionChecks, certificateRequests atomic.Int32
			peer := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.TLS == nil || len(request.TLS.PeerCertificates) != 1 {
					t.Error("native client certificate not sent")
				}
				accepted.Add(1)
				writer.WriteHeader(204)
			}))
			peer.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert}
			peer.StartTLS()
			defer peer.Close()
			cause := errors.New("controlled TLS verification refusal")
			options := providerOptions()
			options.Mode = HTTP1Only
			options.Native.TLS = testRoots(peer)
			options.Native.TLS.VerifyPeerCertificate = func(raw [][]byte, chains [][]*x509.Certificate) error {
				peerChecks.Add(1)
				if len(raw) == 0 || len(chains) == 0 {
					t.Error("certificate verification lost its native inputs")
				}
				if refusing {
					return cause
				}
				return nil
			}
			options.Native.TLS.VerifyConnection = func(state nativetls.ConnectionState) error {
				connectionChecks.Add(1)
				if len(state.PeerCertificates) == 0 || state.Version == 0 {
					t.Error("connection verification lost its native state")
				}
				return nil
			}
			options.Native.TLS.GetClientCertificate = func(info *nativetls.CertificateRequestInfo) (*nativetls.Certificate, error) {
				certificateRequests.Add(1)
				if info == nil || len(info.SignatureSchemes) == 0 {
					t.Error("certificate request lost its native state")
				}
				certificate := peer.TLS.Certificates[0]
				return &nativetls.Certificate{Certificate: certificate.Certificate, PrivateKey: certificate.PrivateKey}, nil
			}
			options.Native.Pinner = sdk.NewPinner(false)
			options.Native.Pinner.AddPins(peer.Listener.Addr().String(), []string{options.Native.Pinner.Fingerprint(peer.Certificate())})
			fixture := bindProvider(t, options)
			receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "native-tls-fields"}, nativeRequest(t, "GET", peer.URL, nil))
			if err != nil {
				t.Fatal(err)
			}
			result := outcome(t, fixture, receipt)
			if refusing {
				if !errors.Is(result.Err(), cause) || peerChecks.Load() == 0 || accepted.Load() != 0 {
					t.Fatal("native TLS refusal lost", result.Err(), peerChecks.Load(), accepted.Load())
				}
			} else if result.Err() != nil || accepted.Load() != 1 || peerChecks.Load() == 0 || connectionChecks.Load() == 0 || certificateRequests.Load() == 0 {
				t.Fatal("native TLS/pin positive control", result.Err(), accepted.Load(), peerChecks.Load(), connectionChecks.Load(), certificateRequests.Load())
			}
		})
	}
}

func TestProviderOwningTLSAndQUICCallbacksRemainRefused(t *testing.T) {
	options := providerOptions()
	options.Native.TLS = &nativetls.Config{GetConfigForClient: func(*nativetls.ClientHelloInfo) (*nativetls.Config, error) { panic("server callback must not run") }}
	if _, err := PrepareV1(options); !errors.Is(err, ErrUnsupported) {
		t.Fatal("owning server callback admitted", err)
	}
	options.Native.TLS = nil
	options.Native.ProxyTLS = &nativetls.Config{GetCertificate: func(*nativetls.ClientHelloInfo) (*nativetls.Certificate, error) {
		panic("server callback must not run")
	}}
	if _, err := PrepareV1(options); !errors.Is(err, ErrUnsupported) {
		t.Fatal("owning proxy callback admitted", err)
	}
	options.Native.ProxyTLS = nil
	options.Native.QUIC = &nativequic.Config{AllowConnectionWindowIncrease: func(*nativequic.Conn, uint64) bool { panic("owning QUIC callback must not run") }}
	if _, err := PrepareV1(options); !errors.Is(err, ErrUnsupported) {
		t.Fatal("owning QUIC callback admitted", err)
	}
}

func TestProviderForceHTTP3AuthorityAndRefusals(t *testing.T) {
	certificate, roots := masqueCertificate(t, "force-h3")
	var accepted, tcpDials atomic.Int32
	endpoint, stop := masqueServer(t, certificate, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		accepted.Add(1)
		_, _ = io.WriteString(writer, "forced")
	}))
	defer stop()
	for _, authority := range []string{"request-option", "native-context", "native-transport"} {
		t.Run(authority, func(t *testing.T) {
			options := providerOptions()
			options.Mode = Native
			options.Native.TLS = &nativetls.Config{RootCAs: roots}
			options.Native.DialContext = func(context.Context, string, string) (net.Conn, error) {
				tcpDials.Add(1)
				return nil, errors.New("forced H3 used TCP")
			}
			ctx, requestOptions := testContext(t), RequestOptionsV1{}
			switch authority {
			case "request-option":
				requestOptions.ForceHTTP3 = true
			case "native-context":
				ctx = sdk.WithForceHTTP3(ctx)
			case "native-transport":
				options.Native.Transport = &sdk.TransportOptions{ForceHTTP3: true}
			}
			fixture := bindProvider(t, options)
			receipt, err := fixture.client.Do(ctx, fault.Correlation{Call: authority}, nativeRequest(t, "GET", endpoint, nil), requestOptions)
			if err != nil {
				t.Fatal(err)
			}
			result := outcome(t, fixture, receipt)
			if result.Err() != nil || !result.Outcome.Value.Complete() || result.Outcome.Value.Metadata().Protocol() != "HTTP/3.0" || string(result.Outcome.Value.DataCopy()) != "forced" {
				t.Fatal("supported forced route", result.Err())
			}
		})
	}
	if accepted.Load() != 3 || tcpDials.Load() != 0 {
		t.Fatal("force authority fell back to TCP", accepted.Load(), tcpDials.Load())
	}
	for _, refusal := range []string{"h1", "h2", "disabled", "no-h3-profile", "http-url", "plain-proxy", "remote-dns"} {
		for _, authority := range []string{"request-option", "native-context"} {
			t.Run(refusal+"-"+authority, func(t *testing.T) {
				options := providerOptions()
				options.Mode = Native
				target := "https://target.invalid/"
				switch refusal {
				case "h1":
					options.Mode = HTTP1Only
				case "h2":
					options.Mode = HTTP2Negotiated
				case "disabled":
					options.Native.Transport = &sdk.TransportOptions{DisableHTTP3: true}
				case "no-h3-profile":
					options.Native.Profile.H3 = nil
				case "http-url":
					target = "http://target.invalid/"
				case "plain-proxy":
					options.ProxyURL = "http://127.0.0.1:1"
				case "remote-dns":
					options.ProxyURL = "socks5h://127.0.0.1:1"
				}
				fixture := bindProvider(t, options)
				body := &closeReader{Reader: strings.NewReader("not acquired")}
				request := nativeRequest(t, "POST", target, body)
				ctx, requestOptions := testContext(t), RequestOptionsV1{ForceHTTP3: true}
				if authority == "native-context" {
					ctx, requestOptions.ForceHTTP3 = sdk.WithForceHTTP3(ctx), false
				}
				if receipt, err := fixture.client.Do(ctx, fault.Correlation{Call: "force-refusal"}, request, requestOptions); receipt != nil || !errors.Is(err, ErrUnsupported) || body.closed.Load() != 0 {
					t.Fatal("force bypassed configured route or acquired input", receipt, err, body.closed.Load())
				}
			})
		}
	}
	for _, mode := range []ProtocolMode{HTTP1Only, HTTP2Negotiated} {
		options := providerOptions()
		options.Mode, options.Native.Transport = mode, &sdk.TransportOptions{ForceHTTP3: true}
		if _, err := PrepareV1(options); !errors.Is(err, ErrInput) {
			t.Fatal("native force contradicted configured mode", mode, err)
		}
	}
}
