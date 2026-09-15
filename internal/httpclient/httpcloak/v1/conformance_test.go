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

package httpcloak

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/sardanioss/http/httptrace"
)

func TestTraceCallbackExitPrecedesTechnicalRelease(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP2, HTTP3} {
		t.Run(string(mode), func(t *testing.T) {
			address, options := peer(t, mode, func(w http.ResponseWriter, r *http.Request) {
				data := make([]byte, 7)
				_, _ = io.ReadFull(r.Body, data)
				w.Header().Set("Content-Length", "2")
				_, _ = io.WriteString(w, "ok")
			})
			fixture := bindFixture(t, options, 1)
			entered := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			ctx := httptrace.WithClientTrace(testContext(t), &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { close(entered); <-release }})
			stream, receipt, err := fixture.client.Open(ctx, fault.Correlation{Call: "trace"}, request(t, "POST", address, strings.NewReader("payload")))
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("trace callback not reached")
			}
			data, err := io.ReadAll(stream)
			if err != nil || string(data) != "ok" {
				t.Fatal("early response unavailable", err)
			}
			wait, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			err = stream.Close(wait)
			cancel()
			if !errors.Is(err, invocation.ErrWait) {
				t.Fatal("blocked trace callback was not retained", err)
			}
			if result, ready := receipt.Result(); ready && result.Released {
				t.Fatal("trace still active after receipt release")
			}
			unblock()
			result := settle(t, fixture, receipt)
			if !result.Final || !result.Released || result.Outcome.Value.WritesObserved() != 1 {
				t.Fatal("trace completion evidence lost")
			}
		})
	}
}
func TestBodylessH2NeedsPeerEnd(t *testing.T) {
	_, options := peer(t, HTTP2, func(http.ResponseWriter, *http.Request) {})
	certPeer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer certPeer.Close()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: certPeer.TLS.Certificates, NextProtos: []string{"h2"}})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	peerDone := make(chan struct{})
	headerSent := make(chan struct{})
	go func() {
		defer close(peerDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		preface := make([]byte, len(http2.ClientPreface))
		if _, err := io.ReadFull(conn, preface); err != nil {
			return
		}
		framer := http2.NewFramer(conn, conn)
		_ = framer.WriteSettings()
		for {
			frame, err := framer.ReadFrame()
			if err != nil {
				return
			}
			if headers, ok := frame.(*http2.HeadersFrame); ok {
				var encoded bytes.Buffer
				encoder := hpack.NewEncoder(&encoded)
				_ = encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
				_ = encoder.WriteField(hpack.HeaderField{Name: "content-length", Value: "64"})
				_ = framer.WriteHeaders(http2.HeadersFrameParam{StreamID: headers.StreamID, BlockFragment: encoded.Bytes(), EndHeaders: true, EndStream: false})
				close(headerSent)
				for {
					if _, err := framer.ReadFrame(); err != nil {
						return
					}
				}
			}
		}
	}()
	address := "https://" + listener.Addr().String()
	options.Timeout = 200 * time.Millisecond
	fixture := bindFixture(t, options, 1)
	receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "head"}, request(t, "HEAD", address, nil))
	result := settle(t, fixture, receipt)
	select {
	case <-headerSent:
	default:
		t.Fatal("test failed before non-ending headers were sent")
	}
	if err == nil || result.Outcome.Value.Complete() {
		t.Fatal("headers without peer END_STREAM certified complete")
	}
	_ = fixture.assembly.Close(testContext(t))
	select {
	case <-peerDone:
	case <-time.After(time.Second):
		t.Fatal("raw H2 peer did not exit")
	}
}
func TestIndependentConformanceAndRuntimePrivacy(t *testing.T) {
	address, options := peer(t, HTTP1, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "verified") })
	fixture := bindFixture(t, options, 1)
	id := fault.Correlation{Call: "conformance"}
	receipt, err := fixture.client.Do(testContext(t), testContext(t), id, request(t, "GET", address, nil))
	if err != nil {
		t.Fatal(err)
	}
	result, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	expected := conformance.Expected[Result]{Context: fault.Context{Provider: ProviderID, Operation: "request", Scope: "fixture", Source: "source", Correlation: id}, Source: fixture.client.access.Info(), Limits: fixture.client.access.Limits(), Shape: invocation.Finite, Present: true, Final: true, Released: true, Attempts: invocation.Attempts{Observed: 1}, Value: func(t testing.TB, value Result) {
		if string(value.DataCopy()) != "verified" || !value.InputComplete() || !value.Complete() {
			t.Fatal("independent peer outcome changed")
		}
	}}
	conformance.Result(t, result, expected)
	conformance.Receive(t, testContext(t), fixture.inbox, []conformance.Expected[Result]{expected})
	conformance.Runtime(t, result.Outcome.Value, &Result{}, address)
	conformance.Runtime(t, OptionsV1{Name: "private-secret"}, &OptionsV1{}, "private-secret")
	conformance.Runtime(t, RequestOptionsV1{ProxyURL: "http://private-secret.invalid"}, &RequestOptionsV1{}, "private-secret")
	conformance.Private(t, fixture.client, address)
	conformance.Facade(t, fixture.client, "Open", "Do", "EvidenceBytes", "Profile", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
}
