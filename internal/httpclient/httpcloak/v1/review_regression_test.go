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
	"compress/zlib"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	officialh3 "github.com/quic-go/quic-go/http3"
	nativehttp "github.com/sardanioss/http"
	"github.com/sardanioss/http/httptrace"
	"github.com/sardanioss/httpcloak/fingerprint"
	"github.com/sardanioss/httpcloak/transport"
)

func TestReviewRejectsAmbiguousRequestFraming(t *testing.T) {
	for _, missing := range []bool{false, true} {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
		defer server.Close()
		fixture := bindFixture(t, OptionsV1{Name: "source", PresetName: "chrome-148", Protocol: HTTP1, Timeout: 200 * time.Millisecond}, 1)
		input := request(t, "POST", server.URL, strings.NewReader("abc"))
		if missing {
			input.Body = nil
		} else {
			input.Header.Set("Transfer-Encoding", "chunked")
		}
		receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "framing"}, input)
		if receipt != nil {
			settle(t, fixture, receipt)
		}
		if err == nil || receipt != nil {
			t.Error("invalid framing was not refused before admission")
		}
	}
}
func TestReviewLowercaseHeadersAreNotDuplicated(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1, HTTP2, HTTP3} {
		address, options := peer(t, mode, func(w http.ResponseWriter, r *http.Request) {
			if len(r.Header.Values("X-Unique")) != 1 {
				t.Error("single native header was duplicated")
			}
			_, _ = io.WriteString(w, "ok")
		})
		fixture := bindFixture(t, options, 1)
		input := request(t, "GET", address, nil)
		input.Header = nativehttp.Header{"x-unique": {"one"}}
		receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "lowercase"}, input)
		if err != nil {
			t.Fatal(err)
		}
		settle(t, fixture, receipt)
	}
}
func TestReviewProfileHeadersCannotBypassManagedInputs(t *testing.T) {
	for _, header := range []fingerprint.HeaderPair{{Key: "X-Test", Value: "one\r\nInjected: yes"}, {Key: "Proxy-Authorization", Value: "private"}, {Key: "Content-Length", Value: "7"}, {Key: "X-Large", Value: strings.Repeat("x", 2048)}} {
		preset := fingerprint.GetStrict("chrome-148")
		preset.HeaderOrder = append(preset.HeaderOrder, header)
		if _, err := Select(OptionsV1{Name: "source", Protocol: HTTP1, MaxHeaderBytes: 1024, Native: NativeOptionsV1{Preset: preset}}); err == nil {
			t.Error("unsafe preset headers bypassed validation")
		}
	}
}
func TestReviewDeflateTrailingInputIsNotComplete(t *testing.T) {
	var encoded bytes.Buffer
	writer := zlib.NewWriter(&encoded)
	_, _ = io.WriteString(writer, "complete")
	_ = writer.Close()
	data := append(bytes.Clone(encoded.Bytes()), []byte("trailing")...)
	address, options := peer(t, HTTP1, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "deflate")
		_, _ = w.Write(data)
	})
	fixture := bindFixture(t, options, 1)
	receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "deflate"}, request(t, "GET", address, nil))
	result := settle(t, fixture, receipt)
	if err == nil || result.Outcome.Value.Complete() {
		t.Fatal("buffered trailing compressed input was ignored")
	}
}
func TestReviewInformationalResponsesAreNotFinal(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1, HTTP2, HTTP3} {
		address, options := peer(t, mode, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Link", "</style>; rel=preload")
			w.WriteHeader(103)
			w.Header().Del("Link")
			_, _ = io.WriteString(w, "final")
		})
		fixture := bindFixture(t, options, 1)
		for range 2 {
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "informational"}, request(t, "GET", address, nil))
			if err != nil {
				t.Fatal(err)
			}
			result := settle(t, fixture, receipt)
			if result.Outcome.Value.Metadata().StatusCode() != 200 || string(result.Outcome.Value.DataCopy()) != "final" {
				t.Error("informational headers became a final response")
			}
		}
	}
}
func TestReviewH3InformationalCallbackErrorIsRetained(t *testing.T) {
	address, options := peer(t, HTTP3, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(103); _, _ = io.WriteString(w, "final") })
	fixture := bindFixture(t, options, 1)
	cause := errors.New("test informational veto")
	ctx := httptrace.WithClientTrace(testContext(t), &httptrace.ClientTrace{Got1xxResponse: func(int, textproto.MIMEHeader) error { return cause }})
	receipt, _ := fixture.client.Do(ctx, testContext(t), fault.Correlation{Call: "veto"}, request(t, "GET", address, nil))
	result := settle(t, fixture, receipt)
	if !errors.Is(result.Err(), cause) || result.Outcome.Value.Complete() {
		t.Fatal("native informational veto disappeared")
	}
}

func TestReviewPresetCredentialsRespectRedirectScope(t *testing.T) {
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("preset credentials crossed redirect origin")
		}
		_, _ = io.WriteString(w, "safe")
	}))
	defer destination.Close()
	target := strings.Replace(destination.URL, "127.0.0.1", "localhost", 1)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test" || r.Header.Get("Cookie") != "session=test" {
			t.Error("configured native credentials lost")
		}
		w.Header().Set("Location", target)
		w.WriteHeader(302)
	}))
	defer origin.Close()
	preset := fingerprint.GetStrict("chrome-148")
	preset.HeaderOrder = append(preset.HeaderOrder, fingerprint.HeaderPair{Key: "Authorization", Value: "Bearer test"}, fingerprint.HeaderPair{Key: "Cookie", Value: "session=test"})
	fixture := bindFixture(t, OptionsV1{Name: "source", Protocol: HTTP1, Native: NativeOptionsV1{Preset: preset}}, 1)
	receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "profile-redirect"}, request(t, "GET", origin.URL, nil), RequestOptionsV1{FollowRedirects: true})
	if err != nil {
		t.Fatal(err)
	}
	if !settle(t, fixture, receipt).Outcome.Value.Complete() {
		t.Fatal("safe redirect did not complete")
	}
}

func TestPresetCredentialsHonorTLSOnly(t *testing.T) {
	for _, configured := range []bool{false, true} {
		for _, runtime := range []bool{false, true} {
			address, options := peer(t, HTTP2, func(w http.ResponseWriter, r *http.Request) {
				present := r.Header.Get("Authorization") == "Bearer test" && r.Header.Get("Cookie") == "session=test"
				if present == runtime {
					t.Error("TLS-only selection did not control preset credentials")
				}
				_, _ = io.WriteString(w, "ok")
			})
			preset := fingerprint.GetStrict("chrome-148")
			preset.HeaderOrder = append(preset.HeaderOrder, fingerprint.HeaderPair{Key: "Authorization", Value: "Bearer test"}, fingerprint.HeaderPair{Key: "Cookie", Value: "session=test"})
			options.PresetName = ""
			options.Native.Preset = preset
			options.Native.Transport = &transport.TransportConfig{TLSOnly: configured}
			fixture := bindFixture(t, options, 1)
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "tls-only-credentials"}, request(t, "GET", address, nil), RequestOptionsV1{TLSOnly: &runtime})
			if err != nil {
				t.Fatal(err)
			}
			settle(t, fixture, receipt)
		}
	}
}

func TestMergedHeaderBudgetRetainsLimitIdentity(t *testing.T) {
	var calls atomic.Int32
	address, options := peer(t, HTTP1, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) })
	preset := fingerprint.GetStrict("chrome-148")
	preset.HeaderOrder = []fingerprint.HeaderPair{{Key: "X-Preset", Value: strings.Repeat("p", 600)}}
	options.PresetName = ""
	options.Native.Preset = preset
	options.MaxHeaderBytes = 1024
	fixture := bindFixture(t, options, 1)
	input := request(t, "GET", address, nil)
	input.Header.Set("X-Call", strings.Repeat("c", 600))
	receipt, _ := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "header-budget"}, input)
	result := settle(t, fixture, receipt)
	if !errors.Is(result.Err(), ErrLimit) || calls.Load() != 0 {
		t.Fatal("merged header overflow lost limit identity or performed I/O")
	}
}

func TestInformationalHeaderBudgetAndLargeFinalBody(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		payload := strings.Repeat("body", 32<<10)
		address, options := peer(t, HTTP1, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Interim", strings.Repeat("h", 600))
			w.WriteHeader(103)
			if overflow {
				w.WriteHeader(103)
			}
			w.Header().Del("X-Interim")
			_, _ = io.WriteString(w, payload)
		})
		options.MaxHeaderBytes = 1024
		fixture := bindFixture(t, options, 1)
		receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "interim-budget"}, request(t, "GET", address, nil))
		result := settle(t, fixture, receipt)
		if overflow {
			if !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.Complete() {
				t.Fatal("interim headers escaped the aggregate receive budget")
			}
		} else if err != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != payload {
			t.Fatal("interim boundary truncated the final response", err)
		}
	}
}

func TestReviewEarlyH3ResponseAbortsBlockedUpload(t *testing.T) {
	releasePeer := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(releasePeer) }) }
	defer release()
	address, options := peer(t, HTTP3, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "2")
		_, _ = io.WriteString(w, "ok")
		w.(http.Flusher).Flush()
		stream := w.(officialh3.HTTPStreamer).HTTPStream()
		_ = stream.Close()
		<-releasePeer
		stream.CancelRead(0)
	})
	options.MaxRequestBytes = 32 << 20
	fixture := bindFixture(t, options, 1)
	input := request(t, "POST", address, io.LimitReader(zeroReader{}, 16<<20))
	input.ContentLength = 16 << 20
	stream, receipt, err := fixture.client.Open(testContext(t), fault.Correlation{Call: "blocked-upload"}, input)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(stream)
	if err != nil || string(data) != "ok" {
		t.Fatal("early response not observed", err)
	}
	stream.op.mu.Lock()
	read := stream.op.data.input
	writes := stream.op.data.writes
	stream.op.mu.Unlock()
	if read >= input.ContentLength || writes != 0 {
		t.Fatal("fixture failed to hold the native upload")
	}
	observed := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stack := make([]byte, 1<<20)
		count := runtime.Stack(stack, true)
		text := string(stack[:count])
		if strings.Contains(text, "github.com/sardanioss/quic-go.(*SendStream).Write") && strings.Contains(text, "github.com/sardanioss/quic-go/http3.(*ClientConn).sendRequestBody") {
			observed = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !observed {
		release()
		_ = stream.Close(testContext(t))
		t.Fatal("native flow-controlled write stack was not observed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = stream.Close(ctx)
	cancel()
	if errors.Is(err, invocation.ErrWait) {
		release()
		_ = stream.Close(testContext(t))
		t.Fatal("response EOF left a flow-controlled upload uninterruptible")
	}
	release()
	result := settle(t, fixture, receipt)
	if result.Outcome.Value.Complete() || result.Outcome.Value.InputComplete() {
		t.Fatal("early response certified an incomplete upload")
	}
}

type zeroReader struct{}

func (zeroReader) Read(data []byte) (int, error) { clear(data); return len(data), nil }
