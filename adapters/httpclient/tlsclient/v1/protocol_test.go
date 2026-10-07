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
	fhttp "github.com/bogdanfinn/fhttp"
	sdk "github.com/bogdanfinn/tls-client"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func observe(t *testing.T, inbox *adapters.Inbox[Result], receipt *adapters.Receipt[Result]) Result {
	t.Helper()
	direct, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if direct.Err() != nil {
		t.Fatal(direct.Err())
	}
	delivery, err := inbox.NextReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	independentReceipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	independent, err := independentReceipt.WaitReleased(testContext(t))
	if err != nil || independent.Info() != direct.Info() {
		t.Fatal("independent evidence mismatch", err)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	value, _ := direct.ValueCopy()
	return value
}
func TestPublicProtocolsStreamsAndBandwidth(t *testing.T) {
	for _, mode := range []string{"h1", "h2"} {
		t.Run(mode, func(t *testing.T) {
			peer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Input") != "yes" {
					t.Error("request headers changed")
				}
				w.Header().Set("Trailer", "X-Final")
				_, _ = io.WriteString(w, "body")
				w.Header().Set("X-Final", "complete")
			}))
			settings := Settings{Name: mode, Mode: pointer(Negotiated), Bandwidth: pointer(true)}
			if mode == "h1" {
				settings.Mode = pointer(HTTP1Only)
			}
			peer.EnableHTTP2 = mode == "h2"
			peer.StartTLS()
			defer peer.Close()
			pool := x509.NewCertPool()
			pool.AddCert(peer.Certificate())
			native := testNative()
			native.Transport = &sdk.TransportOptions{RootCAs: pool}
			prepared, err := Prepare(settings, native)
			if err != nil {
				t.Fatal(err)
			}
			deps := testMechanisms(t, prepared)
			owner, err := prepared.Open(testContext(t), deps)
			if err != nil {
				t.Fatal(err)
			}
			defer owner.Close(context.Background())
			input, _ := fhttp.NewRequest("GET", peer.URL, nil)
			input.Header.Set("X-Input", "yes")
			receipt, err := owner.Client().Do(testContext(t), testContext(t), input)
			if err != nil {
				t.Fatal(err)
			}
			value := observe(t, deps.Evidence, receipt)
			want := "HTTP/2.0"
			if mode == "h1" {
				want = "HTTP/1.1"
			}
			if !value.Complete() || string(value.DataCopy()) != "body" || value.Metadata().Protocol() != want ||
				value.TrailersCopy().Get("X-Final") != "complete" || value.Source().Name != mode || value.Attempts().Exact {
				t.Fatal("native/public response facts changed")
			}
			count, err := owner.Client().Bandwidth(testContext(t))
			read, available := count.ReadBytes()
			if err != nil || !available || read <= int64(len("body")) || count.Source().Name != mode {
				t.Fatal("native bandwidth unavailable or body-only", err)
			}
			stream, receipt, err := owner.Client().Open(testContext(t), input)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(stream)
			if err != nil || string(body) != "body" {
				t.Fatal(err)
			}
			wait, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := receipt.WaitReleased(wait); err == nil {
				t.Fatal("EOF released stream without cleanup")
			}
			if err := stream.Close(testContext(t)); err != nil {
				t.Fatal(err)
			}
			if value := observe(t, deps.Evidence, receipt); !value.Complete() || value.DataCopy() != nil {
				t.Fatal("stream became retained finite response")
			}
		})
	}
}
func TestPublicTruncationAndCancellationEvidence(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "9")
		_, _ = io.WriteString(w, "short")
	}))
	defer peer.Close()
	prepared, err := Prepare(Settings{Name: "partial"}, testNative())
	if err != nil {
		t.Fatal(err)
	}
	deps := testMechanisms(t, prepared)
	owner, err := prepared.Open(testContext(t), deps)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	input, _ := fhttp.NewRequest("GET", peer.URL, nil)
	receipt, err := owner.Client().Do(testContext(t), testContext(t), input)
	if receipt == nil || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("truncated response lost native cause", err)
	}
	result, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	value, _ := result.ValueCopy()
	if !errors.Is(result.Err(), ErrIntegrity) || value.Complete() || string(value.DataCopy()) != "short" {
		t.Fatal("false complete or missing partial bytes", result.Err())
	}
	record, err := deps.Evidence.NextReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := record.Ack(); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if receipt, err := owner.Client().Do(canceled, testContext(t), input); receipt != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled request admitted", err)
	}
}

func TestCleanupWaitDoesNotBecomeFinalPrimary(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = io.WriteString(w, "complete")
	}))
	defer peer.Close()
	prepared, err := Prepare(Settings{Name: "cleanup"}, testNative())
	if err != nil {
		t.Fatal(err)
	}
	deps := testMechanisms(t, prepared)
	owner, err := prepared.Open(testContext(t), deps)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	cleanup, cancel := context.WithCancel(context.Background())
	cancel()
	input, _ := fhttp.NewRequest("GET", peer.URL, nil)
	receipt, directErr := owner.Client().Do(testContext(t), cleanup, input)
	if receipt == nil {
		t.Fatal("valid request refused", directErr)
	}
	snapshot, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	result, _ := snapshot.ValueCopy()
	if snapshot.Primary() != nil || snapshot.Cleanup() != nil || !result.Complete() || string(result.DataCopy()) != "complete" {
		t.Fatal("cleanup waiter contaminated final outcome", snapshot.Err())
	}
	if directErr != nil && !errors.Is(directErr, context.Canceled) {
		t.Fatal("unexpected direct error", directErr)
	}
	record, err := deps.Evidence.NextReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := record.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedTimeoutClosesAbandonedRetainedStream(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Length", "1000")
		_, _ = io.WriteString(writer, "prefix")
		writer.(http.Flusher).Flush()
		<-request.Context().Done()
	}))
	defer peer.Close()
	prepared, err := Prepare(Settings{Name: "timeout", Timeout: pointer(50 * time.Millisecond)}, testNative())
	if err != nil {
		t.Fatal(err)
	}
	deps := testMechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), deps)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	input, _ := fhttp.NewRequest("GET", peer.URL, nil)
	stream, receipt, err := owner.Client().Open(context.Background(), input)
	if err != nil || stream == nil {
		t.Fatal("headers did not arrive", err)
	}
	defer stream.Close(context.Background())
	value, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal("prepared timeout did not initiate mandatory cleanup", err)
	}
	result, _ := value.ValueCopy()
	status, _ := deps.Runtime.Inspect()
	if result.Complete() || status.Active != 1 {
		t.Fatal("abandoned response completed or root retained")
	}
	if err := owner.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
}
