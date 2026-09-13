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

package otel

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	nativeotel "go.opentelemetry.io/otel"
	collog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetric "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestExportPartialSuccessAllSignalsAndIndependentEvidence(t *testing.T) {
	for _, signal := range []Signal{Logs, Traces, Metrics} {
		t.Run(string(signal), func(t *testing.T) {
			var response proto.Message
			options := OptionsV1{}
			switch signal {
			case Logs:
				options.LogsEndpoint = "logs"
				response = &collog.ExportLogsServiceResponse{PartialSuccess: &collog.ExportLogsPartialSuccess{RejectedLogRecords: 1, ErrorMessage: "private-receiver-canary"}}
			case Traces:
				options.TracesEndpoint = "traces"
				response = &coltrace.ExportTraceServiceResponse{PartialSuccess: &coltrace.ExportTracePartialSuccess{RejectedSpans: 1, ErrorMessage: "private-receiver-canary"}}
			case Metrics:
				options.MetricsEndpoint = "metrics"
				options.Instruments = []InstrumentV1{{Name: "count", Kind: "int64-counter"}}
				response = &colmetric.ExportMetricsServiceResponse{PartialSuccess: &colmetric.ExportMetricsPartialSuccess{RejectedDataPoints: 1, ErrorMessage: "private-receiver-canary"}}
			}
			encoded, _ := proto.Marshal(response)
			var requests atomic.Int32
			var diagnostics atomic.Int32
			old := nativeotel.GetErrorHandler()
			nativeotel.SetErrorHandler(nativeotel.ErrorHandlerFunc(func(error) { diagnostics.Add(1) }))
			defer nativeotel.SetErrorHandler(old)
			fixture := newFixture(t, options, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requests.Add(1)
				_, _ = io.Copy(io.Discard, request.Body)
				writer.Header().Set("Content-Type", "application/x-protobuf")
				_, _ = writer.Write(encoded)
			}))
			fixture.allowCloseError = true
			switch signal {
			case Logs:
				emitOne(t, fixture, "record", "message")
			case Traces:
				_, span, err := fixture.client.Start(context.Background(), fault.Correlation{Call: "span"}, SpanInput{Name: "trace"})
				if err != nil {
					t.Fatal(err)
				}
				_, err = span.End(context.Background())
				if err != nil {
					t.Fatal(err)
				}
			case Metrics:
				receipt, err := fixture.client.MeasureInt64(context.Background(), fault.Correlation{Call: "metric"}, "count", 1)
				if result := outcomeOf(t, receipt, err); result.Err() != nil {
					t.Fatal(result.Err())
				}
			}
			result := flushOne(t, fixture)
			entry := result.Outcome.Value.SignalsCopy()[0]
			if !errors.Is(result.Err(), ErrPartial) || entry.Rejected != 1 || entry.Acknowledged != 0 || entry.Effect != PartialEffect ||
				entry.TransportCalls != 1 || requests.Load() != 1 || diagnostics.Load() != 0 {
				t.Fatal("partial response/error/retry boundary failed")
			}
			conformance.Private(t, result.Err(), "private-receiver-canary")
			// No logging/export callback is involved in receiving the original operation
			// and failed flush evidence, even after the caller ignores both receipts.
			for range 2 {
				delivery, err := fixture.inbox.Next(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				observed, _ := delivery.Receipt().Result()
				if observed.Context.Operation == "flush" && !errors.Is(observed.Err(), ErrPartial) {
					t.Fatal("failed export evidence lost")
				}
				if err := delivery.Release(); err != nil {
					t.Fatal(err)
				}
			}
			if signal != Metrics {
				flushOne(t, fixture)
				if requests.Load() != 1 {
					t.Fatal("possibly-effectful event batch retried")
				}
			}
		})
	}
}
func TestExportFailureBoundariesAndNoNativeRetry(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		contentType string
		body        []byte
		expected    error
	}{
		{"refused", 503, "text/plain", []byte("private-response-canary"), ErrExport},
		{"throttled", 429, "text/plain", []byte("private-response-canary"), ErrExport},
		{"bad-protobuf", 200, "application/x-protobuf", []byte{0xff}, ErrProtocol},
		{"wrong-type", 200, "text/plain", []byte("private-response-canary"), ErrProtocol},
		{"wrong-success-status", 201, "application/x-protobuf", nil, ErrProtocol},
		{"response-overflow", 200, "application/x-protobuf", bytes.Repeat([]byte{1}, 1025), ErrLimit},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int32
			fixture := newFixture(t, OptionsV1{MaxResponseBytes: 1024}, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requests.Add(1)
				_, _ = io.Copy(io.Discard, request.Body)
				writer.Header().Set("Content-Type", test.contentType)
				writer.Header().Set("Retry-After", "1")
				writer.WriteHeader(test.status)
				_, _ = writer.Write(test.body)
			}))
			emitOne(t, fixture, "record", "message")
			result := flushOne(t, fixture)
			if !errors.Is(result.Err(), test.expected) || requests.Load() != 1 || result.Outcome.Value.SignalsCopy()[0].Effect != UnknownEffect {
				t.Fatal("failed export misreported")
			}
			conformance.Private(t, result.Err(), "private-response-canary")
			flushOne(t, fixture)
			if requests.Load() != 1 {
				t.Fatal("failed batch automatically retried")
			}
		})
	}
}
func TestExportRequestLimitRejectsBeforeReceiver(t *testing.T) {
	var requests atomic.Int32
	fixture := newFixture(t, OptionsV1{MaxRequestBytes: 1024}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	emitOne(t, fixture, "record", strings.Repeat("x", 2048))
	result := flushOne(t, fixture)
	if !errors.Is(result.Err(), ErrExport) || requests.Load() != 0 || result.Outcome.Value.SignalsCopy()[0].Effect != NotAttempted {
		t.Fatal("oversized request reached receiver")
	}
}
func TestExportCancellationKeepsUnknownEffectAndResourceUse(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	fixture := newFixture(t, OptionsV1{}, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		once.Do(func() { close(entered) })
		<-release
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer close(release)
	emitOne(t, fixture, "record", "message")
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("private-cancel-canary")
	finished := make(chan invocation.Result[Result], 1)
	go func() {
		receipt, err := fixture.client.Flush(ctx, fault.Correlation{Call: "flush"})
		if err != nil {
			finished <- invocation.Result[Result]{Outcome: invocation.Outcome[Result]{Primary: err}}
			return
		}
		result, _ := receipt.Result()
		finished <- result
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("receiver was not entered")
	}
	if err := fixture.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("in-flight export released")
	}
	cancel(cause)
	var result invocation.Result[Result]
	select {
	case result = <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("canceled native export did not return")
	}
	if !errors.Is(result.Err(), cause) || !errors.Is(result.Err(), context.Canceled) || result.Outcome.Value.SignalsCopy()[0].Effect != UnknownEffect {
		t.Fatalf("cancellation evidence: cause=%t canceled=%t effect=%s", errors.Is(result.Err(), cause), errors.Is(result.Err(), context.Canceled), result.Outcome.Value.SignalsCopy()[0].Effect)
	}
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestExportTimeout(t *testing.T) {
	fixture := newFixture(t, OptionsV1{Timeout: 20 * time.Millisecond}, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		<-request.Context().Done()
	}))
	emitOne(t, fixture, "record", "message")
	result := flushOne(t, fixture)
	if !errors.Is(result.Err(), context.DeadlineExceeded) || result.Outcome.Value.SignalsCopy()[0].Effect != UnknownEffect {
		t.Fatal("timeout uncertainty lost")
	}
}

func TestExportProtocolSwitchClosesPeerAndPreservesEvidence(t *testing.T) {
	var upgrade atomic.Bool
	peers := make(chan net.Conn, 1)
	peerClosed := make(chan error, 1)
	fixture := newFixture(t, OptionsV1{Timeout: 100 * time.Millisecond}, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		if !upgrade.Load() {
			writer.Header().Set("Content-Type", "application/x-protobuf")
			return
		}
		connection, buffer, err := writer.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
		_, _ = buffer.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: unexpected\r\n\r\n")
		if err := buffer.Flush(); err != nil {
			t.Error(err)
			return
		}
		peers <- connection
		var data [1]byte
		_, err = connection.Read(data[:])
		peerClosed <- err
	}))
	emitOne(t, fixture, "control-record", "normal response")
	control := flushOne(t, fixture)
	entry := control.Outcome.Value.SignalsCopy()[0]
	if control.Err() != nil || entry.Acknowledged != 1 || entry.Effect != Acknowledged {
		t.Fatal("normal HTTP 200 control failed")
	}
	upgrade.Store(true)
	runWithPeer := func(operation func() error) error {
		t.Helper()
		returned := make(chan error, 1)
		go func() { returned <- operation() }()
		var peer net.Conn
		select {
		case peer = <-peers:
		case <-time.After(3 * time.Second):
			t.Fatal("protocol-switch receiver was not entered")
		}
		defer peer.Close()
		var result error
		select {
		case result = <-returned:
		case <-time.After(time.Second):
			_ = peer.Close()
			select {
			case <-returned:
			case <-time.After(3 * time.Second):
				t.Error("operation did not return after test-owned peer closure")
			}
			t.Fatal("protocol switch outlived the 100 ms export budget")
		}
		select {
		case err := <-peerClosed:
			if !errors.Is(err, io.EOF) {
				t.Fatalf("peer did not observe client-side closure: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("protocol-switch connection remained open")
		}
		return result
	}
	emitOne(t, fixture, "upgrade-record", "unsupported response")
	if err := runWithPeer(func() error {
		_, err := fixture.client.Flush(context.Background(), fault.Correlation{Call: "upgrade-flush"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	observedFailure := false
	for range 4 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		delivery, err := fixture.inbox.Next(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		result, ready := delivery.Receipt().Result()
		if result.Context.Correlation.Call == "upgrade-flush" {
			entry := result.Outcome.Value.SignalsCopy()[0]
			if !ready || !result.Final || !result.Released || !errors.Is(result.Err(), ErrProtocol) ||
				entry.Effect != UnknownEffect || entry.Submitted != 1 || entry.TransportCalls != 1 || entry.Acknowledged != 0 {
				t.Fatal("independent protocol failure evidence changed")
			}
			observedFailure = true
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if !observedFailure || fixture.inbox.Usage().Outstanding != 0 {
		t.Fatal("protocol failure evidence was not independently received and released")
	}
	emitOne(t, fixture, "cleanup-record", "final export")
	fixture.allowCloseError = true
	if err := runWithPeer(func() error { return fixture.assembly.Close(context.Background()) }); !errors.Is(err, ErrProtocol) {
		t.Fatal("final export did not retain the protocol failure")
	}
	status := fixture.assembly.Snapshot().Sources[0]
	if status.Pending || !status.Quiescent || !status.Released {
		t.Fatal("protocol failure prevented actual resource cleanup")
	}
	if err := fixture.assembly.Close(context.Background()); !errors.Is(err, ErrProtocol) {
		t.Fatal("repeated close erased protocol failure history")
	}
}

func TestShutdownActuallyClosesIdleConnectionAndRetainsFailure(t *testing.T) {
	closed := make(chan struct{}, 4)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		writer.Header().Set("Content-Type", "application/x-protobuf")
		writer.WriteHeader(503)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed <- struct{}{}
		}
	}
	server.Start()
	defer server.Close()
	fixture := newFixture(t, OptionsV1{LogsEndpoint: server.URL + "/v1/logs"}, nil)
	fixture.allowCloseError = true
	emitOne(t, fixture, "record", "message")
	err := fixture.assembly.Close(context.Background())
	if !errors.Is(err, ErrExport) {
		t.Fatal("shutdown export failure disappeared")
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("owned connection did not close")
	}
	if retry := fixture.assembly.Close(context.Background()); !errors.Is(retry, ErrExport) {
		t.Fatal("repeat shutdown erased failure")
	}
	if _, err := fixture.client.Emit(context.Background(), fault.Correlation{Call: "late"}, LogRecord{Message: "late"}); err == nil {
		t.Fatal("use after shutdown admitted")
	}
}
func TestRedirectIsNotFollowed(t *testing.T) {
	var hits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer target.Close()
	fixture := newFixture(t, OptionsV1{}, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	emitOne(t, fixture, "record", "message")
	if result := flushOne(t, fixture); !errors.Is(result.Err(), ErrProtocol) || hits.Load() != 0 {
		t.Fatal("redirect escaped configured endpoint")
	}
}

type transportFunc func(*http.Request) (*http.Response, error)

func (function transportFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestExportDoesNotExposeNativeHTTPConnectionThroughContext(t *testing.T) {
	fixture := newFixture(t, OptionsV1{}, nil)
	var exposed atomic.Bool
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { exposed.Store(true) }})
	control := &http.Client{Transport: &http.Transport{}}
	defer control.CloseIdleConnections()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, fixture.server.URL, nil)
	response, err := control.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if !exposed.Load() {
		t.Fatal("native owning-handle negative control did not execute")
	}
	exposed.Store(false)
	emitOne(t, fixture, "record", "message")
	receipt, err := fixture.client.Flush(ctx, fault.Correlation{Call: "flush"})
	if result := outcomeOf(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if exposed.Load() {
		t.Fatal("caller context exposed owning HTTP connection")
	}
}

func TestNativeExportContextRefusesRecursiveTelemetry(t *testing.T) {
	fixture := newFixture(t, OptionsV1{Timeout: 100 * time.Millisecond}, nil)
	transport := fixture.owner.httpClient.Transport.(*boundedTransport)
	original := transport.native
	var recursion error
	transport.native = transportFunc(func(request *http.Request) (*http.Response, error) {
		receipt, err := fixture.client.Emit(request.Context(), fault.Correlation{Call: "recursive"}, LogRecord{Message: "must not enqueue"})
		if receipt != nil {
			t.Error("recursive telemetry acquired evidence")
		}
		recursion = err
		return original.RoundTrip(request)
	})
	emitOne(t, fixture, "record", "message")
	if result := flushOne(t, fixture); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if !errors.Is(recursion, ErrRecursion) {
		t.Fatal("native exporter context lost recursion suppression")
	}
	if fixture.inbox.Usage().Outstanding != 2 {
		t.Fatal("recursive call consumed independent evidence")
	}
}

func TestPartialResponseCountsAndWarnings(t *testing.T) {
	for _, test := range []struct {
		name         string
		rejected     int64
		warning      string
		want         error
		acknowledged int
	}{
		{"partial", 1, "rejected", ErrPartial, 1}, {"warning", 0, "warning", ErrPartial, 2},
		{"negative", -1, "invalid", ErrProtocol, 0}, {"too-many", 3, "invalid", ErrProtocol, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, _ := proto.Marshal(&collog.ExportLogsServiceResponse{PartialSuccess: &collog.ExportLogsPartialSuccess{RejectedLogRecords: test.rejected, ErrorMessage: test.warning}})
			fixture := newFixture(t, OptionsV1{}, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				_, _ = io.Copy(io.Discard, request.Body)
				writer.Header().Set("Content-Type", "application/x-protobuf")
				_, _ = writer.Write(data)
			}))
			emitOne(t, fixture, "first", "first")
			emitOne(t, fixture, "second", "second")
			result := flushOne(t, fixture)
			entry := result.Outcome.Value.SignalsCopy()[0]
			if !errors.Is(result.Err(), test.want) || entry.Acknowledged != test.acknowledged {
				t.Fatal("receiver count validation failed")
			}
		})
	}
}
