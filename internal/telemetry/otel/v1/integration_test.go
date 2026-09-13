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

package otel_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	fzap "github.com/frost-leo/fathomry/internal/logging/zap/v1"
	fzero "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	otel "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/internal/telemetry/otel/v1/zapbridge"
	"github.com/frost-leo/fathomry/internal/telemetry/otel/v1/zerologbridge"
	nativeotel "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	logapi "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	traceapi "go.opentelemetry.io/otel/trace"
	collog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetric "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	sdkzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

type receiver struct {
	mu      sync.Mutex
	logs    []*collog.ExportLogsServiceRequest
	traces  []*coltrace.ExportTraceServiceRequest
	metrics []*colmetric.ExportMetricsServiceRequest
	headers []http.Header
	reject  atomic.Bool
}

func (receiver *receiver) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	var reader io.Reader = request.Body
	if request.Header.Get("Content-Encoding") == "gzip" {
		zipped, err := gzip.NewReader(request.Body)
		if err != nil {
			writer.WriteHeader(400)
			return
		}
		defer zipped.Close()
		reader = zipped
	}
	data, err := io.ReadAll(io.LimitReader(reader, 8<<20))
	if err != nil {
		writer.WriteHeader(400)
		return
	}
	receiver.mu.Lock()
	var decode error
	switch request.URL.Path {
	case "/v1/logs":
		value := &collog.ExportLogsServiceRequest{}
		decode = proto.Unmarshal(data, value)
		receiver.logs = append(receiver.logs, value)
	case "/v1/traces":
		value := &coltrace.ExportTraceServiceRequest{}
		decode = proto.Unmarshal(data, value)
		receiver.traces = append(receiver.traces, value)
	case "/v1/metrics":
		value := &colmetric.ExportMetricsServiceRequest{}
		decode = proto.Unmarshal(data, value)
		receiver.metrics = append(receiver.metrics, value)
	default:
		decode = errors.New("unexpected path")
	}
	receiver.headers = append(receiver.headers, request.Header.Clone())
	receiver.mu.Unlock()
	if decode != nil {
		writer.WriteHeader(400)
		return
	}
	if receiver.reject.Load() {
		writer.WriteHeader(503)
		_, _ = io.WriteString(writer, "synthetic-backend-unavailable")
		return
	}
	writer.Header().Set("Content-Type", "application/x-protobuf")
}
func (receiver *receiver) records() ([]*logpb.LogRecord, []*tracepb.Span, []*metricpb.Metric) {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	var logs []*logpb.LogRecord
	var spans []*tracepb.Span
	var metrics []*metricpb.Metric
	for _, request := range receiver.logs {
		for _, resource := range request.ResourceLogs {
			for _, scope := range resource.ScopeLogs {
				logs = append(logs, scope.LogRecords...)
			}
		}
	}
	for _, request := range receiver.traces {
		for _, resource := range request.ResourceSpans {
			for _, scope := range resource.ScopeSpans {
				spans = append(spans, scope.Spans...)
			}
		}
	}
	for _, request := range receiver.metrics {
		for _, resource := range request.ResourceMetrics {
			for _, scope := range resource.ScopeMetrics {
				metrics = append(metrics, scope.Metrics...)
			}
		}
	}
	return logs, spans, metrics
}
func wireAttrs(attrs []*commonpb.KeyValue) map[string]*commonpb.AnyValue {
	result := make(map[string]*commonpb.AnyValue, len(attrs))
	for _, attr := range attrs {
		result[attr.Key] = attr.Value
	}
	return result
}
func integrationClient(t *testing.T, options otel.OptionsV1) (*otel.Client, *resource.Assembly, *invocation.Inbox[otel.Result]) {
	t.Helper()
	selected, err := otel.Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, otel.LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "telemetry-scope", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[otel.Result](256, 256<<20)
	if err != nil {
		t.Fatal(err)
	}
	client, err := otel.Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := assembly.Close(ctx); err != nil {
			t.Error("telemetry cleanup failed", err)
		}
	})
	return client, assembly, inbox
}
func checked[T any](t *testing.T, receipt *invocation.Receipt[T], err error) invocation.Result[T] {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	result, ready := receipt.Result()
	if !ready || !result.Final {
		t.Fatal("missing result")
	}
	return result
}

func TestIntegrationNativeSignalsTLSGzipResourceScopeAndContext(t *testing.T) {
	receiver := &receiver{}
	server := httptest.NewTLSServer(receiver)
	t.Cleanup(server.Close)
	ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
	options := otel.OptionsV1{Name: "otel", ServiceName: "orders", Scope: "fathomry-test", ScopeVersion: "test.1",
		ResourceSchemaURL: "https://example.test/resource/1", ScopeSchemaURL: "https://example.test/scope/2",
		LogsEndpoint: server.URL + "/v1/logs", TracesEndpoint: server.URL + "/v1/traces", MetricsEndpoint: server.URL + "/v1/metrics",
		TLS: &otel.TLSV1{CA: ca}, Headers: map[string]string{"Authorization": "Bearer synthetic-canary"}, Compression: "gzip",
		ResourceAttributes: map[string]string{"environment": "isolated"}, ScopeAttributes: map[string]string{"component": "test"},
		Instruments: []otel.InstrumentV1{{Name: "rows", Kind: "int64-counter", Unit: "{row}", AttributeKeys: []string{"region"}},
			{Name: "latency", Kind: "float64-histogram", Unit: "s", Boundaries: []float64{0.1, 1, 5}}}}
	traceGlobal, meterGlobal, logGlobal := nativeotel.GetTracerProvider(), nativeotel.GetMeterProvider(), global.GetLoggerProvider()
	client, assembly, inbox := integrationClient(t, options)
	options.Headers["Authorization"] = "changed"
	options.ResourceAttributes["environment"] = "changed"
	incoming := map[string]string{"traceparent": "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01", "baggage": "region=not-a-label"}
	ctx, err := otel.Extract(context.Background(), incoming, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, span, err := client.Start(ctx, fault.Correlation{Call: "operation", Owner: "account"}, otel.SpanInput{Name: "read-orders", Kind: traceapi.SpanKindClient})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = span.End(context.Background()) })
	data := []byte{0, 1, 255}
	receipt, err := client.Emit(ctx, fault.Correlation{Call: "log", Parent: "operation", Owner: "account"}, otel.LogRecord{Message: "received", Severity: logapi.SeverityInfo,
		Attributes: []slog.Attr{slog.Int64("rows", 5), slog.Any("bytes", data), slog.Group("nested", slog.Bool("ok", true)),
			slog.Any("null", nil), slog.Any("labels", []string{"a", "b"})}})
	if result := checked(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	data[0] = 9
	for _, value := range []int64{2, 3} {
		receipt, err := client.MeasureInt64(ctx, fault.Correlation{Call: "count"}, "rows", value, slog.String("region", "west"))
		if result := checked(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
	}
	receipt, err = client.MeasureFloat64(ctx, fault.Correlation{Call: "latency"}, "latency", 0.25)
	if result := checked(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	cause := fault.Kind("test.failure").New(fault.Context{Operation: "read"}, errors.New("private-original-cause"))
	if err := span.RecordError(cause); err != nil {
		t.Fatal(err)
	}
	if err := span.SetStatus(codes.Error, "read failed"); err != nil {
		t.Fatal(err)
	}
	_, err = span.End(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if logs, spans, metrics := receiver.records(); len(logs)+len(spans)+len(metrics) != 0 {
		t.Fatal("unrequested background export")
	}
	receipt, err = client.Flush(context.Background(), fault.Correlation{Call: "flush"})
	if result := checked(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	logs, spans, metrics := receiver.records()
	if len(logs) != 1 || len(spans) != 1 || len(metrics) != 2 {
		t.Fatal("real signal path missing")
	}
	if !bytes.Equal(logs[0].TraceId, spans[0].TraceId) || !bytes.Equal(logs[0].SpanId, spans[0].SpanId) || logs[0].Flags&1 != 1 ||
		!bytes.Equal(spans[0].ParentSpanId, []byte{1, 2, 3, 4, 5, 6, 7, 8}) || spans[0].Status.Code != tracepb.Status_STATUS_CODE_ERROR {
		t.Fatal("wire association/status changed")
	}
	attrs := wireAttrs(logs[0].Attributes)
	if attrs["rows"].GetIntValue() != 5 || !bytes.Equal(attrs["bytes"].GetBytesValue(), []byte{0, 1, 255}) ||
		!wireAttrs(attrs["nested"].GetKvlistValue().Values)["ok"].GetBoolValue() || attrs["fathomry.call"].GetStringValue() != "log" ||
		attrs["fathomry.owner"].GetStringValue() != "account" || attrs["region"] != nil || attrs["null"] == nil || attrs["null"].Value != nil ||
		len(attrs["labels"].GetArrayValue().Values) != 2 {
		t.Fatal("typed data or baggage separation changed")
	}
	for _, metric := range metrics {
		switch metric.Name {
		case "rows":
			data := metric.GetSum()
			if data == nil || len(data.DataPoints) != 1 || data.DataPoints[0].GetAsInt() != 5 || data.AggregationTemporality != metricpb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
				t.Fatal("native sum changed")
			}
			dimensions := wireAttrs(data.DataPoints[0].Attributes)
			if len(dimensions) != 1 || dimensions["region"].GetStringValue() != "west" || len(data.DataPoints[0].Exemplars) != 0 {
				t.Fatal("unbounded automatic metric dimensions")
			}
		case "latency":
			histogram := metric.GetHistogram()
			if histogram == nil || len(histogram.DataPoints) != 1 || histogram.DataPoints[0].Count != 1 ||
				histogram.DataPoints[0].GetSum() != 0.25 || len(histogram.DataPoints[0].ExplicitBounds) != 3 || metric.Unit != "s" {
				t.Fatal("native histogram changed")
			}
		default:
			t.Fatal("unexpected metric")
		}
	}
	receiver.mu.Lock()
	resourceLogs := receiver.logs[0].ResourceLogs[0]
	if wireAttrs(resourceLogs.Resource.Attributes)["service.name"].GetStringValue() != "orders" ||
		wireAttrs(resourceLogs.Resource.Attributes)["environment"].GetStringValue() != "isolated" ||
		resourceLogs.ScopeLogs[0].Scope.Name != "fathomry-test" || resourceLogs.ScopeLogs[0].Scope.Version != "test.1" ||
		wireAttrs(resourceLogs.ScopeLogs[0].Scope.Attributes)["component"].GetStringValue() != "test" ||
		resourceLogs.SchemaUrl != "https://example.test/resource/1" || resourceLogs.ScopeLogs[0].SchemaUrl != "https://example.test/scope/2" {
		t.Error("resource/scope not preserved")
	}
	for _, headers := range receiver.headers {
		if headers.Get("Authorization") != "Bearer synthetic-canary" || headers.Get("Content-Encoding") != "gzip" {
			t.Error("frozen transport options not applied")
		}
	}
	receiver.mu.Unlock()
	for range 6 {
		delivery, err := inbox.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if traceGlobal != nativeotel.GetTracerProvider() || meterGlobal != nativeotel.GetMeterProvider() || logGlobal != global.GetLoggerProvider() {
		t.Fatal("global provider changed")
	}
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationTimestampBoundsUseUTCInstant(t *testing.T) {
	for _, test := range []struct {
		name      string
		timestamp time.Time
		valid     bool
	}{
		{"lower-valid", time.Date(1970, 1, 1, 14, 0, 0, 1, time.FixedZone("east", 14*3600)), true},
		{"upper-valid", time.Date(2262, 1, 1, 1, 0, 0, 0, time.FixedZone("east", 14*3600)), true},
		{"before-epoch", time.Date(1970, 1, 1, 0, 0, 0, 0, time.FixedZone("east", 14*3600)), false},
		{"after-last-year", time.Date(2261, 12, 31, 23, 0, 0, 0, time.FixedZone("west", -14*3600)), false},
		{"overflowing-fixed-zone", time.Date(2261, 1, 1, 0, 0, 0, 0, time.FixedZone("large-offset", -1000000000)), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			receiver := &receiver{}
			server := httptest.NewServer(receiver)
			t.Cleanup(server.Close)
			client, _, inbox := integrationClient(t, otel.OptionsV1{Name: "otel", ServiceName: "timestamps",
				LogsEndpoint: server.URL + "/v1/logs", TracesEndpoint: server.URL + "/v1/traces"})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			receipt, err := client.Emit(ctx, fault.Correlation{Call: "log"}, otel.LogRecord{Message: "timestamp", Time: test.timestamp})
			if test.valid {
				if result := checked(t, receipt, err); result.Err() != nil {
					t.Fatal(result.Err())
				}
			} else if !errors.Is(err, otel.ErrInput) || receipt != nil {
				t.Error("invalid UTC log timestamp was not rejected before admission")
			}
			_, span, err := client.Start(ctx, fault.Correlation{Call: "span"}, otel.SpanInput{Name: "timestamp", Time: test.timestamp})
			if span != nil {
				t.Cleanup(func() { _, _ = span.End(context.Background()) })
			}
			if test.valid {
				if err != nil || span == nil {
					t.Fatal("valid UTC span timestamp rejected", err)
				}
			} else if !errors.Is(err, otel.ErrInput) || span != nil {
				t.Error("invalid UTC span timestamp was not rejected before admission")
			}
			if span != nil {
				receipt, err := span.End(ctx)
				if result := checked(t, receipt, err); result.Err() != nil {
					t.Fatal(result.Err())
				}
			}
			receipt, err = client.Flush(ctx, fault.Correlation{Call: "flush"})
			if result := checked(t, receipt, err); result.Err() != nil {
				t.Fatal(result.Err())
			}
			logs, spans, _ := receiver.records()
			calls := []string{"flush"}
			if test.valid {
				calls = []string{"log", "span", "flush"}
				if len(logs) != 1 || len(spans) != 1 || logs[0].TimeUnixNano != uint64(test.timestamp.UnixNano()) ||
					spans[0].StartTimeUnixNano != uint64(test.timestamp.UnixNano()) {
					t.Fatal("receiver timestamp differs from the accepted UTC instant")
				}
			} else {
				receiver.mu.Lock()
				requests := len(receiver.logs) + len(receiver.traces)
				receiver.mu.Unlock()
				if requests != 0 || len(logs) != 0 || len(spans) != 0 {
					t.Fatal("rejected timestamps reached native export")
				}
			}
			if inbox.Usage().Outstanding != len(calls) {
				t.Fatal("timestamp admission acquired unexpected evidence capacity")
			}
			for _, call := range calls {
				delivery, err := inbox.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				observed, ready := delivery.Receipt().Result()
				if !ready || !observed.Final || observed.Context.Correlation.Call != call || observed.Err() != nil {
					t.Fatal("independent timestamp evidence missing or failed")
				}
				if err := delivery.Release(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func privateDirectory(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	return directory
}
func fileLines(t *testing.T, directory string) (int, bool) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	count, compressed := 0, false
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".log") && !strings.HasSuffix(entry.Name(), ".jsonl") && !strings.HasSuffix(entry.Name(), ".gz") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(entry.Name(), ".gz") {
			compressed = true
			reader, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			data, err = io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
		}
		for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
			if len(line) == 0 {
				continue
			}
			if !json.Valid(line) {
				t.Fatal("local JSON corrupted")
			}
			count++
		}
	}
	return count, compressed
}
func TestIntegrationZapAndZerologPreserveLocalMultiSinkAndRotation(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("existing Zap rotating-file profile is Linux-only")
	}
	receiver := &receiver{}
	server := httptest.NewServer(receiver)
	t.Cleanup(server.Close)
	client, assembly, _ := integrationClient(t, otel.OptionsV1{Name: "otel", ServiceName: "logs", LogsEndpoint: server.URL + "/v1/logs", TracesEndpoint: server.URL + "/v1/traces"})
	ctx, span, err := client.Start(context.Background(), fault.Correlation{Call: "span"}, otel.SpanInput{Name: "logged-work"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = span.End(context.Background()) })
	zapDirectories := []string{privateDirectory(t), privateDirectory(t)}
	zapOptions := fzap.OptionsV1{Name: "zap", MaxEntryBytes: 1024, Outputs: []fzap.OutputV1{
		{Name: "first", Kind: "file", Directory: zapDirectories[0], MaxFileBytes: 1024, MaxBackups: 16, Compress: true},
		{Name: "second", Kind: "file", Directory: zapDirectories[1], MaxFileBytes: 1024, MaxBackups: 16}}}
	zapSelected, err := fzap.Select(zapOptions, zapbridge.New(client))
	if err != nil {
		t.Fatal(err)
	}
	zapSelected = resource.WithLimits(zapSelected, fzap.LimitsV1(zapOptions))
	zapAssembly, err := resource.Assemble(context.Background(), context.Background(), "zap-scope", zapSelected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := zapAssembly.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	zapInbox, _ := invocation.NewInbox[fzap.Result](64, 4<<20)
	zapLogger, err := fzap.Bind(zapAssembly, zapSelected, zapInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	zeroDirectory := privateDirectory(t)
	var local bytes.Buffer
	zeroOptions := fzero.OptionsV1{Name: "zero", MaxRecordBytes: 1024, Sinks: []fzero.SinkV1{
		{Name: "otel", Records: zerologbridge.New(client)},
		{Name: "file", File: &fzero.FileOptionsV1{Directory: zeroDirectory, MaxBytes: 1024, Backups: 16, Compress: true}},
		{Name: "local", Writer: &local}}}
	zeroSelected, err := fzero.Select(zeroOptions)
	if err != nil {
		t.Fatal(err)
	}
	zeroSelected = resource.WithLimits(zeroSelected, fzero.LimitsV1(zeroOptions))
	zeroAssembly, err := resource.Assemble(context.Background(), context.Background(), "zero-scope", zeroSelected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := zeroAssembly.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	zeroInbox, _ := invocation.NewInbox[fzero.Result](64, 4<<20)
	zeroLogger, err := fzero.Bind(zeroAssembly, zeroSelected, zeroInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	for range 8 {
		receipt, err := zapLogger.Log(ctx, fault.Correlation{Call: "zap-call", Parent: "span"}, zapcore.InfoLevel, strings.Repeat("z", 400),
			sdkzap.Int64("rows", 5), sdkzap.Binary("bytes", []byte{1, 2}))
		if result := checked(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		other, err := zeroLogger.Log(ctx, fault.Correlation{Call: "zero-call", Parent: "span"}, fzero.Info, strings.Repeat("r", 400),
			slog.Int64("rows", 5), slog.Group("nested", slog.Bool("ok", true)), slog.Group("empty"))
		if result := checked(t, other, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
	}
	receipt, err := zapLogger.Sync(context.Background(), fault.Correlation{Call: "zap-sync"})
	if result := checked(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	other, err := zeroLogger.Sync(context.Background(), fault.Correlation{Call: "zero-sync"})
	if result := checked(t, other, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if logs, _, _ := receiver.records(); len(logs) != 0 {
		t.Fatal("borrowed logger Sync claimed provider flush")
	}
	flushed, err := client.Flush(context.Background(), fault.Correlation{Call: "flush"})
	if result := checked(t, flushed, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	logs, _, _ := receiver.records()
	if len(logs) != 16 {
		t.Fatal("bridged records missing")
	}
	expectedTraceID := traceapi.SpanContextFromContext(ctx).TraceID()
	for _, record := range logs {
		attrs := wireAttrs(record.Attributes)
		fields := wireAttrs(attrs["attributes"].GetKvlistValue().Values)
		metadata := wireAttrs(attrs["logging"].GetKvlistValue().Values)
		if fields["rows"].GetIntValue() != 5 || len(record.TraceId) != 16 || len(record.SpanId) != 8 ||
			!bytes.Equal(record.TraceId, expectedTraceID[:]) {
			t.Fatal("bridge lost typed association")
		}
		provider := metadata["provider"].GetStringValue()
		if provider != fzap.ProviderID && provider != fzero.ProviderID {
			t.Fatal("original logging source lost")
		}
		if provider == fzero.ProviderID {
			empty, found := fields["empty"]
			if !found || empty == nil {
				t.Fatal("zerolog empty group was dropped")
			}
			group, ok := empty.Value.(*commonpb.AnyValue_KvlistValue)
			if !ok || group.KvlistValue == nil || len(group.KvlistValue.Values) != 0 {
				t.Fatal("zerolog empty group changed wire kind or contents")
			}
		}
	}
	for index, directory := range append(zapDirectories, zeroDirectory) {
		count, gzip := fileLines(t, directory)
		if count != 8 || index != 1 && !gzip {
			t.Fatal("local file fan-out/rotation regressed")
		}
	}
	if bytes.Count(local.Bytes(), []byte{'\n'}) != 8 {
		t.Fatal("zerolog borrowed writer lost records")
	}
	// Unsupported OTLP uint64 must fail only the telemetry branch, not later local output.
	other, err = zeroLogger.Log(ctx, fault.Correlation{Call: "unsigned"}, fzero.Info, "unsigned", slog.Uint64("large", math.MaxUint64))
	result := checked(t, other, err)
	sinks := result.Outcome.Value.SinksCopy()
	if !errors.Is(result.Err(), otel.ErrUnsupported) || sinks[0].Name != "otel" || !sinks[0].Attempted || sinks[0].Accepted ||
		!sinks[1].Accepted || !sinks[2].Accepted {
		t.Fatal("bridge failure suppressed local output")
	}
	other, err = zeroLogger.Sync(context.Background(), fault.Correlation{Call: "unsigned-sync"})
	if result := checked(t, other, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	fileData, err := os.ReadFile(filepath.Join(zeroDirectory, "current.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for _, output := range [][]byte{fileData, local.Bytes()} {
		found := false
		for _, line := range bytes.Split(bytes.TrimSpace(output), []byte{'\n'}) {
			var record struct {
				Message    string                     `json:"message"`
				Attributes map[string]json.RawMessage `json:"attributes"`
			}
			if err := json.Unmarshal(line, &record); err != nil {
				t.Fatal(err)
			}
			if record.Message == "unsigned" && string(record.Attributes["large"]) == "18446744073709551615" {
				found = true
			}
		}
		if !found {
			t.Fatal("later local receiver lost the record refused by telemetry")
		}
	}
	_, err = span.End(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := zeroAssembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := zapAssembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationZerologQueueRefusalRequiresSourceReconstruction(t *testing.T) {
	receiver := &receiver{}
	server := httptest.NewServer(receiver)
	t.Cleanup(server.Close)
	client, _, inbox := integrationClient(t, otel.OptionsV1{Name: "otel", ServiceName: "logs", QueueItems: 1,
		LogsEndpoint: server.URL + "/v1/logs"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var local bytes.Buffer
	options := fzero.OptionsV1{Name: "zero", Sinks: []fzero.SinkV1{
		{Name: "otel", Records: zerologbridge.New(client)}, {Name: "local", Writer: &local}}}
	selection, err := fzero.Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selection = resource.WithLimits(selection, fzero.LimitsV1(options))
	bind := func(scope string) (*fzero.Logger, *resource.Assembly, *invocation.Inbox[fzero.Result]) {
		assembly, err := resource.Assemble(ctx, ctx, scope, selection)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := assembly.Close(cleanup); err != nil {
				t.Error(err)
			}
		})
		inbox, err := invocation.NewInbox[fzero.Result](8, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		logger, err := fzero.Bind(assembly, selection, inbox, nil)
		if err != nil {
			t.Fatal(err)
		}
		return logger, assembly, inbox
	}
	log := func(logger *fzero.Logger, call string) invocation.Result[fzero.Result] {
		receipt, err := logger.Log(ctx, fault.Correlation{Call: call}, fzero.Info, call)
		return checked(t, receipt, err)
	}
	logger, assembly, logInbox := bind("original-logs")
	if result := log(logger, "first"); result.Err() != nil {
		t.Fatal(result.Err())
	}
	refused := log(logger, "queue-full")
	if !errors.Is(refused.Err(), otel.ErrLimit) || !refused.Outcome.Value.SinksCopy()[0].Attempted ||
		refused.Outcome.Value.SinksCopy()[0].Accepted || !refused.Outcome.Value.SinksCopy()[1].Accepted {
		t.Fatal("queue saturation did not refuse only the entered telemetry sink")
	}
	receipt, err := client.Flush(ctx, fault.Correlation{Call: "flush"})
	if result := checked(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	latched := log(logger, "after-flush")
	if !errors.Is(latched.Err(), fzero.ErrState) || latched.Outcome.Value.SinksCopy()[0].Attempted ||
		latched.Outcome.Value.SinksCopy()[0].Accepted || !latched.Outcome.Value.SinksCopy()[1].Accepted || inbox.Usage().Outstanding != 3 {
		t.Fatal("flushing telemetry silently restarted the failed logging sink")
	}
	logs, _, _ := receiver.records()
	if len(logs) != 1 || logs[0].Body.GetStringValue() != "first" || bytes.Count(local.Bytes(), []byte{'\n'}) != 3 ||
		!bytes.Contains(local.Bytes(), []byte(`"message":"after-flush"`)) {
		t.Fatal("independent receivers disagree with the stopped-sink boundary")
	}
	if err := assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	logger, _, recreatedInbox := bind("recreated-logs")
	if result := log(logger, "recreated"); result.Err() != nil {
		t.Fatal(result.Err())
	}
	receipt, err = client.Flush(ctx, fault.Correlation{Call: "flush-recreated"})
	if result := checked(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	logs, _, _ = receiver.records()
	if len(logs) != 2 || logs[1].Body.GetStringValue() != "recreated" || bytes.Count(local.Bytes(), []byte{'\n'}) != 4 ||
		!bytes.Contains(local.Bytes(), []byte(`"message":"recreated"`)) {
		t.Fatal("explicit source reconstruction did not restore native delivery")
	}
	for _, expected := range []struct {
		inbox *invocation.Inbox[fzero.Result]
		call  string
		err   error
	}{{logInbox, "first", nil}, {logInbox, "queue-full", otel.ErrLimit}, {logInbox, "after-flush", fzero.ErrState}, {recreatedInbox, "recreated", nil}} {
		delivery, err := expected.inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		observed, ready := delivery.Receipt().Result()
		if !ready || !observed.Final || observed.Context.Correlation.Call != expected.call || !errors.Is(observed.Err(), expected.err) {
			t.Fatal("logging source reconstruction lost independent call evidence")
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	for _, expected := range []struct {
		call string
		err  error
	}{{"first", nil}, {"queue-full", otel.ErrLimit}, {"flush", nil}, {"recreated", nil}, {"flush-recreated", nil}} {
		delivery, err := inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		observed, ready := delivery.Receipt().Result()
		if !ready || !observed.Final || observed.Context.Correlation.Call != expected.call || !errors.Is(observed.Err(), expected.err) {
			t.Fatal("telemetry lost independent queue or export evidence")
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIntegrationFailureKeepsIndependentLoggingAndTelemetryEvidence(t *testing.T) {
	receiver := &receiver{}
	server := httptest.NewServer(receiver)
	t.Cleanup(server.Close)
	client, assembly, inbox := integrationClient(t, otel.OptionsV1{Name: "otel", ServiceName: "logs", LogsEndpoint: server.URL + "/v1/logs"})
	var local bytes.Buffer
	options := fzero.OptionsV1{Name: "zero", Sinks: []fzero.SinkV1{{Name: "otel", Records: zerologbridge.New(client)}, {Name: "local", Writer: &local}}}
	selection, err := fzero.Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selection = resource.WithLimits(selection, fzero.LimitsV1(options))
	logAssembly, err := resource.Assemble(context.Background(), context.Background(), "logs", selection)
	if err != nil {
		t.Fatal(err)
	}
	defer logAssembly.Close(context.Background())
	logInbox, _ := invocation.NewInbox[fzero.Result](2, 1<<20)
	logger, err := fzero.Bind(logAssembly, selection, logInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := logger.Log(context.Background(), fault.Correlation{Call: "business-log"}, fzero.Info, "local survives")
	if result := checked(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	receiver.reject.Store(true)
	flushed, err := client.Flush(context.Background(), fault.Correlation{Call: "failed-flush"})
	if result := checked(t, flushed, err); !errors.Is(result.Err(), otel.ErrExport) || result.Outcome.Value.SignalsCopy()[0].Effect != otel.UnknownEffect {
		t.Fatal("failed receiver was certified")
	}
	if !strings.Contains(local.String(), "local survives") {
		t.Fatal("failed exporter removed local log")
	}
	logEvidence, err := logInbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	logged, _ := logEvidence.Receipt().Result()
	if logged.Err() != nil || !logged.Outcome.Value.SinksCopy()[0].Accepted {
		t.Fatal("later export rewrote queue-acceptance history")
	}
	for range 2 {
		delivery, err := inbox.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if err := logEvidence.Release(); err != nil {
		t.Fatal(err)
	}
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
