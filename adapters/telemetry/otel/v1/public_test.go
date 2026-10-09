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
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"go.opentelemetry.io/otel/codes"
	logapi "go.opentelemetry.io/otel/log"
	traceapi "go.opentelemetry.io/otel/trace"
	logpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricdata "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

func valuePointer[T any](value T) *T { return &value }
func boundedContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 5*time.Second)
}
func publicOwner(t *testing.T, settings otel.Settings) (*otel.Owner, otel.Dependencies) {
	t.Helper()
	prepared, err := otel.Prepare(settings)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[otel.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	deps := otel.Dependencies{Runtime: runtime, Evidence: inbox}
	owner, err := prepared.Open(context.Background(), deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := boundedContext(t)
		defer cancel()
		if err := owner.Close(ctx); err != nil {
			t.Error("source cleanup", err)
		}
		if !owner.ShutdownComplete() {
			t.Error("native source remained owned")
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error("operation runtime cleanup", err)
		}
		if err := inbox.Seal(); err != nil {
			t.Error(err)
		}
		for {
			delivery, err := inbox.NextReleased(ctx)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Error("evidence cleanup", err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
				break
			}
		}
		if status, err := inbox.Inspect(); err != nil || status.Outstanding != 0 {
			t.Error("evidence retained", status, err)
		}
	})
	return owner, deps
}
func publicResult(t *testing.T, deps otel.Dependencies, receipt *adapters.Receipt[otel.Result], submission error) (otel.Result, error) {
	t.Helper()
	if submission != nil {
		t.Fatal("submission", submission)
	}
	if receipt == nil {
		t.Fatal("missing accepted receipt")
	}
	ctx, cancel := boundedContext(t)
	defer cancel()
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal("actual work did not release", err)
	}
	delivery, err := deps.Evidence.NextReleased(ctx)
	if err != nil {
		t.Fatal("independent evidence", err)
	}
	independentReceipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	independent, err := independentReceipt.WaitReleased(ctx)
	if err != nil || independent.Info().Sequence != snapshot.Info().Sequence {
		t.Fatal("wrong evidence", err)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	value, present := snapshot.ValueCopy()
	if !present {
		return otel.Result{}, snapshot.Err()
	}
	return value, snapshot.Err()
}
func success(t *testing.T, deps otel.Dependencies, receipt *adapters.Receipt[otel.Result], err error) otel.Result {
	t.Helper()
	result, err := publicResult(t, deps, receipt, err)
	if err != nil {
		t.Fatal("accepted operation failed", err)
	}
	return result
}
func attrValue(attrs []*commonpb.KeyValue, name string) *commonpb.AnyValue {
	for _, attr := range attrs {
		if attr.Key == name {
			return attr.Value
		}
	}
	return nil
}

func TestPublicAllSignalsIndependentDecodedOutput(t *testing.T) {
	var mu sync.Mutex
	var logs logpb.ExportLogsServiceRequest
	var traces tracepb.ExportTraceServiceRequest
	var metrics metricpb.ExportMetricsServiceRequest
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			writer.WriteHeader(500)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		requests++
		switch request.URL.Path {
		case "/logs":
			err = proto.Unmarshal(data, &logs)
		case "/traces":
			err = proto.Unmarshal(data, &traces)
		case "/metrics":
			err = proto.Unmarshal(data, &metrics)
		default:
			t.Error("unexpected endpoint")
		}
		if err != nil {
			t.Error(err)
			writer.WriteHeader(500)
			return
		}
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(server.Close)
	identity, err := otel.Tree(otel.Value{Kind: "map", Map: []otel.TypedAttribute{{Key: "build", Value: otel.Value{Kind: "int64", Int64: (1 << 53) + 1}}}})
	if err != nil {
		t.Fatal(err)
	}
	settings := otel.Settings{Name: "all-signals", ServiceName: "fixture", LogsEndpoint: server.URL + "/logs", TracesEndpoint: server.URL + "/traces", MetricsEndpoint: server.URL + "/metrics",
		TypedResourceAttributes: []otel.ConfigAttribute{{Key: "identity", Value: identity}}, TypedScopeAttributes: []otel.ConfigAttribute{{Key: "scope.identity", Value: identity}}}
	kinds := []string{"int64-counter", "int64-updowncounter", "int64-gauge", "int64-histogram", "float64-counter", "float64-updowncounter", "float64-gauge", "float64-histogram"}
	for _, kind := range kinds {
		instrument := otel.Instrument{Name: kind, Kind: kind, AttributeKeys: []string{"dimension"}}
		if strings.HasSuffix(kind, "histogram") {
			instrument.Boundaries = []float64{0, 1 << 53}
		}
		settings.Instruments = append(settings.Instruments, instrument)
	}
	owner, deps := publicOwner(t, settings)
	client, err := owner.Client().WithID("public-correlation")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	spanContext, span, err := client.Start(context.Background(), otel.SpanInput{Name: "original", Time: start, Kind: traceapi.SpanKindServer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := span.End(context.Background()); err != nil {
			t.Error("span cleanup", err)
		}
	})
	if !span.IsRecording() || !span.SpanContext().IsValid() {
		t.Fatal("missing non-owning span facts")
	}
	if err := span.SetName("renamed"); err != nil {
		t.Fatal(err)
	}
	if err := span.AddLink(otel.Link{Context: span.SpanContext(), Attributes: []slog.Attr{slog.Int64("linked", 7)}}); err != nil {
		t.Fatal(err)
	}
	if err := span.AddEventAt("event", start.Add(time.Second), slog.String("phase", "safe")); err != nil {
		t.Fatal(err)
	}
	if err := span.SetAttributes(slog.String("work", "copied"), slog.Any("typed", otel.Value{Kind: "array", Array: []otel.Value{{Kind: "int64", Int64: (1 << 53) + 1}, {}}})); err != nil {
		t.Fatal(err)
	}
	publicErr, err := failure.New(otel.Definitions()[0], failure.Location{Operation: "synthetic"}, errors.New("private-native-canary"))
	if err != nil {
		t.Fatal(err)
	}
	if err := span.RecordError(publicErr); err != nil {
		t.Fatal(err)
	}
	if err := span.SetStatus(codes.Error, "safe status"); err != nil {
		t.Fatal(err)
	}
	binary := []byte{0, 1, 255}
	body := otel.Value{Kind: "map", Map: []otel.TypedAttribute{{Key: "integer", Value: otel.Value{Kind: "int64", Int64: (1 << 53) + 1}}, {Key: "binary", Value: otel.Value{Kind: "bytes", Bytes: binary}}, {Key: "null", Value: otel.Value{}}}}
	receipt, err := client.Emit(spanContext, otel.LogRecord{Time: start.Add(2 * time.Second), Severity: logapi.SeverityError, SeverityText: "error", EventName: "closed-data", Body: &body,
		Attributes: []slog.Attr{slog.Any("empty", otel.AttributeMap{}), slog.Any("typed", otel.Value{Kind: "array", Array: []otel.Value{{Kind: "int64", Int64: (1 << 53) + 1}, {}}})}})
	result := success(t, deps, receipt, err)
	if !result.HasData() || result.SignalsCopy()[0].Accepted != 1 || result.Attribution().ID != "public-correlation" {
		t.Fatal("lost local acceptance/attribution")
	}
	binary[0] = 9
	body.Map[0].Value.Int64 = 2
	for _, kind := range kinds {
		if strings.HasPrefix(kind, "int64") {
			receipt, err = client.MeasureInt64(spanContext, kind, (1<<53)+1, slog.String("dimension", "one"))
		} else {
			receipt, err = client.MeasureFloat64(spanContext, kind, 101.5, slog.String("dimension", "one"))
		}
		success(t, deps, receipt, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := span.EndAt(canceled, start.Add(3*time.Second)); err == nil {
		t.Fatal("canceled End reported release")
	}
	receipt, err = span.EndAt(context.Background(), start.Add(3*time.Second))
	success(t, deps, receipt, err)
	if spanContext.Err() != nil {
		t.Fatal("ending span canceled caller's trace association context")
	}
	receipt, err = client.Flush(context.Background())
	result = success(t, deps, receipt, err)
	if len(result.SignalsCopy()) != 3 {
		t.Fatal("missing signal export facts")
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 3 {
		t.Fatal("unexpected native request count", requests)
	}
	if len(logs.ResourceLogs) != 1 || len(traces.ResourceSpans) != 1 || len(metrics.ResourceMetrics) != 1 {
		t.Fatal("missing independently decoded signals")
	}
	resourceLogs := logs.ResourceLogs[0]
	record := resourceLogs.ScopeLogs[0].LogRecords[0]
	values := record.Body.GetKvlistValue().Values
	if attrValue(values, "integer").GetIntValue() != (1<<53)+1 || !bytes.Equal(attrValue(values, "binary").GetBytesValue(), []byte{0, 1, 255}) ||
		attrValue(values, "null").Value != nil || record.TimeUnixNano != uint64(start.Add(2*time.Second).UnixNano()) || len(record.TraceId) != 16 {
		t.Fatal("typed body/time/context lost")
	}
	if attrValue(resourceLogs.Resource.Attributes, "identity").GetKvlistValue().Values[0].Value.GetIntValue() != (1<<53)+1 ||
		attrValue(resourceLogs.ScopeLogs[0].Scope.Attributes, "scope.identity").GetKvlistValue().Values[0].Value.GetIntValue() != (1<<53)+1 {
		t.Fatal("typed identity lost")
	}
	closed := attrValue(record.Attributes, "typed").GetArrayValue().GetValues()
	if len(closed) != 2 || closed[0].GetIntValue() != (1<<53)+1 || closed[1].Value != nil {
		t.Fatal("public closed-value log attribute lost integer/null data")
	}
	spanRecord := traces.ResourceSpans[0].ScopeSpans[0].Spans[0]
	if spanRecord.Name != "renamed" || spanRecord.StartTimeUnixNano != uint64(start.UnixNano()) || spanRecord.EndTimeUnixNano != uint64(start.Add(3*time.Second).UnixNano()) || len(spanRecord.Links) != 1 || len(spanRecord.Events) != 2 {
		t.Fatal("span mutations/times lost")
	}
	closed = attrValue(spanRecord.Attributes, "typed").GetArrayValue().GetValues()
	if len(closed) != 2 || closed[0].GetIntValue() != (1<<53)+1 || closed[1].Value != nil {
		t.Fatal("public closed-value span attribute lost integer/null data")
	}
	encoded, _ := proto.Marshal(&traces)
	if bytes.Contains(encoded, []byte("private-native-canary")) {
		t.Fatal("private cause escaped")
	}
	nativeMetrics := metrics.ResourceMetrics[0].ScopeMetrics[0].Metrics
	if len(nativeMetrics) != 8 {
		t.Fatal("not all synchronous instruments exported", len(nativeMetrics))
	}
	for _, metric := range nativeMetrics {
		if strings.HasPrefix(metric.Name, "int64") {
			switch metric.Name {
			case "int64-counter", "int64-updowncounter":
				point := metric.GetSum().DataPoints[0]
				if _, ok := point.Value.(*metricdata.NumberDataPoint_AsInt); !ok || point.GetAsInt() != (1<<53)+1 {
					t.Fatal("counter integer field lost")
				}
			case "int64-gauge":
				point := metric.GetGauge().DataPoints[0]
				if _, ok := point.Value.(*metricdata.NumberDataPoint_AsInt); !ok || point.GetAsInt() != (1<<53)+1 {
					t.Fatal("gauge integer field lost")
				}
			case "int64-histogram":
				counts := metric.GetHistogram().DataPoints[0].BucketCounts
				if len(counts) != 3 || counts[0] != 0 || counts[1] != 0 || counts[2] != 1 {
					t.Fatal("integer histogram misclassified", counts)
				}
			}
		}
	}
}

func TestPublicStrictPreparationAndFailures(t *testing.T) {
	configured, err := configsource.Prepare(context.Background(), configsource.Schema[otel.Settings]{Version: 1, Defaults: otel.Settings{Name: "loaded"}, Validate: func(_ context.Context, value otel.Settings) error { return otel.Validate(value) }},
		[]configsource.Layer{{Kind: configsource.Local, Encoding: configsource.JSON, Content: []byte(`{"service_name":"fixture","logs_endpoint":"http://127.0.0.1:1/logs","sample_ratio":0,"queued_calls":0,"resource_typed_attributes":[{"key":"revision","value":{"nodes":[{"kind":"int64","int64":9007199254740993}]}}]}`)}})
	if err != nil {
		t.Fatal("strict Settings failed", err)
	}
	settings, err := configured.ValueCopy()
	if err != nil || settings.SampleRatio == nil || *settings.SampleRatio != 0 || settings.QueuedCalls == nil || *settings.QueuedCalls != 0 {
		t.Fatal("zero semantics", err)
	}
	prepared, err := otel.Prepare(settings)
	if err != nil {
		t.Fatal(err)
	}
	settings.TypedResourceAttributes[0].Value.Nodes[0].Int64 = 0
	one, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	two, err := otel.Compose(prepared, prepared)
	if err != nil {
		t.Fatal(err)
	}
	if two.SourceWorkBytes != 2*one.SourceWorkBytes || two.Runtime.MaxActive != 2*one.Runtime.MaxActive || prepared.Metadata().SourceBytes <= 0 {
		t.Fatal("overlapping source accounting")
	}
	runtime, err := adapters.New(context.Background(), adapters.Options{MaxActive: 1, MaxWorkBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[otel.Result](one.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	if owner, err := prepared.Open(context.Background(), otel.Dependencies{Runtime: runtime, Evidence: inbox}); owner != nil || !errors.Is(err, otel.ErrLimit) {
		t.Fatal("insufficient policy constructed source", err)
	}
	if state, err := inbox.Inspect(); err != nil || state.Outstanding != 0 {
		t.Fatal("preflight acquired evidence", err)
	}
	for _, value := range []otel.Settings{
		{Name: "invalid", ServiceName: "test", LogsEndpoint: "http://127.0.0.1:1/logs", Timeout: valuePointer(time.Duration(0))},
		{Name: "invalid", ServiceName: "test", LogsEndpoint: "http://127.0.0.1:1/logs", SampleRatio: valuePointer(math.NaN())},
	} {
		if err := otel.Validate(value); err == nil {
			t.Fatal("invalid final configuration accepted")
		}
	}
}

func TestPublicDependentDefaultsComeFromFrozenNativePreparation(t *testing.T) {
	value := otel.Settings{Name: "small-queue", ServiceName: "fixture", LogsEndpoint: "http://127.0.0.1:1/logs",
		QueueItems: valuePointer(1), QueueBytes: valuePointer(1024), MaxRecordBytes: valuePointer(1024)}
	prepared, err := otel.Prepare(value)
	if err != nil {
		t.Fatal("omitted batch size did not follow the native queue-dependent default", err)
	}
	metadata := prepared.Metadata()
	if metadata.BatchSize != 1 || metadata.QueueItems != 1 || metadata.QueueBytes != 1024 {
		t.Fatal("dependent native defaults were not frozen", metadata)
	}
	value.BatchSize = valuePointer(0)
	if _, err := otel.Prepare(value); err == nil {
		t.Fatal("explicit zero batch size was replaced by the native default")
	}
}

type forbiddenValue struct{ called *bool }

func TestReturnedSetupErrorsRemainPublicAndAnnotatable(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(peer.Close)
	owner, deps := publicOwner(t, otel.Settings{Name: "safe-errors", ServiceName: "fixture",
		LogsEndpoint: peer.URL + "/logs", TracesEndpoint: peer.URL + "/traces"})
	client := owner.Client()
	receipt, setup := client.Emit(context.Background(), otel.LogRecord{Severity: 25})
	if _, ok := failure.Inspect(setup); !ok {
		t.Fatal("single setup failure lost its public occurrence", setup)
	}
	if _, err := otel.ErrorAttributes(setup); err != nil {
		t.Fatal("own setup error cannot be safely projected", err)
	}
	if _, err := publicResult(t, deps, receipt, nil); !errors.Is(err, otel.ErrInput) {
		t.Fatal("setup evidence lost original failure", err)
	}
	_, failed, startError := client.Start(context.Background(), otel.SpanInput{})
	if failed != nil {
		t.Fatal("invalid start returned a live span")
	}
	if _, ok := failure.Inspect(startError); !ok {
		t.Fatal("single Start failure lost its public occurrence", startError)
	}
	ctx, cancel := boundedContext(t)
	defer cancel()
	delivery, err := deps.Evidence.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	_, span, err := client.Start(context.Background(), otel.SpanInput{Name: "annotation"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = span.End(context.Background()) })
	if err := span.RecordError(setup); err != nil {
		t.Fatal("own Emit error cannot annotate a span", err)
	}
	if err := span.RecordError(startError); err != nil {
		t.Fatal("own Start error cannot annotate a span", err)
	}
	receipt, err = span.End(ctx)
	success(t, deps, receipt, err)
}

func (value forbiddenValue) LogValue() slog.Value { *value.called = true; panic("must not execute") }
func TestPublicRefusalAndSafeErrorProjection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(server.Close)
	owner, deps := publicOwner(t, otel.Settings{Name: "refusal", ServiceName: "fixture", LogsEndpoint: server.URL + "/logs", TracesEndpoint: server.URL + "/traces"})
	client := owner.Client()
	called := false
	receipt, err := client.Emit(context.Background(), otel.LogRecord{Attributes: []slog.Attr{slog.Any("dynamic", forbiddenValue{&called})}})
	_, outcome := publicResult(t, deps, receipt, err)
	if !errors.Is(outcome, otel.ErrUnsupported) || called {
		t.Fatal("dynamic formatter ran or refusal absent", outcome)
	}
	receipt, err = client.Emit(context.Background(), otel.LogRecord{Message: "still healthy"})
	success(t, deps, receipt, err)
	bad := errors.New("private-native-canary")
	if _, err := otel.ErrorAttributes(bad); !errors.Is(err, otel.ErrUnsupported) {
		t.Fatal("unknown error accepted", err)
	}
	original, err := failure.New(otel.Definitions()[0], failure.Location{Operation: "safe"}, bad)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "telemetry_otel", BaseLocale: "en", Resources: otel.Resources(), Directory: "resources", Definitions: otel.Definitions()})
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(catalog)
	if err != nil {
		t.Fatal(err)
	}
	presenter, err = presenter.WithLocale("zh-CN")
	if err != nil {
		t.Fatal(err)
	}
	attributes, err := otel.ErrorAttributes(presenter.Present(original))
	if err != nil {
		t.Fatal(err)
	}
	for _, attr := range attributes {
		if strings.Contains(attr.Value.String(), "private-native-canary") {
			t.Fatal("private cause escaped")
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if receipt, err := client.Emit(canceled, otel.LogRecord{Message: "canceled"}); receipt != nil || err == nil {
		t.Fatal("canceled caller admitted")
	}
	if _, span, err := client.Start(context.Background(), otel.SpanInput{Name: "invalid", Attributes: []slog.Attr{slog.Any("dynamic", forbiddenValue{&called})}}); span != nil || err == nil {
		t.Fatal("failed Start falsely succeeded")
	}
	ctx, stop := boundedContext(t)
	defer stop()
	delivery, err := deps.Evidence.NextReleased(ctx)
	if err != nil {
		t.Fatal("failed Start evidence lost", err)
	}
	evidenceReceipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := evidenceReceipt.WaitReleased(ctx)
	if err != nil || snapshot.Err() == nil {
		t.Fatal("missing accepted failure", err)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
}
