//go:build otel_elastic_service

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
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	zap "github.com/frost-leo/fathomry/internal/logging/zap/v1"
	zerolog "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	otel "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/internal/telemetry/otel/v1/zapbridge"
	"github.com/frost-leo/fathomry/internal/telemetry/otel/v1/zerologbridge"
	"go.opentelemetry.io/otel/codes"
	logapi "go.opentelemetry.io/otel/log"
	traceapi "go.opentelemetry.io/otel/trace"
	sdkzap "go.uber.org/zap"
)

type elasticConfig struct {
	Endpoint        string `json:"endpoint"`
	Username        string `json:"username"`
	Password        string `json:"password"`
	CAPEM           string `json:"ca_pem"`
	ExpectedVersion string `json:"expected_version"`
	Retain          bool   `json:"retain_test_data"`
	ObservationFile string `json:"observation_file"`
}

type elasticObserver struct {
	config elasticConfig
	client *http.Client
}

func elasticFixture(t *testing.T) *elasticObserver {
	t.Helper()
	path := os.Getenv("FATHOMRY_OTEL_ELASTIC_TEST_CONFIG")
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("explicit private Elastic fixture required")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 128<<10 {
		t.Fatal("private fixture size/permissions invalid")
	}
	var config elasticConfig
	decoder := json.NewDecoder(io.LimitReader(file, 128<<10+1))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) || config.ExpectedVersion == "" ||
		config.Username == "" || config.Password == "" {
		t.Fatal("private Elastic fixture invalid")
	}
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		t.Fatal("fixture must select explicit HTTPS endpoint")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(config.CAPEM)) {
		t.Fatal("fixture CA invalid")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
	observer := &elasticObserver{config: config, client: &http.Client{Transport: transport, Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}}
	t.Cleanup(transport.CloseIdleConnections)
	return observer
}
func (observer *elasticObserver) request(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(observer.config.Endpoint, "/")+path, bytes.NewReader(data))
	if err != nil {
		return 0, nil, err
	}
	request.SetBasicAuth(observer.config.Username, observer.config.Password)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := observer.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err = io.ReadAll(io.LimitReader(response.Body, 2<<20+1))
	if len(data) > 2<<20 {
		return response.StatusCode, nil, errors.New("bounded response exceeded")
	}
	return response.StatusCode, data, err
}
func (observer *elasticObserver) must(t *testing.T, ctx context.Context, method, path string, body any) []byte {
	t.Helper()
	status, data, err := observer.request(ctx, method, path, body)
	if err != nil || status < 200 || status >= 300 {
		t.Fatalf("Elastic operation failed: method=%s status=%d transport_failed=%t", method, status, err != nil)
	}
	return data
}
func drainService[T any](t *testing.T, ctx context.Context, inbox *invocation.Inbox[T]) {
	t.Helper()
	for inbox.Usage().Outstanding > 0 {
		delivery, err := inbox.Next(ctx)
		if err != nil {
			t.Fatal("independent evidence unavailable")
		}
		result, err := delivery.Receipt().WaitReleased(ctx)
		if err != nil || result.Err() != nil {
			t.Fatal("accepted service operation failed")
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestElasticServiceSignalsAndLoggingBridges(t *testing.T) {
	observer := elasticFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var server struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if json.Unmarshal(observer.must(t, ctx, "GET", "/", nil), &server) != nil || server.Version.Number != observer.config.ExpectedVersion {
		t.Fatal("unqualified Elastic version")
	}
	var random [6]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	namespace := "gh34" + hex.EncodeToString(random[:])
	streams := []string{"logs-fathomry34.otel-" + namespace, "traces-fathomry34.otel-" + namespace, "metrics-fathomry34.otel-" + namespace}
	for _, stream := range streams {
		status, _, err := observer.request(ctx, "GET", "/_data_stream/"+stream, nil)
		if err != nil || status != 404 {
			t.Fatal("isolated test stream already exists or cannot be checked")
		}
	}
	t.Logf("Elastic version=%s test_namespace=%s", server.Version.Number, namespace)
	began := time.Now().UTC()
	retain := false
	t.Cleanup(func() {
		if retain {
			t.Logf("Retained owner-requested test streams: %s", strings.Join(streams, ","))
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		for _, stream := range streams {
			status, _, err := observer.request(cleanup, "DELETE", "/_data_stream/"+stream, nil)
			if err != nil || status != 200 && status != 404 {
				t.Errorf("test stream cleanup failed: %s status=%d", stream, status)
			}
			status, _, err = observer.request(cleanup, "GET", "/_data_stream/"+stream, nil)
			if err != nil || status != 404 {
				t.Errorf("test stream cleanup unconfirmed: %s", stream)
			}
		}
	})
	authRequest, _ := http.NewRequest("GET", "https://example.invalid", nil)
	authRequest.SetBasicAuth(observer.config.Username, observer.config.Password)
	endpoint := strings.TrimRight(observer.config.Endpoint, "/") + "/_otlp/v1/"
	options := otel.OptionsV1{Name: "elastic-otel", ServiceName: "fathomry-gh34-acceptance", Scope: "fathomry.issue34", ScopeVersion: "1",
		LogsEndpoint: endpoint + "logs", TracesEndpoint: endpoint + "traces", MetricsEndpoint: endpoint + "metrics",
		Headers: map[string]string{"Authorization": authRequest.Header.Get("Authorization")}, TLS: &otel.TLSV1{CA: observer.config.CAPEM},
		ResourceAttributes: map[string]string{"data_stream.dataset": "fathomry34", "data_stream.namespace": namespace, "test.marker": namespace},
		Instruments: []otel.InstrumentV1{{Name: "fathomry34.rows", Kind: "int64-counter", Unit: "{row}"},
			{Name: "fathomry34.inflight", Kind: "int64-gauge", Unit: "{item}"},
			{Name: "fathomry34.latency", Kind: "float64-histogram", Unit: "s", Boundaries: []float64{0.1, 0.5, 1, 5}}}}
	selected, err := otel.Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, otel.LimitsV1(options))
	assembly, err := resource.Assemble(ctx, ctx, "elastic", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := assembly.Close(cleanup); err != nil {
			t.Error("telemetry cleanup failed", err)
		}
	})
	inbox, _ := invocation.NewInbox[otel.Result](64, 64*otel.EvidenceBytesV1(options))
	client, err := otel.Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	work, span, err := client.Start(ctx, fault.Correlation{Call: "service-span", Owner: namespace}, otel.SpanInput{Name: "elastic-visible-work"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = span.End(context.Background()) })
	receipt, err := client.Emit(work, fault.Correlation{Call: "native-log", Owner: namespace}, otel.LogRecord{Message: "fathomry34 direct log",
		Severity: logapi.SeverityInfo, Attributes: []slog.Attr{slog.Int64("rows", 15), slog.Bool("verified", true)}})
	if result := checked(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	zapOptions := zap.OptionsV1{Name: "elastic-zap"}
	zapSelection, err := zap.Select(zapOptions, zapbridge.New(client))
	if err != nil {
		t.Fatal(err)
	}
	zapSelection = resource.WithLimits(zapSelection, zap.LimitsV1(zapOptions))
	zapAssembly, err := resource.Assemble(ctx, ctx, "elastic-zap", zapSelection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := zapAssembly.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	zapInbox, _ := invocation.NewInbox[zap.Result](4, 1<<20)
	zapLogger, err := zap.Bind(zapAssembly, zapSelection, zapInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	zapReceipt, err := zapLogger.Log(work, fault.Correlation{Call: "zap-log", Owner: namespace}, 0, "fathomry34 zap log", sdkzap.Int64("rows", 15))
	if result := checked(t, zapReceipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	var local bytes.Buffer
	zeroOptions := zerolog.OptionsV1{Name: "elastic-zero", Sinks: []zerolog.SinkV1{{Name: "otel", Records: zerologbridge.New(client)}, {Name: "local", Writer: &local}}}
	zeroSelection, err := zerolog.Select(zeroOptions)
	if err != nil {
		t.Fatal(err)
	}
	zeroSelection = resource.WithLimits(zeroSelection, zerolog.LimitsV1(zeroOptions))
	zeroAssembly, err := resource.Assemble(ctx, ctx, "elastic-zero", zeroSelection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := zeroAssembly.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	zeroInbox, _ := invocation.NewInbox[zerolog.Result](4, 1<<20)
	zeroLogger, err := zerolog.Bind(zeroAssembly, zeroSelection, zeroInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	zeroReceipt, err := zeroLogger.Log(work, fault.Correlation{Call: "zero-log", Owner: namespace}, zerolog.Info, "fathomry34 zerolog log", slog.Int64("rows", 15))
	if result := checked(t, zeroReceipt, err); result.Err() != nil || !strings.Contains(local.String(), "fathomry34 zerolog log") {
		t.Fatal("local logging/bridge failed")
	}
	for point := 1; point <= 5; point++ {
		receipt, err := client.MeasureInt64(work, fault.Correlation{Call: "rows", Owner: namespace}, "fathomry34.rows", int64(point))
		if result := checked(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		receipt, err = client.MeasureInt64(work, fault.Correlation{Call: "inflight", Owner: namespace}, "fathomry34.inflight", int64(6-point))
		if result := checked(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		receipt, err = client.MeasureFloat64(work, fault.Correlation{Call: "latency", Owner: namespace}, "fathomry34.latency", float64(point)/10)
		if result := checked(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		receipt, err = client.Flush(ctx, fault.Correlation{Call: "flush", Owner: namespace})
		result := checked(t, receipt, err)
		if result.Err() != nil {
			for _, signal := range result.Outcome.Value.SignalsCopy() {
				t.Logf("signal=%s submitted=%d acknowledged=%d rejected=%d effect=%s error=%v", signal.Signal, signal.Submitted, signal.Acknowledged, signal.Rejected, signal.Effect, signal.Err)
			}
			t.Fatal("real native OTLP export failed")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := span.SetStatus(codes.Ok, ""); err != nil {
		t.Fatal(err)
	}
	_, err = span.End(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := zeroAssembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := zapAssembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := assembly.Close(ctx); err != nil {
		t.Fatal("final telemetry cleanup failed", err)
	}
	drainService(t, ctx, inbox)
	drainService(t, ctx, zapInbox)
	drainService(t, ctx, zeroInbox)
	observer.must(t, ctx, "POST", "/"+strings.Join(streams, ",")+"/_refresh", nil)
	documents := make(map[string]json.RawMessage)
	counts := make(map[string]int)
	observed := make(map[string][]map[string]any)
	for _, stream := range streams {
		data := observer.must(t, ctx, "POST", "/"+stream+"/_search", map[string]any{"size": 50, "sort": []any{map[string]string{"@timestamp": "asc"}}})
		var result struct {
			Hits struct {
				Hits []struct {
					Source json.RawMessage `json:"_source"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if json.Unmarshal(data, &result) != nil || len(result.Hits.Hits) == 0 {
			t.Fatal("independent backend query found no test data")
		}
		counts[stream] = len(result.Hits.Hits)
		documents[stream] = data
		for _, hit := range result.Hits.Hits {
			var source map[string]any
			if json.Unmarshal(hit.Source, &source) != nil {
				t.Fatal("invalid indexed test document")
			}
			observed[stream] = append(observed[stream], source)
		}
	}
	verifyElasticDocuments(t, observed, streams, namespace, traceapi.SpanContextFromContext(work))
	ended := time.Now().UTC()
	if observer.config.ObservationFile != "" {
		data, err := json.MarshalIndent(struct {
			Version, Namespace, Start, End string
			Counts                         map[string]int
			Documents                      map[string]json.RawMessage
		}{
			server.Version.Number, namespace, began.Format(time.RFC3339Nano), ended.Format(time.RFC3339Nano), counts, documents}, "", "  ")
		if err != nil || os.WriteFile(observer.config.ObservationFile, data, 0600) != nil {
			t.Fatal("cannot retain bounded synthetic service observations")
		}
	}
	t.Logf("Queried test streams successfully: counts=%v UTC_start=%s UTC_end=%s", counts, began.Format(time.RFC3339Nano), ended.Format(time.RFC3339Nano))
	retain = observer.config.Retain
}

func object(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func verifyElasticDocuments(t *testing.T, observed map[string][]map[string]any, streams []string, namespace string, span traceapi.SpanContext) {
	t.Helper()
	for _, documents := range observed {
		for _, document := range documents {
			attributes := object(object(document["resource"])["attributes"])
			if attributes["service.name"] != "fathomry-gh34-acceptance" || attributes["test.marker"] != namespace ||
				object(document["scope"])["name"] != "fathomry.issue34" || object(document["data_stream"])["namespace"] != namespace {
				t.Fatal("backend resource/scope/isolation metadata changed")
			}
		}
	}
	if len(observed[streams[0]]) != 3 || len(observed[streams[1]]) != 1 {
		t.Fatal("backend event count changed")
	}
	seen := make(map[string]bool)
	for _, document := range observed[streams[0]] {
		if document["trace_id"] != span.TraceID().String() || document["span_id"] != span.SpanID().String() || document["severity_number"] != float64(9) {
			t.Fatal("backend log association or severity changed")
		}
		attributes := object(document["attributes"])
		call, _ := attributes["fathomry.call"].(string)
		if seen[call] {
			t.Fatal("backend duplicated an event")
		}
		seen[call] = true
		fields := attributes
		if call != "native-log" {
			fields = object(attributes["attributes"])
		}
		if fields["rows"] != float64(15) {
			t.Fatal("backend structured integer changed")
		}
		body := object(document["body"])["text"]
		if body != map[string]string{"native-log": "fathomry34 direct log", "zap-log": "fathomry34 zap log", "zero-log": "fathomry34 zerolog log"}[call] {
			t.Fatal("backend log body changed")
		}
	}
	trace := observed[streams[1]][0]
	if trace["trace_id"] != span.TraceID().String() || trace["span_id"] != span.SpanID().String() || trace["name"] != "elastic-visible-work" ||
		object(trace["status"])["code"] != "Ok" || trace["duration"].(float64) <= 0 {
		t.Fatal("backend span changed")
	}
	points := make(map[string][]float64)
	for _, document := range observed[streams[2]] {
		for name, value := range object(document["metrics"]) {
			if name == "fathomry34.latency" {
				value = object(value)["sum"]
			}
			number, ok := value.(float64)
			if !ok {
				t.Fatal("backend numeric metric lost its type")
			}
			if name != "fathomry34.inflight" && document["temporality"] != "cumulative" {
				t.Fatal("backend temporality changed")
			}
			points[name] = append(points[name], number)
		}
	}
	for name, expected := range map[string][]float64{"fathomry34.rows": {1, 3, 6, 10, 15, 15}, "fathomry34.inflight": {5, 4, 3, 2, 1, 1}, "fathomry34.latency": {0.1, 0.3, 0.6, 1, 1.5, 1.5}} {
		if len(points[name]) != len(expected) {
			t.Fatal("backend metric sample count changed")
		}
		for index, value := range expected {
			if math.Abs(points[name][index]-value) > 1e-9 {
				t.Fatal("backend aggregation changed")
			}
		}
	}
}
