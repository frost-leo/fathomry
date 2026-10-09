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

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"time"

	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	traceapi "go.opentelemetry.io/otel/trace"
	logpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := direct(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("otel direct public consumer passed")
}

type protocolPeer struct {
	server      *httptest.Server
	mu          sync.Mutex
	logs        logpb.ExportLogsServiceRequest
	traces      tracepb.ExportTraceServiceRequest
	metrics     metricpb.ExportMetricsServiceRequest
	requests    int
	connections int
	active      map[net.Conn]bool
	changed     chan struct{}
}

func newPeer() *protocolPeer {
	peer := &protocolPeer{active: make(map[net.Conn]bool), changed: make(chan struct{})}
	peer.server = httptest.NewUnstartedServer(http.HandlerFunc(peer.serveHTTP))
	peer.server.Config.ConnState = func(connection net.Conn, state http.ConnState) {
		peer.mu.Lock()
		defer peer.mu.Unlock()
		switch state {
		case http.StateNew:
			peer.connections++
			peer.active[connection] = true
		case http.StateClosed, http.StateHijacked:
			delete(peer.active, connection)
		default:
			return
		}
		close(peer.changed)
		peer.changed = make(chan struct{})
	}
	peer.server.Start()
	return peer
}

func (peer *protocolPeer) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	data, err := io.ReadAll(io.LimitReader(request.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 || request.Header.Get("Content-Type") != "application/x-protobuf" {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	peer.requests++
	switch request.URL.Path {
	case "/logs":
		err = proto.Unmarshal(data, &peer.logs)
	case "/traces":
		err = proto.Unmarshal(data, &peer.traces)
	case "/metrics":
		err = proto.Unmarshal(data, &peer.metrics)
	default:
		err = errors.New("unexpected telemetry endpoint")
	}
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	writer.Header().Set("Content-Type", "application/x-protobuf")
}

func (peer *protocolPeer) requestCount() int {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return peer.requests
}

func (peer *protocolPeer) waitClosed(ctx context.Context) error {
	for {
		peer.mu.Lock()
		closed := peer.connections > 0 && len(peer.active) == 0
		changed := peer.changed
		peer.mu.Unlock()
		if closed {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return errors.New("native cleanup did not close the observed peer connections")
		}
	}
}

func cleanup(close func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = close(ctx)
}

func complete(ctx context.Context, inbox *adapters.Inbox[otel.Result], peer *protocolPeer, receipt *adapters.Receipt[otel.Result], submission error) (otel.Result, error) {
	if submission != nil {
		return otel.Result{}, submission
	}
	if receipt == nil {
		return otel.Result{}, errors.New("accepted operation has no public receipt")
	}
	direct, err := receipt.WaitReleased(ctx)
	if err != nil {
		return otel.Result{}, err
	}
	if err := direct.Err(); err != nil {
		return otel.Result{}, err
	}
	status, err := inbox.Inspect()
	if err != nil || status.Outstanding < 2 {
		return otel.Result{}, errors.New("direct observation prematurely acknowledged independent evidence")
	}
	delivery, err := inbox.NextReleased(ctx)
	if err != nil {
		return otel.Result{}, err
	}
	observedReceipt, err := delivery.Receipt()
	if err != nil {
		return otel.Result{}, err
	}
	observed, err := observedReceipt.WaitReleased(ctx)
	if err != nil || observed.Info() != direct.Info() || !observed.Info().Released || observed.Err() != nil {
		return otel.Result{}, errors.New("independent operation attribution or outcome changed")
	}
	before := peer.requestCount()
	if err := delivery.Retry(); err != nil {
		return otel.Result{}, err
	}
	delivery, err = inbox.NextReleased(ctx)
	if err != nil {
		return otel.Result{}, err
	}
	again, err := delivery.Receipt()
	if err != nil {
		return otel.Result{}, err
	}
	repeated, err := again.WaitReleased(ctx)
	if err != nil || repeated.Info() != direct.Info() || peer.requestCount() != before {
		return otel.Result{}, errors.New("evidence retry changed identity or repeated native export")
	}
	value, present := direct.ValueCopy()
	independent, independentlyPresent := observed.ValueCopy()
	if !present || !independentlyPresent || value.Attribution() != independent.Attribution() || value.Source().Provider != otel.ProviderID {
		return otel.Result{}, errors.New("public result lost source attribution")
	}
	directSignals, independentSignals := value.SignalsCopy(), independent.SignalsCopy()
	if len(directSignals) != len(independentSignals) {
		return otel.Result{}, errors.New("independent evidence changed the signal set")
	}
	for index, signal := range directSignals {
		observed := independentSignals[index]
		if signal.Signal != observed.Signal || signal.Accepted != observed.Accepted || signal.Submitted != observed.Submitted ||
			signal.Acknowledged != observed.Acknowledged || signal.Rejected != observed.Rejected || signal.TransportCalls != observed.TransportCalls ||
			signal.Sampled != observed.Sampled || signal.Effect != observed.Effect || signal.Err != nil || observed.Err != nil {
			return otel.Result{}, errors.New("independent evidence changed signal-specific facts")
		}
	}
	if err := delivery.Ack(); err != nil {
		return otel.Result{}, err
	}
	return value, nil
}

func direct(ctx context.Context) error {
	peer := newPeer()
	defer peer.server.Close()
	identity, err := otel.Tree(otel.Value{Kind: "int64", Int64: (1 << 53) + 1})
	if err != nil {
		return err
	}
	prepared, err := otel.Prepare(otel.Settings{Name: "direct", ServiceName: "public-consumer",
		LogsEndpoint: peer.server.URL + "/logs", TracesEndpoint: peer.server.URL + "/traces", MetricsEndpoint: peer.server.URL + "/metrics",
		TypedResourceAttributes: []otel.ConfigAttribute{{Key: "build", Value: identity}},
		Instruments:             []otel.Instrument{{Name: "size", Kind: "int64-histogram", Boundaries: []float64{1 << 53}, AttributeKeys: []string{"operation"}}}})
	if err != nil {
		return err
	}
	policy, err := prepared.Policy()
	if err != nil {
		return err
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer cleanup(runtime.Close)
	inbox, err := adapters.NewInbox[otel.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := prepared.Open(ctx, otel.Dependencies{Runtime: runtime, Evidence: inbox})
	if owner != nil {
		defer cleanup(owner.Close)
	}
	if err != nil {
		return err
	}
	client, err := owner.Client().WithID("independent-direct")
	if err != nil {
		return err
	}
	profile, err := client.Profile(ctx)
	if err != nil || profile.Protocol.Value != "otlp-http-protobuf" || owner.Info().Name != "direct" {
		return errors.New("prepared source facts were not independently observable")
	}
	state, err := traceapi.ParseTraceState("vendor=direct")
	if err != nil {
		return err
	}
	parent := traceapi.NewSpanContext(traceapi.SpanContextConfig{TraceID: traceapi.TraceID{1, 2, 3, 4}, SpanID: traceapi.SpanID{5, 6, 7, 8},
		TraceFlags: traceapi.FlagsSampled, TraceState: state, Remote: true})
	start := time.Unix(1_600_000_000, 42)
	spanContext, span, err := client.Start(traceapi.ContextWithRemoteSpanContext(ctx, parent), otel.SpanInput{Name: "starting", Time: start})
	if err != nil {
		return err
	}
	defer func() { _, _ = span.End(context.Background()) }()
	if !span.IsRecording() || span.SpanContext().TraceState().String() != "vendor=direct" {
		return errors.New("public span lost recording or inherited context facts")
	}
	headers, err := otel.Inject(spanContext, false)
	if err != nil {
		return err
	}
	extracted, err := otel.Extract(ctx, headers, false)
	if err != nil || traceapi.SpanContextFromContext(extracted).SpanID() != span.SpanContext().SpanID() {
		return errors.New("public propagation lost span identity")
	}
	if err := span.SetName("direct-work"); err != nil {
		return err
	}
	if err := span.AddLink(otel.Link{Context: parent}); err != nil {
		return err
	}
	if err := span.AddEventAt("measured", start.Add(time.Second), slog.Int64("exact", (1<<53)+1)); err != nil {
		return err
	}
	body := otel.Value{Kind: "map", Map: []otel.TypedAttribute{{Key: "exact", Value: otel.Value{Kind: "int64", Int64: (1 << 53) + 1}}}}
	receipt, err := client.Emit(extracted, otel.LogRecord{Body: &body, Time: start.Add(time.Second), Severity: 9, EventName: "direct-event"})
	logged, err := complete(ctx, inbox, peer, receipt, err)
	if err != nil || len(logged.SignalsCopy()) != 1 || logged.SignalsCopy()[0].Accepted != 1 || logged.SignalsCopy()[0].Effect != otel.NotAttempted {
		return errors.Join(err, errors.New("log acceptance was not distinct from export"))
	}
	body.Map[0].Value.Int64 = 0
	receipt, err = client.MeasureInt64(spanContext, "size", (1<<53)+1, slog.String("operation", "direct"))
	if _, err = complete(ctx, inbox, peer, receipt, err); err != nil {
		return err
	}
	receipt, err = span.EndAt(ctx, start.Add(2*time.Second))
	if _, err = complete(ctx, inbox, peer, receipt, err); err != nil {
		return err
	}
	if peer.requestCount() != 0 {
		return errors.New("source started an unrequested background export")
	}
	receipt, err = client.Flush(ctx)
	flushed, err := complete(ctx, inbox, peer, receipt, err)
	if err != nil || len(flushed.SignalsCopy()) != 3 {
		return errors.Join(err, errors.New("manual flush did not report all three signals"))
	}
	for _, signal := range flushed.SignalsCopy() {
		if signal.Submitted != 1 || signal.Acknowledged != 1 || signal.Effect != otel.Acknowledged {
			return errors.New("manual export did not preserve signal-specific receiver outcomes")
		}
	}
	if err := peer.verify(span.SpanContext(), start); err != nil {
		return err
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		return errors.Join(err, errors.New("native source cleanup remained incomplete"))
	}
	if err := peer.waitClosed(ctx); err != nil {
		return err
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	statistics, err := runtime.Inspect()
	if err != nil || statistics.Active != 0 || statistics.WorkBytes != 0 || !statistics.Closed {
		return errors.New("source cleanup left public runtime work owned")
	}
	if err := inbox.Seal(); err != nil {
		return err
	}
	delivery, err := inbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	final, err := delivery.Receipt()
	if err != nil {
		return err
	}
	snapshot, err := final.WaitReleased(ctx)
	if err != nil || snapshot.Err() != nil || snapshot.Info().Operation != "telemetry.otel.open" || !snapshot.Info().Released {
		return errors.New("independent receiver lost the final source cleanup outcome")
	}
	if err := delivery.Ack(); err != nil {
		return err
	}
	if _, err := inbox.NextReleased(ctx); !errors.Is(err, io.EOF) {
		return errors.New("sealed evidence did not finish after final cleanup acknowledgement")
	}
	status, err := inbox.Inspect()
	if err != nil || status.Outstanding != 0 || status.Bytes != 0 {
		return errors.New("source cleanup evidence remained owned")
	}
	return nil
}

func attribute(fields []*commonpb.KeyValue, key string) *commonpb.AnyValue {
	for _, field := range fields {
		if field.Key == key {
			return field.Value
		}
	}
	return nil
}

func (peer *protocolPeer) verify(span traceapi.SpanContext, start time.Time) error {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.requests != 3 || len(peer.logs.ResourceLogs) != 1 || len(peer.traces.ResourceSpans) != 1 || len(peer.metrics.ResourceMetrics) != 1 {
		return errors.New("independent peer did not decode exactly three manual signal exports")
	}
	logs := peer.logs.ResourceLogs[0]
	if attribute(logs.Resource.Attributes, "build").GetIntValue() != (1<<53)+1 || len(logs.ScopeLogs) != 1 || len(logs.ScopeLogs[0].LogRecords) != 1 {
		return errors.New("typed resource identity or log count changed on wire")
	}
	record := logs.ScopeLogs[0].LogRecords[0]
	traceID, spanID := span.TraceID(), span.SpanID()
	if !bytes.Equal(record.TraceId, traceID[:]) || !bytes.Equal(record.SpanId, spanID[:]) || record.TimeUnixNano != uint64(start.Add(time.Second).UnixNano()) ||
		record.EventName != "direct-event" || attribute(record.Body.GetKvlistValue().GetValues(), "exact").GetIntValue() != (1<<53)+1 {
		return errors.New("public log lost copied typed body, timestamp or propagated trace context")
	}
	traces := peer.traces.ResourceSpans[0]
	if len(traces.ScopeSpans) != 1 || len(traces.ScopeSpans[0].Spans) != 1 {
		return errors.New("independent peer lost the native span")
	}
	trace := traces.ScopeSpans[0].Spans[0]
	if trace.Name != "direct-work" || len(trace.Links) != 1 || trace.Links[0].TraceState != "vendor=direct" || len(trace.Events) != 1 ||
		trace.StartTimeUnixNano != uint64(start.UnixNano()) || trace.EndTimeUnixNano != uint64(start.Add(2*time.Second).UnixNano()) {
		return errors.New("public span lost name, exact timestamps or link trace-state")
	}
	metrics := peer.metrics.ResourceMetrics[0]
	if len(metrics.ScopeMetrics) != 1 || len(metrics.ScopeMetrics[0].Metrics) != 1 {
		return errors.New("independent peer lost the declared metric")
	}
	histogram := metrics.ScopeMetrics[0].Metrics[0].GetHistogram()
	if histogram == nil || len(histogram.DataPoints) != 1 {
		return errors.New("declared int64 histogram changed native family")
	}
	point := histogram.DataPoints[0]
	if point.Count != 1 || len(point.BucketCounts) != 2 || point.BucketCounts[0] != 0 || point.BucketCounts[1] != 1 ||
		len(point.ExplicitBounds) != 1 || point.ExplicitBounds[0] != 1<<53 || attribute(point.Attributes, "operation").GetStringValue() != "direct" {
		return errors.New("int64 histogram comparison lost exact bucket placement")
	}
	return nil
}
