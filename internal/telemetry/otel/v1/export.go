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
	"io"
	"net/http"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	collog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetric "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Signal identifies a telemetry signal, not an execution or business result.
type Signal string

const (
	Logs    Signal = "logs"
	Traces  Signal = "traces"
	Metrics Signal = "metrics"
)

// Effect describes one export boundary. Acknowledged is an OTLP receiver
// response, never indexing, storage durability or an Item/Run terminal state.
type Effect string

const (
	NotAttempted  Effect = ""
	UnknownEffect Effect = "unknown"
	Acknowledged  Effect = "acknowledged"
	PartialEffect Effect = "partial"
)

// SignalResult is copied evidence without payloads/endpoints. Accepted counts
// records queued or measurements recorded; Submitted counts items passed to the
// exporter. Acknowledged/Rejected count receiver-declared data items, not calls.
// TransportCalls counts RoundTrip entries, not every TCP/native wire attempt.
// Effect=Unknown retains uncertainty; errors never prove zero external effect.
type SignalResult struct {
	private
	Signal                                                      Signal
	Accepted, Submitted, Acknowledged, Rejected, TransportCalls int
	Sampled                                                     bool
	Effect                                                      Effect
	Err                                                         error
}

// Result is immutable and shared by receipt and Inbox. Error objects are retained
// for deliberate errors.Is/As inspection, not cloned or safe raw presentation.
type Result struct {
	private
	signals []SignalResult
}

func (result Result) SignalsCopy() []SignalResult {
	return append([]SignalResult(nil), result.signals...)
}
func successful(result SignalResult) invocation.Outcome[Result] {
	return invocation.Outcome[Result]{Value: Result{signals: []SignalResult{result}}, Present: true}
}

// Flush explicitly drains bounded event batches and collects cumulative metrics.
// Signals run logs, traces, metrics; failure stops only the affected signal unless
// the caller context is canceled. Submitted event batches leave the queue even
// after unknown effects; unsubmitted batches stay queued. There are no SDK retries,
// background exports or automatic re-enqueue. Metrics are cumulative snapshots.
// All export errors reach the independent receipt/Inbox, never otel.Handle.
func (client *Client) Flush(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	return client.run(ctx, id, "flush", func(work context.Context) invocation.Outcome[Result] {
		result, err := client.owner.flush(work)
		return invocation.Outcome[Result]{Value: result, Present: true, Primary: err}
	})
}

func (owner *owner) makeExporters(ctx context.Context) error {
	value := owner.settings
	if value.LogsEndpoint != "" {
		compression := otlploghttp.NoCompression
		if value.Compression == "gzip" {
			compression = otlploghttp.GzipCompression
		}
		exporter, err := otlploghttp.New(ctx, otlploghttp.WithEndpointURL(value.LogsEndpoint),
			otlploghttp.WithHeaders(value.Headers), otlploghttp.WithCompression(compression),
			otlploghttp.WithTimeout(value.Timeout), otlploghttp.WithMaxRequestSize(value.MaxRequestBytes),
			otlploghttp.WithRetry(otlploghttp.RetryConfig{Enabled: false}), otlploghttp.WithHTTPClient(owner.httpClient))
		if exporter != nil {
			owner.logExporter = exporter
		}
		if err != nil {
			return nativeFailure(ErrInput, "logs-exporter", ctx, err)
		}
	}
	if value.TracesEndpoint != "" {
		compression := otlptracehttp.NoCompression
		if value.Compression == "gzip" {
			compression = otlptracehttp.GzipCompression
		}
		exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(value.TracesEndpoint),
			otlptracehttp.WithHeaders(value.Headers), otlptracehttp.WithCompression(compression),
			otlptracehttp.WithEncoding(otlptracehttp.EncodingProtobuf),
			otlptracehttp.WithTimeout(value.Timeout), otlptracehttp.WithMaxRequestSize(value.MaxRequestBytes),
			otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}), otlptracehttp.WithHTTPClient(owner.httpClient))
		if exporter != nil {
			owner.traceExporter = exporter
		}
		if err != nil {
			return nativeFailure(ErrInput, "traces-exporter", ctx, err)
		}
	}
	if value.MetricsEndpoint != "" {
		compression := otlpmetrichttp.NoCompression
		if value.Compression == "gzip" {
			compression = otlpmetrichttp.GzipCompression
		}
		exporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(value.MetricsEndpoint),
			otlpmetrichttp.WithHeaders(value.Headers), otlpmetrichttp.WithCompression(compression),
			otlpmetrichttp.WithTimeout(value.Timeout), otlpmetrichttp.WithMaxRequestSize(value.MaxRequestBytes),
			otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{Enabled: false}), otlpmetrichttp.WithHTTPClient(owner.httpClient))
		if exporter != nil {
			owner.metricExporter = exporter
		}
		if err != nil {
			return nativeFailure(ErrInput, "metrics-exporter", ctx, err)
		}
	}
	return nil
}
func (owner *owner) flush(ctx context.Context) (Result, error) {
	result := Result{}
	var causes []error
	if owner.logExporter != nil {
		entry := SignalResult{Signal: Logs}
		for len(owner.pendingLogs) > 0 {
			if ctx.Err() != nil {
				entry.Err = nativeFailure(ErrExport, "logs-flush", ctx, ctx.Err())
				break
			}
			count := owner.logBatchCount()
			records := make([]sdklog.Record, count)
			charge := 0
			for index, item := range owner.pendingLogs[:count] {
				records[index] = item.record
				charge += item.bytes
			}
			part := exportBatch(ctx, Logs, count, func(work context.Context) error { return owner.logExporter.Export(work, records) })
			owner.unreserve(charge, count)
			clear(owner.pendingLogs[:count])
			owner.pendingLogs = owner.pendingLogs[count:]
			combine(&entry, part)
			if part.Err != nil {
				break
			}
		}
		result.signals = append(result.signals, entry)
		causes = append(causes, entry.Err)
	}
	if owner.traceExporter != nil {
		entry := SignalResult{Signal: Traces}
		for len(owner.pendingSpans) > 0 {
			if ctx.Err() != nil {
				entry.Err = nativeFailure(ErrExport, "traces-flush", ctx, ctx.Err())
				break
			}
			count := min(owner.settings.BatchSize, len(owner.pendingSpans), max(1, owner.settings.MaxRequestBytes/owner.settings.MaxRecordBytes))
			part := exportBatch(ctx, Traces, count, func(work context.Context) error {
				return owner.traceExporter.ExportSpans(work, owner.pendingSpans[:count])
			})
			owner.unreserve(count*owner.settings.MaxRecordBytes, count)
			clear(owner.pendingSpans[:count])
			owner.pendingSpans = owner.pendingSpans[count:]
			combine(&entry, part)
			if part.Err != nil {
				break
			}
		}
		result.signals = append(result.signals, entry)
		causes = append(causes, entry.Err)
	}
	if owner.metricExporter != nil && owner.reader != nil {
		entry := SignalResult{Signal: Metrics}
		var data metricdata.ResourceMetrics
		if err := owner.reader.Collect(ctx, &data); err != nil {
			entry.Err = nativeFailure(ErrExport, "collect", ctx, err)
		} else if count := dataPoints(data); count > 0 {
			entry = exportBatch(ctx, Metrics, count, func(work context.Context) error { return owner.metricExporter.Export(work, &data) })
		}
		result.signals = append(result.signals, entry)
		causes = append(causes, entry.Err)
	}
	return result, joined(ErrExport, "flush", causes...)
}

func (owner *owner) logBatchCount() int {
	count, bytes := 0, 0
	for _, record := range owner.pendingLogs[:min(owner.settings.BatchSize, len(owner.pendingLogs))] {
		if count > 0 && record.bytes > owner.settings.MaxRequestBytes-bytes {
			break
		}
		count++
		bytes += record.bytes
	}
	return count
}
func dataPoints(data metricdata.ResourceMetrics) int {
	count := 0
	for _, scope := range data.ScopeMetrics {
		for _, metric := range scope.Metrics {
			switch data := metric.Data.(type) {
			case metricdata.Sum[int64]:
				count += len(data.DataPoints)
			case metricdata.Sum[float64]:
				count += len(data.DataPoints)
			case metricdata.Gauge[int64]:
				count += len(data.DataPoints)
			case metricdata.Gauge[float64]:
				count += len(data.DataPoints)
			case metricdata.Histogram[int64]:
				count += len(data.DataPoints)
			case metricdata.Histogram[float64]:
				count += len(data.DataPoints)
			}
		}
	}
	return count
}
func combine(result *SignalResult, part SignalResult) {
	result.Submitted += part.Submitted
	result.Acknowledged += part.Acknowledged
	result.Rejected += part.Rejected
	result.TransportCalls += part.TransportCalls
	if part.Effect != NotAttempted {
		result.Effect = part.Effect
	}
	result.Err = part.Err
}

type exchangeKey struct{}
type exchange struct {
	signal                Signal
	count                 int
	calls                 int
	acknowledged, partial bool
	rejected              int
}

func exportBatch(ctx context.Context, signal Signal, count int, export func(context.Context) error) SignalResult {
	observed := &exchange{signal: signal, count: count}
	work := context.WithValue(valueFreeContext{ctx}, operationKey{}, true)
	if scope, ok := ctx.Value(scopeKey{}).(invocation.Scope); ok {
		work = context.WithValue(work, scopeKey{}, scope)
	}
	work = context.WithValue(work, exchangeKey{}, observed)
	work = context.WithValue(work, networkContextKey{}, work)
	err := export(work)
	result := SignalResult{Signal: signal, Submitted: count, TransportCalls: observed.calls}
	if observed.calls > 0 {
		result.Effect = UnknownEffect
	}
	kind := ErrExport
	if observed.acknowledged {
		result.Effect = Acknowledged
		result.Acknowledged = count - observed.rejected
		result.Rejected = observed.rejected
		if observed.partial {
			result.Effect = PartialEffect
			kind = ErrPartial
			if err == nil {
				err = failure(ErrPartial, "receiver-response")
			}
		}
	}
	result.Err = nativeFailure(kind, "export", ctx, err)
	return result
}

type boundedTransport struct {
	native   http.RoundTripper
	settings settings
}

func (transport *boundedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	observed, ok := request.Context().Value(exchangeKey{}).(*exchange)
	if !ok {
		if request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, failure(ErrState, "uncontrolled-export")
	}
	if request.ContentLength < 0 && transport.settings.Compression == "gzip" && request.Body != nil {
		// Native gzip exporters deliberately omit ContentLength. Bound the
		// compressed representation too, before any network entry.
		body, err := io.ReadAll(io.LimitReader(request.Body, int64(transport.settings.MaxRequestBytes)+1))
		closeErr := request.Body.Close()
		if err != nil || closeErr != nil {
			return nil, joined(ErrExport, "request-body", err, closeErr)
		}
		if len(body) > transport.settings.MaxRequestBytes {
			return nil, failure(ErrLimit, "request-bytes")
		}
		request = request.Clone(request.Context())
		request.Body = io.NopCloser(bytes.NewReader(body))
		request.ContentLength = int64(len(body))
	}
	if request.ContentLength < 0 || request.ContentLength > int64(transport.settings.MaxRequestBytes) {
		if request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, failure(ErrLimit, "request-bytes")
	}
	observed.calls++
	response, err := transport.native.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode == http.StatusSwitchingProtocols {
		return nil, failure(ErrProtocol, "protocol-switch", response.Body.Close())
	}
	data, readErr := io.ReadAll(io.LimitReader(response.Body, int64(transport.settings.MaxResponseBytes)+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		return nil, joined(ErrProtocol, "response-read", readErr, closeErr)
	}
	if len(data) > transport.settings.MaxResponseBytes {
		return nil, failure(ErrLimit, "response-bytes")
	}
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return nil, failure(ErrUnsupported, "response-encoding")
	}
	if response.StatusCode == http.StatusOK {
		if response.Header.Get("Content-Type") != "application/x-protobuf" {
			return nil, failure(ErrProtocol, "response-type")
		}
		if err := observed.response(data); err != nil {
			return nil, err
		}
	} else if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil, failure(ErrProtocol, "response-status")
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	return response, nil
}
func (observed *exchange) response(data []byte) error {
	var rejected int64
	var message string
	var err error
	switch observed.signal {
	case Logs:
		var response collog.ExportLogsServiceResponse
		err = proto.Unmarshal(data, &response)
		if partial := response.PartialSuccess; partial != nil {
			rejected, message = partial.RejectedLogRecords, partial.ErrorMessage
		}
	case Traces:
		var response coltrace.ExportTraceServiceResponse
		err = proto.Unmarshal(data, &response)
		if partial := response.PartialSuccess; partial != nil {
			rejected, message = partial.RejectedSpans, partial.ErrorMessage
		}
	case Metrics:
		var response colmetric.ExportMetricsServiceResponse
		err = proto.Unmarshal(data, &response)
		if partial := response.PartialSuccess; partial != nil {
			rejected, message = partial.RejectedDataPoints, partial.ErrorMessage
		}
	default:
		return failure(ErrState, "signal")
	}
	if err != nil || rejected < 0 || rejected > int64(observed.count) {
		return failure(ErrProtocol, "response", err)
	}
	observed.acknowledged, observed.partial, observed.rejected = true, rejected != 0 || message != "", int(rejected)
	return nil
}
