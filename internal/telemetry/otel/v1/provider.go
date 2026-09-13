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
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.opentelemetry.io/otel/attribute"
	logapi "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	traceapi "go.opentelemetry.io/otel/trace"
)

// Source is an opaque assembly token. It exposes no native provider or transport.
type Source struct {
	private
	owner *owner
}

// Client is a concurrent, non-owning capability. All aliases use original
// resource admission. It exposes no global installation or shutdown authority.
type Client struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}
type owner struct {
	settings                              settings
	gate                                  chan struct{}
	transport                             *http.Transport
	network                               *networkOwner
	httpClient                            *http.Client
	logs                                  *sdklog.LoggerProvider
	logger                                logapi.Logger
	traces                                *sdktrace.TracerProvider
	tracer                                traceapi.Tracer
	metrics                               *sdkmetric.MeterProvider
	reader                                *sdkmetric.ManualReader
	instruments                           map[string]instrument
	logExporter                           sdklog.Exporter
	traceExporter                         sdktrace.SpanExporter
	metricExporter                        sdkmetric.Exporter
	pendingLogs                           []queuedLog
	pendingSpans                          []sdktrace.ReadOnlySpan
	queueItems, queueBytes, emittingBytes int
	closed                                bool
}
type queuedLog struct {
	record sdklog.Record
	bytes  int
}

// Select validates and freezes authorized options/layers before construction.
// Nonempty OTEL_* settings are rejected, not temporarily erased. Process
// environment and native diagnostic globals must not be concurrently changed.
// Selection is reusable across independent assemblies; no runtime handle is shared.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	if err := checkEnvironment(); err != nil {
		return resource.Selection[Source]{}, err
	}
	if err := bootstrapBound(options); err != nil {
		return resource.Selection[Source]{}, err
	}
	format := options.Format
	if format == 0 {
		format = 1
	}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaulted(options), Validate: validate},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: format, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, construct), nil
}
func construct(ctx context.Context, value settings) (resource.Resource[Source], error) {
	owner := &owner{settings: value, gate: make(chan struct{}, 1)}
	owned := resource.Resource[Source]{Acquired: true, Capability: Source{owner: owner}, Release: owner.close}
	if err := checkEnvironment(); err != nil {
		return owned, err
	}
	var tlsConfig *tls.Config
	if value.TLS != nil {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(value.TLS.CA)) {
			return owned, failure(ErrInput, "tls-ca")
		}
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
		if value.TLS.Certificate != "" {
			certificate, err := tls.X509KeyPair([]byte(value.TLS.Certificate), []byte(value.TLS.Key))
			if err != nil {
				return owned, failure(ErrInput, "tls-client", err)
			}
			tlsConfig.Certificates = []tls.Certificate{certificate}
		}
	}
	owner.network = newNetwork(value, tlsConfig)
	owner.transport = &http.Transport{
		Proxy: nil, DialContext: owner.network.dialPlain, DialTLSContext: owner.network.dialTLS,
		TLSClientConfig: tlsConfig, TLSHandshakeTimeout: value.Timeout, ResponseHeaderTimeout: value.Timeout,
		MaxResponseHeaderBytes: int64(value.MaxResponseBytes), DisableCompression: true,
		MaxConnsPerHost: 1, MaxIdleConns: 3, MaxIdleConnsPerHost: 1, IdleConnTimeout: 30 * time.Second,
		ForceAttemptHTTP2: false,
	}
	owner.httpClient = &http.Client{Transport: &boundedTransport{native: owner.transport, settings: value},
		Timeout: value.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return failure(ErrProtocol, "redirect") }}
	if err := owner.makeExporters(ctx); err != nil {
		return owned, err
	}
	attrs := stringAttributes(value.ResourceAttributes)
	attrs = append(attrs, attribute.String("service.name", value.ServiceName))
	res := sdkresource.NewWithAttributes(value.ResourceSchemaURL, attrs...)
	scopeAttrs := stringAttributes(value.ScopeAttributes)
	if value.LogsEndpoint != "" {
		owner.logs = sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(logCapture{owner}),
			sdklog.WithAttributeCountLimit(MaxAttributes+6), sdklog.WithAttributeValueLengthLimit(value.MaxRecordBytes))
		owner.logger = owner.logs.Logger(value.Scope, logapi.WithInstrumentationVersion(value.ScopeVersion),
			logapi.WithSchemaURL(value.ScopeSchemaURL), logapi.WithInstrumentationAttributes(scopeAttrs...))
	}
	if value.TracesEndpoint != "" {
		owner.traces = sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithSpanProcessor(spanCapture{owner}),
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(value.SampleRatio))),
			sdktrace.WithRawSpanLimits(sdktrace.SpanLimits{AttributeValueLengthLimit: value.MaxRecordBytes,
				AttributeCountLimit: MaxAttributes + 6, EventCountLimit: 32, LinkCountLimit: 16,
				AttributePerEventCountLimit: MaxAttributes, AttributePerLinkCountLimit: MaxAttributes}),
			sdktrace.WithoutPanicRecording())
		owner.tracer = owner.traces.Tracer(value.Scope, traceapi.WithInstrumentationVersion(value.ScopeVersion),
			traceapi.WithSchemaURL(value.ScopeSchemaURL), traceapi.WithInstrumentationAttributes(scopeAttrs...))
	}
	if value.MetricsEndpoint != "" {
		owner.reader = sdkmetric.NewManualReader()
		owner.metrics = sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(owner.reader),
			sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter), sdkmetric.WithCardinalityLimit(value.MetricCardinality))
		if err := owner.makeInstruments(scopeAttrs); err != nil {
			return owned, err
		}
	}
	if err := ctx.Err(); err != nil {
		return owned, nativeFailure(ErrState, "construct", ctx, err)
	}
	return owned, nil
}
func stringAttributes(values map[string]string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(values))
	for key, value := range values {
		attrs = append(attrs, attribute.String(key, value))
	}
	return attrs
}

// Bind connects operation evidence independently of any telemetry export path.
func Bind(assembly *resource.Assembly, selected resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Client, error) {
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	value, limits := source.owner.settings, access.Limits()
	if limits.Active > value.ActiveCalls || limits.Queued > value.QueuedCalls || limits.Bytes < value.reservation() || limits.MaxLeases < 2 ||
		limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}

type operationKey struct{}

func (client *Client) begin(ctx context.Context, id fault.Correlation, operation string, shape invocation.Shape) (*invocation.Call[Result], error) {
	if client == nil || client.owner == nil || ctx == nil {
		return nil, failure(ErrInput, operation)
	}
	if active, _ := ctx.Value(operationKey{}).(bool); active {
		return nil, failure(ErrRecursion, operation)
	}
	value := client.owner.settings
	return invocation.Begin(ctx, client.access, invocation.Request{Name: operation, Correlation: id, Shape: shape,
		Bytes: value.reservation(), EvidenceBytes: value.evidenceReservation(), Admission: invocation.Budget{Limit: value.Timeout}},
		client.inbox, client.observer)
}
func (owner *owner) enter(ctx context.Context) error {
	if ctx == nil {
		return failure(ErrInput, "context")
	}
	select {
	case owner.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			owner.leave()
			return nativeFailure(ErrState, "entry", ctx, err)
		}
		if err := checkEnvironment(); err != nil {
			owner.leave()
			return err
		}
		if owner.closed {
			owner.leave()
			return failure(ErrState, "closed")
		}
		return nil
	case <-ctx.Done():
		return nativeFailure(ErrState, "entry", ctx, ctx.Err())
	}
}
func (owner *owner) leave() { <-owner.gate }
func (client *Client) run(ctx context.Context, id fault.Correlation, operation string, run func(context.Context) invocation.Outcome[Result]) (*invocation.Receipt[Result], error) {
	call, err := client.begin(ctx, id, operation, invocation.Finite)
	if err != nil {
		return nil, err
	}
	_ = call.Execute(ctx, invocation.Budget{Limit: client.owner.settings.Timeout}, func(work context.Context, scope invocation.Scope) invocation.Outcome[Result] {
		if err := client.owner.enter(work); err != nil {
			return invocation.Outcome[Result]{Primary: err}
		}
		defer client.owner.leave()
		work = context.WithValue(work, scopeKey{}, scope)
		return run(context.WithValue(work, operationKey{}, true))
	})
	return call.Receipt(), nil
}
func (owner *owner) reserve(bytes int) error {
	if owner.queueItems == owner.settings.QueueItems || bytes > owner.settings.QueueBytes-owner.queueBytes {
		return failure(ErrLimit, "queue")
	}
	owner.queueItems++
	owner.queueBytes += bytes
	return nil
}
func (owner *owner) unreserve(bytes, items int) { owner.queueBytes -= bytes; owner.queueItems -= items }

// close runs only after resource has joined all admitted calls and live spans.
// Capture processors/manual reader have no background work. Exporter shutdown is
// called directly even if SDK provider shutdown skipped a processor on cancellation.
func (owner *owner) close(ctx context.Context) resource.ReleaseResult {
	select {
	case owner.gate <- struct{}{}:
		defer owner.leave()
	case <-ctx.Done():
		return resource.ReleaseResult{Err: nativeFailure(ErrCleanup, "join", ctx, ctx.Err()), Continue: owner.close}
	}
	if owner.closed {
		return owner.finishClose(ctx)
	}
	var causes []error
	if owner.logExporter != nil || owner.traceExporter != nil || owner.metricExporter != nil {
		work, cancel, err := (invocation.Budget{Limit: owner.settings.Timeout}).Context(ctx, invocation.Cleanup)
		if err == nil {
			_, err = owner.flush(context.WithValue(work, operationKey{}, true))
			cancel()
		}
		causes = append(causes, err)
	}
	if owner.queueItems != 0 {
		causes = append(causes, failure(ErrUndelivered, "pending-records"))
	}
	for _, shutdown := range owner.shutdowns() {
		causes = append(causes, nativeFailure(ErrCleanup, "shutdown", ctx, shutdown(ctx)))
	}
	if owner.transport != nil {
		owner.transport.CloseIdleConnections()
	}
	owner.pendingLogs, owner.pendingSpans, owner.instruments = nil, nil, nil
	owner.queueItems, owner.queueBytes = 0, 0
	owner.closed = true
	result := owner.finishClose(ctx)
	result.Err = joined(ErrCleanup, "release", append(causes, result.Err)...)
	return result
}

func (owner *owner) finishClose(ctx context.Context) resource.ReleaseResult {
	done, err := owner.network.stop(ctx)
	result := resource.ReleaseResult{Quiescent: done, Released: done, Err: err}
	if !done {
		result.Continue = owner.finishClose
	}
	return result
}
func (owner *owner) shutdowns() []func(context.Context) error {
	var functions []func(context.Context) error
	if owner.logs != nil {
		functions = append(functions, owner.logs.Shutdown)
	}
	if owner.traces != nil {
		functions = append(functions, owner.traces.Shutdown)
	}
	if owner.metrics != nil {
		functions = append(functions, owner.metrics.Shutdown)
	} else if owner.reader != nil {
		functions = append(functions, owner.reader.Shutdown)
	}
	if owner.logExporter != nil {
		functions = append(functions, owner.logExporter.Shutdown)
	}
	if owner.traceExporter != nil {
		functions = append(functions, owner.traceExporter.Shutdown)
	}
	if owner.metricExporter != nil {
		functions = append(functions, owner.metricExporter.Shutdown)
	}
	return functions
}

func (client *Client) association(id fault.Correlation) []attribute.KeyValue {
	info := client.access.Info()
	return []attribute.KeyValue{attribute.String("fathomry.call", strings.Clone(id.Call)),
		attribute.String("fathomry.parent", strings.Clone(id.Parent)), attribute.String("fathomry.owner", strings.Clone(id.Owner)),
		attribute.String("fathomry.source", info.Configuration.Identity.Name), attribute.String("fathomry.provider", ProviderID),
		attribute.String("fathomry.scope", info.Scope)}
}

func associationBytes(attributes []attribute.KeyValue) int {
	bytes := 0
	for _, attr := range attributes {
		bytes += len(attr.Key) + len(attr.Value.AsString()) + 32
	}
	return bytes
}
