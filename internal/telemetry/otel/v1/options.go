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
	"math"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/resource"
	"golang.org/x/net/http/httpguts"
)

// TLSV1 contains explicit PEM material, never filenames or insecure verification.
// CA is required for HTTPS. Certificate and Key must either both be absent or
// form one client certificate/key pair. Material is copied at preparation.
type TLSV1 struct {
	private
	CA, Certificate, Key string
}

// InstrumentV1 declares one synchronous native instrument. Kind is one of
// int64/float64-counter, -updowncounter, -gauge, or -histogram.
// AttributeKeys is an allowlist, not automatic context/baggage promotion.
// Histograms require 1–32 finite, strictly increasing explicit bucket boundaries.
// Other kinds must omit Boundaries. Names are unique in the single owned scope.
type InstrumentV1 struct {
	private
	Name, Kind, Unit, Description string
	AttributeKeys                 []string
	Boundaries                    []float64
}

// OptionsV1 is process-local bootstrap, not a durable DTO. Format defaults to 1;
// any other value is rejected. Select borrows all inputs until return and then
// freezes copies; callers must not mutate them concurrently during preparation.
//
// Empty signal endpoints disable that signal; at least one must be enabled.
// URLs include the exact path. Only HTTP/protobuf is supported: HTTPS with
// explicit CA material, or HTTP to a literal loopback IP. Headers are explicit;
// no environment, files, global provider, proxy or detector is authorized.
//
// Numeric bootstrap zero selects the documented default except QueuedCalls
// (zero rejects overload); SampleRatio nil defaults to one, pointer-to-zero
// selects parent-based root sampling zero. Layer values are final: explicit
// zero does not reapply bootstrap defaults. Durations use nanoseconds in layers.
type OptionsV1 struct {
	private
	Name                                          string
	Format                                        uint32
	LogsEndpoint, TracesEndpoint, MetricsEndpoint string
	Headers                                       map[string]string
	TLS                                           *TLSV1
	ServiceName                                   string
	ResourceAttributes                            map[string]string
	// Scope defaults to fathomry, ScopeVersion to 1. Schema URLs are independent
	// optional declarations for resource and instrumentation attributes.
	Scope, ScopeVersion, ResourceSchemaURL, ScopeSchemaURL string
	ScopeAttributes                                        map[string]string
	// Timeout: 5s default, 1ms–1min. Applies to admission and execution separately.
	Timeout time.Duration
	// ActiveCalls: 16 default, 1–64; QueuedCalls: 0–64. Spans hold active calls.
	ActiveCalls, QueuedCalls int
	// QueueItems: 512 default, 1–4096, shared by pending logs, spans and live spans.
	QueueItems int
	// QueueBytes: 4MiB default, MaxRecordBytes–16MiB. Each live span reserves
	// MaxRecordBytes until its completed record leaves the queue.
	QueueBytes int
	// MaxRecordBytes: 64KiB default, 1KiB–1MiB, logical input charge, not RSS.
	MaxRecordBytes int
	// BatchSize: 128 default, 1–QueueItems. Export is explicit, not timer-driven.
	BatchSize int
	// MaxRequestBytes: 1MiB default, 1KiB–4MiB, serialized request ceiling.
	MaxRequestBytes int
	// MaxResponseBytes: 64KiB default, 1KiB–256KiB, before native error parsing.
	MaxResponseBytes int
	// Compression is none (default) or gzip for outgoing requests.
	Compression string
	SampleRatio *float64
	// MetricCardinality: 128 default, 1–1024; instrument count times this limit
	// must not exceed 4096. Native overflow preserves totals, not all labels.
	MetricCardinality int
	Instruments       []InstrumentV1
}

type tlsSettings struct {
	CA          string `json:"ca_pem"`
	Certificate string `json:"certificate_pem"`
	Key         string `json:"key_pem"`
}
type instrumentSettings struct {
	Name          string    `json:"name"`
	Kind          string    `json:"kind"`
	Unit          string    `json:"unit"`
	Description   string    `json:"description"`
	AttributeKeys []string  `json:"attribute_keys"`
	Boundaries    []float64 `json:"boundaries"`
}
type settings struct {
	LogsEndpoint       string               `json:"logs_endpoint"`
	TracesEndpoint     string               `json:"traces_endpoint"`
	MetricsEndpoint    string               `json:"metrics_endpoint"`
	Headers            map[string]string    `json:"headers"`
	TLS                *tlsSettings         `json:"tls"`
	ServiceName        string               `json:"service_name"`
	ResourceAttributes map[string]string    `json:"resource_attributes"`
	Scope              string               `json:"scope"`
	ScopeVersion       string               `json:"scope_version"`
	ResourceSchemaURL  string               `json:"resource_schema_url"`
	ScopeSchemaURL     string               `json:"scope_schema_url"`
	ScopeAttributes    map[string]string    `json:"scope_attributes"`
	Timeout            time.Duration        `json:"timeout_ns"`
	ActiveCalls        int                  `json:"active_calls"`
	QueuedCalls        int                  `json:"queued_calls"`
	QueueItems         int                  `json:"queue_items"`
	QueueBytes         int                  `json:"queue_bytes"`
	MaxRecordBytes     int                  `json:"max_record_bytes"`
	BatchSize          int                  `json:"batch_size"`
	MaxRequestBytes    int                  `json:"max_request_bytes"`
	MaxResponseBytes   int                  `json:"max_response_bytes"`
	Compression        string               `json:"compression"`
	SampleRatio        float64              `json:"sample_ratio"`
	MetricCardinality  int                  `json:"metric_cardinality"`
	Instruments        []instrumentSettings `json:"instruments"`
}

func defaulted(options OptionsV1) settings {
	value := settings{
		LogsEndpoint: options.LogsEndpoint, TracesEndpoint: options.TracesEndpoint, MetricsEndpoint: options.MetricsEndpoint,
		Headers: options.Headers, ServiceName: options.ServiceName, ResourceAttributes: options.ResourceAttributes,
		Scope: options.Scope, ScopeVersion: options.ScopeVersion, ResourceSchemaURL: options.ResourceSchemaURL, ScopeSchemaURL: options.ScopeSchemaURL, ScopeAttributes: options.ScopeAttributes,
		Timeout: options.Timeout, ActiveCalls: options.ActiveCalls, QueuedCalls: options.QueuedCalls,
		QueueItems: options.QueueItems, QueueBytes: options.QueueBytes, MaxRecordBytes: options.MaxRecordBytes,
		BatchSize: options.BatchSize, MaxRequestBytes: options.MaxRequestBytes, MaxResponseBytes: options.MaxResponseBytes,
		Compression: options.Compression, SampleRatio: 1, MetricCardinality: options.MetricCardinality,
	}
	if options.TLS != nil {
		value.TLS = &tlsSettings{options.TLS.CA, options.TLS.Certificate, options.TLS.Key}
	}
	for _, instrument := range options.Instruments {
		value.Instruments = append(value.Instruments, instrumentSettings{instrument.Name, instrument.Kind, instrument.Unit,
			instrument.Description, instrument.AttributeKeys, instrument.Boundaries})
	}
	if value.Scope == "" {
		value.Scope = "fathomry"
	}
	if value.ScopeVersion == "" {
		value.ScopeVersion = "1"
	}
	if value.Timeout == 0 {
		value.Timeout = 5 * time.Second
	}
	if value.ActiveCalls == 0 {
		value.ActiveCalls = 16
	}
	if value.QueueItems == 0 {
		value.QueueItems = 512
	}
	if value.QueueBytes == 0 {
		value.QueueBytes = 4 << 20
	}
	if value.MaxRecordBytes == 0 {
		value.MaxRecordBytes = 64 << 10
	}
	if value.BatchSize == 0 {
		value.BatchSize = min(128, value.QueueItems)
	}
	if value.MaxRequestBytes == 0 {
		value.MaxRequestBytes = 1 << 20
	}
	if value.MaxResponseBytes == 0 {
		value.MaxResponseBytes = 64 << 10
	}
	if value.Compression == "" {
		value.Compression = "none"
	}
	if options.SampleRatio != nil {
		value.SampleRatio = *options.SampleRatio
	}
	if value.MetricCardinality == 0 {
		value.MetricCardinality = 128
	}
	return value
}

func boundedString(value string, maximum int) bool {
	return len(value) <= maximum && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}
func nameValid(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for index, char := range value {
		letter := char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z'
		if !letter && !(index > 0 && (char >= '0' && char <= '9' || strings.ContainsRune("._-/", char))) {
			return false
		}
	}
	return true
}
func validate(value settings) error {
	if value.ActiveCalls < 1 || value.ActiveCalls > 64 || value.QueuedCalls < 0 || value.QueuedCalls > 64 ||
		value.Timeout < time.Millisecond || value.Timeout > time.Minute ||
		value.QueueItems < 1 || value.QueueItems > 4096 || value.QueueBytes < value.MaxRecordBytes || value.QueueBytes > 16<<20 ||
		value.MaxRecordBytes < 1024 || value.MaxRecordBytes > 1<<20 || value.BatchSize < 1 || value.BatchSize > value.QueueItems ||
		value.MaxRequestBytes < 1024 || value.MaxRequestBytes > 4<<20 || value.MaxResponseBytes < 1024 || value.MaxResponseBytes > 256<<10 ||
		value.Compression != "none" && value.Compression != "gzip" ||
		math.IsNaN(value.SampleRatio) || math.IsInf(value.SampleRatio, 0) || value.SampleRatio < 0 || value.SampleRatio > 1 ||
		value.MetricCardinality < 1 || value.MetricCardinality > 1024 ||
		len(value.Instruments) > 32 || len(value.Instruments)*value.MetricCardinality > 4096 {
		return failure(ErrInput, "options")
	}
	if value.ServiceName == "" || !boundedString(value.ServiceName, 128) || !nameValid(value.Scope) ||
		!boundedString(value.ScopeVersion, 64) || !schemaURLValid(value.ResourceSchemaURL) || !schemaURLValid(value.ScopeSchemaURL) {
		return failure(ErrInput, "identity")
	}
	for _, attributes := range []map[string]string{value.ResourceAttributes, value.ScopeAttributes} {
		if len(attributes) > 16 {
			return failure(ErrLimit, "identity-attributes")
		}
		for key, text := range attributes {
			if !keyValid(key) || !boundedString(text, 256) || key == "service.name" {
				return failure(ErrInput, "identity-attributes")
			}
		}
	}
	if len(value.Headers) > 16 {
		return failure(ErrLimit, "headers")
	}
	seenHeaders := map[string]bool{}
	for key, text := range value.Headers {
		lower := strings.ToLower(key)
		if !httpguts.ValidHeaderFieldName(key) || len(key) > 128 || !httpguts.ValidHeaderFieldValue(text) ||
			!boundedString(text, 2048) || seenHeaders[lower] ||
			slices.Contains([]string{"host", "content-type", "content-length", "content-encoding", "accept-encoding", "connection", "user-agent"}, lower) {
			return failure(ErrInput, "headers")
		}
		seenHeaders[lower] = true
	}
	count, https := 0, false
	for _, endpoint := range []string{value.LogsEndpoint, value.TracesEndpoint, value.MetricsEndpoint} {
		if endpoint == "" {
			continue
		}
		count++
		parsed, err := url.Parse(endpoint)
		if err != nil || !boundedString(endpoint, 2048) || parsed.User != nil || parsed.Host == "" ||
			parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.Opaque != "" ||
			parsed.Path == "" || parsed.RawPath != "" || strings.TrimSpace(parsed.Path) != parsed.Path {
			return failure(ErrInput, "endpoint")
		}
		switch parsed.Scheme {
		case "https":
			https = true
		case "http":
			if address := net.ParseIP(parsed.Hostname()); address == nil || !address.IsLoopback() {
				return failure(ErrUnsupported, "cleartext")
			}
		default:
			return failure(ErrUnsupported, "protocol")
		}
	}
	if count == 0 || https != (value.TLS != nil) {
		return failure(ErrInput, "signals-tls")
	}
	if value.TLS != nil && (value.TLS.CA == "" || !boundedString(value.TLS.CA, 64<<10) ||
		!boundedString(value.TLS.Certificate, 64<<10) || !boundedString(value.TLS.Key, 32<<10) ||
		(value.TLS.Certificate == "") != (value.TLS.Key == "")) {
		return failure(ErrInput, "tls")
	}
	if (len(value.Instruments) != 0) != (value.MetricsEndpoint != "") {
		return failure(ErrInput, "instruments")
	}
	names := make(map[string]bool)
	for _, instrument := range value.Instruments {
		if !nameValid(instrument.Name) || names[strings.ToLower(instrument.Name)] ||
			!boundedString(instrument.Unit, 63) || !boundedString(instrument.Description, 256) || len(instrument.AttributeKeys) > 8 {
			return failure(ErrInput, "instrument")
		}
		names[strings.ToLower(instrument.Name)] = true
		if !slices.Contains([]string{"int64-counter", "float64-counter", "int64-updowncounter", "float64-updowncounter",
			"int64-gauge", "float64-gauge", "int64-histogram", "float64-histogram"}, instrument.Kind) {
			return failure(ErrUnsupported, "instrument-kind")
		}
		keys := map[string]bool{}
		for _, key := range instrument.AttributeKeys {
			if !keyValid(key) || keys[key] || key == "otel.metric.overflow" {
				return failure(ErrInput, "metric-key")
			}
			keys[key] = true
		}
		histogram := strings.HasSuffix(instrument.Kind, "-histogram")
		if histogram && (len(instrument.Boundaries) < 1 || len(instrument.Boundaries) > 32) ||
			!histogram && len(instrument.Boundaries) != 0 {
			return failure(ErrInput, "boundaries")
		}
		for index, boundary := range instrument.Boundaries {
			if math.IsNaN(boundary) || math.IsInf(boundary, 0) || index > 0 && boundary <= instrument.Boundaries[index-1] {
				return failure(ErrInput, "boundaries")
			}
		}
	}
	return nil
}

func schemaURLValid(value string) bool {
	if value == "" {
		return true
	}
	parsed, err := url.Parse(value)
	return err == nil && boundedString(value, 512) && (parsed.Scheme == "http" || parsed.Scheme == "https") &&
		parsed.Host != "" && parsed.User == nil && parsed.RawQuery == "" && !parsed.ForceQuery && parsed.Fragment == ""
}

func checkEnvironment() error {
	for _, pair := range os.Environ() {
		key, value, _ := strings.Cut(pair, "=")
		if strings.HasPrefix(key, "OTEL_") && value != "" {
			return failure(ErrEnvironment, "ambient-configuration")
		}
	}
	return nil
}

// LimitsV1 returns bootstrap admission charges. Overlays require composition to
// supply the corresponding resolved limits. Charges are logical work envelopes,
// not a measured heap bound; resident SDK aggregation and queue limits are separate.
func LimitsV1(options OptionsV1) resource.Limits { return defaulted(options).limits() }

// EvidenceBytesV1 returns the declared per-operation inbox charge for bootstrap
// options. Overlays require the corresponding resolved response limit. Arbitrary
// caller-owned cancellation/error graphs and consumer retention are not heap-capped.
func EvidenceBytesV1(options OptionsV1) int64 { return defaulted(options).evidenceReservation() }
func (value settings) reservation() int64 {
	metricWork := int64(len(value.Instruments)*value.MetricCardinality) * (16 << 10)
	return int64(8*value.MaxRequestBytes+4*value.MaxRecordBytes+value.MaxResponseBytes+(1<<20)) + metricWork
}
func (value settings) evidenceReservation() int64 {
	return int64(4*value.MaxResponseBytes + (32 << 10))
}
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: value.ActiveCalls, Queued: value.QueuedCalls,
		Bytes: int64(value.ActiveCalls) * value.reservation(), QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: 2}
}

// Profile describes the actual frozen profile without endpoints, headers or data.
// Build versions must be inspected separately with compatibility.Inspect.
// No backend/service version or deployment support is inferred from configuration.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "otel-explicit-batch",
		Protocol: compatibility.Fact{Kind: compatibility.Declared, Value: "otlp-http-protobuf"},
		Native:   compatibility.Fact{Kind: compatibility.NotApplicable},
		Options:  client.owner.settings.profileOptions()}
}

func (value settings) profileOptions() []compatibility.Option {
	options := []compatibility.Option{
		{Name: "logs", Value: strconv.FormatBool(value.LogsEndpoint != "")},
		{Name: "traces", Value: strconv.FormatBool(value.TracesEndpoint != "")},
		{Name: "metrics", Value: strconv.FormatBool(value.MetricsEndpoint != "")},
		{Name: "compression", Value: value.Compression}, {Name: "retry", Value: "off"},
		{Name: "temporality", Value: "cumulative"}, {Name: "exemplars", Value: "off"},
		{Name: "sampling", Value: "parent-based-ratio"}, {Name: "sample-ratio", Value: strconv.FormatFloat(value.SampleRatio, 'g', -1, 64)},
	}
	for _, item := range []struct {
		name  string
		value int
	}{
		{"active-calls", value.ActiveCalls}, {"queued-calls", value.QueuedCalls}, {"queue-items", value.QueueItems},
		{"queue-bytes", value.QueueBytes}, {"max-record-bytes", value.MaxRecordBytes}, {"batch-size", value.BatchSize},
		{"max-request-bytes", value.MaxRequestBytes}, {"max-response-bytes", value.MaxResponseBytes},
		{"metric-cardinality", value.MetricCardinality}, {"instruments", len(value.Instruments)},
	} {
		options = append(options, compatibility.Option{Name: item.name, Value: strconv.Itoa(item.value)})
	}
	options = append(options, compatibility.Option{Name: "timeout-ns", Value: strconv.FormatInt(int64(value.Timeout), 10)})
	return options
}
func bootstrapBound(options OptionsV1) error {
	if len(options.Instruments) > 32 || len(options.Headers) > 16 || len(options.ResourceAttributes) > 16 ||
		len(options.ScopeAttributes) > 16 {
		return failure(ErrLimit, "bootstrap")
	}
	bytes := 0
	add := func(text string) { bytes += len(text) }
	for _, text := range []string{options.Name, options.LogsEndpoint, options.TracesEndpoint, options.MetricsEndpoint,
		options.ServiceName, options.Scope, options.ScopeVersion, options.ResourceSchemaURL, options.ScopeSchemaURL, options.Compression} {
		add(text)
	}
	for _, values := range []map[string]string{options.Headers, options.ResourceAttributes, options.ScopeAttributes} {
		for key, value := range values {
			add(key)
			add(value)
		}
	}
	if options.TLS != nil {
		add(options.TLS.CA)
		add(options.TLS.Certificate)
		add(options.TLS.Key)
	}
	for _, instrument := range options.Instruments {
		if len(instrument.AttributeKeys) > 8 || len(instrument.Boundaries) > 32 {
			return failure(ErrLimit, "bootstrap")
		}
		add(instrument.Name)
		add(instrument.Kind)
		add(instrument.Unit)
		add(instrument.Description)
		for _, key := range instrument.AttributeKeys {
			add(key)
		}
	}
	if bytes > 1<<20 {
		return failure(ErrLimit, "bootstrap")
	}
	return nil
}
