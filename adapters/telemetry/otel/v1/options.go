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
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	"time"
)

const ProviderID = "telemetry.otel.v1"

// Settings is strict-loadable configuration format 1. Nil optional fields select
// native defaults; explicit zero is retained and validated. Durations are integer
// nanoseconds. Empty endpoints disable signals; at least one is required.
// Explicit JSON may contain credentials; fmt and slog are redacted.
type Settings struct {
	Name                    string            `json:"name" mapstructure:"name"`
	Version                 uint32            `json:"format" mapstructure:"format"`
	LogsEndpoint            string            `json:"logs_endpoint" mapstructure:"logs_endpoint"`
	TracesEndpoint          string            `json:"traces_endpoint" mapstructure:"traces_endpoint"`
	MetricsEndpoint         string            `json:"metrics_endpoint" mapstructure:"metrics_endpoint"`
	Headers                 map[string]string `json:"headers" mapstructure:"headers"`
	TLS                     *TLS              `json:"tls" mapstructure:"tls"`
	ServiceName             string            `json:"service_name" mapstructure:"service_name"`
	ResourceAttributes      map[string]string `json:"resource_attributes" mapstructure:"resource_attributes"`
	TypedResourceAttributes []ConfigAttribute `json:"resource_typed_attributes" mapstructure:"resource_typed_attributes"`
	Scope                   *string           `json:"scope" mapstructure:"scope"`
	ScopeVersion            *string           `json:"scope_version" mapstructure:"scope_version"`
	ResourceSchemaURL       string            `json:"resource_schema_url" mapstructure:"resource_schema_url"`
	ScopeSchemaURL          string            `json:"scope_schema_url" mapstructure:"scope_schema_url"`
	ScopeAttributes         map[string]string `json:"scope_attributes" mapstructure:"scope_attributes"`
	TypedScopeAttributes    []ConfigAttribute `json:"scope_typed_attributes" mapstructure:"scope_typed_attributes"`
	Timeout                 *time.Duration    `json:"timeout_ns" mapstructure:"timeout_ns"`
	ActiveCalls             *int              `json:"active_calls" mapstructure:"active_calls"`
	QueuedCalls             *int              `json:"queued_calls" mapstructure:"queued_calls"`
	QueueItems              *int              `json:"queue_items" mapstructure:"queue_items"`
	QueueBytes              *int              `json:"queue_bytes" mapstructure:"queue_bytes"`
	MaxRecordBytes          *int              `json:"max_record_bytes" mapstructure:"max_record_bytes"`
	BatchSize               *int              `json:"batch_size" mapstructure:"batch_size"`
	MaxRequestBytes         *int              `json:"max_request_bytes" mapstructure:"max_request_bytes"`
	MaxResponseBytes        *int              `json:"max_response_bytes" mapstructure:"max_response_bytes"`
	Compression             *string           `json:"compression" mapstructure:"compression"`
	SampleRatio             *float64          `json:"sample_ratio" mapstructure:"sample_ratio"`
	MetricCardinality       *int              `json:"metric_cardinality" mapstructure:"metric_cardinality"`
	Instruments             []Instrument      `json:"instruments" mapstructure:"instruments"`
}

// TLS authorizes explicit CA and optional client PEM material, never filenames
// or insecure verification. Certificate and Key must be supplied together.
type TLS struct {
	CA          string `json:"ca_pem" mapstructure:"ca_pem"`
	Certificate string `json:"certificate_pem" mapstructure:"certificate_pem"`
	Key         string `json:"key_pem" mapstructure:"key_pem"`
}

// Instrument declares one of the eight int64/float64 counter, updowncounter,
// gauge or histogram kinds. Histograms require 1..32 increasing finite buckets.
// Names are unique; at most 32 instruments and 8 dimension keys are admitted.
type Instrument struct {
	Name          string    `json:"name" mapstructure:"name"`
	Kind          string    `json:"kind" mapstructure:"kind"`
	Unit          string    `json:"unit" mapstructure:"unit"`
	Description   string    `json:"description" mapstructure:"description"`
	AttributeKeys []string  `json:"attribute_keys" mapstructure:"attribute_keys"`
	Boundaries    []float64 `json:"boundaries" mapstructure:"boundaries"`
}

// Dependencies borrows independently owned operation and evidence mechanisms.
// Neither an Owner nor a Client closes these mechanisms. Evidence must be
// received independently, including the source's final cleanup result.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
}

// Validate is offline configuration validation, not backend readiness.
func Validate(value Settings) error { _, err := Prepare(value); return err }

func selectedValue[T any](value *T) (result T) {
	if value != nil {
		return *value
	}
	return result
}

func nativeOptions(value Settings) native.OptionsV1 {
	result := native.OptionsV1{Name: value.Name, Format: value.Version,
		LogsEndpoint: value.LogsEndpoint, TracesEndpoint: value.TracesEndpoint, MetricsEndpoint: value.MetricsEndpoint,
		Headers: value.Headers, ServiceName: value.ServiceName, ResourceAttributes: value.ResourceAttributes,
		Scope: selectedValue(value.Scope), ScopeVersion: selectedValue(value.ScopeVersion),
		ResourceSchemaURL: value.ResourceSchemaURL, ScopeSchemaURL: value.ScopeSchemaURL, ScopeAttributes: value.ScopeAttributes,
		Timeout: selectedValue(value.Timeout), ActiveCalls: selectedValue(value.ActiveCalls), QueuedCalls: selectedValue(value.QueuedCalls),
		QueueItems: selectedValue(value.QueueItems), QueueBytes: selectedValue(value.QueueBytes), MaxRecordBytes: selectedValue(value.MaxRecordBytes),
		BatchSize: selectedValue(value.BatchSize), MaxRequestBytes: selectedValue(value.MaxRequestBytes), MaxResponseBytes: selectedValue(value.MaxResponseBytes),
		Compression: selectedValue(value.Compression), SampleRatio: value.SampleRatio, MetricCardinality: selectedValue(value.MetricCardinality)}
	if value.TLS != nil {
		result.TLS = &native.TLSV1{CA: value.TLS.CA, Certificate: value.TLS.Certificate, Key: value.TLS.Key}
	}
	for _, instrument := range value.Instruments {
		result.Instruments = append(result.Instruments, native.InstrumentV1{Name: instrument.Name, Kind: instrument.Kind,
			Unit: instrument.Unit, Description: instrument.Description, AttributeKeys: instrument.AttributeKeys, Boundaries: instrument.Boundaries})
	}
	convert := func(values []ConfigAttribute) []native.ConfigAttribute {
		if values == nil {
			return nil
		}
		result := make([]native.ConfigAttribute, len(values))
		for index, attribute := range values {
			result[index].Key = attribute.Key
			result[index].Value.Nodes = make([]native.ValueNode, len(attribute.Value.Nodes))
			for node, value := range attribute.Value.Nodes {
				result[index].Value.Nodes[node] = native.ValueNode(value)
			}
		}
		return result
	}
	result.TypedResourceAttributes = convert(value.TypedResourceAttributes)
	result.TypedScopeAttributes = convert(value.TypedScopeAttributes)
	return result
}
