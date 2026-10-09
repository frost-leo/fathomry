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
	"encoding/json"
	"net/url"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

// Prepared contains one inert, immutable final configuration and its budgets.
// Reusing Select creates independent native owners, never a shared provider.
type Prepared struct {
	private
	configuration resource.Prepared[settings]
	metadata      Metadata
}

// Metadata describes the exact frozen selection before native acquisition.
// Bytes are declared local accounting envelopes, not measured heap/RSS, kernel
// socket storage or storage retained by callers. SourceBytes remains reserved
// while a source is retired but not released; overlapping generations add it.
// WorkBytes is per admitted operation, and EvidenceBytes is per independent
// evidence reservation. Neither is included in SourceBytes.
//
// QueueBytes is logical input capacity shared by logs, ended spans and live spans.
// SpanBytes is a live/ended span's share of that queue, not an additional queue.
// MaxActiveSpans is the ceiling with no competing work, evidence or queued logs;
// it does not reserve a separate Flush slot. Disabled signals have no queue or
// span capacity even when their inactive configuration fields are nonzero.
type Metadata struct {
	Limits                                            resource.Limits
	WorkBytes, EvidenceBytes, SourceBytes             int64
	ConfigurationBytes, QueueStorageBytes             int64
	MetricStorageBytes, TransportBytes                int64
	QueueItems, QueueBytes, MaxActiveSpans            int
	SpanBytes                                         int64
	MaxRecordBytes, MaxRequestBytes, MaxResponseBytes int
	BatchSize, Instruments, MetricCardinality         int
	MaxConnections                                    int
	Timeout                                           time.Duration
	Logs, Traces, Metrics                             bool
}

// PrepareV1 resolves layers once, validates them and freezes the data used by
// both Metadata and Select. It creates no SDK provider, transport, timer or
// goroutine and performs no network I/O. Inputs are borrowed until return.
func PrepareV1(options OptionsV1, layers ...resource.Layer) (Prepared, error) {
	if err := checkEnvironment(); err != nil {
		return Prepared{}, err
	}
	if err := bootstrapBound(options); err != nil {
		return Prepared{}, err
	}
	format := options.Format
	if format == 0 {
		format = 1
	}
	var metadata Metadata
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaulted(options), Validate: func(value settings) error {
		if err := validate(value); err != nil {
			return err
		}
		var err error
		metadata, err = value.metadata()
		return err
	}}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: format, Layers: layers})
	if err != nil {
		return Prepared{}, err
	}
	return Prepared{configuration: prepared, metadata: metadata}, nil
}

func (prepared Prepared) Metadata() Metadata { return prepared.metadata }

// Description returns detached, non-secret configuration provenance.
func (prepared Prepared) Description() resource.Description {
	return prepared.configuration.Description()
}

// Select uses only this frozen preparation. Apply resource.WithLimits using
// Metadata().Limits (or a valid narrower policy) before assembly. The zero
// Prepared value produces an invalid selection, never default construction.
func (prepared Prepared) Select() resource.Selection[Source] {
	return resource.Select(prepared.configuration, construct)
}

func (value settings) metadata() (Metadata, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return Metadata{}, failure(ErrInput, "preparation", err)
	}
	metadata := Metadata{Limits: value.limits(), WorkBytes: value.reservation(), EvidenceBytes: value.evidenceReservation(),
		MaxRecordBytes: value.MaxRecordBytes, MaxRequestBytes: value.MaxRequestBytes, MaxResponseBytes: value.MaxResponseBytes,
		BatchSize: value.BatchSize, Instruments: len(value.Instruments), MetricCardinality: value.MetricCardinality,
		Timeout: value.Timeout, Logs: value.LogsEndpoint != "", Traces: value.TracesEndpoint != "", Metrics: value.MetricsEndpoint != ""}
	// Serialized preparation, decoded settings, native resource/scope attributes
	// and exporter configuration coexist. PEM additionally owns parsed TLS data.
	metadata.ConfigurationBytes = 4*int64(len(encoded)) + 64<<10
	if value.TLS != nil {
		metadata.ConfigurationBytes += 4 * int64(len(value.TLS.CA)+len(value.TLS.Certificate)+len(value.TLS.Key))
	}
	if metadata.Logs || metadata.Traces {
		metadata.QueueItems, metadata.QueueBytes = value.QueueItems, value.QueueBytes
		// Input charging is not native storage: reserve copied SDK attributes,
		// capture/snapshot containers and queue-entry bookkeeping separately.
		metadata.QueueStorageBytes = 4*int64(value.QueueBytes) + 512*int64(value.QueueItems)
	}
	if metadata.Traces {
		metadata.SpanBytes = int64(value.MaxRecordBytes)
		metadata.MaxActiveSpans = min(value.ActiveCalls, value.QueueItems, value.QueueBytes/value.MaxRecordBytes)
	}
	// Eight declared dimensions, both cumulative histogram counter banks and
	// up to 33 buckets fit the same 16 KiB per-series envelope used for collection.
	// The configured cardinality includes the native overflow series.
	metadata.MetricStorageBytes = int64(len(value.Instruments)) * (int64(value.MetricCardinality)*(16<<10) + 4096)
	origins := make(map[string]bool)
	signals := 0
	for _, endpoint := range []string{value.LogsEndpoint, value.TracesEndpoint, value.MetricsEndpoint} {
		if endpoint == "" {
			continue
		}
		signals++
		parsed, _ := url.Parse(endpoint)
		origins[parsed.Scheme+"://"+parsed.Host] = parsed.Scheme == "https"
	}
	metadata.MaxConnections = len(origins)
	metadata.TransportBytes = int64(signals) * (64 << 10)
	for _, secure := range origins {
		// One physical HTTP/1 connection per origin: native read/write buffers,
		// bounded parsed response headers and transport/connection state.
		metadata.TransportBytes += 2*(4<<10) + 4*int64(value.MaxResponseBytes) + 64<<10
		if secure {
			// Go TLS permits a 256 KiB certificate handshake and 64 KiB other
			// handshake messages; peer certificate/decoded state may stay idle.
			metadata.TransportBytes += 4*(256<<10) + 2*(64<<10)
		}
	}
	metadata.SourceBytes = metadata.ConfigurationBytes + metadata.QueueStorageBytes + metadata.MetricStorageBytes + metadata.TransportBytes
	return metadata, nil
}
