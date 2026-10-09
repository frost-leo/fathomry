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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestPreparedFinalBudgetAndFrozenConstruction(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer server.Close()
	options := OptionsV1{Name: "prepared", ServiceName: "original", LogsEndpoint: server.URL + "/v1/logs",
		TracesEndpoint: server.URL + "/v1/traces", ActiveCalls: 3, QueueItems: 4, QueueBytes: 8192,
		MaxRecordBytes: 1024, MaxRequestBytes: 1024, MaxResponseBytes: 1024,
		Headers: map[string]string{"X-Synthetic": "frozen-secret"}, ResourceAttributes: map[string]string{"deployment": "original"}}
	baseline, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	layer := resource.Layer{Kind: resource.Local, Content: []byte("max_response_bytes: 65536\nsample_ratio: 0\nactive_calls: 2\nqueued_calls: 0\n")}
	prepared, err := PrepareV1(options, layer)
	if err != nil {
		t.Fatal(err)
	}
	metadata := prepared.Metadata()
	if requests.Load() != 0 || metadata.WorkBytes-baseline.Metadata().WorkBytes != 65536-1024 ||
		metadata.EvidenceBytes != 4*65536+32<<10 || metadata.MaxActiveSpans != 2 || metadata.SpanBytes != 1024 ||
		metadata.Limits.Active != 2 || metadata.Limits.Queued != 0 || metadata.Limits.Bytes != 2*metadata.WorkBytes ||
		metadata.Limits.QueuedBytes != 0 || metadata.MaxConnections != 1 || metadata.SourceBytes <= baseline.Metadata().SourceBytes {
		t.Fatalf("final preparation metadata mismatch: %+v", metadata)
	}
	if metadata.SourceBytes != metadata.ConfigurationBytes+metadata.QueueStorageBytes+metadata.MetricStorageBytes+metadata.TransportBytes {
		t.Fatal("source components do not compose")
	}
	options.Headers["X-Synthetic"] = "mutated"
	options.ResourceAttributes["deployment"] = "mutated"
	layer.Content[0] = 'X'
	changed := prepared.Metadata()
	changed.Limits.Active = 64
	if prepared.Metadata() != metadata || strings.Contains(fmt.Sprintf("%+v", prepared), "frozen-secret") {
		t.Fatal("prepared metadata alias or formatting leak")
	}
	if _, err := json.Marshal(prepared); err == nil {
		t.Fatal("prepared runtime token serialized")
	}
	for range 2 {
		selected := resource.WithLimits(prepared.Select(), metadata.Limits)
		assembly, err := resource.Assemble(context.Background(), context.Background(), "prepared", selected)
		if err != nil {
			t.Fatal(err)
		}
		inbox, err := invocation.NewInbox[Result](1, metadata.EvidenceBytes)
		if err != nil {
			t.Fatal(err)
		}
		client, err := Bind(assembly, selected, inbox, nil)
		if err != nil {
			t.Fatal("exact frozen budget did not bind", err)
		}
		actual, err := client.owner.settings.metadata()
		if err != nil || actual != metadata || client.owner.settings.Headers["X-Synthetic"] != "frozen-secret" ||
			client.owner.settings.ResourceAttributes["deployment"] != "original" || client.owner.settings.SampleRatio != 0 {
			t.Fatal("native construction diverged from frozen preparation", err)
		}
		if requests.Load() != 0 {
			t.Fatal("preparation or construction exported")
		}
		if err := assembly.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPreparedResidentCapacityAndSignalIsolation(t *testing.T) {
	options := validOptions()
	options.TracesEndpoint = options.LogsEndpoint
	options.QueueItems, options.QueueBytes, options.MaxRecordBytes = 8, 4096, 1024
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	base := prepared.Metadata()
	if base.MaxActiveSpans != 4 || base.QueueBytes != 4096 || base.QueueItems != 8 || base.MetricStorageBytes != 0 {
		t.Fatal("live-span capacity did not include the shared queue")
	}
	options.QueueBytes *= 2
	prepared, err = PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Metadata().QueueStorageBytes-base.QueueStorageBytes != 4*4096 || prepared.Metadata().MaxActiveSpans != 8 {
		t.Fatal("larger native queue missing from source accounting")
	}
	options = OptionsV1{Name: "metrics", ServiceName: "metrics", MetricsEndpoint: "http://127.0.0.1:1/v1/metrics",
		MetricCardinality: 2, Instruments: []InstrumentV1{{Name: "count", Kind: "int64-counter"}}}
	prepared, err = PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	metric := prepared.Metadata()
	if metric.QueueStorageBytes != 0 || metric.QueueItems != 0 || metric.QueueBytes != 0 || metric.MaxActiveSpans != 0 ||
		metric.SpanBytes != 0 || metric.MetricStorageBytes != 2*(16<<10)+4096 || !metric.Metrics || metric.Logs || metric.Traces {
		t.Fatal("metric-only source obtained disabled signal capacity")
	}
	options.MetricCardinality = 3
	prepared, err = PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Metadata().MetricStorageBytes-metric.MetricStorageBytes != 16<<10 || prepared.Metadata().WorkBytes-metric.WorkBytes != 16<<10 {
		t.Fatal("native cardinality not charged for both residence and collection")
	}
	options.Instruments[0].AttributeKeys = []string{"changed"}
	selected := resource.WithLimits(prepared.Select(), prepared.Metadata().Limits)
	assembly, err := resource.Assemble(context.Background(), context.Background(), "frozen", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	source, _, err := resource.Bind(assembly, selected)
	if err != nil || len(source.owner.settings.Instruments[0].AttributeKeys) != 0 {
		t.Fatal("prepared instrument configuration was borrowed", err)
	}
}

func TestPreparedRejectsFinalInvalidDataAndZeroSelection(t *testing.T) {
	for _, content := range []string{"timeout_ns: 0", "queue_items: 0", "sample_ratio: 2", "max_response_bytes: 0", "unknown: true"} {
		prepared, err := PrepareV1(validOptions(), resource.Layer{Kind: resource.Local, Content: []byte(content)})
		if err == nil || prepared.Metadata() != (Metadata{}) {
			t.Fatal("invalid final data retained a usable preparation", content)
		}
	}
	var prepared Prepared
	if _, err := resource.Assemble(context.Background(), context.Background(), "invalid", prepared.Select()); err == nil {
		t.Fatal("zero prepared selection constructed defaults")
	}
}
