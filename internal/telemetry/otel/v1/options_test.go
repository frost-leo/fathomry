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
	"errors"
	"math"
	"testing"

	"github.com/frost-leo/fathomry/internal/resource"
)

func validOptions() OptionsV1 {
	return OptionsV1{Name: "test", ServiceName: "test", LogsEndpoint: "http://127.0.0.1:1/v1/logs"}
}

func TestOptionsDefaultsLayersAndFrozenCopies(t *testing.T) {
	options := validOptions()
	options.ResourceAttributes = map[string]string{"environment": "test"}
	layers := []resource.Layer{{Kind: resource.Variables, Content: []byte("sample_ratio: 0\nheaders: {}")}}
	selected, err := Select(options, layers...)
	if err != nil {
		t.Fatal(err)
	}
	options.ResourceAttributes["environment"] = "mutated"
	layers[0].Content[0] = 'X'
	selected = resource.WithLimits(selected, LimitsV1(options))
	for range 2 {
		assembly, err := resource.Assemble(context.Background(), context.Background(), "frozen", selected)
		if err != nil {
			t.Fatal(err)
		}
		source, _, err := resource.Bind(assembly, selected)
		if err != nil || source.owner.settings.ResourceAttributes["environment"] != "test" || source.owner.settings.SampleRatio != 0 {
			t.Fatal("prepared input was not frozen")
		}
		if err := assembly.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	prepared := defaulted(validOptions())
	if prepared.SampleRatio != 1 || prepared.ActiveCalls != 16 || prepared.QueueItems != 512 || prepared.QueueBytes != 4<<20 ||
		prepared.MaxRecordBytes != 64<<10 || prepared.BatchSize != 128 || prepared.Timeout != 5e9 {
		t.Fatal("defaults changed")
	}
	zero := 0.0
	explicit := validOptions()
	explicit.SampleRatio = &zero
	if defaulted(explicit).SampleRatio != 0 {
		t.Fatal("explicit zero lost")
	}
}
func TestInvalidOptions(t *testing.T) {
	tests := map[string]func(*OptionsV1){
		"version":                      func(value *OptionsV1) { value.Format = 2 },
		"none":                         func(value *OptionsV1) { value.LogsEndpoint = "" },
		"remote-clear":                 func(value *OptionsV1) { value.LogsEndpoint = "http://example.com/v1/logs" },
		"dns-clear":                    func(value *OptionsV1) { value.LogsEndpoint = "http://localhost/v1/logs" },
		"userinfo":                     func(value *OptionsV1) { value.LogsEndpoint = "http://private:canary@127.0.0.1/v1/logs" },
		"query":                        func(value *OptionsV1) { value.LogsEndpoint += "?secret=canary" },
		"tls":                          func(value *OptionsV1) { value.LogsEndpoint = "https://example.test/v1/logs" },
		"negative":                     func(value *OptionsV1) { value.QueueItems = -1 },
		"bytes":                        func(value *OptionsV1) { value.QueueBytes = 512 },
		"batch":                        func(value *OptionsV1) { value.BatchSize = 513 },
		"header":                       func(value *OptionsV1) { value.Headers = map[string]string{"Authorization": "value\r\ncanary"} },
		"duplicate-header":             func(value *OptionsV1) { value.Headers = map[string]string{"X-Test": "a", "x-test": "b"} },
		"type-header":                  func(value *OptionsV1) { value.Headers = map[string]string{"Content-Type": "text/plain"} },
		"compression":                  func(value *OptionsV1) { value.Compression = "zstd" },
		"nan":                          func(value *OptionsV1) { number := math.NaN(); value.SampleRatio = &number },
		"instruments-without-endpoint": func(value *OptionsV1) { value.Instruments = []InstrumentV1{{Name: "count", Kind: "int64-counter"}} },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			value := validOptions()
			change(&value)
			if _, err := Select(value); err == nil {
				t.Fatal("invalid options accepted")
			}
		})
	}
	for _, layer := range []string{"unknown: 1", "queue_items: 0", "timeout_ns: 0", "sample_ratio: null", "headers: {x: 1}", "queue_items: 1.0", "compression: zstd", "queue_items: 1\nqueue_items: 2"} {
		if _, err := Select(validOptions(), resource.Layer{Kind: resource.Base, Content: []byte(layer)}); err == nil {
			t.Errorf("invalid layer accepted: %s", layer)
		}
	}
}
func TestEnvironmentRejectedAtPreparationAndConstruction(t *testing.T) {
	for _, key := range []string{"OTEL_RESOURCE_ATTRIBUTES", "OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_CERTIFICATE", "OTEL_GO_X_OBSERVABILITY", "OTEL_TRACES_SAMPLER"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "synthetic-canary")
			if _, err := Select(validOptions()); !errors.Is(err, ErrEnvironment) {
				t.Fatal("ambient input accepted")
			}
		})
	}
	selection, err := Select(validOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("OTEL_SERVICE_NAME", "synthetic-canary")
	assembly, err := resource.Assemble(context.Background(), context.Background(), "test", resource.WithLimits(selection, LimitsV1(validOptions())))
	if !errors.Is(err, ErrEnvironment) || assembly == nil || assembly.Snapshot().Sources[0].Pending {
		t.Fatal("changed environment reached native construction")
	}
}
func FuzzOptionsLayers(f *testing.F) {
	f.Add([]byte("queue_items: 8\nbatch_size: 2"))
	f.Add([]byte("sample_ratio: 0"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			return
		}
		_, _ = Select(validOptions(), resource.Layer{Kind: resource.Base, Content: data})
	})
}
