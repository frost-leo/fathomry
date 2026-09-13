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

package zerologbridge

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	zerolog "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	otel "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	collog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

func TestAllSeveritiesThroughActualRecordWriter(t *testing.T) {
	var mu sync.Mutex
	var severities []int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, _ := io.ReadAll(request.Body)
		var logs collog.ExportLogsServiceRequest
		if proto.Unmarshal(data, &logs) != nil {
			writer.WriteHeader(400)
			return
		}
		mu.Lock()
		for _, resource := range logs.ResourceLogs {
			for _, scope := range resource.ScopeLogs {
				for _, record := range scope.LogRecords {
					severities = append(severities, int32(record.SeverityNumber))
				}
			}
		}
		mu.Unlock()
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(server.Close)
	options := otel.OptionsV1{Name: "otel", ServiceName: "bridge", LogsEndpoint: server.URL + "/v1/logs"}
	selected, err := otel.Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, otel.LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "bridge", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assembly.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	inbox, _ := invocation.NewInbox[otel.Result](32, 32<<20)
	client, err := otel.Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	sink := New(client)
	logging := zerolog.OptionsV1{Name: "zero", MinLevel: zerolog.Trace, Sinks: []zerolog.SinkV1{{Name: "otel", MinLevel: zerolog.Trace, Records: sink}}}
	selection, err := zerolog.Select(logging)
	if err != nil {
		t.Fatal(err)
	}
	selection = resource.WithLimits(selection, zerolog.LimitsV1(logging))
	logAssembly, err := resource.Assemble(context.Background(), context.Background(), "logs", selection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := logAssembly.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	logInbox, _ := invocation.NewInbox[zerolog.Result](16, 4<<20)
	logger, err := zerolog.Bind(logAssembly, selection, logInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, level := range []zerolog.Level{zerolog.Trace, zerolog.Debug, zerolog.Info, zerolog.Warn, zerolog.Error, zerolog.Fatal, zerolog.Panic} {
		receipt, err := logger.Log(context.Background(), fault.Correlation{Call: "level"}, level, "message")
		if err != nil {
			t.Fatal(err)
		}
		result, _ := receipt.Result()
		if result.Err() != nil {
			t.Fatal(result.Err())
		}
	}
	if err := sink.WriteRecord(context.Background(), zerolog.Record{}); err == nil {
		t.Fatal("absent record accepted")
	}
	receipt, err := client.Flush(context.Background(), fault.Correlation{Call: "flush"})
	if err != nil {
		t.Fatal(err)
	}
	result, _ := receipt.Result()
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	mu.Lock()
	defer mu.Unlock()
	expected := []int32{1, 5, 9, 13, 17, 21, 24}
	if len(severities) != len(expected) {
		t.Fatal("missing severities")
	}
	for index, severity := range severities {
		if severity != expected[index] {
			t.Fatal("severity mapping changed")
		}
	}
	if New(nil).WriteRecord(context.Background(), zerolog.Record{}) == nil {
		t.Fatal("nil sink accepted")
	}
}
