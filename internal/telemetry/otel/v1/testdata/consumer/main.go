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
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	otel "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	collog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}
func run() error {
	var received atomic.Int32
	closed := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(io.LimitReader(request.Body, 1<<20))
		if err != nil {
			writer.WriteHeader(400)
			return
		}
		var logs collog.ExportLogsServiceRequest
		if proto.Unmarshal(data, &logs) != nil || len(logs.ResourceLogs) != 1 || len(logs.ResourceLogs[0].ScopeLogs) != 1 ||
			len(logs.ResourceLogs[0].ScopeLogs[0].LogRecords) != 1 || logs.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Body.GetStringValue() != "consumer" {
			writer.WriteHeader(400)
			return
		}
		received.Add(1)
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			select {
			case closed <- struct{}{}:
			default:
			}
		}
	}
	server.Start()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := otel.OptionsV1{Name: "consumer", ServiceName: "consumer", LogsEndpoint: server.URL + "/v1/logs"}
	selected, err := otel.Select(options)
	if err != nil {
		return err
	}
	selected = resource.WithLimits(selected, otel.LimitsV1(options))
	assembly, err := resource.Assemble(ctx, ctx, "consumer", selected)
	if err != nil {
		return err
	}
	defer assembly.Close(ctx)
	inbox, err := invocation.NewInbox[otel.Result](4, 4<<20)
	if err != nil {
		return err
	}
	client, err := otel.Bind(assembly, selected, inbox, nil)
	if err != nil {
		return err
	}
	receipt, err := client.Emit(ctx, fault.Correlation{Call: "emit"}, otel.LogRecord{Message: "consumer"})
	if err != nil {
		return err
	}
	result, _ := receipt.Result()
	if err := result.Err(); err != nil {
		return err
	}
	queued := received.Load() == 0
	receipt, err = client.Flush(ctx, fault.Correlation{Call: "flush"})
	if err != nil {
		return err
	}
	result, _ = receipt.Result()
	if err := result.Err(); err != nil {
		return err
	}
	if err := assembly.Close(ctx); err != nil {
		return err
	}
	released := false
	select {
	case <-closed:
		released = true
	case <-ctx.Done():
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{
		"go.opentelemetry.io/otel", "go.opentelemetry.io/otel/sdk", "go.opentelemetry.io/otel/trace", "go.opentelemetry.io/otel/metric",
		"go.opentelemetry.io/otel/log", "go.opentelemetry.io/otel/sdk/log", "go.opentelemetry.io/otel/sdk/metric",
		"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp", "go.opentelemetry.io/otel/exporters/otlp/otlptrace",
		"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp",
		"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp", "go.opentelemetry.io/proto/otlp",
	}})
	if err != nil {
		return err
	}
	modules := map[string]string{}
	for _, module := range build.SDKs {
		modules[module.Path.Value] = module.Version.Value
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Go                         string
		Modules                    map[string]string
		Queued, Received, Released bool
	}{build.Go.Value, modules, queued, received.Load() == 1, released})
}
