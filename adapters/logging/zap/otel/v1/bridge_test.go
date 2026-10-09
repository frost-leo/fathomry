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

package zapotel_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	bridge "github.com/frost-leo/fathomry/adapters/logging/zap/otel/v1"
	zap "github.com/frost-leo/fathomry/adapters/logging/zap/v1"
	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	logpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

func ptr[T any](value T) *T { return &value }
func receiver[T any](inbox *adapters.Inbox[T]) <-chan error {
	done := make(chan error, 1)
	go func() {
		for {
			delivery, err := inbox.NextReleased(context.Background())
			if errors.Is(err, io.EOF) {
				done <- nil
				return
			}
			if err != nil {
				done <- err
				return
			}
			if err := delivery.Ack(); err != nil {
				done <- err
				return
			}
		}
	}()
	return done
}
func TestMixedAndTelemetryOnlyActualOTLPAndIndependentOutcomes(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "otel-only", true: "mixed"}[mixed], func(t *testing.T) {
			var mu sync.Mutex
			var records []*logs.LogRecord
			var peerErr error
			peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var reader io.Reader = request.Body
				if request.Header.Get("Content-Encoding") == "gzip" {
					decoded, err := gzip.NewReader(request.Body)
					if err != nil {
						mu.Lock()
						peerErr = err
						mu.Unlock()
						writer.WriteHeader(400)
						return
					}
					defer decoded.Close()
					reader = decoded
				}
				data, err := io.ReadAll(reader)
				var message logpb.ExportLogsServiceRequest
				if err == nil {
					err = proto.Unmarshal(data, &message)
				}
				mu.Lock()
				if err != nil {
					peerErr = err
				} else {
					for _, resource := range message.ResourceLogs {
						for _, scope := range resource.ScopeLogs {
							records = append(records, scope.LogRecords...)
						}
					}
				}
				mu.Unlock()
				writer.Header().Set("Content-Type", "application/x-protobuf")
				writer.WriteHeader(200)
			}))
			defer peer.Close()
			settings := otel.Settings{Name: "telemetry", Version: 1, ServiceName: "test", LogsEndpoint: peer.URL + "/v1/logs"}
			policy, err := otel.Recommend(settings)
			if err != nil {
				t.Fatal(err)
			}
			telemetryRuntime, _ := adapters.New(context.Background(), policy.Runtime)
			telemetryInbox, _ := adapters.NewInbox[otel.Result](policy.Evidence)
			telemetryDone := receiver(telemetryInbox)
			telemetry, err := otel.Open(context.Background(), settings, otel.Dependencies{Runtime: telemetryRuntime, Evidence: telemetryInbox})
			if err != nil {
				t.Fatal(err)
			}
			sink, err := bridge.New(telemetry.Client())
			if err != nil {
				t.Fatal(err)
			}
			directory := t.TempDir()
			if err := os.Chmod(directory, 0700); err != nil {
				t.Fatal(err)
			}
			loggerSettings := zap.Settings{Name: "logging", Version: 1, Structured: true, ExtensionLevel: ptr("warn"), Caller: ptr(true)}
			if mixed {
				loggerSettings.Outputs = []zap.Output{{Name: "local", Kind: "file", Directory: directory, Level: ptr("info")}}
			}
			loggerPolicy, err := zap.Recommend(loggerSettings)
			if err != nil {
				t.Fatal(err)
			}
			loggerRuntime, _ := adapters.New(context.Background(), loggerPolicy.Runtime)
			loggerInbox, _ := adapters.NewInbox[zap.Result](loggerPolicy.Evidence)
			loggerDone := receiver(loggerInbox)
			logger, err := zap.Open(context.Background(), loggerSettings, zap.Dependencies{Runtime: loggerRuntime, Evidence: loggerInbox, Structured: sink})
			if err != nil {
				t.Fatal(err)
			}
			ctx, err := otel.Extract(context.Background(), map[string]string{"traceparent": "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"}, false)
			if err != nil {
				t.Fatal(err)
			}
			stamp := time.Date(2024, 2, 3, 4, 5, 6, 789, time.UTC)
			call := func(entry zap.Entry, fields ...zapcore.Field) zap.Result {
				t.Helper()
				receipt, err := logger.Client().LogEntry(ctx, entry, fields...)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := receipt.WaitReleased(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				result, ok := snapshot.ValueCopy()
				if !ok {
					t.Fatal("missing effects", snapshot.Err())
				}
				return result
			}
			filtered := call(zap.Entry{Time: stamp, Level: zapcore.InfoLevel, Message: "local only"})
			states := filtered.SinksCopy()
			if states[len(states)-1].State != zap.Filtered {
				t.Fatal("remote filter")
			}
			rejected := call(zap.Entry{Time: stamp, Level: zapcore.ErrorLevel, Message: "remote refusal"}, sdk.Uint64("unsigned", math.MaxUint64))
			states = rejected.SinksCopy()
			if states[len(states)-1].State != zap.Failed || states[len(states)-1].Err == nil {
				t.Fatal("remote refusal hidden")
			}
			if mixed && states[0].State != zap.Written {
				t.Fatal("local sibling disabled")
			}
			if rejected.NativeCorrelation().Call == "" || filtered.NativeCorrelation().Call == "" || rejected.NativeCorrelation().Call == filtered.NativeCorrelation().Call {
				t.Fatal("refused/filtered evidence lost native identity")
			}
			accepted := call(zap.Entry{Time: stamp, Level: zapcore.WarnLevel, Message: "typed"},
				zap.Field("nested", logging.Group(logging.Field{Key: "null", Value: logging.Null()}, logging.Field{Key: "empty", Value: logging.Array()}, logging.Field{Key: "integer", Value: logging.Int64(1<<53 + 1)}, logging.Field{Key: "binary", Value: logging.Binary([]byte{255, 0, 1})})))
			states = accepted.SinksCopy()
			if states[len(states)-1].State != zap.Written {
				t.Fatal("healthy next record refused", states[len(states)-1].Err)
			}
			absent := call(zap.Entry{Level: zapcore.WarnLevel, Message: "absent time"})
			if absent.SinksCopy()[len(states)-1].State != zap.Written {
				t.Fatal("absent time")
			}
			loop, err := telemetry.StartExport(context.Background(), otel.ExportOptions{Interval: 5 * time.Millisecond, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			progress, cancelProgress := context.WithTimeout(context.Background(), 2*time.Second)
			tick := time.NewTicker(time.Millisecond)
			for {
				status, err := loop.Status()
				if err != nil {
					t.Fatal(err)
				}
				if status.Succeeded > 0 {
					break
				}
				select {
				case <-tick.C:
				case <-progress.Done():
					t.Fatal("composed export loop made no progress", progress.Err())
				}
			}
			tick.Stop()
			cancelProgress()
			if err := logger.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !logger.ShutdownComplete() {
				t.Fatal("logging still active")
			}
			if err := loggerRuntime.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := loop.Stop(context.Background()); err != nil {
				t.Fatal(err)
			}
			receipt, err := telemetry.Client().Flush(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := receipt.WaitReleased(context.Background())
			if err != nil || snapshot.Err() != nil {
				t.Fatal("flush", err, snapshot.Err())
			}
			if err := telemetry.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := telemetryRuntime.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			_ = loggerInbox.Seal()
			_ = telemetryInbox.Seal()
			if err := <-loggerDone; err != nil {
				t.Fatal(err)
			}
			if err := <-telemetryDone; err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if peerErr != nil || len(records) != 2 {
				t.Fatal("wire records", len(records), peerErr)
			}
			if records[0].TimeUnixNano != uint64(stamp.UnixNano()) || records[1].TimeUnixNano != 0 || len(records[0].TraceId) != 16 || records[0].TraceId[0] != 1 {
				t.Fatal("time/trace fidelity")
			}
			attributes := find(records[0].Attributes, "attributes").GetKvlistValue().Values
			loggerMetadata := find(records[0].Attributes, "logging").GetKvlistValue().Values
			if find(loggerMetadata, "call").GetStringValue() != accepted.NativeCorrelation().Call {
				t.Fatal("OTLP event cannot be linked to logging evidence")
			}
			nested := find(attributes, "nested").GetKvlistValue().Values
			if find(nested, "integer").GetIntValue() != 1<<53+1 || string(find(nested, "binary").GetBytesValue()) != string([]byte{255, 0, 1}) {
				t.Fatal("typed wire loss")
			}
			if find(nested, "null").Value != nil || find(nested, "empty").GetArrayValue() == nil {
				t.Fatal("null/empty loss")
			}
			entries, err := os.ReadDir(directory)
			if err != nil {
				t.Fatal(err)
			}
			if !mixed && len(entries) != 0 {
				t.Fatal("otel-only acquired local output")
			}
			if mixed {
				data, err := os.ReadFile(filepath.Join(directory, "current.log"))
				if err != nil {
					t.Fatal(err)
				}
				lines := strings.Split(strings.TrimSpace(string(data)), "\n")
				if len(lines) != 4 {
					t.Fatal("local output count", len(lines))
				}
				var local map[string]json.RawMessage
				if err := json.Unmarshal([]byte(lines[1]), &local); err != nil {
					t.Fatal(err)
				}
				if string(local["unsigned"]) != "18446744073709551615" {
					t.Fatal("local unsigned shrunk")
				}
			}
		})
	}
}
func find(values []*common.KeyValue, key string) *common.AnyValue {
	for _, value := range values {
		if value.Key == key {
			return value.Value
		}
	}
	return &common.AnyValue{}
}

func TestSharedRuntimeRefusalBeforeLoggingEffects(t *testing.T) {
	telemetryPolicy, _ := otel.Recommend(otel.Settings{Name: "otel", Version: 1, ServiceName: "test", LogsEndpoint: "http://127.0.0.1:1/v1/logs"})
	loggerSettings := zap.Settings{Name: "logger", Version: 1, Structured: true}
	loggerPolicy, _ := zap.Recommend(loggerSettings)
	// Capacity is deliberately ample: identity, not saturation, must refuse nesting.
	loggerPolicy.Runtime.MaxWorkBytes += telemetryPolicy.Runtime.MaxWorkBytes
	loggerPolicy.Runtime.MaxActive += telemetryPolicy.Runtime.MaxActive
	runtime, _ := adapters.New(context.Background(), loggerPolicy.Runtime)
	inbox, _ := adapters.NewInbox[otel.Result](telemetryPolicy.Evidence)
	done := receiver(inbox)
	telemetry, err := otel.Open(context.Background(), otel.Settings{Name: "otel", Version: 1, ServiceName: "test", LogsEndpoint: "http://127.0.0.1:1/v1/logs"}, otel.Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	sink, _ := bridge.New(telemetry.Client())
	evidence, _ := adapters.NewInbox[zap.Result](loggerPolicy.Evidence)
	logger, err := zap.Open(context.Background(), loggerSettings, zap.Dependencies{Runtime: runtime, Evidence: evidence, Structured: sink})
	if logger != nil || !errors.Is(err, zap.ErrUnsupported) {
		t.Fatal("unsafe nested root accepted", err)
	}
	if status, _ := evidence.Inspect(); status.Outstanding != 0 {
		t.Fatal("refusal accepted evidence")
	}
	if err := telemetry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = inbox.Seal()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
