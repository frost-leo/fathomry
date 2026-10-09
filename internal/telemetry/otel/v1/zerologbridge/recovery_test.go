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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	zerolog "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	otel "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	collog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logproto "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

type recoveryFixture struct {
	client         *otel.Client
	logger         *zerolog.Logger
	telemetry      *resource.Assembly
	logs           *resource.Assembly
	telemetryInbox *invocation.Inbox[otel.Result]
	logInbox       *invocation.Inbox[zerolog.Result]
	local          bytes.Buffer
	mu             sync.Mutex
	received       []*logproto.LogRecord
}

func newRecoveryFixture(t *testing.T, options otel.OptionsV1, evidence int) *recoveryFixture {
	t.Helper()
	fixture := new(recoveryFixture)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(io.LimitReader(request.Body, 4<<20))
		if err != nil {
			t.Error(err)
			writer.WriteHeader(400)
			return
		}
		if request.URL.Path == "/logs" {
			var value collog.ExportLogsServiceRequest
			if err := proto.Unmarshal(data, &value); err != nil {
				t.Error(err)
				writer.WriteHeader(400)
				return
			}
			fixture.mu.Lock()
			for _, resource := range value.ResourceLogs {
				for _, scope := range resource.ScopeLogs {
					fixture.received = append(fixture.received, scope.LogRecords...)
				}
			}
			fixture.mu.Unlock()
		}
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(server.Close)
	options.Name, options.ServiceName = "telemetry", "managed-recovery"
	if options.LogsEndpoint != "" {
		options.LogsEndpoint = server.URL + "/logs"
	}
	if options.TracesEndpoint != "" {
		options.TracesEndpoint = server.URL + "/traces"
	}
	prepared, err := otel.PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	selection := resource.WithLimits(prepared.Select(), prepared.Metadata().Limits)
	fixture.telemetry, err = resource.Assemble(context.Background(), context.Background(), "telemetry", selection)
	if err != nil {
		t.Fatal(err)
	}
	fixture.telemetryInbox, err = invocation.NewInbox[otel.Result](evidence, int64(evidence)*prepared.Metadata().EvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	fixture.client, err = otel.Bind(fixture.telemetry, selection, fixture.telemetryInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.telemetry.Close(context.Background()); err != nil {
			t.Error(err)
		}
		drainRecovery(t, fixture.telemetryInbox)
		for _, source := range fixture.telemetry.Snapshot().Sources {
			if source.Pending {
				t.Error("telemetry retained source work")
			}
		}
	})
	logOptions := zerolog.OptionsV1{Name: "logging", Caller: true, Sinks: []zerolog.SinkV1{
		{Name: "remote", Kind: "managed-record"},
		{Name: "local", Kind: "writer"},
	}}
	logPrepared, err := zerolog.PrepareV1(logOptions)
	if err != nil {
		t.Fatal(err)
	}
	logSelection, err := logPrepared.Select(zerolog.BindingsV1{
		ManagedRecords: map[string]zerolog.ManagedRecordWriter{"remote": New(fixture.client)},
		Writers:        map[string]io.Writer{"local": &fixture.local},
	})
	if err != nil {
		t.Fatal(err)
	}
	logSelection = resource.WithLimits(logSelection, logPrepared.Metadata().Limits)
	fixture.logs, err = resource.Assemble(context.Background(), context.Background(), "logging", logSelection)
	if err != nil {
		t.Fatal(err)
	}
	fixture.logInbox, err = invocation.NewInbox[zerolog.Result](8, 8*logPrepared.Metadata().EvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	fixture.logger, err = zerolog.Bind(fixture.logs, logSelection, fixture.logInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.logs.Close(context.Background()); err != nil {
			t.Error(err)
		}
		drainRecovery(t, fixture.logInbox)
	})
	return fixture
}

func drainRecovery[T any](t *testing.T, inbox *invocation.Inbox[T]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for inbox.Usage().Outstanding > 0 {
		record, err := inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := record.Receipt().WaitReleased(ctx); err != nil {
			t.Fatal(err)
		}
		if err := record.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if inbox.Usage().ReservedBytes != 0 {
		t.Fatal("evidence byte reservation leaked")
	}
}
func (fixture *recoveryFixture) log(t *testing.T, message string, attrs ...slog.Attr) invocation.Result[zerolog.Result] {
	t.Helper()
	receipt, err := fixture.logger.Log(context.Background(), fault.Correlation{Call: message}, zerolog.Info, message, attrs...)
	if err != nil || receipt == nil {
		t.Fatal("logging admission", err)
	}
	result, ok := receipt.Result()
	if !ok || !result.Final || !result.Released {
		t.Fatal("logging did not actually release")
	}
	drainRecovery(t, fixture.logInbox)
	return result
}
func (fixture *recoveryFixture) flush(t *testing.T) {
	t.Helper()
	drainRecovery(t, fixture.telemetryInbox)
	receipt, err := fixture.client.Flush(context.Background(), fault.Correlation{Call: "flush"})
	if err != nil || receipt == nil {
		t.Fatal("flush admission", err)
	}
	result, ok := receipt.Result()
	if !ok || !result.Final || !result.Released || result.Err() != nil {
		t.Fatal("flush failed", result.Err())
	}
	drainRecovery(t, fixture.telemetryInbox)
}
func (fixture *recoveryFixture) messages() []string {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	var result []string
	for _, record := range fixture.received {
		result = append(result, record.Body.GetStringValue())
	}
	return result
}
func assertRejected(t *testing.T, result invocation.Result[zerolog.Result]) {
	t.Helper()
	sinks := result.Outcome.Value.SinksCopy()
	if result.Err() == nil || len(sinks) != 2 || !sinks[0].Attempted || sinks[0].Accepted || sinks[0].Stopped || sinks[0].BytesKnown || !sinks[1].Accepted {
		t.Fatal("record rejection lost independent output facts", result.Err())
	}
}
func assertAccepted(t *testing.T, result invocation.Result[zerolog.Result]) {
	t.Helper()
	sinks := result.Outcome.Value.SinksCopy()
	if result.Err() != nil || len(sinks) != 2 || !sinks[0].Accepted || sinks[0].Stopped || !sinks[1].Accepted {
		t.Fatal("healthy independent record was not accepted", result.Err())
	}
}
func keyValue(values []*common.KeyValue, key string) *common.AnyValue {
	for _, item := range values {
		if item.Key == key {
			return item.Value
		}
	}
	return nil
}

func TestManagedBridgeOverflowPreservesLocalUint64AndNextExactWireInt64(t *testing.T) {
	fixture := newRecoveryFixture(t, otel.OptionsV1{LogsEndpoint: "enabled"}, 16)
	first := fixture.log(t, "overflow", slog.Uint64("number", math.MaxUint64))
	assertRejected(t, first)
	decoder := json.NewDecoder(bytes.NewReader(fixture.local.Bytes()))
	decoder.UseNumber()
	var local map[string]any
	if err := decoder.Decode(&local); err != nil {
		t.Fatal(err)
	}
	if local["attributes"].(map[string]any)["number"] != json.Number("18446744073709551615") {
		t.Fatal("local uint64 lost precision")
	}
	assertAccepted(t, fixture.log(t, "exact", slog.Int64("number", (1<<53)+1)))
	fixture.flush(t)
	if got := fixture.messages(); len(got) != 1 || got[0] != "exact" {
		t.Fatal("rejected record retried or legal record lost", got)
	}
	fixture.mu.Lock()
	number := keyValue(keyValue(fixture.received[0].Attributes, "attributes").GetKvlistValue().Values, "number")
	fixture.mu.Unlock()
	if _, ok := number.Value.(*common.AnyValue_IntValue); !ok || number.GetIntValue() != (1<<53)+1 {
		t.Fatal("actual OTLP integer field lost precision")
	}
	assertRejected(t, first)
}

func TestManagedBridgeQueueFullRecoversAfterExplicitFlush(t *testing.T) {
	fixture := newRecoveryFixture(t, otel.OptionsV1{LogsEndpoint: "enabled", QueueItems: 1, BatchSize: 1}, 16)
	assertAccepted(t, fixture.log(t, "queued"))
	rejected := fixture.log(t, "queue-full")
	assertRejected(t, rejected)
	if !errors.Is(rejected.Err(), otel.ErrLimit) {
		t.Fatal("queue refusal identity lost")
	}
	fixture.flush(t)
	assertAccepted(t, fixture.log(t, "after-flush"))
	fixture.flush(t)
	if got := fixture.messages(); fmt.Sprint(got) != "[queued after-flush]" {
		t.Fatal("queue recovery replayed or lost records", got)
	}
	assertRejected(t, rejected)
}

func TestManagedBridgeClosedDataAndOriginalAbsentTimeCaller(t *testing.T) {
	fixture := newRecoveryFixture(t, otel.OptionsV1{LogsEndpoint: "enabled"}, 8)
	var pcs [1]uintptr
	runtime.Callers(1, pcs[:])
	stamp := time.Date(2020, 3, 4, 5, 6, 7, 8, time.FixedZone("offset", 3600))
	value := zerolog.Value{Kind: "map", Map: []zerolog.Attribute{
		{Key: "null", Value: zerolog.Value{}},
		{Key: "binary", Value: zerolog.Value{Kind: "bytes", Bytes: []byte{0, 255, 1}}},
		{Key: "text", Value: zerolog.Value{Kind: "bytestring", Bytes: []byte("utf8")}},
		{Key: "empty", Value: zerolog.Value{Kind: "map"}},
		{Key: "array", Value: zerolog.Value{Kind: "array", Array: []zerolog.Value{{Kind: "float32", Float32: 1.25}, {Kind: "duration", Duration: 31 * time.Nanosecond}, {Kind: "time", Time: stamp}}}},
	}}
	receipt, err := fixture.logger.LogEntry(context.Background(), fault.Correlation{Call: "typed-original"}, zerolog.Entry{PC: pcs[0], Level: zerolog.Info, Message: "typed"}, slog.Any("value", value))
	if err != nil || receipt == nil {
		t.Fatal(err)
	}
	result, ok := receipt.Result()
	if !ok || !result.Released {
		t.Fatal("typed bridge work did not release")
	}
	assertAccepted(t, result)
	drainRecovery(t, fixture.logInbox)
	fixture.flush(t)
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.received) != 1 || fixture.received[0].TimeUnixNano != 0 {
		t.Fatal("bridge invented a timestamp for absent original time")
	}
	wire := fixture.received[0]
	fields := keyValue(keyValue(wire.Attributes, "attributes").GetKvlistValue().Values, "value").GetKvlistValue().Values
	if keyValue(fields, "null") == nil || keyValue(fields, "null").Value != nil || !bytes.Equal(keyValue(fields, "binary").GetBytesValue(), []byte{0, 255, 1}) || keyValue(fields, "text").GetStringValue() != "utf8" {
		t.Fatal("closed null/binary/text distinctions changed on actual wire")
	}
	if _, ok := keyValue(fields, "empty").Value.(*common.AnyValue_KvlistValue); !ok || len(keyValue(fields, "empty").GetKvlistValue().Values) != 0 {
		t.Fatal("empty map lost presence")
	}
	array := keyValue(fields, "array").GetArrayValue().Values
	if len(array) != 3 || array[0].GetDoubleValue() != 1.25 || array[1].GetIntValue() != 31 || array[2].GetStringValue() != stamp.UTC().Format(time.RFC3339Nano) {
		t.Fatal("array scalar semantics changed on actual wire")
	}
	metadata := keyValue(wire.Attributes, "logging").GetKvlistValue().Values
	caller := keyValue(metadata, "caller").GetKvlistValue().Values
	if !strings.HasSuffix(keyValue(caller, "function").GetStringValue(), "TestManagedBridgeClosedDataAndOriginalAbsentTimeCaller") || !strings.HasSuffix(keyValue(caller, "file").GetStringValue(), "recovery_test.go") {
		t.Fatal("bridge replaced original caller with wrapper")
	}
	var local map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(fixture.local.Bytes()), &local); err != nil {
		t.Fatal(err)
	}
	if _, present := local["time"]; present {
		t.Fatal("native local sink invented absent timestamp")
	}
}

func TestManagedBridgeEvidenceFullRecoversWithoutChangingCapacity(t *testing.T) {
	fixture := newRecoveryFixture(t, otel.OptionsV1{LogsEndpoint: "enabled", QueueItems: 8}, 1)
	assertAccepted(t, fixture.log(t, "first"))
	rejected := fixture.log(t, "evidence-full")
	assertRejected(t, rejected)
	if !errors.Is(rejected.Err(), invocation.ErrEvidence) || fixture.telemetryInbox.Usage().Outstanding != 1 {
		t.Fatal("not actual independent evidence saturation")
	}
	drainRecovery(t, fixture.telemetryInbox)
	receipt, err := fixture.client.Emit(context.Background(), fault.Correlation{Call: "direct-control"}, otel.LogRecord{Message: "direct-control"})
	if err != nil || receipt == nil {
		t.Fatal("healthy destination direct control", err)
	}
	result, ok := receipt.Result()
	if !ok || result.Err() != nil {
		t.Fatal("direct control failed", result.Err())
	}
	drainRecovery(t, fixture.telemetryInbox)
	assertAccepted(t, fixture.log(t, "after-drain"))
	fixture.flush(t)
	if got := fixture.messages(); fmt.Sprint(got) != "[first direct-control after-drain]" {
		t.Fatal("evidence recovery replayed or lost records", got)
	}
}

func TestManagedBridgeTemporaryAdmissionRecoversAfterSpanEnds(t *testing.T) {
	fixture := newRecoveryFixture(t, otel.OptionsV1{LogsEndpoint: "enabled", TracesEndpoint: "enabled", ActiveCalls: 1}, 16)
	_, span, err := fixture.client.Start(context.Background(), fault.Correlation{Call: "held-span"}, otel.SpanInput{Name: "held"})
	if err != nil || span == nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = span.End(context.Background()) })
	rejected := fixture.log(t, "active-full")
	assertRejected(t, rejected)
	if !errors.Is(rejected.Err(), resource.ErrCapacity) {
		t.Fatal("not actual source admission saturation", rejected.Err())
	}
	if receipt, err := span.End(context.Background()); err != nil || receipt == nil {
		t.Fatal("span release", err)
	}
	assertAccepted(t, fixture.log(t, "after-span"))
	fixture.flush(t)
	if got := fixture.messages(); fmt.Sprint(got) != "[after-span]" {
		t.Fatal("admission refusal was retried", got)
	}
}

func TestManagedBridgeTerminalDestinationLatchesOnlyRemote(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprint(closed), func(t *testing.T) {
			options := otel.OptionsV1{TracesEndpoint: "enabled"}
			if closed {
				options.LogsEndpoint = "enabled"
			}
			fixture := newRecoveryFixture(t, options, 8)
			if closed {
				if err := fixture.telemetry.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			first := fixture.log(t, "terminal")
			reports := first.Outcome.Value.SinksCopy()
			if first.Err() == nil || !reports[0].Attempted || !reports[0].Stopped || reports[0].Accepted || !reports[1].Accepted {
				t.Fatal("terminal destination was not stopped")
			}
			later := fixture.log(t, "later")
			reports = later.Outcome.Value.SinksCopy()
			if later.Err() == nil || reports[0].Attempted || !reports[0].Stopped || !reports[1].Accepted {
				t.Fatal("terminal destination was retried or sibling stopped")
			}
			if len(fixture.messages()) != 0 {
				t.Fatal("terminal destination received a record")
			}
		})
	}
}

type hostileMatching struct{ matches int }

func (*hostileMatching) Error() string       { panic("Error called") }
func (value *hostileMatching) Is(error) bool { value.matches++; panic("Is called") }
func (value *hostileMatching) As(any) bool   { value.matches++; panic("As called") }

type cycleError struct{}

func (*cycleError) Error() string       { panic("Error called") }
func (value *cycleError) Unwrap() error { return value }

func TestManagedRefusalClassificationBoundedAndSemantic(t *testing.T) {
	hostile := new(hostileMatching)
	known := otel.ErrLimit.New(fault.Context{}, hostile)
	if !independentRefusal(known, 16) || hostile.matches != 0 {
		t.Fatal("known record boundary inspected opaque cause")
	}
	if independentRefusal(hostile, 16) || independentRefusal(new(cycleError), 16) {
		t.Fatal("unknown/cyclic error became retry authority")
	}
	canceled := invocation.ErrBudget.New(fault.Context{}, context.Canceled, hostile)
	if !independentRefusal(canceled, 16) || hostile.matches != 0 {
		t.Fatal("known canceled admission inspected private cause")
	}
	for _, operation := range []string{"entry", "emit"} {
		if !independentRefusal(otel.ErrState.New(fault.Context{Operation: operation}, context.Canceled, hostile), 16) || hostile.matches != 0 {
			t.Fatal("known pre-emission cancellation stopped a healthy target", operation)
		}
	}
	for _, terminal := range []error{
		otel.ErrUnsupported.New(fault.Context{Operation: "logs-disabled"}, context.Canceled),
		otel.ErrState.New(fault.Context{Operation: "closed"}, context.Canceled),
		otel.ErrEnvironment.New(fault.Context{}, context.Canceled),
		otel.ErrCleanup.New(fault.Context{}, context.Canceled),
		errors.Join(known, otel.ErrState.New(fault.Context{Operation: "closed"})),
	} {
		if independentRefusal(terminal, 32) {
			t.Fatal("terminal semantic boundary overruled by nested cancellation")
		}
	}
	if independentRefusal(errors.Join(known, known, known), 2) {
		t.Fatal("node budget not enforced")
	}
	if !independentRefusal(invocation.ErrFailed.New(fault.Context{}, resource.ErrAdmission.New(fault.Context{}, resource.ErrCapacity.New(fault.Context{}))), 16) {
		t.Fatal("actual admission wrappers lost recoverability")
	}
}
