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

package zapbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	otel "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	collog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

type forbidden struct{ called *bool }

func (value forbidden) MarshalLogObject(zapcore.ObjectEncoder) error {
	*value.called = true
	panic("unexpected marshaler")
}
func TestFieldTranslationAndBorrowedSync(t *testing.T) {
	var mu sync.Mutex
	var records []*collog.ExportLogsServiceRequest
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, _ := io.ReadAll(request.Body)
		value := &collog.ExportLogsServiceRequest{}
		if proto.Unmarshal(data, value) != nil {
			writer.WriteHeader(400)
			return
		}
		mu.Lock()
		records = append(records, value)
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
	metadata := []zapcore.Field{sdk.String("fathomry.call", "bridge"), sdk.String("fathomry.provider", "logging.zap.v1"),
		sdk.String("fathomry.source", "original"), sdk.String("fathomry.scope", "original")}
	scalarFields := []zapcore.Field{sdk.String("text", "value"), sdk.Bool("bool", true), sdk.Int32("signed", 2), sdk.Uint64("unsigned", 3),
		sdk.Float32("float32", 1.25), sdk.Float64("float64", 2.5), sdk.Duration("duration", time.Second),
		sdk.Time("time", time.Unix(1, 2).UTC()), sdk.Binary("binary", []byte{0, 255}), sdk.ByteString("bytestring", []byte("text")),
		sdk.Time("fulltime", time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)),
		{Key: "raw-bool-zero", Type: zapcore.BoolType, Integer: 0},
		{Key: "raw-bool-one", Type: zapcore.BoolType, Integer: 1},
		{Key: "raw-bool-two", Type: zapcore.BoolType, Integer: 2},
		{Key: "raw-bool-negative", Type: zapcore.BoolType, Integer: -1},
		{Key: "raw-int64-min", Type: zapcore.Int64Type, Integer: math.MinInt64},
		{Key: "raw-int64-max", Type: zapcore.Int64Type, Integer: math.MaxInt64},
		{Key: "raw-int32", Type: zapcore.Int32Type, Integer: math.MaxInt64},
		{Key: "raw-int16", Type: zapcore.Int16Type, Integer: 1<<16 + 9},
		{Key: "raw-int8", Type: zapcore.Int8Type, Integer: 257},
		{Key: "raw-uint64", Type: zapcore.Uint64Type, Integer: math.MaxInt64},
		{Key: "raw-uint32", Type: zapcore.Uint32Type, Integer: -1},
		{Key: "raw-uint16", Type: zapcore.Uint16Type, Integer: -1},
		{Key: "raw-uint8", Type: zapcore.Uint8Type, Integer: -1},
		{Key: "raw-float32", Type: zapcore.Float32Type, Integer: 1<<40 | int64(math.Float32bits(1.25))},
		{Key: "raw-float64", Type: zapcore.Float64Type, Integer: int64(math.Float64bits(2.5))},
	}
	native := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		EncodeDuration: zapcore.NanosDurationEncoder, EncodeTime: zapcore.RFC3339NanoTimeEncoder,
	})
	encoded, err := native.EncodeEntry(zapcore.Entry{}, scalarFields)
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Free()
	var nativeValues map[string]json.RawMessage
	if err := json.Unmarshal(encoded.Bytes(), &nativeValues); err != nil || len(nativeValues) != len(scalarFields) {
		t.Fatal("native scalar encoding control failed")
	}
	fields := append(append([]zapcore.Field(nil), metadata...), scalarFields...)
	fields = append(fields, sdk.Error(fault.Kind("test.failure").New(fault.Context{Operation: "read"}, errors.New("private-cause"))), sdk.Skip())
	for _, level := range []zapcore.Level{zapcore.DebugLevel, zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel} {
		if err := sink.Write(context.Background(), zapcore.Entry{Level: level, Message: "message"}, fields); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	count := len(records)
	mu.Unlock()
	if count != 0 {
		t.Fatal("Sync performed exporter flush")
	}
	called := false
	for _, field := range []zapcore.Field{sdk.Object("object", forbidden{&called}), sdk.Uint64("too-large", math.MaxUint64), sdk.NamedError("raw", errors.New("private-cause"))} {
		if err := sink.Write(context.Background(), zapcore.Entry{Level: zapcore.InfoLevel}, append(metadata, field)); err == nil {
			t.Fatal("unsupported native field accepted")
		}
	}
	if called {
		t.Fatal("native user marshaler invoked")
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
	if len(records) != 1 || len(records[0].ResourceLogs[0].ScopeLogs[0].LogRecords) != 4 {
		t.Fatal("translation/export count changed")
	}
	severities := []int32{5, 9, 13, 17}
	severityTexts := []string{"debug", "info", "warn", "error"}
	for index, record := range records[0].ResourceLogs[0].ScopeLogs[0].LogRecords {
		if int32(record.SeverityNumber) != severities[index] || record.SeverityText != severityTexts[index] {
			t.Error("native severity mapping changed")
		}
		attrs := wireAttributes(t, record.Attributes)
		container, found := attrs["attributes"]
		if !found || container.GetKvlistValue() == nil {
			t.Fatal("structured attribute map missing")
		}
		values := wireAttributes(t, container.GetKvlistValue().Values)
		if len(values) != len(scalarFields)+1 {
			t.Fatal("translated fields missing or unexpected fields added")
		}
		for _, field := range scalarFields {
			value, found := values[field.Key]
			if !found {
				t.Fatalf("translated field %s missing", field.Key)
			}
			actual := scalarJSON(t, field.Type, value)
			if !bytes.Equal(actual, nativeValues[field.Key]) {
				t.Errorf("translated field %s disagrees with native Zap encoding: got %s, want %s", field.Key, actual, nativeValues[field.Key])
			}
		}
		diagnostic, found := values["error"]
		if !found || diagnostic.GetKvlistValue() == nil {
			t.Fatal("safe fault projection missing")
		}
		faultFields := wireAttributes(t, diagnostic.GetKvlistValue().Values)
		if len(faultFields) != 9 {
			t.Fatal("safe fault projection fields missing")
		}
		for name, expected := range map[string]string{"kind": "test.failure", "operation": "read", "provider": "", "scope": "", "source": "", "call": "", "parent": "", "owner": ""} {
			value, found := faultFields[name]
			if actual, ok := value.GetValue().(*commonpb.AnyValue_StringValue); !found || !ok || actual.StringValue != expected {
				t.Errorf("safe fault field %s changed", name)
			}
		}
		if value, ok := faultFields["has_causes"].GetValue().(*commonpb.AnyValue_BoolValue); !ok || !value.BoolValue {
			t.Error("safe fault cause-presence field changed")
		}
		wire, err := proto.Marshal(record)
		if err != nil || bytes.Contains(wire, []byte("private-cause")) {
			t.Fatal("native cause text reached the wire record")
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("cancel-cause")
	cancel(cause)
	if err := sink.Sync(ctx); !errors.Is(err, cause) {
		t.Fatal("Sync cancellation cause lost")
	}
	if New(nil).Sync(context.Background()) == nil {
		t.Fatal("nil sink accepted")
	}
}

func wireAttributes(t *testing.T, attrs []*commonpb.KeyValue) map[string]*commonpb.AnyValue {
	t.Helper()
	values := make(map[string]*commonpb.AnyValue, len(attrs))
	for _, attr := range attrs {
		if attr == nil || attr.Value == nil {
			t.Fatal("wire attribute missing its key/value")
		}
		if _, found := values[attr.Key]; found {
			t.Fatal("duplicate wire attribute")
		}
		values[attr.Key] = attr.Value
	}
	return values
}

func scalarJSON(t *testing.T, kind zapcore.FieldType, value *commonpb.AnyValue) []byte {
	t.Helper()
	var scalar any
	var valid bool
	switch kind {
	case zapcore.StringType, zapcore.ByteStringType, zapcore.TimeType, zapcore.TimeFullType:
		_, valid = value.GetValue().(*commonpb.AnyValue_StringValue)
		scalar = value.GetStringValue()
	case zapcore.BoolType:
		_, valid = value.GetValue().(*commonpb.AnyValue_BoolValue)
		scalar = value.GetBoolValue()
	case zapcore.Int64Type, zapcore.Int32Type, zapcore.Int16Type, zapcore.Int8Type,
		zapcore.Uint64Type, zapcore.Uint32Type, zapcore.Uint16Type, zapcore.Uint8Type, zapcore.DurationType:
		_, valid = value.GetValue().(*commonpb.AnyValue_IntValue)
		scalar = value.GetIntValue()
	case zapcore.Float64Type, zapcore.Float32Type:
		_, valid = value.GetValue().(*commonpb.AnyValue_DoubleValue)
		scalar = value.GetDoubleValue()
	case zapcore.BinaryType:
		_, valid = value.GetValue().(*commonpb.AnyValue_BytesValue)
		scalar = value.GetBytesValue()
	}
	if !valid {
		t.Fatal("translated scalar changed its wire type")
	}
	encoded, err := json.Marshal(scalar)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
