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

package zap

import (
	"context"
	"runtime"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/logging/zap/v1"
	"go.uber.org/zap/zapcore"
)

// Entry carries original ingress facts. Zero Time/PC mean absent, not now/the
// wrapper. Name is an optional bounded per-record hierarchical name.
type Entry struct {
	private
	Time          time.Time
	PC            uintptr
	Level         zapcore.Level
	Message, Name string
}

func now() time.Time { return time.Now().UTC() }

// Log captures time and the business caller at this entry. Direct cancellation
// belongs to ctx, unlike the explicitly owned restricted slog gateway.
func (client *Client) Log(ctx context.Context, level zapcore.Level, message string, fields ...zapcore.Field) (*adapters.Receipt[Result], error) {
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:])
	return client.LogEntry(ctx, Entry{Time: now(), PC: pcs[0], Level: level, Message: message}, fields...)
}
func (client *Client) LogEntry(ctx context.Context, entry Entry, fields ...zapcore.Field) (*adapters.Receipt[Result], error) {
	frozen, err := freezeFields(fields)
	if err != nil {
		return nil, err
	}
	return client.finite(ctx, "log", func(operation *operation) (*invocation.Receipt[native.Result], error) {
		return operation.native.LogEntry(operation.context, operation.id, native.Entry{Time: entry.Time, PC: entry.PC, Level: entry.Level, Message: entry.Message, Name: entry.Name}, frozen...)
	})
}

// Sync preserves per-destination native maintenance, including historical file
// errors. It never becomes telemetry Flush, exporter shutdown or a retry.
func (client *Client) Sync(ctx context.Context) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "sync", func(operation *operation) (*invocation.Receipt[native.Result], error) {
		return operation.native.Sync(operation.context, operation.id)
	})
}

// Enabled is a low-cost advisory policy query, not admission/effect evidence or
// a promise that a later Follow call will use the same generation.
func (client *Client) Enabled(ctx context.Context, level zapcore.Level) (bool, error) {
	if client == nil || client.lifetime == nil || ctx == nil {
		return false, fail(ErrInput, "enabled")
	}
	if level < zapcore.DebugLevel || level > zapcore.ErrorLevel {
		return false, fail(ErrUnsupported, "level")
	}
	if ctx.Err() != nil || client.lifetime.Err() != nil {
		return false, fail(ErrState, "enabled", ctx.Err(), context.Cause(ctx), client.lifetime.Err(), context.Cause(client.lifetime))
	}
	handle := client.direct
	if client.source != nil {
		lease, err := client.source.Acquire(ctx)
		if err != nil {
			return false, err
		}
		defer lease.Release()
		handle, err = lease.Value()
		if err != nil {
			return false, err
		}
	}
	if handle.state == nil || handle.state.inspection == nil || handle.state.call.Context().Err() != nil {
		return false, fail(ErrState, "enabled")
	}
	return handle.state.inspection.Enabled(level), nil
}

// Field admits closed shared data alongside supported native Zap fields. It
// grants no arbitrary reflection/marshaler authority. Inputs are borrowed until
// the operation or With boundary freezes them.
func Field(key string, value logging.Value) zapcore.Field {
	return zapcore.Field{Key: key, Type: zapcore.ReflectType, Interface: value}
}

func fieldLimits() logging.Limits {
	return logging.Limits{MaxFields: native.MaxFields, MaxNodes: 256, MaxDepth: 8, MaxBytes: native.MaxFieldBytes}
}

func freezeFields(fields []zapcore.Field) ([]zapcore.Field, error) {
	if len(fields) > native.MaxFields {
		return nil, fail(ErrLimit, "fields")
	}
	result := make([]zapcore.Field, len(fields))
	for index, field := range fields {
		switch field.Type {
		case zapcore.ReflectType:
			if value, ok := field.Interface.(logging.Value); ok {
				frozen, err := logging.Freeze(value, fieldLimits())
				if err != nil {
					return nil, err
				}
				field = native.Field(field.Key, nativeValue(frozen))
			}
		case zapcore.ErrorType:
			original, ok := field.Interface.(error)
			if !ok {
				return nil, fail(ErrUnsupported, "error")
			}
			projected, err := logging.SafeError(original)
			if err != nil {
				return nil, err
			}
			field = native.Field(field.Key, nativeValue(projected))
		}
		result[index] = field
	}
	frozen, err := native.FreezeFields(result)
	return frozen, translate(err, "fields")
}

func nativeValue(value logging.Value) native.Value {
	switch value.Kind() {
	case logging.NullKind:
		return native.Value{}
	case logging.BoolKind:
		return native.Value{Kind: "bool", Bool: value.Bool()}
	case logging.Int64Kind:
		return native.Value{Kind: "int64", Int64: value.Int64()}
	case logging.Uint64Kind:
		return native.Value{Kind: "uint64", Uint64: value.Uint64()}
	case logging.Float32Kind:
		return native.Value{Kind: "float32", Float32: value.Float32()}
	case logging.Float64Kind:
		return native.Value{Kind: "float64", Float64: value.Float64()}
	case logging.StringKind:
		return native.Value{Kind: "string", String: value.StringValue()}
	case logging.BinaryKind:
		return native.Value{Kind: "bytes", Bytes: value.BytesCopy()}
	case logging.ByteStringKind:
		return native.Value{Kind: "bytestring", Bytes: value.BytesCopy()}
	case logging.TimeKind:
		return native.Value{Kind: "time", Time: value.Time()}
	case logging.DurationKind:
		return native.Value{Kind: "duration", Duration: value.Duration()}
	case logging.ArrayKind:
		result := native.Value{Kind: "array"}
		for _, item := range value.ElementsCopy() {
			result.Array = append(result.Array, nativeValue(item))
		}
		return result
	case logging.GroupKind:
		result := native.Value{Kind: "map"}
		for _, field := range value.FieldsCopy() {
			result.Map = append(result.Map, native.Attribute{Key: field.Key, Value: nativeValue(field.Value)})
		}
		return result
	}
	return native.Value{Kind: "invalid"}
}
func publicValue(value native.Value) logging.Value {
	switch value.Kind {
	case "", "null":
		return logging.Null()
	case "bool":
		return logging.Bool(value.Bool)
	case "int64":
		return logging.Int64(value.Int64)
	case "uint64":
		return logging.Uint64(value.Uint64)
	case "float32":
		return logging.Float32(value.Float32)
	case "float64":
		return logging.Float64(value.Float64)
	case "string":
		return logging.String(value.String)
	case "bytes":
		return logging.Binary(value.Bytes)
	case "bytestring":
		return logging.ByteString(value.Bytes)
	case "time":
		return logging.Time(value.Time)
	case "duration":
		return logging.Duration(value.Duration)
	case "array":
		items := make([]logging.Value, len(value.Array))
		for index, item := range value.Array {
			items[index] = publicValue(item)
		}
		return logging.Array(items...)
	case "map":
		fields := make([]logging.Field, len(value.Map))
		for index, field := range value.Map {
			fields[index] = logging.Field{Key: field.Key, Value: publicValue(field.Value)}
		}
		return logging.Group(fields...)
	}
	panic("unreachable: native closed logging value")
}

type structuredBridge struct{ sink StructuredSink }

func (bridge *structuredBridge) Write(ctx context.Context, entry zapcore.Entry, fields []zapcore.Field) error {
	values, err := native.FieldsValues(fields)
	if err != nil {
		return translate(err, "structured-data")
	}
	record := Record{Time: entry.Time, Level: entry.Level, Message: entry.Message, Name: entry.LoggerName, Caller: Caller{Defined: entry.Caller.Defined, PC: entry.Caller.PC, File: entry.Caller.File, Line: entry.Caller.Line, Function: entry.Caller.Function}}
	if metadata, ok := native.EntryMetadata(ctx); ok {
		record.PC = metadata.PC
	}
	for _, field := range values {
		record.Fields = append(record.Fields, logging.Field{Key: field.Key, Value: publicValue(field.Value)})
	}
	return bridge.sink.Write(ctx, record)
}
func (bridge *structuredBridge) Sync(ctx context.Context) error { return bridge.sink.Sync(ctx) }
