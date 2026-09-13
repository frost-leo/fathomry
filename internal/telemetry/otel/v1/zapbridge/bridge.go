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
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	otel "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	logapi "go.opentelemetry.io/otel/log"
	"go.uber.org/zap/zapcore"
)

// Sink implements the existing Zap StructuredSink by structural typing, without
// importing the Fathomry Zap provider. It borrows Client and owns no resources.
type Sink struct{ client *otel.Client }

// New returns a concurrent borrowed sink. Order telemetry before its logging
// consumers and keep its independent evidence inbox drained.
func New(client *otel.Client) *Sink { return &Sink{client: client} }

// Write translates actual supported Zap fields, never JSON or user marshalers.
// Framework correlation remains fixed, original logger metadata is under logging,
// and user fields under attributes. Errors export only safe fault diagnostics.
// Uint64 values outside OTLP's int64 range explicitly fail this sink, not local
// outputs. Nil means OTel queue acceptance only, not export.
func (sink *Sink) Write(ctx context.Context, entry zapcore.Entry, fields []zapcore.Field) error {
	fail := func() error {
		return otel.ErrUnsupported.New(fault.Context{Provider: otel.ProviderID, Operation: "zap-bridge"})
	}
	if sink == nil || sink.client == nil || len(fields) > 70 || entry.Level < zapcore.DebugLevel || entry.Level > zapcore.ErrorLevel ||
		len(entry.LoggerName) > 128 || len(entry.Caller.File) > 4096 || len(entry.Caller.Function) > 4096 || entry.Stack != "" {
		return fail()
	}
	id := fault.Correlation{}
	metadata := []slog.Attr{slog.String("name", entry.LoggerName)}
	attrs := make([]slog.Attr, 0, len(fields))
	for _, field := range fields {
		if field.Type == zapcore.SkipType {
			continue
		}
		switch field.Key {
		case "fathomry.call", "fathomry.parent", "fathomry.owner", "fathomry.provider", "fathomry.source", "fathomry.scope":
			if field.Type != zapcore.StringType {
				return fail()
			}
			switch field.Key {
			case "fathomry.call":
				id.Call = field.String
			case "fathomry.parent":
				id.Parent = field.String
			case "fathomry.owner":
				id.Owner = field.String
			case "fathomry.provider":
				metadata = append(metadata, slog.String("provider", field.String))
			case "fathomry.source":
				metadata = append(metadata, slog.String("source", field.String))
			case "fathomry.scope":
				metadata = append(metadata, slog.String("scope", field.String))
			}
			continue
		}
		var attr slog.Attr
		switch field.Type {
		case zapcore.StringType:
			attr = slog.String(field.Key, field.String)
		case zapcore.BoolType:
			attr = slog.Bool(field.Key, field.Integer == 1)
		case zapcore.Int64Type:
			attr = slog.Int64(field.Key, field.Integer)
		case zapcore.Int32Type:
			attr = slog.Int64(field.Key, int64(int32(field.Integer)))
		case zapcore.Int16Type:
			attr = slog.Int64(field.Key, int64(int16(field.Integer)))
		case zapcore.Int8Type:
			attr = slog.Int64(field.Key, int64(int8(field.Integer)))
		case zapcore.Uint64Type:
			attr = slog.Uint64(field.Key, uint64(field.Integer))
		case zapcore.Uint32Type:
			attr = slog.Uint64(field.Key, uint64(uint32(field.Integer)))
		case zapcore.Uint16Type:
			attr = slog.Uint64(field.Key, uint64(uint16(field.Integer)))
		case zapcore.Uint8Type:
			attr = slog.Uint64(field.Key, uint64(uint8(field.Integer)))
		case zapcore.Float64Type:
			attr = slog.Float64(field.Key, math.Float64frombits(uint64(field.Integer)))
		case zapcore.Float32Type:
			attr = slog.Float64(field.Key, float64(math.Float32frombits(uint32(field.Integer))))
		case zapcore.DurationType:
			attr = slog.Duration(field.Key, time.Duration(field.Integer))
		case zapcore.TimeType:
			attr = slog.Time(field.Key, time.Unix(0, field.Integer).UTC())
		case zapcore.TimeFullType:
			value, ok := field.Interface.(time.Time)
			if !ok {
				return fail()
			}
			attr = slog.Time(field.Key, value)
		case zapcore.BinaryType, zapcore.ByteStringType:
			value, ok := field.Interface.([]byte)
			if !ok {
				return fail()
			}
			if field.Type == zapcore.BinaryType {
				attr = slog.Any(field.Key, value)
			} else {
				attr = slog.String(field.Key, string(value))
			}
		case zapcore.ErrorType:
			value, ok := field.Interface.(*fault.Error)
			if !ok || value == nil {
				return fail()
			}
			diagnostic := value.Diagnostic()
			location := diagnostic.Context
			attr = slog.Group(field.Key, slog.String("kind", string(diagnostic.Kind)),
				slog.String("operation", location.Operation), slog.String("provider", location.Provider),
				slog.String("scope", location.Scope), slog.String("source", location.Source),
				slog.String("call", location.Correlation.Call), slog.String("parent", location.Correlation.Parent),
				slog.String("owner", location.Correlation.Owner), slog.Bool("has_causes", diagnostic.HasCauses))
		default:
			return fail()
		}
		attrs = append(attrs, attr)
	}
	if entry.Caller.Defined {
		metadata = append(metadata, slog.Group("caller", slog.String("file", entry.Caller.File),
			slog.Int("line", entry.Caller.Line), slog.String("function", entry.Caller.Function)))
	}
	severity := map[zapcore.Level]logapi.Severity{zapcore.DebugLevel: logapi.SeverityDebug, zapcore.InfoLevel: logapi.SeverityInfo,
		zapcore.WarnLevel: logapi.SeverityWarn, zapcore.ErrorLevel: logapi.SeverityError}[entry.Level]
	receipt, err := sink.client.Emit(ctx, id, otel.LogRecord{Time: entry.Time, Severity: severity, SeverityText: entry.Level.String(),
		Message: entry.Message, Attributes: []slog.Attr{slog.GroupAttrs("logging", metadata...), slog.Any("attributes", otel.AttributeMap(attrs))}})
	if err != nil {
		return err
	}
	result, _ := receipt.Result()
	return result.Err()
}

// Sync has no private buffer to drain. It deliberately does NOT Flush or close
// telemetry. Composition separately calls Client.Flush with its own correlation,
// budget and evidence; Zap's maintenance does not gain that ownership.
func (sink *Sink) Sync(ctx context.Context) error {
	if sink == nil || sink.client == nil || ctx == nil {
		return otel.ErrInput.New(fault.Context{Provider: otel.ProviderID, Operation: "zap-sync"})
	}
	if ctx.Err() != nil {
		return otel.ErrState.New(fault.Context{Provider: otel.ProviderID, Operation: "zap-sync"}, ctx.Err(), context.Cause(ctx))
	}
	return nil
}
