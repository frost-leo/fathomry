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
	"github.com/frost-leo/fathomry/internal/fault"
	zap "github.com/frost-leo/fathomry/internal/logging/zap/v1"
	otel "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	logapi "go.opentelemetry.io/otel/log"
	"go.uber.org/zap/zapcore"
	"log/slog"
	"math"
	"strings"
	"time"
)

// Sink is explicit typed composition. Neither logger nor telemetry core imports
// the other core; this bridge borrows Client and owns no resources.
type Sink struct{ client *otel.Client }

func New(client *otel.Client) *Sink { return &Sink{client: client} }
func unsupported(causes ...error) error {
	return otel.ErrUnsupported.New(fault.Context{Provider: otel.ProviderID, Operation: "zap-bridge"}, causes...)
}

// Write consumes the logging integration's closed snapshot, not encoder
// callbacks or local JSON. Nil means queue acceptance only. Local-capable but
// OTLP-incompatible data refuses this destination without poisoning its source.
func (sink *Sink) Write(ctx context.Context, entry zapcore.Entry, fields []zapcore.Field) error {
	if sink == nil || sink.client == nil || len(fields) > 70 || entry.Level < zapcore.DebugLevel || entry.Level > zapcore.ErrorLevel || len(entry.LoggerName) > 128 || len(entry.Caller.File) > 4096 || len(entry.Caller.Function) > 4096 || entry.Stack != "" {
		return unsupported()
	}
	values, err := zap.FieldsValues(fields)
	if err != nil {
		return unsupported(err)
	}
	id := fault.Correlation{}
	metadata := otel.Value{Kind: "map", Map: []otel.TypedAttribute{{Key: "name", Value: otel.Value{Kind: "string", String: entry.LoggerName}}}}
	attributes := otel.Value{Kind: "map"}
	for _, field := range values {
		converted, err := value(field.Value)
		if err != nil {
			return err
		}
		if strings.HasPrefix(field.Key, "fathomry.") {
			if field.Value.Kind != "string" {
				return unsupported()
			}
			switch field.Key {
			case "fathomry.call":
				id.Call = field.Value.String
			case "fathomry.parent":
				id.Parent = field.Value.String
			case "fathomry.owner":
				id.Owner = field.Value.String
			case "fathomry.provider", "fathomry.source", "fathomry.scope":
				metadata.Map = append(metadata.Map, otel.TypedAttribute{Key: strings.TrimPrefix(field.Key, "fathomry."), Value: converted})
			default:
				return unsupported()
			}
		} else {
			attributes.Map = append(attributes.Map, otel.TypedAttribute{Key: field.Key, Value: converted})
		}
	}
	if entry.Caller.Defined {
		metadata.Map = append(metadata.Map, otel.TypedAttribute{Key: "caller", Value: otel.Value{Kind: "map", Map: []otel.TypedAttribute{
			{Key: "file", Value: otel.Value{Kind: "string", String: entry.Caller.File}},
			{Key: "line", Value: otel.Value{Kind: "int64", Int64: int64(entry.Caller.Line)}},
			{Key: "function", Value: otel.Value{Kind: "string", String: entry.Caller.Function}},
		}}})
	}
	timestamp := entry.Time
	if timestamp.IsZero() {
		timestamp = time.Unix(0, 0).UTC()
	}
	severity := map[zapcore.Level]logapi.Severity{zapcore.DebugLevel: logapi.SeverityDebug, zapcore.InfoLevel: logapi.SeverityInfo, zapcore.WarnLevel: logapi.SeverityWarn, zapcore.ErrorLevel: logapi.SeverityError}[entry.Level]
	receipt, err := sink.client.Emit(ctx, id, otel.LogRecord{Time: timestamp, Severity: severity, SeverityText: entry.Level.String(), Message: entry.Message, Attributes: []slog.Attr{slog.Any("logging", metadata), slog.Any("attributes", attributes)}})
	if err != nil {
		return err
	}
	result, _ := receipt.Result()
	return result.Err()
}

// Sync owns no buffer and deliberately does not Flush or close telemetry.
func (sink *Sink) Sync(ctx context.Context) error {
	if sink == nil || sink.client == nil || ctx == nil {
		return otel.ErrInput.New(fault.Context{Provider: otel.ProviderID, Operation: "zap-sync"})
	}
	if ctx.Err() != nil {
		return otel.ErrState.New(fault.Context{Provider: otel.ProviderID, Operation: "zap-sync"}, ctx.Err(), context.Cause(ctx))
	}
	return nil
}
func value(input zap.Value) (otel.Value, error) {
	switch input.Kind {
	case "", "null":
		return otel.Value{}, nil
	case "bool":
		return otel.Value{Kind: "bool", Bool: input.Bool}, nil
	case "int64":
		return otel.Value{Kind: "int64", Int64: input.Int64}, nil
	case "uint64":
		if input.Uint64 > math.MaxInt64 {
			return otel.Value{}, unsupported()
		}
		return otel.Value{Kind: "int64", Int64: int64(input.Uint64)}, nil
	case "float32":
		return otel.Value{Kind: "float64", Float64: float64(input.Float32)}, nil
	case "float64":
		return otel.Value{Kind: "float64", Float64: input.Float64}, nil
	case "string":
		return otel.Value{Kind: "string", String: input.String}, nil
	case "bytes":
		return otel.Value{Kind: "bytes", Bytes: input.Bytes}, nil
	case "bytestring":
		return otel.Value{Kind: "string", String: string(input.Bytes)}, nil
	case "duration":
		return otel.Value{Kind: "int64", Int64: int64(input.Duration)}, nil
	case "time":
		timestamp := input.Time.UTC()
		if timestamp.Year() < 1 || timestamp.Year() > 9999 {
			return otel.Value{}, unsupported()
		}
		return otel.Value{Kind: "string", String: timestamp.Format(time.RFC3339Nano)}, nil
	case "array":
		result := otel.Value{Kind: "array"}
		for _, item := range input.Array {
			converted, err := value(item)
			if err != nil {
				return otel.Value{}, err
			}
			result.Array = append(result.Array, converted)
		}
		return result, nil
	case "map":
		result := otel.Value{Kind: "map"}
		for _, item := range input.Map {
			converted, err := value(item.Value)
			if err != nil {
				return otel.Value{}, err
			}
			result.Map = append(result.Map, otel.TypedAttribute{Key: item.Key, Value: converted})
		}
		return result, nil
	}
	return otel.Value{}, unsupported()
}
