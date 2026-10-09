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

package zapotel

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strings"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	zap "github.com/frost-leo/fathomry/adapters/logging/zap/v1"
	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	logapi "go.opentelemetry.io/otel/log"
	"go.uber.org/zap/zapcore"
)

// Sink borrows a public telemetry capability. It owns no exporter, source,
// Runtime or receiver. It is synchronous and cannot isolate a slow destination.
type Sink struct {
	private
	client *otel.Client
}

// New constructs an inert bridge. Keep telemetry alive and admissible until
// producer logging work ends; then stop export scheduling, close telemetry and
// only finally finish both independent evidence receivers.
func New(client *otel.Client) (*Sink, error) {
	if client == nil || !client.StableDestination() {
		return nil, refuse("client")
	}
	frozen := *client
	return &Sink{client: &frozen}, nil
}

// CheckRuntime refuses root-within-root admission against the same allowance.
// Default composition must use independently budgeted existing Runtime owners.
func (sink *Sink) CheckRuntime(runtime *adapters.Runtime) error {
	if sink == nil || sink.client == nil || runtime == nil {
		return refuse("runtime")
	}
	if sink.client.UsesRuntime(runtime) {
		return refuse("shared-runtime")
	}
	return nil
}

// Write preserves typed data without parsing local JSON. Nil means only OTel
// queue acceptance; export/receiver/back-end/evidence acknowledgement are separate.
// Full-range unsigned values and other local-only values fail only this target.
func (sink *Sink) Write(ctx context.Context, record zap.Record) error {
	if sink == nil || sink.client == nil || ctx == nil {
		return refuse("write")
	}
	if record.Level < zapcore.DebugLevel || record.Level > zapcore.ErrorLevel || len(record.Name) > 128 || len(record.Message) > 64<<10 || len(record.Caller.File) > 4096 || len(record.Caller.Function) > 4096 {
		return refuse("record")
	}
	fields, err := logging.FreezeFields(record.Fields, logging.Limits{MaxFields: 256, MaxNodes: 512, MaxDepth: 8, MaxBytes: 128 << 10})
	if err != nil {
		return err
	}
	attributes := otel.Value{Kind: "map"}
	metadata := otel.Value{Kind: "map", Map: []otel.TypedAttribute{{Key: "name", Value: otel.Value{Kind: "string", String: record.Name}}}}
	for _, field := range fields {
		converted, err := value(field.Value)
		if err != nil {
			return err
		}
		if strings.HasPrefix(field.Key, "fathomry.") {
			switch field.Key {
			case "fathomry.call", "fathomry.parent", "fathomry.owner", "fathomry.provider", "fathomry.source", "fathomry.scope":
				if field.Value.Kind() != logging.StringKind {
					return refuse("correlation")
				}
				metadata.Map = append(metadata.Map, otel.TypedAttribute{Key: strings.TrimPrefix(field.Key, "fathomry."), Value: converted})
			default:
				return refuse("correlation")
			}
		} else {
			attributes.Map = append(attributes.Map, otel.TypedAttribute{Key: field.Key, Value: converted})
		}
	}
	if record.Caller.Defined {
		metadata.Map = append(metadata.Map, otel.TypedAttribute{Key: "caller", Value: otel.Value{Kind: "map", Map: []otel.TypedAttribute{
			{Key: "file", Value: otel.Value{Kind: "string", String: record.Caller.File}},
			{Key: "line", Value: otel.Value{Kind: "int64", Int64: int64(record.Caller.Line)}},
			{Key: "function", Value: otel.Value{Kind: "string", String: record.Caller.Function}},
		}}})
	}
	timestamp := record.Time
	if timestamp.IsZero() {
		timestamp = time.Unix(0, 0).UTC()
	}
	severity := map[zapcore.Level]logapi.Severity{zapcore.DebugLevel: logapi.SeverityDebug, zapcore.InfoLevel: logapi.SeverityInfo, zapcore.WarnLevel: logapi.SeverityWarn, zapcore.ErrorLevel: logapi.SeverityError}[record.Level]
	receipt, err := sink.client.Emit(ctx, otel.LogRecord{Time: timestamp, Severity: severity, SeverityText: record.Level.String(), Message: record.Message, Attributes: []slog.Attr{slog.Any("logging", metadata), slog.Any("attributes", attributes)}})
	if err != nil {
		return err
	}
	if receipt == nil {
		return refuse("receipt")
	}
	snapshot, err := receipt.Wait(ctx)
	return errors.Join(err, snapshot.Err())
}

// Sync has no private queue. It does not Flush or close telemetry.
func (sink *Sink) Sync(ctx context.Context) error {
	if sink == nil || sink.client == nil || ctx == nil {
		return refuse("sync")
	}
	if ctx.Err() != nil {
		return errors.Join(ctx.Err(), context.Cause(ctx))
	}
	return nil
}
func value(input logging.Value) (otel.Value, error) {
	switch input.Kind() {
	case logging.NullKind:
		return otel.Value{}, nil
	case logging.BoolKind:
		return otel.Value{Kind: "bool", Bool: input.Bool()}, nil
	case logging.Int64Kind:
		return otel.Value{Kind: "int64", Int64: input.Int64()}, nil
	case logging.Uint64Kind:
		if input.Uint64() > math.MaxInt64 {
			return otel.Value{}, refuse("unsigned-range")
		}
		return otel.Value{Kind: "int64", Int64: int64(input.Uint64())}, nil
	case logging.Float32Kind:
		return otel.Value{Kind: "float64", Float64: float64(input.Float32())}, nil
	case logging.Float64Kind:
		return otel.Value{Kind: "float64", Float64: input.Float64()}, nil
	case logging.StringKind:
		return otel.Value{Kind: "string", String: input.StringValue()}, nil
	case logging.ByteStringKind:
		return otel.Value{Kind: "string", String: string(input.BytesCopy())}, nil
	case logging.BinaryKind:
		return otel.Value{Kind: "bytes", Bytes: input.BytesCopy()}, nil
	case logging.DurationKind:
		return otel.Value{Kind: "int64", Int64: int64(input.Duration())}, nil
	case logging.TimeKind:
		timestamp := input.Time().UTC()
		if timestamp.Year() < 1 || timestamp.Year() > 9999 {
			return otel.Value{}, refuse("time-range")
		}
		return otel.Value{Kind: "string", String: timestamp.Format(time.RFC3339Nano)}, nil
	case logging.ArrayKind:
		result := otel.Value{Kind: "array"}
		for _, item := range input.ElementsCopy() {
			converted, err := value(item)
			if err != nil {
				return otel.Value{}, err
			}
			result.Array = append(result.Array, converted)
		}
		return result, nil
	case logging.GroupKind:
		result := otel.Value{Kind: "map"}
		for _, field := range input.FieldsCopy() {
			converted, err := value(field.Value)
			if err != nil {
				return otel.Value{}, err
			}
			result.Map = append(result.Map, otel.TypedAttribute{Key: field.Key, Value: converted})
		}
		return result, nil
	}
	return otel.Value{}, refuse("kind")
}
