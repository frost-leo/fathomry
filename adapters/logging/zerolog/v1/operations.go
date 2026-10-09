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

package zerolog

import (
	"context"
	"log/slog"
	"runtime"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
)

// Entry preserves original time and return PC. Zero means absent, not wrapper
// time/caller. Severity never performs native Fatal/Panic actions.
type Entry struct {
	private
	Time    time.Time
	PC      uintptr
	Level   Level
	Message string
}

func now() time.Time { return time.Now().UTC() }

// Log is caller-cancelable and captures its actual business ingress time/PC.
func (client *Client) Log(ctx context.Context, level Level, message string, attributes ...slog.Attr) (*adapters.Receipt[Result], error) {
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:])
	return client.LogEntry(ctx, Entry{Time: now(), PC: pcs[0], Level: level, Message: message}, attributes...)
}
func (client *Client) LogEntry(ctx context.Context, entry Entry, attributes ...slog.Attr) (*adapters.Receipt[Result], error) {
	if len(attributes) > native.MaxAttributes {
		return nil, fail(ErrLimit, "attributes")
	}
	return client.finite(ctx, "log", func(operation *operation) (*invocation.Receipt[native.Result], error) {
		frozen, err := freezeAttributes(attributes, operation.limit)
		if err != nil {
			return nil, err
		}
		return operation.native.LogEntry(operation.context, operation.id, native.Entry{Time: entry.Time, PC: entry.PC, Level: native.Level(entry.Level), Message: entry.Message}, frozen...)
	})
}

// Sync maintains owned files only; borrowed byte/record sinks are never synced.
func (client *Client) Sync(ctx context.Context) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "sync", func(operation *operation) (*invocation.Receipt[native.Result], error) {
		return operation.native.Sync(operation.context, operation.id)
	})
}

// Rotate explicitly rotates nonempty owned files with the selected native policy.
func (client *Client) Rotate(ctx context.Context) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "rotate", func(operation *operation) (*invocation.Receipt[native.Result], error) {
		return operation.native.Rotate(operation.context, operation.id)
	})
}

// Enabled is only a frozen-threshold hint, not native readiness/admission or a
// promise that a later Follow call will borrow the same generation.
func (client *Client) Enabled(ctx context.Context, level Level) (bool, error) {
	if client == nil || client.lifetime == nil || ctx == nil {
		return false, fail(ErrInput, "enabled")
	}
	switch level {
	case Trace, Debug, Info, Warn, Error, Fatal, Panic:
	default:
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
	return handle.state.inspection.Enabled(native.Level(level)), nil
}

// Attribute adds closed data alongside admitted native slog scalar/group values.
// No arbitrary Resolve, Error, Stringer, reflection or marshaler is invoked.
func Attribute(key string, value logging.Value) slog.Attr { return slog.Any(key, value) }
func fieldLimits(limit int) logging.Limits {
	return logging.Limits{MaxFields: native.MaxAttributes, MaxNodes: native.MaxNodes, MaxDepth: native.MaxDepth, MaxBytes: int64(limit)}
}

func freezeAttributes(values []slog.Attr, limit int) ([]slog.Attr, error) {
	nodes := native.MaxNodes
	remaining := int64(limit)
	converted, err := nativeAttributes(values, &remaining, &nodes, 1)
	if err != nil {
		return nil, err
	}
	frozen, err := native.FreezeAttributes(converted, limit)
	return frozen, translate(err, "attributes")
}
func nativeAttributes(values []slog.Attr, remaining *int64, nodes *int, depth int) ([]slog.Attr, error) {
	if depth > native.MaxDepth || len(values) > native.MaxAttributes || len(values) > *nodes {
		return nil, fail(ErrLimit, "attributes")
	}
	*nodes -= len(values)
	result := make([]slog.Attr, len(values))
	for index, attr := range values {
		if len(attr.Key) > 128 {
			return nil, fail(ErrLimit, "attribute-key")
		}
		switch attr.Value.Kind() {
		case slog.KindGroup:
			if depth > 1 && len(attr.Value.Group()) == 0 {
				return nil, fail(ErrUnsupported, "noncanonical-group")
			}
			nested, err := nativeAttributes(attr.Value.Group(), remaining, nodes, depth+1)
			if err != nil {
				return nil, err
			}
			attr.Value = slog.GroupValue(nested...)
		case slog.KindAny, slog.KindLogValuer:
			switch value := attr.Value.Any().(type) {
			case logging.Value:
				if err := chargeClosed(attr.Key, value, remaining, nodes, depth); err != nil {
					return nil, err
				}
				attr.Value = slog.AnyValue(nativeValue(value))
			case error:
				frozen, err := logging.SafeError(value)
				if err != nil {
					return nil, err
				}
				if err := chargeClosed(attr.Key, frozen, remaining, nodes, depth); err != nil {
					return nil, err
				}
				attr.Value = slog.AnyValue(nativeValue(frozen))
			}
		}
		result[index] = attr
	}
	return result, nil
}

// Charge the aggregate closed payload before conversion can copy bytes/containers.
// Legacy scalar accounting stays solely native; these charges are a necessary
// pre-copy bound, not a replacement for the final combined native validation.
func chargeClosed(key string, value logging.Value, remaining *int64, nodes *int, depth int) error {
	if *remaining < 1 {
		return fail(ErrLimit, "attributes")
	}
	usage, err := logging.MeasureFields([]logging.Field{{Key: key, Value: value}}, logging.Limits{MaxFields: native.MaxAttributes, MaxNodes: *nodes + 1, MaxDepth: native.MaxDepth - depth + 1, MaxBytes: *remaining})
	if err != nil {
		return err
	}
	*remaining -= usage.Bytes
	*nodes -= usage.Nodes - 1
	return nil
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
	panic("unreachable: validated closed native record")
}
