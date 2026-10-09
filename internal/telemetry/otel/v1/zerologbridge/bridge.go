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
	"log/slog"
	"math"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	zerolog "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	otel "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	logapi "go.opentelemetry.io/otel/log"
)

// Sink supports both legacy fail-stop RecordWriter and explicitly selected
// managed independent-event output. It borrows telemetry, never Flushes/Closes
// it, and owns no worker, queue or retry policy.
type Sink struct{ client *otel.Client }

var _ zerolog.RecordWriter = (*Sink)(nil)
var _ zerolog.ManagedRecordWriter = (*Sink)(nil)

// New creates no native resources. Telemetry must remain alive and admissible
// until logging producers finish; its evidence is received independently.
func New(client *otel.Client) *Sink { return &Sink{client: client} }

func problem(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: otel.ProviderID, Operation: "zerolog-" + operation}, causes...)
}

// WriteRecord preserves the legacy any-error failure-stop contract selected by
// Kind=record. A recoverable error is still returned, never swallowed or retried.
func (sink *Sink) WriteRecord(ctx context.Context, record zerolog.Record) error {
	return sink.WriteManagedRecord(ctx, record).Err
}

// WriteManagedRecord classifies failure of this one independent record. Rejected
// authorizes a later separate event, not replay or a promise of destination
// health. Stopped covers closed/disabled or uncertain destination authority.
func (sink *Sink) WriteManagedRecord(ctx context.Context, record zerolog.Record) zerolog.RecordOutcome {
	refuse := func(err error) zerolog.RecordOutcome {
		return zerolog.RecordOutcome{State: zerolog.RecordRejected, Err: err}
	}
	stopped := func(err error) zerolog.RecordOutcome {
		return zerolog.RecordOutcome{State: zerolog.RecordStopped, Err: err}
	}
	if sink == nil || sink.client == nil {
		return stopped(problem(otel.ErrInput, "bridge"))
	}
	if ctx == nil {
		return refuse(problem(otel.ErrInput, "context"))
	}
	if ctx.Err() != nil {
		return refuse(problem(otel.ErrState, "canceled", ctx.Err(), context.Cause(ctx)))
	}
	severity, found := map[zerolog.Level]logapi.Severity{zerolog.Trace: logapi.SeverityTrace, zerolog.Debug: logapi.SeverityDebug,
		zerolog.Info: logapi.SeverityInfo, zerolog.Warn: logapi.SeverityWarn, zerolog.Error: logapi.SeverityError,
		zerolog.Fatal: logapi.SeverityFatal, zerolog.Panic: logapi.SeverityFatal4}[record.Level()]
	if !found {
		return refuse(problem(otel.ErrInput, "record"))
	}
	fields, err := zerolog.AttributesValues(record.AttributesCopy(), 1<<20)
	if err != nil {
		return refuse(problem(otel.ErrUnsupported, "data", err))
	}
	attributes := otel.Value{Kind: "map"}
	for _, field := range fields {
		converted, err := convertValue(field.Value)
		if err != nil {
			return refuse(err)
		}
		attributes.Map = append(attributes.Map, otel.TypedAttribute{Key: field.Key, Value: converted})
	}
	source := record.Source()
	metadata := otel.Value{Kind: "map", Map: []otel.TypedAttribute{
		{Key: "provider", Value: otel.Value{Kind: "string", String: source.Configuration.Identity.Provider}},
		{Key: "source", Value: otel.Value{Kind: "string", String: source.Configuration.Identity.Name}},
		{Key: "scope", Value: otel.Value{Kind: "string", String: source.Scope}},
		{Key: "revision", Value: otel.Value{Kind: "string", String: source.Configuration.Revision}},
	}}
	caller := record.Caller()
	if caller.Defined {
		metadata.Map = append(metadata.Map, otel.TypedAttribute{Key: "caller", Value: otel.Value{Kind: "map", Map: []otel.TypedAttribute{
			{Key: "file", Value: otel.Value{Kind: "string", String: caller.File}},
			{Key: "line", Value: otel.Value{Kind: "int64", Int64: int64(caller.Line)}},
			{Key: "function", Value: otel.Value{Kind: "string", String: caller.Function}},
		}}})
	}
	stamp := record.Time()
	if stamp.IsZero() {
		stamp = time.Unix(0, 0).UTC()
	}
	receipt, err := sink.client.Emit(ctx, record.Correlation(), otel.LogRecord{Time: stamp, Severity: severity, SeverityText: string(record.Level()),
		Message: record.Message(), Attributes: []slog.Attr{slog.Any("logging", metadata), slog.Any("attributes", attributes)}})
	if err != nil {
		if independentRefusal(err, 128) {
			return refuse(err)
		}
		return stopped(err)
	}
	if receipt == nil {
		return stopped(problem(otel.ErrState, "missing-receipt"))
	}
	result, ok := receipt.Result()
	if !ok || !result.Final || !result.Released {
		return stopped(problem(otel.ErrState, "pending-receipt"))
	}
	if err := result.Err(); err != nil {
		if result.Outcome.Cleanup == nil && independentRefusal(result.Outcome.Primary, 128) {
			return refuse(err)
		}
		return stopped(err)
	}
	signals := result.Outcome.Value.SignalsCopy()
	if !result.Outcome.Present || len(signals) != 1 || signals[0].Signal != otel.Logs || signals[0].Accepted != 1 || signals[0].Err != nil {
		return stopped(problem(otel.ErrState, "acceptance"))
	}
	return zerolog.RecordOutcome{State: zerolog.RecordAccepted}
}

// Native semantic boundaries decide policy before their deliberately retained
// caller causes. Unknown/incomplete graphs stop conservatively; no errors.Is/As
// callback is invoked while searching caller-owned cancellation graphs.
func independentRefusal(original error, limit int) bool {
	remaining := limit
	var visit func(error) bool
	visit = func(err error) bool {
		if err == nil || remaining <= 0 {
			return false
		}
		remaining--
		if err == context.Canceled || err == context.DeadlineExceeded {
			return true
		}
		if value, ok := err.(*fault.Error); ok && value != nil {
			diagnostic := value.Diagnostic()
			switch diagnostic.Kind {
			case otel.ErrLimit, otel.ErrInput, invocation.ErrEvidence, resource.ErrCapacity:
				return true
			case otel.ErrUnsupported:
				return diagnostic.Context.Operation != "logs-disabled"
			case otel.ErrState:
				return (diagnostic.Context.Operation == "entry" || diagnostic.Context.Operation == "emit") && boundedCancellation(value, &remaining)
			case invocation.ErrBudget, invocation.ErrWait:
				return boundedCancellation(value, &remaining)
			case invocation.ErrFailed, resource.ErrAdmission:
			default:
				return false
			}
		}
		switch value := err.(type) {
		case interface{ Unwrap() []error }:
			causes := value.Unwrap()
			if len(causes) == 0 || len(causes) > remaining {
				return false
			}
			for _, cause := range causes {
				if !visit(cause) {
					return false
				}
			}
			return true
		case interface{ Unwrap() error }:
			return visit(value.Unwrap())
		}
		return false
	}
	return visit(original)
}

func boundedCancellation(original error, remaining *int) bool {
	var visit func(error) bool
	visit = func(err error) bool {
		if err == nil || *remaining <= 0 {
			return false
		}
		*remaining--
		if err == context.Canceled || err == context.DeadlineExceeded {
			return true
		}
		switch value := err.(type) {
		case interface{ Unwrap() []error }:
			for _, cause := range value.Unwrap() {
				if *remaining == 0 {
					return false
				}
				if visit(cause) {
					return true
				}
			}
		case interface{ Unwrap() error }:
			return visit(value.Unwrap())
		}
		return false
	}
	return visit(original)
}

func convertValue(value zerolog.Value) (otel.Value, error) {
	switch value.Kind {
	case "", "null":
		return otel.Value{}, nil
	case "bool":
		return otel.Value{Kind: "bool", Bool: value.Bool}, nil
	case "int64":
		return otel.Value{Kind: "int64", Int64: value.Int64}, nil
	case "uint64":
		if value.Uint64 > math.MaxInt64 {
			return otel.Value{}, problem(otel.ErrUnsupported, "uint64")
		}
		return otel.Value{Kind: "int64", Int64: int64(value.Uint64)}, nil
	case "float32":
		return otel.Value{Kind: "float64", Float64: float64(value.Float32)}, nil
	case "float64":
		return otel.Value{Kind: "float64", Float64: value.Float64}, nil
	case "string":
		return otel.Value{Kind: "string", String: value.String}, nil
	case "bytes":
		return otel.Value{Kind: "bytes", Bytes: value.Bytes}, nil
	case "bytestring":
		return otel.Value{Kind: "string", String: string(value.Bytes)}, nil
	case "time":
		stamp := value.Time.UTC()
		if stamp.Year() < 1 || stamp.Year() > 9999 {
			return otel.Value{}, problem(otel.ErrUnsupported, "time")
		}
		return otel.Value{Kind: "string", String: stamp.Format(time.RFC3339Nano)}, nil
	case "duration":
		return otel.Value{Kind: "int64", Int64: int64(value.Duration)}, nil
	case "array":
		result := otel.Value{Kind: "array"}
		for _, item := range value.Array {
			converted, err := convertValue(item)
			if err != nil {
				return otel.Value{}, err
			}
			result.Array = append(result.Array, converted)
		}
		return result, nil
	case "map":
		result := otel.Value{Kind: "map"}
		for _, item := range value.Map {
			converted, err := convertValue(item.Value)
			if err != nil {
				return otel.Value{}, err
			}
			result.Map = append(result.Map, otel.TypedAttribute{Key: item.Key, Value: converted})
		}
		return result, nil
	}
	return otel.Value{}, problem(otel.ErrUnsupported, "value")
}
