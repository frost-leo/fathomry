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

package zerologotel

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	zerolog "github.com/frost-leo/fathomry/adapters/logging/zerolog/v1"
	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	logapi "go.opentelemetry.io/otel/log"
)

// Sink borrows a copied generation-stable public telemetry facade. No exporter,
// Runtime, source, queue or receiver is owned; the sink must not be retargeted
// or overwritten while borrowed. It deliberately has no Sync/Close method.
type Sink struct {
	private
	client *otel.Client
}

func New(client *otel.Client) (*Sink, error) {
	if client == nil || !client.StableDestination() {
		return nil, refuse("client")
	}
	frozen := *client
	return &Sink{client: &frozen}, nil
}
func (sink *Sink) CheckRuntime(runtime *adapters.Runtime) error {
	if sink == nil || sink.client == nil || runtime == nil || sink.client.UsesRuntime(runtime) {
		return refuse("shared-runtime")
	}
	return nil
}

// WriteRecord is the explicit legacy route: its returned errors remain fail-stop
// when the caller selects a legacy record sink. Choose managed-record to enable
// the qualified independent-event outcomes of WriteManagedRecord.
func (sink *Sink) WriteRecord(ctx context.Context, record zerolog.Record) error {
	return sink.WriteManagedRecord(ctx, record).Err
}

// WriteManagedRecord never retries. Record conversion/known admission refusal
// affects this event; closed/disabled/unknown target state stops the destination.
// Acknowledgement is local telemetry queue acceptance, not export/durability.
func (sink *Sink) WriteManagedRecord(ctx context.Context, record zerolog.Record) zerolog.RecordOutcome {
	reject := func(err error) zerolog.RecordOutcome {
		return zerolog.RecordOutcome{State: zerolog.RecordRejected, Err: err}
	}
	stop := func(err error) zerolog.RecordOutcome {
		return zerolog.RecordOutcome{State: zerolog.RecordStopped, Err: err}
	}
	if sink == nil || sink.client == nil {
		return stop(refuse("client"))
	}
	if ctx == nil {
		return reject(refuse("context"))
	}
	severity, found := map[zerolog.Level]logapi.Severity{zerolog.Trace: logapi.SeverityTrace, zerolog.Debug: logapi.SeverityDebug, zerolog.Info: logapi.SeverityInfo, zerolog.Warn: logapi.SeverityWarn, zerolog.Error: logapi.SeverityError, zerolog.Fatal: logapi.SeverityFatal, zerolog.Panic: logapi.SeverityFatal4}[record.Level()]
	if !found {
		return reject(refuse("record"))
	}
	attrs := otel.Value{Kind: "map"}
	for _, field := range record.FieldsCopy() {
		converted, err := value(field.Value)
		if err != nil {
			return reject(err)
		}
		attrs.Map = append(attrs.Map, otel.TypedAttribute{Key: field.Key, Value: converted})
	}
	source, correlation := record.Source(), record.Correlation()
	metadata := otel.Value{Kind: "map", Map: []otel.TypedAttribute{
		{Key: "provider", Value: otel.Value{Kind: "string", String: source.Provider}},
		{Key: "source", Value: otel.Value{Kind: "string", String: source.Name}},
		{Key: "scope", Value: otel.Value{Kind: "string", String: source.Scope}},
		{Key: "revision", Value: otel.Value{Kind: "string", String: source.Revision}},
		{Key: "call", Value: otel.Value{Kind: "string", String: correlation.Call}},
		{Key: "parent", Value: otel.Value{Kind: "string", String: correlation.Parent}},
		{Key: "owner", Value: otel.Value{Kind: "string", String: correlation.Owner}},
	}}
	caller := record.Caller()
	if caller.Defined {
		metadata.Map = append(metadata.Map, otel.TypedAttribute{Key: "caller", Value: otel.Value{Kind: "map", Map: []otel.TypedAttribute{
			{Key: "file", Value: otel.Value{Kind: "string", String: caller.File}},
			{Key: "line", Value: otel.Value{Kind: "int64", Int64: int64(caller.Line)}},
			{Key: "function", Value: otel.Value{Kind: "string", String: caller.Function}},
		}}})
	}
	timestamp := record.Time()
	if timestamp.IsZero() {
		timestamp = time.Unix(0, 0).UTC()
	}
	receipt, err := sink.client.Emit(ctx, otel.LogRecord{Time: timestamp, Severity: severity, SeverityText: string(record.Level()), Message: record.Message(), Attributes: []slog.Attr{slog.Any("logging", metadata), slog.Any("attributes", attrs)}})
	classification := err
	if receipt != nil {
		snapshot, waitErr := receipt.Wait(ctx)
		if waitErr != nil && !snapshot.Info().Resolved {
			return stop(combine(err, waitErr))
		}
		classification = combine(classification, combine(snapshot.Primary(), snapshot.Cleanup()))
		err = combine(err, snapshot.Err())
	} else if err == nil {
		return stop(refuse("missing-receipt"))
	}
	if err != nil {
		// A stopped capability can surface as canceled admission. Inspect its
		// existing non-owning profile without borrowing the caller's cancellation;
		// a live target's caller cancellation remains an independent-event failure.
		if _, stateErr := sink.client.Profile(context.WithoutCancel(ctx)); stateErr != nil && classify(stateErr) == zerolog.RecordStopped {
			return stop(combine(err, stateErr))
		}
		return zerolog.RecordOutcome{State: classify(classification), Err: err}
	}
	return zerolog.RecordOutcome{State: zerolog.RecordAccepted}
}
func combine(left, right error) error {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return errors.Join(left, right)
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
