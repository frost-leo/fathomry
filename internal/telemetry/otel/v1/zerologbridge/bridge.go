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

	"github.com/frost-leo/fathomry/internal/fault"
	zerolog "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	otel "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	logapi "go.opentelemetry.io/otel/log"
)

// Sink is a concurrent borrowed RecordWriter. It has no flush/close methods;
// zerolog does not maintain borrowed sinks. Telemetry must outlive the logger.
type Sink struct{ client *otel.Client }

var _ zerolog.RecordWriter = (*Sink)(nil)

// New borrows client and creates no native provider, exporter or goroutine.
func New(client *otel.Client) *Sink { return &Sink{client: client} }

// WriteRecord preserves the immutable record's time, typed nested attributes,
// explicit context and original logging metadata. Uint64 must fit OTLP int64.
// Fatal/Panic are severities only. Acceptance is not export or durable evidence.
// The existing zerolog owner permanently stops a sink after any returned error;
// draining telemetry does not reset it. Composition must recreate that logger
// source to recover, while later local sinks keep their independent behavior.
func (sink *Sink) WriteRecord(ctx context.Context, record zerolog.Record) error {
	if sink == nil || sink.client == nil {
		return otel.ErrInput.New(fault.Context{Provider: otel.ProviderID, Operation: "zerolog-bridge"})
	}
	severity, found := map[zerolog.Level]logapi.Severity{zerolog.Trace: logapi.SeverityTrace, zerolog.Debug: logapi.SeverityDebug,
		zerolog.Info: logapi.SeverityInfo, zerolog.Warn: logapi.SeverityWarn, zerolog.Error: logapi.SeverityError,
		zerolog.Fatal: logapi.SeverityFatal, zerolog.Panic: logapi.SeverityFatal4}[record.Level()]
	if !found {
		return otel.ErrInput.New(fault.Context{Provider: otel.ProviderID, Operation: "zerolog-record"})
	}
	source := record.Source()
	metadata := slog.Group("logging", slog.String("provider", source.Configuration.Identity.Provider),
		slog.String("source", source.Configuration.Identity.Name), slog.String("scope", source.Scope),
		slog.String("revision", source.Configuration.Revision))
	receipt, err := sink.client.Emit(ctx, record.Correlation(), otel.LogRecord{Time: record.Time(), Severity: severity,
		SeverityText: string(record.Level()), Message: record.Message(), Attributes: []slog.Attr{metadata, slog.Any("attributes", otel.AttributeMap(record.AttributesCopy()))}})
	if err != nil {
		return err
	}
	result, _ := receipt.Result()
	return result.Err()
}
