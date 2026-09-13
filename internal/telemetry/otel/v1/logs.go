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

package otel

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"go.opentelemetry.io/otel/attribute"
	logapi "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// LogRecord is borrowed only during Emit. Time zero uses the observation time.
// Message is a UTF-8 string body. Severity 0–24 has no panic/exit side effects.
// Attributes support slog scalars, groups, nil and []byte/string/int64/float64/bool.
// AttributeMap preserves nested empty groups without slog constructor filtering.
// Unsigned integers must fit int64; durations are nanoseconds, times RFC3339Nano.
// No LogValuer, Stringer, arbitrary error or reflection callback is invoked.
type LogRecord struct {
	private
	Time         time.Time
	Severity     logapi.Severity
	SeverityText string
	EventName    string
	Message      string
	Attributes   []slog.Attr
}

// Emit queues one independently copied native SDK record. A successful receipt
// proves queue acceptance only. Full queues reject without overwriting. The
// independent inbox must be drained even if the caller ignores its receipt.
func (client *Client) Emit(ctx context.Context, id fault.Correlation, input LogRecord) (*invocation.Receipt[Result], error) {
	if client == nil || client.owner == nil {
		return nil, failure(ErrInput, "emit")
	}
	if client.owner.logger == nil {
		return nil, failure(ErrUnsupported, "logs-disabled")
	}
	limit := client.owner.settings.MaxRecordBytes
	if input.Severity < 0 || input.Severity > 24 || !timestampValid(input.Time) ||
		!boundedString(input.Message, limit) || !boundedString(input.SeverityText, 32) || !boundedString(input.EventName, 128) {
		return nil, failure(ErrInput, "log-record")
	}
	return client.run(ctx, id, "emit", func(work context.Context) invocation.Outcome[Result] {
		budget := dataBudget{bytes: limit, nodes: MaxNodes}
		association := client.association(id)
		if err := budget.charge(256+associationBytes(association)+len(input.Message)+len(input.SeverityText)+len(input.EventName), 0); err != nil {
			return invocation.Outcome[Result]{Primary: err}
		}
		attrs, err := freezeAttributes(input.Attributes, &budget, 1)
		if err != nil {
			return invocation.Outcome[Result]{Primary: err}
		}
		charge := limit - budget.bytes
		if err := client.owner.reserve(charge); err != nil {
			return invocation.Outcome[Result]{Primary: err}
		}
		var record logapi.Record
		observed := time.Now().UTC()
		timestamp := input.Time.UTC().Round(0)
		if input.Time.IsZero() {
			timestamp = observed
		}
		record.SetTimestamp(timestamp)
		record.SetObservedTimestamp(observed)
		record.SetSeverity(input.Severity)
		record.SetSeverityText(strings.Clone(input.SeverityText))
		record.SetEventName(strings.Clone(input.EventName))
		record.SetBody(attribute.StringValue(strings.Clone(input.Message)))
		record.AddAttributes(attrs...)
		record.AddAttributes(association...)
		if err := work.Err(); err != nil {
			client.owner.unreserve(charge, 1)
			return invocation.Outcome[Result]{Primary: nativeFailure(ErrState, "emit", work, err)}
		}
		client.owner.emittingBytes = charge
		client.owner.logger.Emit(work, record)
		client.owner.emittingBytes = 0
		return successful(SignalResult{Signal: Logs, Accepted: 1})
	})
}

type logCapture struct{ owner *owner }

func (logCapture) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }
func (capture logCapture) OnEmit(_ context.Context, record *sdklog.Record) error {
	capture.owner.pendingLogs = append(capture.owner.pendingLogs, queuedLog{record.Clone(), capture.owner.emittingBytes})
	return nil
}
func (logCapture) ForceFlush(context.Context) error { return nil }
func (logCapture) Shutdown(context.Context) error   { return nil }
