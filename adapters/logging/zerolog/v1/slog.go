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
	ingress "github.com/frost-leo/fathomry/adapters/logging/slog/v1"
	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"log/slog"
)

// Slog captures a generation for an explicitly owned restricted gateway. Caller
// slog contexts supply association, not the gateway's cancellation lifetime.
func (client *Client) Slog(ctx context.Context) (*ingress.Handler, error) {
	view, err := client.Retain(ctx)
	if err != nil {
		return nil, err
	}
	handler, err := view.Slog()
	if err != nil {
		_ = view.Close(context.Background())
	}
	return handler, err
}

// Slog permits one gateway family per retained View. Its Close joins all
// preparation/emission stacks before releasing that generation's reservation.
func (view *View) Slog() (*ingress.Handler, error) {
	if view == nil || view.family == nil {
		return nil, fail(ErrInput, "slog")
	}
	family := view.family
	family.mu.Lock()
	if family.gateway || family.closing {
		family.mu.Unlock()
		return nil, fail(ErrState, "slog")
	}
	family.gateway = true
	defer family.mu.Unlock()
	limit := family.state.prepared.metadata.MaxRecordBytes
	convertLevel := func(level slog.Level) (Level, error) {
		switch level {
		case -8:
			return Trace, nil
		case slog.LevelDebug:
			return Debug, nil
		case slog.LevelInfo:
			return Info, nil
		case slog.LevelWarn:
			return Warn, nil
		case slog.LevelError:
			return Error, nil
		case 12:
			return Fatal, nil
		case 16:
			return Panic, nil
		}
		return "", fail(ErrUnsupported, "slog-level")
	}
	fields := func(values []logging.Field) []slog.Attr {
		result := make([]slog.Attr, len(values))
		for index, field := range values {
			result[index] = Attribute(field.Key, field.Value)
		}
		return result
	}
	handler, err := ingress.New(ingress.Options{Limits: fieldLimits(limit), Timeout: family.state.prepared.metadata.Timeout, MaxActive: maxChildren, MaxMessageBytes: limit, MaxViews: maxDerivedViews, MaxRetainedBytes: derivedBytes, Levels: []int{-8, -4, 0, 4, 8, 12, 16}}, ingress.Binding{
		Lifetime: family.call.Context(),
		Enabled: func(ctx context.Context, level slog.Level) bool {
			severity, err := convertLevel(level)
			if err != nil {
				return true
			}
			enabled, err := view.Enabled(ctx, severity)
			return enabled || err != nil
		},
		Validate: func(values []logging.Field) error {
			if len(values) > 64-len(view.attributes) {
				return fail(ErrLimit, "attributes")
			}
			all := append(append([]slog.Attr(nil), view.attributes...), fields(values)...)
			_, err := freezeAttributes(all, limit)
			return err
		},
		Emit: func(ctx context.Context, record ingress.Record) ingress.Attempt {
			severity, err := convertLevel(record.Level)
			if err != nil {
				return ingress.Attempt{Err: err}
			}
			receipt, err := view.LogEntry(ctx, Entry{Time: record.Time, PC: record.PC, Level: severity, Message: record.Message}, fields(record.Fields)...)
			if receipt == nil {
				return ingress.Attempt{Err: err}
			}
			snapshot, waitErr := receipt.Wait(ctx)
			return ingress.Attempt{Admitted: true, Err: joinErrors("slog", err, joinErrors("slog", waitErr, snapshot.Err()))}
		},
		Release: view.releaseFamily,
	})
	family.handler = handler
	return handler, err
}
