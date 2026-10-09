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
	ingress "github.com/frost-leo/fathomry/adapters/logging/slog/v1"
	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"go.uber.org/zap/zapcore"
	"log/slog"
)

// Slog retains one generation for an explicitly owned restricted gateway.
// The returned handler must be closed. Supplied slog contexts provide values
// and trace association; their cancellation does not suppress the attempt.
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

// Slog permits one gateway family per retained View family. Handler derivations
// share bounded storage and Close authority; this is not arbitrary slog support.
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
	fields := func(values []logging.Field) []zapcore.Field {
		result := make([]zapcore.Field, len(values))
		for index, value := range values {
			result[index] = Field(value.Key, value.Value)
		}
		return result
	}
	convertLevel := func(level slog.Level) (zapcore.Level, error) {
		switch level {
		case slog.LevelDebug:
			return zapcore.DebugLevel, nil
		case slog.LevelInfo:
			return zapcore.InfoLevel, nil
		case slog.LevelWarn:
			return zapcore.WarnLevel, nil
		case slog.LevelError:
			return zapcore.ErrorLevel, nil
		}
		return 0, fail(ErrUnsupported, "slog-level")
	}
	handler, err := ingress.New(ingress.Options{Limits: fieldLimits(), Timeout: family.state.prepared.metadata.Timeout, MaxActive: maxChildren, MaxMessageBytes: 64 << 10, MaxViews: maxDerivedViews, MaxRetainedBytes: derivedBytes, Levels: []int{-4, 0, 4, 8}}, ingress.Binding{
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
			all := append(append([]zapcore.Field(nil), view.fields...), fields(values)...)
			_, err := freezeFields(all)
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
