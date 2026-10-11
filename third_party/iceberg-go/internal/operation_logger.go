// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package internal

import (
	"context"
	"log/slog"
)

type operationLoggerKey struct{}

// WithOperationLogger borrows a logger for the lifetime of ctx.
func WithOperationLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, operationLoggerKey{}, logger)
}

// OperationLogger resolves an explicit operation logger without global mutation.
func OperationLogger(ctx context.Context) *slog.Logger {
	if ctx != nil {
		if logger, ok := ctx.Value(operationLoggerKey{}).(*slog.Logger); ok && logger != nil {
			return logger
		}
	}
	return slog.Default()
}
