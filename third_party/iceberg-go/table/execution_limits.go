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

package table

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
)

// WithWorkerLimit bounds each native scan, write, manifest, remote-planning and
// maintenance worker pool reached with ctx. It does not cap total goroutines or
// memory across simultaneously active pools. The limit must be positive. A child
// context cannot increase an inherited limit. No metadata or globals are changed.
// The native defaults remain in effect when this option is absent.
func WithWorkerLimit(ctx context.Context, limit int) (context.Context, error) {
	if ctx == nil || limit <= 0 {
		return nil, fmt.Errorf("%w: worker limit requires a context and a positive count", iceberg.ErrInvalidArgument)
	}
	return internal.WithWorkerLimit(ctx, limit), nil
}

type commitRetryLimitKey struct{}

// WithCommitRetryLimit caps known-conflict retries without changing table
// properties. Zero allows the initial commit attempt only; unknown outcomes are
// never made retryable. The count must be in [0, 1000], and a child context cannot
// increase an inherited cap. Without this option native table properties apply.
func WithCommitRetryLimit(ctx context.Context, retries int) (context.Context, error) {
	if ctx == nil || retries < 0 || retries > 1000 {
		return nil, fmt.Errorf("%w: commit retry limit must be between 0 and 1000", iceberg.ErrInvalidArgument)
	}
	if previous, ok := ctx.Value(commitRetryLimitKey{}).(uint); ok {
		retries = min(retries, int(previous))
	}
	return context.WithValue(ctx, commitRetryLimitKey{}, uint(retries)), nil
}

func limitCommitRetries(ctx context.Context, cfg retryConfig) retryConfig {
	if retries, ok := ctx.Value(commitRetryLimitKey{}).(uint); ok {
		cfg.numRetries = min(cfg.numRetries, retries)
	}
	return cfg
}

// WithOperationLogger routes operation diagnostics, including best-effort cleanup
// and reporter panics, to a borrowed source-local logger. It does not install a
// global logger. The owner must retain the handler until all work with ctx joins.
// Error/path attributes may contain private native data; the handler owns their
// policy and presentation. Contextless format-helper warnings carry no such data.
func WithOperationLogger(ctx context.Context, logger *slog.Logger) (context.Context, error) {
	if ctx == nil || logger == nil {
		return nil, fmt.Errorf("%w: operation logger requires a context and non-nil logger", iceberg.ErrInvalidArgument)
	}
	return internal.WithOperationLogger(ctx, logger), nil
}
