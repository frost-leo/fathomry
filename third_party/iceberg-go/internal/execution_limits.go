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
	"runtime"

	"github.com/apache/iceberg-go/config"
)

type workerLimitKey struct{}

// WithWorkerLimit carries an already validated worker cap without global state.
func WithWorkerLimit(ctx context.Context, limit int) context.Context {
	if previous, ok := workerLimit(ctx); ok {
		limit = min(limit, previous)
	}
	return context.WithValue(ctx, workerLimitKey{}, limit)
}

func workerLimit(ctx context.Context) (int, bool) {
	if ctx == nil {
		return 0, false
	}
	limit, ok := ctx.Value(workerLimitKey{}).(int)
	return limit, ok
}

// WorkerCount clamps an explicit pool size to the context's cap. An omitted
// size uses the cap when set, otherwise the native GOMAXPROCS default.
func WorkerCount(ctx context.Context, requested int) int {
	if limit, ok := workerLimit(ctx); ok {
		if requested <= 0 {
			return limit
		}
		return min(limit, requested)
	}
	if requested <= 0 {
		return runtime.GOMAXPROCS(0)
	}
	return requested
}

// ConfiguredWorkerCount uses the native import-time worker setting only when
// no scope cap or explicit worker count was supplied.
func ConfiguredWorkerCount(ctx context.Context, requested int) int {
	if _, ok := workerLimit(ctx); ok || requested > 0 {
		return WorkerCount(ctx, requested)
	}
	return config.EnvConfig.MaxWorkers
}
