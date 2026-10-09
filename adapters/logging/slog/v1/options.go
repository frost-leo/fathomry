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

package slog

import (
	"context"
	stdslog "log/slog"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Options is an explicit provider-selected ingress envelope. No implicit native
// defaults are used. Timeout is 1ms..1min; message bytes are 1..1MiB. MaxViews
// (1..4096) is cumulative, including the initial view; retained bytes (256B..1GiB)
// charge each retained field tree and path. MaxActive (1..1024) bounds concurrent
// ingress/derivation preparation stacks without a queue or worker; the retained
// provider-family budget must include each stack. Levels lists 1..16 exact slog levels.
// These are declared bounds, not forced callback termination or hard process RSS.
type Options struct {
	Limits           logging.Limits
	Timeout          time.Duration
	MaxMessageBytes  int
	MaxViews         int
	MaxActive        int
	MaxRetainedBytes int64
	Levels           []int
}

// Record is a frozen ingress record, not a universal native logger request.
// Zero Time/PC are absent; the provider must not invent wrapper time or caller.
// Values are immutable for the callback; retained copies remain callback-owned.
type Record struct {
	private
	Time    time.Time
	PC      uintptr
	Level   stdslog.Level
	Message string
	Fields  []logging.Field
}

// Attempt separates actual provider admission from the operation's failure.
// Admitted does not mean emitted, accepted by every output, exported or durable.
type Attempt struct {
	Admitted bool
	Err      error
}

// Binding contains explicit assembly authority, never loadable configuration.
// Enabled and Validate must be pure, bounded and concurrent-safe. Emit must
// preserve actual provider admission/results and must not require progress from
// its own callback stack. Callbacks must return normally and cannot be forcibly
// stopped. Release only releases the retained view, never its source/Runtime.
// All clones share it. Lifetime cancellation stops attempts; when Release is
// nonnil, callers must still join Close to confirm actual view release.
type Binding struct {
	Lifetime context.Context
	Enabled  func(context.Context, stdslog.Level) bool
	Validate func([]logging.Field) error
	Emit     func(context.Context, Record) Attempt
	Release  func(context.Context) resource.ReleaseResult
}
