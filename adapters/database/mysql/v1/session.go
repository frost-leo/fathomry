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

package mysql

import (
	"context"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/database/mysql/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// SessionOptions bounds native acquisition and retained use after admission.
// Zero Timeout selects one minute; supported values are 1ms–1min. It is not a
// guarantee that native cleanup finishes before a caller's waiting deadline.
type SessionOptions struct {
	Timeout time.Duration
}

// Session retains one source generation and physical pooled connection without
// starting a transaction. It does not promise pristine session state. Query/Exec
// are serialized; Close always retires the connection instead of returning it
// for reuse. Copies share one owner, and no native handle is exposed.
type Session struct {
	private
	session *session
	native  *native.Session
}

// Retain uses ctx for the full lifetime, unlike preparation's setup-only context.
// DDL and explicit transaction-control SQL do not end this connection ownership.
// Call Close or cancel the owning context; neither action proves remote effects
// absent or a server-side advisory lock released immediately.
func (client *Client) Retain(ctx context.Context, options SessionOptions) (*Session, error) {
	if client == nil {
		return nil, fail(ErrInput, "session")
	}
	var retained *Session
	receipt, err := client.dispatch(ctx, ctx, "session", func(value *session, database *native.Database) {
		guard, err := value.call.Hold()
		if err != nil {
			_ = value.call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		handle, nativeReceipt, err := database.Retain(value.lifetime, value.family.correlation(value.call), native.SessionOptionsV1{Timeout: options.Timeout})
		if handle == nil {
			value.finish(nativeReceipt, err)
			_ = guard.Release()
			return
		}
		retained = &Session{session: value, native: handle}
		if err = value.keep(guard, nativeReceipt, func(ctx context.Context) { _, _ = handle.Close(ctx) }); err != nil {
			_ = value.call.Resolve(adapters.Outcome[Result]{Primary: err})
		}
	})
	_, err = result(receipt, err)
	return retained, err
}

// Receipt exposes final lifecycle evidence. It stays unresolved until native
// retirement and has no statement/transaction result merely because Close succeeds.
func (retained *Session) Receipt() *adapters.Receipt[Result] {
	if retained == nil || retained.session == nil {
		return nil
	}
	return retained.session.call.Receipt()
}

// Query consumes bounded rows on the original physical session, without reconnect.
func (retained *Session) Query(ctx context.Context, query string, args ...any) (Result, error) {
	if retained == nil || retained.session == nil {
		return Result{}, fail(ErrState, "query")
	}
	return retained.session.finite(ctx, "query", func(work context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return retained.native.Query(work, id, query, args...)
	})
}

// Exec preserves statement acknowledgement separately from commit or cleanup.
func (retained *Session) Exec(ctx context.Context, query string, args ...any) (Result, error) {
	if retained == nil || retained.session == nil {
		return Result{}, fail(ErrState, "exec")
	}
	return retained.session.finite(ctx, "exec", func(work context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return retained.native.Exec(work, id, query, args...)
	})
}

// Close requests physical retirement using original reservations and waits with
// ctx. A canceled wait still requests cleanup. Retry with a fresh waiting context
// on the same handle; concurrent business calls are refused, not silently aborted.
// Receipt distinguishes actual release, lifetime cancellation and cleanup failure.
func (retained *Session) Close(ctx context.Context) error {
	if retained == nil || retained.session == nil {
		return fail(ErrState, "session-close")
	}
	_, err := retained.session.finalize(ctx, "session-close", retained.native.Close)
	return err
}
