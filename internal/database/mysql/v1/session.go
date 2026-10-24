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
	"database/sql"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// SessionOptionsV1 bounds native acquisition and retained use after admission.
// Zero Timeout selects one minute; supported values are 1ms–1min. Cleanup owns
// its actual lifetime independently of a caller waiting for Close.
type SessionOptionsV1 struct {
	Timeout time.Duration
}

// Session retains one physical pooled connection without starting a transaction.
// Query/Exec are serialized and never reconnect. DDL's implicit commit does not
// end this ownership. Copies share authority; Close always retires the connection.
type Session struct {
	private
	*sessionState
}
type sessionState struct {
	mu        sync.Mutex
	db        *Database
	conn      *sql.Conn
	owned     *managedConn
	call      *invocation.Call[Result]
	lifetime  context.Context
	cancel    context.CancelFunc
	requested chan struct{}
	closeOnce sync.Once
}

// Retain reserves the root and evidence before acquiring one connection. ctx
// owns the session lifetime, not just setup. The receipt stays unresolved until
// actual retirement; no initial or closing result implies a transaction outcome.
func (db *Database) Retain(ctx context.Context, id fault.Correlation, options SessionOptionsV1) (*Session, *invocation.Receipt[Result], error) {
	if options.Timeout == 0 {
		options.Timeout = time.Minute
	}
	if ctx == nil || options.Timeout < time.Millisecond || options.Timeout > time.Minute {
		return nil, nil, failure(ErrInput, "session")
	}
	call, err := db.beginCall(ctx, id, "session", invocation.Session, nil)
	if err != nil {
		return nil, nil, err
	}
	receipt := call.Receipt()
	lifetime, cancel := context.WithTimeout(ctx, options.Timeout)
	work, stop := context.WithTimeout(lifetime, db.owner.settings.Timeout)
	_, _ = call.Attempt()
	conn, owned, err := db.owner.take(work)
	acquisitionError, cause := work.Err(), context.Cause(work)
	stop()
	if err != nil {
		cancel()
		call.Complete(connectionOutcome(err))
		return nil, receipt, nil
	}
	if acquisitionError != nil {
		cancel()
		call.Complete(invocation.Outcome[Result]{
			Primary: failure(ErrConnect, "session-acquire", acquisitionError, cause),
			Cleanup: give(conn, owned, true),
		})
		return nil, receipt, nil
	}
	guard, err := call.Scope().Hold()
	if err != nil {
		cancel()
		call.Complete(invocation.Outcome[Result]{Primary: err, Cleanup: give(conn, owned, true)})
		return nil, receipt, nil
	}
	session := &Session{sessionState: &sessionState{
		db: db, conn: conn, owned: owned, call: call,
		lifetime: lifetime, cancel: cancel, requested: make(chan struct{}),
	}}
	go session.retire(guard)
	return session, receipt, nil
}

func (session *Session) requestClose() {
	session.closeOnce.Do(func() { close(session.requested) })
}

func (session *Session) retire(guard *invocation.Guard) {
	defer guard.End()
	select {
	case <-session.lifetime.Done():
	case <-session.requested:
	}
	session.requestClose()
	session.mu.Lock()
	defer session.mu.Unlock()
	primary := joined(ErrState, "session-lifetime", session.lifetime.Err(), context.Cause(session.lifetime))
	session.cancel()
	_, _ = session.call.Attempt()
	cleanup := give(session.conn, session.owned, true)
	session.conn, session.owned = nil, nil
	session.call.Complete(invocation.Outcome[Result]{Primary: primary, Cleanup: cleanup})
}

func (session *Session) lock() error {
	if session == nil || session.sessionState == nil {
		return failure(ErrState, "session")
	}
	if !session.mu.TryLock() {
		return failure(ErrState, "concurrent-session")
	}
	select {
	case <-session.requested:
		session.mu.Unlock()
		return failure(ErrState, "session-closed", session.lifetime.Err(), context.Cause(session.lifetime))
	default:
	}
	if session.lifetime.Err() != nil || session.owned.wire.closed.Load() {
		session.mu.Unlock()
		return failure(ErrState, "session-ended", session.lifetime.Err(), context.Cause(session.lifetime))
	}
	return nil
}

// Query uses the original physical session and its existing root reservation.
func (session *Session) Query(ctx context.Context, id fault.Correlation, query string, args ...any) (*invocation.Receipt[Result], error) {
	if err := session.lock(); err != nil {
		return nil, err
	}
	defer session.mu.Unlock()
	return session.db.statement(ctx, id, query, args, true, nil, nil, session)
}

// Exec preserves statement evidence without inventing commit/rollback evidence.
func (session *Session) Exec(ctx context.Context, id fault.Correlation, query string, args ...any) (*invocation.Receipt[Result], error) {
	if err := session.lock(); err != nil {
		return nil, err
	}
	defer session.mu.Unlock()
	return session.db.statement(ctx, id, query, args, false, nil, nil, session)
}

// Close requests physical retirement using already-held ownership, then waits
// with ctx. A canceled wait still requests cleanup. Repeat Close joins the same
// retirement and evidence; overlapping business work is refused, not interrupted.
// Successful local release does not acknowledge remote lock release or rollback.
func (session *Session) Close(ctx context.Context) (*invocation.Receipt[Result], error) {
	if session == nil || session.sessionState == nil || ctx == nil {
		return nil, failure(ErrInput, "session-close")
	}
	select {
	case <-session.requested:
	default:
		if !session.mu.TryLock() {
			select {
			case <-session.requested:
			default:
				return nil, failure(ErrState, "concurrent-session")
			}
		} else {
			session.requestClose()
			session.mu.Unlock()
		}
	}
	receipt := session.call.Receipt()
	if _, err := receipt.WaitReleased(ctx); err != nil {
		return receipt, failure(ErrCleanup, "session-close-wait", err, ctx.Err(), context.Cause(ctx))
	}
	return receipt, nil
}
