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

package redis

import (
	"context"

	native "github.com/frost-leo/fathomry/internal/cache/redis/v9"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Session is a callback-scoped pinned connection without raw native or cleanup
// authority. Copies share one gate; concurrent/reentrant use and use after return
// fail rather than queuing behind their own root reservation.
type Session struct {
	private
	state *sessionState
}
type sessionState struct {
	op     *operation
	native *native.Session
	ended  bool
}

func (view View) Dedicated(ctx, cleanup context.Context, routeKey string, run func(context.Context, *Session) error) (Result, error) {
	return view.session(ctx, cleanup, routeKey, nil, run, false)
}
func (view View) Watch(ctx, cleanup context.Context, keys []string, run func(context.Context, *Session) error) (Result, error) {
	if len(keys) == 0 || len(keys) > 65536 {
		return Result{}, problem(view.capability, ErrInput, "watch")
	}
	return view.session(ctx, cleanup, "", keys, run, true)
}
func (view View) session(ctx, cleanup context.Context, route string, keys []string, run func(context.Context, *Session) error, watch bool) (Result, error) {
	if cleanup == nil || run == nil {
		return Result{}, problem(view.capability, ErrInput, "session")
	}
	return result(view.dispatch(ctx, "session", Lifecycle, nil, func(op *operation, bound *native.Client) {
		callback := func(lifetime context.Context, borrowed *native.Session) error {
			if err := op.claimRoot(); err != nil {
				return err
			}
			session := &Session{state: &sessionState{op: op, native: borrowed}}
			defer func() { op.family.gate.Lock(); session.state.ended = true; op.family.gate.Unlock() }()
			return run(lifetime, session)
		}
		var receipt *invocation.Receipt[native.Result]
		var err error
		defer func() { op.finishCallback(receipt, err) }()
		if watch {
			receipt, err = bound.Watch(op.lifetime, cleanup, op.correlation(), keys, callback)
		} else {
			receipt, err = bound.Dedicated(op.lifetime, cleanup, op.correlation(), route, callback)
		}
	}))
}
func (session *Session) enter() (*sessionState, func(), error) {
	if session == nil || session.state == nil {
		return nil, nil, fail(ErrState, "session")
	}
	state := session.state
	if !state.op.family.gate.TryLock() {
		return nil, nil, problem(state.op.capability, ErrState, "session")
	}
	if state.ended || state.op.lifetime.Err() != nil {
		state.op.family.gate.Unlock()
		return nil, nil, problem(state.op.capability, ErrState, "session")
	}
	return state, state.op.family.gate.Unlock, nil
}
func (session *Session) Execute(ctx context.Context, command Command) (Result, error) {
	state, end, err := session.enter()
	if err != nil {
		return Result{}, err
	}
	defer end()
	return state.op.child(ctx, "execute", command.capability, Commands, []Command{command}, func(ctx context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return state.native.Execute(ctx, id, command.native)
	})
}
func (session *Session) Pipeline(ctx context.Context, commands ...Command) (Result, error) {
	return session.batch(ctx, false, commands)
}

// Transaction sends one MULTI/EXEC on the pinned connection. Cross-slot Cluster
// commands are refused by that server; they are never split. No WATCH retry runs.
func (session *Session) Transaction(ctx context.Context, commands ...Command) (Result, error) {
	return session.batch(ctx, true, commands)
}
func (session *Session) batch(ctx context.Context, transaction bool, commands []Command) (Result, error) {
	state, end, err := session.enter()
	if err != nil {
		return Result{}, err
	}
	defer end()
	name := "pipeline"
	if transaction {
		name = "transaction"
	}
	return state.op.child(ctx, name, state.op.capability, Commands, commands, func(ctx context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		if transaction {
			return state.native.Transaction(ctx, id, nativeCommands(commands)...)
		}
		return state.native.Pipeline(ctx, id, nativeCommands(commands)...)
	})
}

// TransactionResult keeps command effects separate from local session cleanup.
type TransactionResult struct {
	private
	lifecycle, execution Result
}

func (result TransactionResult) Lifecycle() Result { return result.lifecycle }
func (result TransactionResult) Execution() Result { return result.execution }
func (view View) Transaction(ctx, cleanup context.Context, routeKey string, commands ...Command) (TransactionResult, error) {
	var execution Result
	lifecycle, err := view.Dedicated(ctx, cleanup, routeKey, func(ctx context.Context, session *Session) error {
		var err error
		execution, err = session.Transaction(ctx, commands...)
		return err
	})
	return TransactionResult{lifecycle: lifecycle, execution: execution}, err
}
