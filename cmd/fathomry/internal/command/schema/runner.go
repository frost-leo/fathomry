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

package schema

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
)

// Runner borrows one explicitly supplied public client. Copies share authority.
// The command entry owns that source/runtime, incremental released-evidence
// reception and WorkBytes of scratch capacity. No external assets are accepted.
// Incomplete cleanup stays reachable through this same runner; Close never closes
// the borrowed client/source. Methods refuse overlap rather than queueing.
type Runner struct {
	private
	state *runnerState
}
type runnerState struct {
	mu                      sync.Mutex
	closed                  atomic.Bool
	dialect, target, quoted string
	options                 databasecmd.Options
	contract                *contract
	pg                      *postgres.Client
	mysql                   *mysql.Client
	pending                 *connection
	cleanup                 error
}

// PostgreSQL selects an existing schema. Construction validates assets and target
// spelling only; it does not inspect the server, install history or acquire a pool.
func PostgreSQL(client *postgres.Client, target string, options databasecmd.Options) (*Runner, error) {
	if client == nil {
		return nil, fail(databasecmd.ErrInput)
	}
	result, err := newRunner("postgresql", target, options)
	if err == nil {
		result.state.pg = client
	}
	return result, err
}

// MySQL selects an existing database, checked against the pinned connection's
// DATABASE() before mutation. Operator metadata privileges are checked at use.
func MySQL(client *mysql.Client, target string, options databasecmd.Options) (*Runner, error) {
	if client == nil {
		return nil, fail(databasecmd.ErrInput)
	}
	result, err := newRunner("mysql", target, options)
	if err == nil {
		result.state.mysql = client
	}
	return result, err
}
func newRunner(dialect, target string, options databasecmd.Options) (*Runner, error) {
	quoted, options, err := validate(dialect, target, options)
	if err != nil {
		return nil, err
	}
	contract, err := load(dialect)
	if err != nil {
		return nil, err
	}
	return &Runner{state: &runnerState{dialect: dialect, target: target, quoted: quoted, options: options, contract: contract}}, nil
}

func validate(dialect, target string, options databasecmd.Options) (string, databasecmd.Options, error) {
	normalized, err := options.Normalize()
	if err != nil {
		return "", options, fail(databasecmd.ErrInput, err)
	}
	quoted, err := quoteTarget(dialect, target)
	return quoted, normalized, err
}
func (runner *Runner) enter(ctx context.Context) (*runnerState, error) {
	if runner == nil || runner.state == nil || ctx == nil {
		return nil, fail(databasecmd.ErrInput)
	}
	state := runner.state
	if !state.mu.TryLock() {
		return nil, fail(databasecmd.ErrBusy)
	}
	if state.closed.Load() {
		state.mu.Unlock()
		return nil, fail(databasecmd.ErrClosed)
	}
	if state.pending != nil {
		state.mu.Unlock()
		return nil, fail(databasecmd.ErrCleanup)
	}
	if ctx.Err() != nil {
		state.mu.Unlock()
		return nil, fail(databasecmd.ErrDatabase, ctx.Err(), context.Cause(ctx))
	}
	return state, nil
}
func (state *runnerState) finish() (bool, postgres.TransactionOutcome, error) {
	if state.pending == nil {
		return true, postgres.TransactionUnobserved, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), state.options.CleanupTimeout)
	defer cancel()
	released, outcome, err := state.pending.close(ctx)
	if released && state.pending.cleanup != nil && state.cleanup == nil {
		state.cleanup = fail(databasecmd.ErrCleanup, state.pending.cleanup)
		state.closed.Store(true)
	}
	if released {
		state.pending = nil
	}
	return released, outcome, err
}

// Close seals this runner and joins its last retained operation with ctx. Expired
// waiting is not release; retry Close on the same handle. In-flight methods are
// not silently interrupted and return databasecmd.ErrBusy, retaining cleanup ownership.
func (runner *Runner) Close(ctx context.Context) error {
	if runner == nil || runner.state == nil || ctx == nil {
		return fail(databasecmd.ErrInput)
	}
	state := runner.state
	state.closed.Store(true)
	if !state.mu.TryLock() {
		return fail(databasecmd.ErrBusy)
	}
	defer state.mu.Unlock()
	if state.pending == nil {
		return state.cleanup
	}
	released, _, err := state.pending.close(ctx)
	if released && state.pending.cleanup != nil && state.cleanup == nil {
		state.cleanup = fail(databasecmd.ErrCleanup, state.pending.cleanup)
	}
	if released {
		state.pending = nil
	}
	return errors.Join(err, state.cleanup)
}

// Released reports only absence of runner-owned retained native work, not remote
// lock release or source shutdown. It refuses overlapping inspection.
func (runner *Runner) Released() bool {
	if runner == nil || runner.state == nil {
		return false
	}
	if !runner.state.mu.TryLock() {
		return false
	}
	defer runner.state.mu.Unlock()
	return runner.state.pending == nil
}

// Status performs bounded, lock-coherent inspection without persistent writes.
// A changing state is a completed contention observation, not permission to apply.
func (runner *Runner) Status(ctx context.Context) (result Status, err error) {
	result.State = Indeterminate
	state, err := runner.enter(ctx)
	if err != nil {
		return result, err
	}
	defer state.mu.Unlock()
	defer func() {
		if _, _, cleanup := state.finish(); cleanup != nil {
			err = fail(databasecmd.ErrCleanup, err, cleanup)
		}
	}()
	session, err := state.begin(ctx, false)
	if err != nil {
		return state.status(Indeterminate, "unavailable"), err
	}
	locked, err := state.prepare(session)
	if err != nil {
		if errors.Is(err, databasecmd.ErrProfile) {
			return state.status(Incompatible, "profile"), err
		}
		return state.status(Indeterminate, "profile_or_access"), err
	}
	if !locked {
		return state.status(Changing, "installer_busy"), nil
	}
	return state.inspect(session)
}

// Plan is read-only even when the target is empty. Applicable means the observed
// state/privileges permit this initial installation, not a reservation for Apply.
func (runner *Runner) Plan(ctx context.Context) (result Plan, err error) {
	result.Status.State = Indeterminate
	result.Steps = []Step{}
	state, err := runner.enter(ctx)
	if err != nil {
		return result, err
	}
	defer state.mu.Unlock()
	defer func() {
		if _, _, cleanup := state.finish(); cleanup != nil {
			err = fail(databasecmd.ErrCleanup, err, cleanup)
		}
	}()
	session, err := state.begin(ctx, false)
	if err != nil {
		return result, err
	}
	locked, err := state.prepare(session)
	if err != nil {
		return result, err
	}
	if !locked {
		result.Status = state.status(Changing, "installer_busy")
		return result, nil
	}
	result.Status, err = state.inspect(session)
	if err != nil {
		return result, err
	}
	if result.Status.State == Current {
		result.Applicable = true
		return result, nil
	}
	if result.Status.State != NotInstalled {
		return result, nil
	}
	if err = state.privileges(session, true); err != nil {
		return result, err
	}
	result.Applicable = true
	for _, entry := range append([]asset{state.contract.manifest.Bootstrap}, state.contract.manifest.Migrations[0].Steps...) {
		result.Steps = append(result.Steps, Step{ID: entry.ID, SHA256: entry.SHA256})
	}
	return result, nil
}

// Apply installs only a positively empty target or no-ops on a verified current
// one. It never resumes/adopts/repairs. Result and cleanup error are independent;
// acknowledgements learned during cleanup remain evidence of this attempt even
// after cancellation. A lost reply is not rollback; use Status to reconcile it.
func (runner *Runner) Apply(ctx context.Context) (result Change, err error) {
	result.Before.State, result.After.State = Indeterminate, Indeterminate
	result.Effect = NotStarted
	state, err := runner.enter(ctx)
	if err != nil {
		return result, err
	}
	defer state.mu.Unlock()
	mutated := false
	commitSent := false
	var verified Status
	defer func() {
		released, outcome, cleanup := state.finish()
		result.CleanupComplete = released
		if state.dialect == "postgresql" && mutated {
			if commitSent && verified.State == Current && outcome == postgres.CommitAcknowledged {
				result.Effect = Applied
				result.After = verified
			} else if result.Effect != Applied && !commitSent && outcome == postgres.RollbackAcknowledged {
				result.Effect = RolledBack
			}
		}
		if cleanup != nil {
			err = fail(databasecmd.ErrCleanup, err, cleanup)
		}
	}()
	session, err := state.begin(ctx, true)
	if err != nil {
		return result, err
	}
	locked, err := state.prepare(session)
	if err != nil {
		return result, err
	}
	if !locked {
		result.Before = state.status(Changing, "installer_busy")
		return result, fail(databasecmd.ErrBusy)
	}
	result.Before, err = state.inspect(session)
	if err != nil {
		return result, err
	}
	if result.Before.State == Current {
		result.After = result.Before
		result.Effect = Unchanged
		return result, nil
	}
	if result.Before.State != NotInstalled {
		return result, fail(databasecmd.ErrSchema)
	}
	if err = state.privileges(session, true); err != nil {
		return result, err
	}
	mutated = true
	result.Effect = Unknown
	if err = state.install(session, &result); err != nil {
		return result, err
	}
	if session.mysql != nil {
		result.Effect = Applied
	}
	verified, err = state.inspect(session)
	if err != nil {
		return result, err
	}
	if verified.State != Current {
		return result, fail(databasecmd.ErrSchema)
	}
	if session.pg != nil {
		commitSent = true
		value, commitError := session.pg.Commit(session.ctx)
		if value.TransactionOutcome() == postgres.CommitAcknowledged {
			result.Effect = Applied
			result.After = verified
		}
		if commitError != nil {
			return result, fail(databasecmd.ErrDatabase, commitError)
		}
		if result.Effect != Applied {
			return result, fail(databasecmd.ErrDatabase)
		}
	} else {
		result.Effect = Applied
		result.After = verified
	}
	return result, nil
}
